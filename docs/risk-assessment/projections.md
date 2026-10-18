# Working notes: projections

> The notes of one area of the risk assessment of 18/10/2026, kept as written. Their ids (PROJ-1, …) map to the
> F-numbers of [RISK-ASSESSMENT.md](../RISK-ASSESSMENT.md) (§14), which holds the consolidated findings: where the two
> differ, the report holds. Nothing was run; these are traces from the code, with lesser items not triaged there.

## 1. Examined

**Read in full**
- `core/.../projection/`: `ProjectionActor.scala`, `SubscribableProjectionActor.scala`, `SubscriptionsState.scala`, `PurgedIds.scala`, `AggregateListenerActor.scala`
- `core/.../util/AfterCommit.scala`, `util/Backoff.scala`
- `core/.../delivery/SequenceGate.scala`

**Read where they meet the projection**
- `EventsBusActor.scala` (whole).
- `AggregateOwner.scala`: restore, `caughtUp`, `republish`, `delivered`.
- `PostgresEventStoreState.scala`: `readPart`, the outbox reads, and the failure checks that block a permanent delete or a squash.
- `ProjectionBuilder.scala` (whole).
- `Projections.scala`: settings, `start`, routing, targeted rebuild, `serveUnchanged`, `connect`.
- `ProjectionBuild.scala`.
- `PostgresDocumentStore.scala`: table creation, `cacheAfterCommit`, update/insert, `clearSpace`.
- `metrics/Metrics.scala`, the `MessageTooLargeException` marker in `ClusterSerializer`, core `reference.conf`.
- The F163 scenarios in `ProjectionDeliverySpec`.

**Docs:** CLAUDE.md, ARCHITECTURE §4.7–4.9 and §5–7, KNOWN-ISSUES.md (all), the archive's delivery, document-store and rebuild sections, SCALING.md, and book chapters 8, 10 (setup), 11 and 15 (warning).

**Not examined**
- `FlowControl.scala` internals, `MemoryDocumentStore`, and the other specs.
- The mpjsons and pgjdbc sources: their behaviour is taken from standard or documented defaults and marked as such.
- Neula's code.

Nothing was run: I reasoned from the code only.

## 2. Findings

### PROJ-1 — A failure whose version is already applied cannot be cleared by a retry; it becomes a false "versions missing" failure, and skipping that moves the subscription past the aggregate, so its next event is acknowledged without being applied
- **Severity:** High · **Likelihood:** Unlikely per aggregate, Possible during mass retries · **Confidence:** Confirmed (code)
- **Location:** `ProjectionActor.scala`
  - `retry` 1024–1029
  - `drainFrom` 682 and 669–670
  - `requestMissingVersions` 717–719
  - `receiveDelivery` 586–587
  - `skipFailedVersions` 1043–1044
  - Where stale rows come from: `afterBatch` 743–749, `loadFailures` 992–1003 (called only from 398–401 and 416–419), `forget` 600 via `ServeUnchanged` 420–424
  - Elsewhere: `AggregateOwner.republish` 868–878; `PostgresEventStoreState` 492 and 627 (a delete or squash is refused while a row exists)
- **Precondition:** a `subscription_failures` row whose `failed_version` is at or below the stored subscription version (a "stale row"). Sources, all in the code:
  - (a) A retry's batch commits but `clearFailure` fails. The code sets `queue.retryOf = None` first and logs "it is retried again at the next start".
  - (b) `loadFailures` fails at the first `GetSubscriptionRequests`. On a single node it is never asked again; later deliveries apply normally and nothing clears the row.
  - (c) `ServeUnchanged`'s `forget` drops in-memory failures (`failed --= …`), and the following `ConnectedTo` does not reload them, because `failuresLoaded` is already true.
  - (d) `recordFailure` commits but its answer is lost, then the 30 s retry succeeds.
- **Scenario:** projection P failed on cart 42 at v7. After a fix, `RetryAllFailedSubscriptions` runs; v7–v9 commit; `clearFailure` hits a 5 s pool timeout. Cart 42 stays idle.
- **Mechanism**
  1. Until a restart the row is invisible. The status lists only the in-memory `failed`, and Retry/Skip iterate `failed.keys`, so they act on nothing. Squash and permanent delete of cart 42 are refused ("a subscription of it is failed").
  2. After a restart, `loadFailures` reloads the row and `retry` runs. `drainFrom` has no pending deliveries and does `gap = Some(next.increment)`, so it asks from v10, not v7.
  3. The owner's `republish` has nothing to send (`if (start <= version) … else None`).
  4. Ten minutes later: `giveUp(…, from = v10, "versions from 10 are missing: asked 6 times …")`. From then on every new event of cart 42 is answered `MessageFailed` and not applied.
  5. An operator reads "versions missing" and runs `SkipFailedVersion`: `if (applied < failure.version) advance(key, applied, failure.version)` moves the subscription to v10 while the cart is at v9.
  6. The next command writes v10. In `drainFrom`, `delivery.last <= applied` sends `MessageAck` without applying it.
- **Impact**
  - Event listeners lose that event for good; state listeners lose it until the next event. Nothing records it.
  - Before the skip, the aggregate's new events are refused, and every retry fails again 10 minutes later while the aggregate is idle.
  - A stale row silently blocks deletes (GDPR flows) and squashes.
  - The stored error points at the wrong cause.
- **Validation:** use a `MemorySubscriptionsState` whose `clearFailure` throws once.
  1. The listener fails at v3; fix it; `RetryFailedSubscription`; v3 applies.
  2. Restart the actor with `gapGiveUpAfter = 1 s`. Expect no failure; the bug gives a "missing" failure at v4.
  3. `SkipFailedVersion`. Expect `lastVersion == 3`; the bug gives 4.
  4. Publish v4. Expect the listener to see it; with the bug it does not.
- **Fix**
  - In `retry` and `loadFailures`, when `applied >= failure.version`, clear the row at once and send `Republish`.
  - Keep `retryOf`, and retry `clearFailure`, until the clear succeeds.
  - Never let a skip advance past the aggregate's stored version.
  - Have the owner answer a range beyond its version with an explicit "nothing after N" (a new tolerant message, D68).
  - Retry `loadFailures` on a timer; do not drop failures in `ServeUnchanged`.
  - Trade-off: one read per retry, and one more message type.
- **Regression test:** the validation, plus `ServeUnchanged` → `ConnectedTo` keeping failures, plus a failed `loadFailures` retried by the timer.

### PROJ-2 — Exception messages that carry event and document values go into `subscription_failures.error`, the logs, the status and messages between nodes; erasure does not reach them
- **Severity:** Medium · **Likelihood:** Likely · **Confidence:** the paths are Confirmed (code); the message contents are Strongly suspected (`MatchError` is standard Scala; pgjdbc's default is `logServerErrorDetail=true`)
- **Location**
  - `ProjectionActor.scala`: 759 (`e.getClass.getSimpleName + ": " + e.getMessage`), 762–764 (WARN at the first attempt, `+ e`), 774–785 (failure kept untruncated, `log.error(e, …)`, `MessageFailed(…, error, …)`), 587 (every later delivery answered with `failed(key).error`), 965 (the builder's unreadable-event text), 1059 (status)
  - `SubscriptionsState.scala` 275–284 (4 000 characters stored)
  - `EventsBusActor.scala` 475–476 (one WARN with the error per `MessageFailed`, no rate limit, unlike F92)
  - `ProjectionBuilder.scala` 271, 289, 358–359
  - Erasure: only the targeted rebuild's `resetVersions` (`SubscriptionsState.scala` 468) deletes these rows, and only for projections that are named; ARCHITECTURE §4.9's "Not reached" list omits this table.
- **Scenario**
  - A new event class `CustomerContactChanged(name, email, …)` ships (D67) before a listener has a case for it. The listener throws `MatchError`, whose message is the event's `toString`.
  - Result: a WARN, then an ERROR with stack, then a stored row like "MatchError: CustomerContactChanged(<name>,<e-mail>,…)", then one bus WARN per later event of that aggregate.
  - A squash event's `MatchError` carries the whole state.
  - pgjdbc errors behave the same way: `Detail: Key (…)=(…)`, "JSON data, line 1: …", and batch statements with their parameters inlined.
  - `HistoryRewriter` rebuilds only the projections it is given. Side-effect listeners (`AggregateListenerActor`, which is unmanaged, D58) and rows on other aggregates keep the text.
- **Impact**
  - Personal data sits in a table erasure misses, in WARN/ERROR logs, in admin-tool status answers, and in messages between nodes.
  - The in-memory copy and `MessageFailed` are untruncated. A `MessageFailed` over the frame becomes `Status.Failure(MessageTooLargeException)` at the bus, which logs it as a failed subscription answer, so the delivery never ends (Strongly suspected).
  - It is hard to spot because it looks like normal diagnostics.
- **Validation:** a listener throws `MatchError` on an event holding a marker string. Assert the row and the captured log contain the marker. Run `HistoryRewriter` naming another projection; assert the row still contains it.
- **Fix**
  - Store and send a sanitized text: the class plus a bounded message from a configurable `describeFailure`. For `MatchError`, keep only the value's class. Cap the text at about 1 KB in memory, in `MessageFailed` and in the status.
  - Log the full exception once, to a restricted logger.
  - Rate-limit the bus's per-`MessageFailed` warning.
  - Have the rewrite scrub `subscription_failures.error` for the aggregate for every subscriber.
  - Document `logServerErrorDetail=false`.
  - Trade-off: less detail in the stored record.
- **Regression test:** the validation, plus a bounded `MessageFailed.error`.

### PROJ-3 — Async delayed queries ignore their deadline while the projection keeps applying updates, and a search that throws during a replay drops every parked query
- **Severity:** Medium · **Likelihood:** Possible (sustained load or a catch-up) · **Confidence:** Confirmed (code)
- **Location:** `ProjectionActor.scala` 495–496, 1116–1120, 1162–1178, 1082–1088, 365 with 875–878, 1090–1112
- **Mechanism**
  - `parkOrSearchAgain` does `if (searchedAt < updatesApplied) searchAgain else park`. `searchAgain` is `delayIfNotAvailableAsync(…, q.until)`, which never compares `until` with the current time. Only parked queries reach `answerAtDeadline`.
  - `AsyncDelayedQuery` is not taken while grouping, so `flushGroup()` runs just before it is handled. Any due batch raises `updatesApplied`, so under steady deliveries the query is searched again on every pass and never parks.
  - `search().onComplete` has no `try`. `replayQueries` empties `delayedQueries` before its loop, so a search that throws synchronously (for example `() => Future.successful(store.getDocument(id))` during a DB error) aborts the loop. That query and all after it are never answered.
- **Scenario:** a read-your-writes query (book ch. 8, `maxMillis = 2000`) for a version that never comes, made while the projection works through a 10 000-event backlog. The query loops for the whole catch-up and the asker times out. A pool timeout during a replay leaves the parked queries unanswered.
- **Impact:** callers hit their ask timeouts instead of the documented answer at the deadline; repeated searches add load during the busiest periods; the only trace is one "failed on ReplayQueries$" error.
- **Validation**
  - Like `SearchedNotes` (F163 spec): a delivery for another aggregate every 2 ms; `FindNoteVersion(unknown, 200)`. Expect `None` within about 300 ms; the bug gives no answer while deliveries flow.
  - Two parked async queries whose search throws on its second call; trigger a replay. Expect both answered; the bug answers neither.
- **Fix:** answer at the deadline in `parkOrSearchAgain`; search again at most once and then park; wrap `search()` in `try`; handle each query in `replayQueries` on its own.
- **Regression test:** both validations.

### PROJ-4 — Push subscriptions do not scale: every update runs every filter of its type on the projection's thread, expiry and cancel are linear, and nothing is bounded
- **Severity:** Medium · **Likelihood:** Possible · **Confidence:** Confirmed (code) for the costs; times are estimated
- **Location:** `SubscribableProjectionActor.scala`
  - 81–83: subscriptions are kept per type as a `List`.
  - 148–174 `deliverUpdate`: runs every subscription's filter of that type for each update.
  - 185–207 `clearIdleSubscriptions`: for each expired subscription, `length` and `filterNot` over the whole list.
  - 209–228 `handleUnsubscribe`: the same.
  - 100–126: no cap on subscriptions, and listeners are not watched.
  - 83 and 150–152: the updates cache is bounded by time only.
- **Scenario (estimated):** 50 000 open pages (book ch. 11 pattern).
  - At 1 000 updates/s that is 5·10^7 filter calls per second on one thread, so the projection lags.
  - A web node with 15 000 sessions dies. Ten minutes later their expiry costs about 1.5·10^9 list steps: seconds to tens of seconds with the projection blocked.
  - A graceful stop of that node sends 15 000 cancels, each linear.
  - A client looping on subscribe grows the map without limit for 10 minutes, and dead listeners receive pushes until they expire.
- **Impact:** projection lag and unanswered queries for all tenants during web-tier restarts or client bugs.
- **Validation:** an actor benchmark: expire 15 000 of 50 000 subscriptions in one tick; measure the lag with 20 000 subscriptions at 1 000 updates/s.
- **Fix:** per-type `LinkedHashSet` or `HashMap`; an optional routing key in `handleSubscribe` and `sendUpdate`; watch listeners; caps per projection and per listener; a cache bounded by count and bytes.
- **Regression test:** the benchmark with a time budget, and a terminated listener's subscription removed.

### PROJ-5 — A targeted rebuild of a large type or organization, and a purge's last step, block the projection for the whole clear
- **Severity:** Medium · **Likelihood:** Possible (every D40 import ends in `RebuildAggregates(OneSpace)`) · **Confidence:** Confirmed (code); sizes and times are estimated
- **Location**
  - `ProjectionActor.scala` 430–439: `aggregatesOf()` on the actor thread, one transaction for `clearForRebuild` and `resetVersions`, then `toSet`.
  - `ProjectionActor.scala` 447–455: `PurgeSpace` clears in one transaction on the actor thread.
  - `Projections.scala` 524–527: every id of the target is read into a `Vector`.
  - `SubscriptionsState.scala` 463–474: all deletes in one transaction; the eviction of every id runs under the monitor all projections share.
  - `PostgresDocumentStore.scala` 73–80 and 556–564: no index on `space_id`, so `clearSpace` scans the whole table.
- **Scenario (estimated):** `OneType` over 10 million aggregates.
  - About 10 000 page reads on the projection's thread.
  - About 45 MB of heap per million ids, so about 450 MB.
  - One transaction deleting millions of subscription rows, plus a full-table scan per document table.
- **Impact:** the projection answers and applies nothing for any tenant for minutes; OOM risk on 1 GB heaps (E2's size); the long transaction holds back vacuum on the hot tables.
- **Validation:** bench a `OneType` reset over 1 million aggregates (handling time and heap), and `EXPLAIN` `clearSpace`.
- **Fix:** reset and clear page by page between messages (the pending target is already re-runnable); add a `space_id` index, built concurrently.
- **Regression test:** a paged reset with bounded memory, with queries answered meanwhile.

### PROJ-6 — `GetProjectionStatus` returns every failure with its full error, so in a cluster it fails once there are a few hundred failures
- **Severity:** Low–Medium · **Likelihood:** Possible · **Confidence:** Confirmed (code); the threshold is estimated
- **Location:** `ProjectionActor.scala` 487 and 1059–1061 (errors untruncated, see 759 and 774); `Projections.scala` 439 (the supervisor forwards the query, and the instance answers across nodes); the 248 KiB frame limit (SCALING §6).
- **Scenario:** 5 000 failures of about 1 KB each. An admin tool on another node gets `MessageTooLargeException`. Threshold ≈ 250 failures (estimated).
- **Impact:** during a mass failure, operators cannot list the failed aggregates through the API; the metrics still count them.
- **Validation:** a two-node test with 1 000 failures, asking for the status from the other node.
- **Fix:** counts plus the first N failures with truncated errors, a paged `GetProjectionFailures`, and errors truncated when a failure is recorded.
- **Regression test:** the validation.

### PROJ-7 — The updates cache replays one type's updates through another type's filter, so the missed updates are lost
- **Severity:** Low · **Likelihood:** Possible · **Confidence:** Confirmed (code)
- **Location:** `SubscribableProjectionActor.scala` 46 (cache entries have no type), 150–151, 113–118 (unchecked `asInstanceOf[DATA]`), 123–125 (one catch around the whole subscribe).
- **Mechanism:** a typed filter's lambda throws `ClassCastException` on the first entry of the other type. The replay stops and the error is logged, but the subscription stays registered, so the client never learns it missed updates.
- **Validation:** push `B`, then `A`, with a cache; subscribe for `A` with a typed `missedUpdatesFilter`. Expect `A` replayed; the bug sends nothing.
- **Fix:** store the type in each entry and replay only entries of the subscription's type, each in its own `Try`.
- **Regression test:** the validation.

## 3. Lesser findings
- **L1:** a retried failing batch rejoins a group at each of its 9 attempts (765 → 643–647 → 886–892). Each attempt rolls back up to 100 batches and re-runs 99 of them alone; under a listener bug that hits many aggregates, throughput drops to one transaction per aggregate (Confirmed).
- **L2:** `retryRateLimited` dedupes with `Vector.contains` (1006–1010). `RetryAllFailedSubscriptions` during a running retry wave is O(n²): about 2.5·10^9 comparisons for 50 000 failures (estimated) (Confirmed).
- **L3:** `eventBus` is whichever bus delivered last (589). In a cluster it can name a node that left, so gap requests (722), `Republish` (1035) and a skip's request (1050) go to dead letters until a live bus delivers again (Confirmed).
- **L4:** F85's loop has no exit when `recordFailure` can never succeed, for example an error text with a raw U+0000 coming from a `MatchError`. It logs an ERROR every 30 s forever (787–791) (Hypothesis: depends on how mpjsons escapes U+0000).
- **L5:** a `Metrics` implementation that throws in `afterBatch` (740, called inside the foreach at 926) leaves the rest of a committed group at `processing = true` forever: those aggregates stall and the status does not show it (Confirmed; the Metrics contract already forbids throwing).
- **L6:** `subscription_failures.failed_since` is a `TIMESTAMP` written in the JVM's time zone (269, 281, 289), unlike events (UTC, F65). A zone or DST difference shifts it, and it orders the retries at start (Confirmed).
- **L7:** the purged ids (558, 577) live only in memory. A restart or singleton move inside the drop window can let a stray delivery recreate a purged organization's documents (Confirmed; unlikely).
- **L8:** the builder reads versions through `ProjectionsSettings.subscriptionsState` (`ProjectionBuilder` 313), the projection through its own (797). Nothing enforces one instance (the book only asks for one per system); with two, the reconciliation reads a stale cache after a reset and skips aggregates (Confirmed).
- **L9:** every non-delivery message flushes the group (365): `SendUpdate`, `RenewSubscription`, queries. Push or query traffic breaks the batches up (Confirmed; performance).
- **L10:** `AggregateListenerActor.receiveQuery` is `case m => ()` (6–8), so it silently swallows every unknown message, including `MessageTooLargeException` failures (Confirmed).
- **L11:** any sender can cancel, renew or take over a push subscription by its id (67–70, 106–109, 178–183). If clients choose ids, one user can cancel another's updates (Confirmed; host concern).

## 4. Known, re-checked
- **F48** (closed: "one writer keeps caches valid") is incomplete. A node's `PostgresSubscriptionsState` caches every subscriber's versions of an aggregate, loaded by any projection on that node (`SubscriptionsState` 341–344, 480), and they stay frozen for subscribers whose writer runs elsewhere.
  - After a singleton moves to such a node, stale-low entries cause spurious gaps and failed groups, and `noDBSession` listeners repeat their side effects for the delta.
  - Stale-high entries (after a purge and a re-import with the same ids) acknowledge deliveries without applying them (669–670).
  - This needs per-node subscribers that share the node's state (open question 1).
- **F121 / SCALING §7:** when the oversized delivery is an aggregate's last event, no gap is ever seen. The projection drops `Status.Failure(MessageTooLargeException)` as unhandled (it carries no aggregate id), so no failure is ever recorded, and the poller re-publishes every 30 minutes forever.
- **F126 / F93:** the window counts event bytes only (`EventsBusActor` 375). State deliveries also carry the state, which is deserialized per message on another node. A state projection's mailbox can therefore hold up to about 10 000 × 248 KiB ≈ 2.4 GB per node's bus (estimated), not the 64 MB in SCALING §5 and §9.
- **F161:** `afterCommit`/`sendUpdate` called in a listener's first part (the `(id, events) => { …; session => … }` shape of F109) runs immediately, before the commit (846–849). It runs again when a failed group remakes the work (929).

## 5. Checked and safe
- **SequenceGate:** overlaps (F5), `startsSequence` (F98) and off-by-one are correct for contiguous deliveries; contiguity is guaranteed by F128's runs and by the builder's runs.
- **Acks:** a covered delivery is acknowledged only after its batch (F86); a delivery at or below the applied version is acknowledged at once.
- **`AfterCommit`:** a rollback or a failed commit drops the actions; actions are isolated; maps and thread-locals are cleaned in `finally`. `committing`/`afterCommit` (F161) run only after a successful commit.
- **D61 groups:** a failed group falls back to one batch at a time; `noDBSession` batches run alone; failures while making the work are retried (F109).
- **Commit outcome unknown or stale-low cache:** the subscription compare-and-set rolls back duplicate application; F77 evicts the entry.
- **DB down for 10 minutes:** version reads retry every 30 s without counting attempts; nothing is answered until a failure can be stored (F85); the projection recovers within about 30 s after the DB returns.
- **Gaps that cannot fill:** given up after 10 minutes (F90). Squash is handled by `startsAtSquash`, and D51 blocks a squash while a subscriber lags. Undo keeps its versions (`readPart` does not filter). Permanent delete is gated by failure rows. A purge forgets the aggregates and has a drop window.
- **Parallel mode with a targeted reset:** row locks and the compare-and-set keep documents and versions consistent.
- **An interrupted targeted rebuild** re-runs the reset on the new node, which evicts that node's cache.
- **`PostgresSubscriptionsState` concurrency:** monitor, stripes and F115 tracking are correct.
- **Futures:** no `sender()` or `context` is used in callbacks.
- **Backoff:** no overflow.
- **`PurgedIds`:** the time comparisons are safe when `nanoTime` wraps.
- **Document store inside a group:** the cache entry is removed at once, re-read inside the transaction, and set after the commit.
- **Chunk path:** a failed version read goes back to the builder as `Status.Failure`; a failed chunk is handed to the queues.

## 6. Open questions
1. Does Neula run per-node subscribers that share the node's `PostgresSubscriptionsState` with singletons? This decides the F48 risk.
2. Does Neula's JDBC URL set `logServerErrorDetail`, and do its listeners throw `MatchError` on new or squash event classes? This decides how much of PROJ-2 applies.
3. How many push subscriptions do Neula's projections hold, how much do they churn when the web tier restarts, and are their ids ever chosen by clients?
4. Does Neula use `delayIfNotAvailableAsync` on busy projections?
5. How large are Neula's biggest aggregate types and organizations (PROJ-5)?
6. How much do owners' command latencies grow while many projections catch up on the shared 32-thread blocking dispatcher? This was not measured.
7. Does mpjsons escape U+0000 in stored events (L4)?

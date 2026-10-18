# Working notes: projections (second risk assessment)

- **Area:** projections, prefix R2-PROJ
- **Scope:** `core/.../projection/*` (`ProjectionActor` with its queues, gate, groups, parallel mode, retries,
  `subscription_failures`, `afterCommit` and F262's `workOf`; `SubscribableProjectionActor` with F216/F241/F267;
  `SubscriptionsState` and its cache), `core/.../util/AfterCommit.scala`; followed into `Errors`, `AggregateOwner`
  (re-publication), `ClusterSerializer`, `Projections` (callers of the drop).
- **Date:** 18/10/2026 · **Commit:** 2453711 (1.0.1-SNAPSHOT) · read only, nothing run.
- Line numbers are those of 2453711. Times and sizes are estimated unless a source is named.

## Findings

### R2-PROJ-1 — A batch that fails on the database every time is retried every 30 s for ever: never recorded, absent from the status, logged as "cannot reach its database"
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code) for the path; Strongly suspected for which
  errors repeat (PostgreSQL SQLStates, host settings)
- **Where:** `Errors.scala:54-57` (`transient`: SQLState classes 08, 40, 53, 57, or no state); `ProjectionActor.scala:781-790`
  (transient branch: no attempt counted, `schedule(retries.max)`), `1126-1133` (`status`: `retrying` needs
  `failedAttempts > 0` or `retryOf`).
- **Scenario:** a listener's statement of one aggregate always exceeds a role `statement_timeout` (57014; the first
  assessment recommended role timeouts), or a statement runs past the book's `socketTimeout=600` (pgjdbc reports 08006),
  or a big transaction hits "out of shared memory" (53200), or host code throws a `SQLException` without a state.
- **Mechanism:** the failure is classed transient whatever its cause and whether it repeats; the batch is put back and
  tried every 30 s with no count, so `retries.exhausted` is never reached and `giveUp` never runs.
- **Impact:** the aggregate is stuck for that projection with no failure record, no entry in `ProjectionStatus`
  (`retrying`, `waitingForGap` and `failures` all omit it), no metric; the only trace is a WARN a minute that the
  database cannot be reached, while it can. Its outbox rows stay (poller re-publishes every 30 min, "Undelivered" logged),
  a permanent delete or a squash of it waits for ever (D51, delete gate), and each re-publication appends one more
  delivery to its pending queue. Operators cannot retry or skip what is not listed.
- **Validation:** a listener that throws `new PSQLException("canceling statement due to statement timeout",
  PSQLState.QUERY_CANCELED)` for one aggregate; `retries` of 3 × 10 ms, `retries.max` 50 ms; after 5 s expect a recorded
  failure (bug: none, `GetProjectionStatus` empty, the batch tried ~100 times).
- **Suggested fix:** count transient attempts separately with a long cap (e.g. one hour of consecutive failures), then
  record a failure that says "database"; narrow the set (08, 40001, 40P01, 53300, 57P01-57P03, a pool timeout); list
  queues waiting on the database in the status (`waitingForDatabase`) and count them in a metric.
- **Regression test:** the validation, plus the F254 scenario (database down 2 min) still leaving no record.
- **Continues:** F254.

### R2-PROJ-2 — F259 treats "subscription at version 0" as "never had the aggregate": live events of a new aggregate, or of a retried failure at version 1, are applied as history and their side effects are skipped
- **Severity** Medium · **Likelihood** Possible (retry path on any installation; first-delivery path in a cluster) ·
  **Confidence** Confirmed (code)
- **Where:** `ProjectionActor.scala:708` (`liveFrom` set on any gap at `applied == ZERO`), `723` (`history`), `538`
  (`fromBuilder`), `869` (`asHistory`), `776`; `SequenceGate.scala` `decide` (Gap at 0 when `first > 1`).
- **Scenario A (retry):** the book's own example: a listener fails at version 1 of cart 43 (unique index) and gives up;
  deliveries of it fail; after a fix, `RetryFailedSubscription` asks the owner from 1; before the re-publication arrives,
  a live event v(N+1) of the cart arrives (it is accepted: `retry` took the key out of `failed`). The gate sees Gap(1)
  at applied 0, sets `liveFrom = N+1`; the re-published 1..N+1 is one batch starting below it, so `history = true`.
- **Scenario B (first delivery late):** a `Local` type in a cluster (F131): v1 written through node A, v2 through node B
  moments later; node B's bus delivers v2 to the projection singleton before node A's v1 (different senders, no order;
  or v1 held by A's flow control). Gap(1) at 0, `liveFrom = 2`; when v1 arrives, the batch 1..2 is history.
- **Variant:** `liveFrom` lives in the queue only; if a reset aggregate's gap is given up and later retried, the new
  queue has none, and the history is applied as live (the side effects F259 was meant to stop).
- **Impact:** `rebuilding` is true for live events: subscribable projections push nothing for them, and side-effect
  listeners written as the book says (`if (!rebuilding) mailer.sendOnce(...)`, chapter 9) never send. In A, every event
  1..N+1 the listener never applied loses its effect; whether it does depends on a race with the next command. No log.
  The book (ch. 9) says this happens only after a targeted reset or for a projection added late.
- **Validation:** A: `MemorySubscriptionsState`, listener recording `rebuilding`; fail v1 until a flag flips, give up;
  flip; `RetryFailedSubscription`; deliver v3 live before answering the `RequestRepublish` with 1..3; expect
  `rebuilding == false` for 1..3 (bug: true). B: deliver v2 then v1 to a fresh projection; expect live (bug: history).
- **Suggested fix:** mark history only for aggregates this instance reset (`ResetForRebuild`, `PurgedAggregates`: keep
  their ids with a time window, as `PurgedIds`) or, better, let the owner say which versions are history: those below
  its oldest outbox version were delivered live before; never set `liveFrom` while `retryOf` is defined.
- **Regression test:** both validations, and F259's spec still green.
- **Continues:** F259.

### R2-PROJ-3 — A group that fails on the database is run again one batch at a time: up to 100 more transactions, each waiting out the pool, on the projection's thread
- **Severity** Medium · **Likelihood** Possible (pool exhaustion under load, failover) · **Confidence** Confirmed (code);
  times estimated
- **Where:** `ProjectionActor.scala:984-994` (`case Failure(e) if grouped.size > 1` falls back regardless of
  `Errors.transient`), `991` (`run(workOf(...))` remakes each work, re-running the listener's first part), `781-790`.
- **Scenario:** the pool is exhausted (book: 5 s `connectionTimeoutMillis`, DBCP's "Cannot get a connection" has no
  SQLState, so transient). A group of 100 batches fails after 5 s; the fallback borrows 100 more times in sequence.
- **Mechanism:** F254 classifies the failure only per batch, after the fallback; the fallback is meant to isolate one
  listener's failure, not the database's.
- **Impact (estimated):** about 100 × 5 s ≈ 8 min of the projection's thread blocked per group (more where a dead
  pooled connection's validation waits for `socketTimeout`), during which it answers no query, status or delivery, and
  adds 100 waiters to the exhausted pool; with several projections on the 32-thread blocking dispatcher, owners starve.
  Each 30 s retry forms the group again and repeats it.
- **Validation:** a `SubscriptionsState` whose `localTx` sleeps 200 ms then throws `new SQLException("pool")`; 100
  due aggregates with `maxBatchesPerTransaction = 100`; time one flush: expect ~0.2 s (bug ~20 s).
- **Suggested fix:** in `applyGroup`, on a transient failure hand every batch to `afterBatch(..., Failure(e))` (requeued,
  one retry later); fall back one batch at a time only for other failures.
- **Regression test:** the validation.
- **Continues:** F254 (and D61's fallback).

### R2-PROJ-4 — Since F201, a "versions missing" failure can no longer be skipped: versions that exist but cannot arrive (an event over the frame between nodes) block the aggregate for good
- **Severity** Medium · **Likelihood** Unlikely (cluster with an event above ~248 KiB, SCALING §6) · **Confidence**
  Confirmed (code); the oversized-message behaviour per `ClusterSerializer.scala:81, 366-369` and SCALING §6
- **Where:** `ProjectionActor.scala:1105-1124` (`advance` skipped and the request made from `failure.version` when the
  error starts with `MissingVersions`), `747-749`; `AggregateOwner.scala:289-292`.
- **Scenario:** an events projection runs as a singleton on node B; cart 7's event v12 serializes over the limit, so
  node B receives `Status.Failure(MessageTooLargeException)` (unhandled by the projection). v13 makes a gap; after 10 min
  "versions from 12 are missing". The operator skips, as SCALING §6 and 1.0.0 implied.
- **Mechanism:** the skip marks nothing and asks from 12 again; the owner has v12 and re-publishes it; the part is too
  large again (the bus cannot split one event); the next live event re-opens the gap; 10 min later the same failure.
  Retry does the same. In 1.0.0 the skip advanced to 12 and asked from 13.
- **Impact:** the projection never catches up on that aggregate; while the record exists a squash or permanent delete
  (erasure) of it is refused, and after each skip the delete event itself sits behind the gap. Only a targeted rebuild
  (managed projections that implement `clearForRebuild`, built on the projection's node) gets past it; unmanaged
  projections have no way out short of raising the frame size.
- **Validation:** two-node `TestCluster`, frame 256 KiB, an events projection on the other node, an event of 300 KiB
  then a small one; `gapGiveUpAfter = 1 s`; `SkipFailedVersion`; expect the small event applied (bug: missing failure
  again).
- **Suggested fix:** decide a "missing" skip by the owner's version: the owner answers every `RepublishRange` (or a new
  query) with its version; when it has the versions (`version >= failure.version`), advance as 1.0.0 did; when it has
  not, keep F201's behaviour. Or a `SkipFailedVersion(id, force = true)` documented for this case.
- **Regression test:** the validation, plus F201's "skip marks nothing" scenario for versions that do not exist.
- **Continues:** F201 (and F121).

### R2-PROJ-5 — F217's batched drop re-reads the rows it already deleted: quadratic while anything holds back vacuum
- **Severity** Low · **Likelihood** Possible · **Confidence** Strongly suspected (PostgreSQL's handling of dead tuples)
- **Where:** `SubscriptionsState.scala:447-452` (`DELETE ... WHERE id IN (SELECT id ... WHERE subscriber_type_id = X
  LIMIT 10000)` until 0); callers `Projections.scala:64-76` (`retire`, under `SchemaLock`, at every node's start) and
  `742-751` (`drop`, on the supervisor's thread).
- **Mechanism:** each statement starts its scan at the beginning of the subscriber's range (index or heap); the rows the
  earlier statements deleted are dead but still there until vacuum, and while a snapshot older than them exists (a
  pg_dump, a long report, `hot_standby_feedback`) index entries cannot be marked dead and pages not pruned, so iteration
  k visits about k × 10 000 dead rows.
- **Impact (estimated):** 10 M rows → 1 000 statements, ~5·10^9 dead-row visits: from minutes to hours instead of one
  pass; under `SchemaLock` the other nodes' starts wait on the lock meanwhile.
- **Validation:** 2 M rows for one subscriber, a concurrent `REPEATABLE READ` transaction held open, time the drop and
  `EXPLAIN (ANALYZE, BUFFERS)` the 100th statement.
- **Suggested fix:** keyset deletion: `... AND (subscription_type, aggregate_id) > (last) ORDER BY ... LIMIT 10000
  RETURNING`, continuing from the last key; run the drop off the schema lock where it can.
- **Regression test:** a bench in `bench/` with a held snapshot; time linear in the row count.
- **Continues:** F217.

## Lesser items
- F254: the transient branch waits `retries.max` (30 s) for every database error, so a deadlock or serialization
  failure between parallel workers (40P01, 40001), or a short pool saturation, now delays its aggregate 30 s instead of
  the back-off's 1 s (`ProjectionActor.scala:790`). Confirmed (code).
- `ServeUnchanged` empties `failuresToRetry`, and the next `ConnectedTo` does not retry again (`failuresLoaded`): failures
  queued in a start-up retry wave wait for an operator after a stale-serve episode (`ProjectionActor.scala:429, 1054`).
  Confirmed (code).
- F82: when `recordFailure` fails for an unreadable chunk item, `giveUp` leaves an empty queue with a back-off; its next
  drain finds nothing and forgets it. The item is neither applied, recorded nor reported to the builder as a gap, and the
  chunk's later items of that aggregate are skipped (`ProjectionActor.scala:1024-1028, 823-827, 732`). Confirmed (code);
  needs a database error at that moment.
- `delayIfNotAvailableAsyncCustom`: a `found` that throws does so in the `onComplete` callback; the asker is never
  answered (`ProjectionActor.scala:1250`). F234 covered only a search that throws as it starts. Confirmed (code).
- F216: the library now watches every listener and unwatches it when its last subscription ends, and takes `Terminated`
  of listeners before `receiveQuery`. A host projection that watched its subscribers itself (useful before F216) loses
  its own watch on unwatch and never sees the `Terminated` (`SubscribableProjectionActor.scala:86, 155, 218`). Confirmed
  (code); depends on host code.
- F216: each subscribe scans the whole updates cache (up to 10 000 entries) and runs `missedUpdatesFilter` on its type's
  entries; a reconnect storm of 50 000 subscribes is ~5·10^8 steps on the projection's thread (estimated)
  (`SubscribableProjectionActor.scala:132-141`).
- F216: the 100 000 cap is per projection only: one client that keeps subscribing (new pages, new listener actors)
  refuses everyone's subscriptions for up to the TTL (`SubscribableProjectionActor.scala:123-127`). Host concern.
- F201: a late `NothingToRepublish` (answer to an earlier, lower request) closes a newer gap at a higher version and drops
  its recheck timer; the waiting delivery is then picked up only by the next event or the poller's re-publication
  (3-6 min) (`ProjectionActor.scala:620-626`). Hypothesis: needs the direct answer to overtake the bus's delivery.

## Fixes checked and found sound
- F262: early `afterCommit` actions captured per `workOf` and re-registered in the committing transaction; remade work
  (group fallback, chunk hand-over, retries) drops the old ones; parallel mode stays on one pool thread.
- F241: renew and cancel checked against the registered listener; singleton proxies forward the sender; the book's
  pattern (session actor renews and cancels, also from `postStop`) keeps working.
- F216: per-type sets, watch/unwatch bookkeeping and `Terminated` cleanup consistent; no stray `Terminated`
  (Pekko suppresses it after unwatch); cache bounded by count.
- F267: cache entries typed, replay per type, each in its own `Try`.
- F228: status bounded (100 failures, 500 characters, 1 000 ids, total kept).
- F219: per-transaction `Open` in a concurrent map by session identity; cleanup in `finally`; rollback drops actions.
- F234: deadline checked before searching again; a search that throws at start fails its own query only.
- F201 (stale-record parts): clear at drain when applied, clear retried until it succeeds, `NothingToRepublish` handled,
  failure load retried, `ServeUnchanged` keeps failures — sound; the skip change is R2-PROJ-4.
- F254 (database outage itself): no records while the database is down, recovery within ~30 s — sound; see R2-PROJ-1, -3.
- F217 (retired build's rows in transactions of 10 000): bounded transactions as intended; cost in R2-PROJ-5.

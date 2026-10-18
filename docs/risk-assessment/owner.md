# Working notes: aggregate owner

> The notes of one area of the risk assessment of 18/10/2026, kept as written. Their ids (OWN-1, …) map to the
> F-numbers of [RISK-ASSESSMENT.md](../RISK-ASSESSMENT.md) (§14), which holds the consolidated findings: where the two
> differ, the report holds. Nothing was run; these are traces from the code, with lesser items not triaged there.

## 1. Examined

**Read in full**
- `core/.../aggregate/`: `AggregateOwner.scala`, `AggregateRestorer.scala`, `OutboxWriter.scala`, `SquashScheduler.scala`.
- `api/`: `AggregateContext.scala`, `Command.scala`, `CommandResult.scala`, `Event.scala`, `AggregateVersion.scala`, `Exceptions.scala`, `CustomCommandResponse.scala`, `Aggregate.scala`, `package.scala`.

**Followed along the owner's paths**
- `PostgresEventStoreState`:
  - writes and reads: `persist`, `serializedChecked`, `overwriteEvents`, `processEvents`, `readAndProcessOwnEvents`, `readPart`;
  - snapshots: `readSnapshot`, `writeSnapshot`, `storedVersion`, `historyGeneration`, `historyChangedFor`;
  - squash and detach: `lockChain`, `squash`, `blocker`, `detachDuplicate`, `squashCandidates`;
  - rewrite and copies: `rewriteEverywhere`, `copiesOf`.
- The procedures `add_events`, `add_undo_event` and `add_duplication_event`.
- `MemoryEventStoreState.squash` and `detachDuplicate`.
- `Idempotency`, `CommandResponseState`, `AggregateCommandBusActor`, `Owners` (`LocalOwners`, `ShardedOwners`), `HistoryRewriter`, `Metrics`, `AfterCommit`, `Backoff`, core `reference.conf`, and `SpaceImport` (id handling).
- mpjsons 0.7.0 sources from the local `~/.m2`: `BeanDeserializer`, `MPJsonsConfig` and the error formatter.

**Docs**
- CLAUDE.md; ARCHITECTURE §2–§4, §10, §12, §13.3.
- KNOWN-ISSUES: decisions and reviews 1–9. The archive: F7–F183 (the commands/aggregates and store entries).
- SCALING §1–§3 and §7–§9; design M3 and M6 (the rewrite).

**Not examined**
- `EphemeralEventStoreState` internals, `ClusterSerializer`, and `EventsBusActor` beyond its routing lines.
- Pekko's fatal-error handling: taken from my knowledge of Pekko, not read.
- Neula's code.
- Nothing was run (no sbt, no tests).

## 2. Findings

### OWN-1 — A snapshot of a root class that gained a field is read with null, 0 or false in that field, silently
- **Severity:** High · **Likelihood:** Possible · **Confidence:** Confirmed (code, including the mpjsons 0.7.0 source)
- **Location:**
  - `AggregateRestorer.usableSnapshot` 63–78: it filters only on `contextVersion` and `schemaVersion` (67–68).
  - `PostgresEventStoreState.readSnapshot` 225–229: `mpjsons.deserialize[AGGREGATE_ROOT](rs.string(4), stateClassName)`.
  - mpjsons `BeanDeserializer` says: "absent keys get zero / false / null / None".
  - `SnapshotPolicy` doc (AggregateContext.scala:131–134): "Change `schemaVersion` when the state class changes shape" — a manual step only.
- **Preconditions:** a context with `snapshots` (H8 recommends them for long histories). A release adds a field to the root class or to a case class inside it, and `schemaVersion` is not raised.
- **Scenario:**
  - `Limits(max)` gains `warnAt: Int = 80`, or `Item` gains `vatRate: BigDecimal`.
  - After the deploy every restore takes the snapshot, giving `warnAt = 0` and `vatRate = null`.
- **Mechanism:**
  - A removed or renamed field fails ("No field [..]"), so the restore falls back to a full replay (safe).
  - An added field passes silently with a zero value. R10 ("never fail because of a snapshot") means no structural check exists.
  - Command handlers decide on the wrong state and persist events. `nextRoot` keeps the wrong state, publishes it to state listeners, and writes it as the next snapshot.
- **Impact:**
  - Wrong decisions are stored as permanent events (I6 broken between the snapshot path and a replay), and NPEs follow later.
  - Specs without snapshots pass, so this is hard to detect.
- **Validation:** store a snapshot whose JSON lacks the new field; restore with the new class; compare with a full replay. They differ.
- **Fix:**
  - Store a shape fingerprint with each snapshot: a hash of the field names and generic types of the root and of the case classes it reaches. Java reflection is enough, so D75 is respected.
  - Treat a mismatch as unusable (full replay, then a new snapshot). Optionally, compare a sampled snapshot restore with a replay after each start.
  - Trade-off: one replay per aggregate after a harmless shape change.
- **Regression test:** a snapshot with an old fingerprint is not used (`reactivecqrs.restore` measured with `from=events`), and the state equals a replay.

### OWN-2 — Squashing a duplicate cuts it from its bases without recording lineage, so an erasure of the base never reaches it
- **Severity:** High · **Likelihood:** Possible · **Confidence:** Confirmed (code, both stores)
- **Location:**
  - `PostgresEventStoreState.squash` 758–785: line 782 `DELETE FROM aggregates WHERE id = $id AND base_id <> $id` inserts no `aggregate_lineage` row.
  - By contrast, `detachDuplicate` 750–753 does insert it.
  - `MemoryEventStoreState.squash` 244–261 behaves the same.
  - `copiesOf` 715–719 reads the chain rows plus the lineage.
  - `squashCandidates` 800–804 also picks duplicates. The squash is reached through `AggregateOwner.squashHistory` and `SquashScheduler`.
- **Preconditions:** duplication and squash used on a type. Base B holds personal data. Duplicate D is squashed.
- **Scenario:**
  1. D is copied from B at version 10. The scheduler squashes D up to version 120.
  2. The squash event is `toEvent(D's state)`, which holds B's values.
  3. D's chain rows go, and no lineage row is written.
  4. Later, `HistoryRewriter.rewrite(B, EraseCustomer)` walks `copiesOf(B)`, does not find D, and reports `complete`.
- **Impact:** D's squash event, its snapshots and its projections keep the erased data. Nothing in the store links D to B any more. This contradicts D50 ("Squashing a duplicate detaches it the same way") and D76 ("lineage that every detach records").
- **Validation (both stores):** squash a duplicate, then assert `copiesOf(B)` contains D. It fails today.
- **Fix:**
  - In `squash`, insert the lineage of the aggregate's own chain before deleting it (same SQL as `detachDuplicate`). In the memory store, record `chainOf(aggregateId)` first.
  - Already-squashed duplicates have lost the link: add a note to the runbook.
- **Regression test:** as Validation, plus rewrite(B) changes D's squash event.

### OWN-3 — After a base is erased, its attached duplicates keep its values in projections, and a lost `HistoryChanged` is never repaired (boundary with HistoryRewriter)
- **Severity:** High · **Likelihood:** Likely wherever duplicates and state-receiving projections exist · **Confidence:** Confirmed (code)
- **Location:**
  - `AggregateOwner.rewriteHistory` 780–787: `if (count.events + count.archived > 0) { historyChangedForDuplicates(); restore() }`.
  - `historyChangedForDuplicates` 796–799 logs "their owners keep the states of before" on failure.
  - `HistoryRewriter.walk` 67–69: count 0 goes to `unchanged`. `rewrite` 49 rebuilds only `rewritten`.
  - `Projections.scala:533`: `OneAggregate(id)` covers only that id.
  - `caughtUp` 318: a sharded owner never compares with the store.
- **Scenario:**
  - Rewrite(B) changes B and rebuilds B.
  - D, an attached duplicate, gets `HistoryRewritten(D, 0, 0, …)`, so it is "unchanged" and never rebuilt. D's documents show B's old values until D's next event. The report says `complete`.
  - If `duplicatesReading` fails, or the node dies between the commit and the tell, D's sharded owner keeps serving the old values: through reads, and through new events that copy them.
  - A rerun, the documented remedy ("Run it again for the rest", HistoryRewriter.scala:20–21), changes nothing: both B and D answer count 0, which skips both the notice and the restore.
- **Impact:** personal data stays in read models, and possibly in a live owner, after an erasure reported complete.
- **Fix:**
  - The rewriter rebuilds every attached duplicate that reads a rewritten aggregate.
  - The owner always sends `HistoryChanged` on a rewrite.
  - A sharded owner restores when the stored generation differs from its own (one lookup per rewrite).
- **Regression test:** rewrite(B) leaves no old value in D's document. A rerun after a failed `duplicatesReading` refreshes D's owner.

### OWN-4 — A command or rewrite still running when the history changes without a new version commits on the old history; erasures can be undone
- **Severity:** High · **Likelihood:** Unlikely · **Confidence:** Confirmed (code)
- **Location:**
  - `persistOrAnswer` 467: the only check is `command.baseVersion != version`.
  - `runHandler` 421–428: the state and the events to rewrite are captured before the handler runs.
  - `rewriteHistory` 770–793: the version stays.
  - `squashHistory` 720: a `SquashMayChangeState` squash restores, and the version stays.
  - The F146 path at 498: a rewrite without a new event.
  - `overwriteEvents` 138–149 updates by version and class, without checking what is stored.
- **Scenario A (lost update):**
  1. A rewrite command R (Neula's anonymization uses `RewriteHistoryConcurrentCommand`, F73) reads its events and runs its handler.
  2. `RewriteEverywhere` runs in the same owner. It erases event 3, and the version stays V.
  3. R finishes: `baseVersion == version`, so `overwriteEvents` writes R's copy of event 3, built from the un-erased one.
  4. The erased value is back. Two rewrite-only commands overwrite each other the same way.
- **Scenario B:** a handler that copies a state value into its event, started before the erasure, persists that value after it.
- **Mechanism:** `generation` is refreshed by `restore()` but never compared at persist time. The M6 design assumes "the owner runs the rewrite between commands" (M6-organizations.md:472). In fact it runs between messages, while handlers run in futures.
- **Impact:** personal data reappears after a rewrite reported complete, and the rewriter's later projection rebuild re-reads it. Silent.
- **Validation:** a handler blocked on a promise; send `RewriteEverywhere`; release the handler; event 3 holds the old value again.
- **Fix:**
  - Keep `baseGeneration` in `Pending`. In `persistOrAnswer`, a generation change counts as an own conflict (concurrent kinds run again, others get the conflict).
  - Alternatively, refuse `RewriteEverywhere` while handlers run, as `materialize` does (740).
  - Trade-off: more conflicts, only on aggregates being rewritten.
- **Regression test:** as Validation, for `RewriteEverywhere`, a rewrite-only command and a `SquashMayChangeState` squash, in both modes.

### OWN-5 — The command timeout cannot fire while the owner waits on JDBC, and no socket, statement or lock timeout exists
- **Severity:** High (availability) · **Likelihood:** Possible · **Confidence:** Strongly suspected (the mechanism is confirmed in code; how long a hang lasts depends on driver and OS settings)
- **Location:**
  - `start` 339: `scheduleOnce(commandTimeout, self, CommandTimedOut)` is a message to the owner's own mailbox.
  - The owner's synchronous JDBC: `persist` 494–505, `restore` 227, `caughtUp` 318, `storedResponse` 402, squash, rewrite.
  - core `reference.conf`: blocking-dispatcher `fixed-pool-size = 32`.
  - No `queryTimeout`, `socketTimeout`, `statement_timeout` or `lock_timeout` in code or docs (grep).
  - `SquashScheduler.reading` 66/79.
- **Scenario:**
  1. A database failover leaves in-flight statements on half-open sockets (PG JDBC defaults: `socketTimeout=0`, `tcpKeepAlive=false`), or behind a long chain lock (OWN-10), or behind `VACUUM FULL`/pg_repack (ARCHITECTURE §4.6).
  2. 32 owners block, and every owner on the node stalls, along with projections on that dispatcher.
  3. Callers get no `CommandHandlingError`, only their own ask timeouts, and they retry elsewhere.
  4. When the sockets finally fail, possibly hours later, the backlog runs. Each timeout counts from when the owner takes the command (328), so old commands are applied late.
- **Impact:** a node-wide stall that readiness does not see; duplicates for non-idempotent retries. R5's promise does not hold. Review 9 deferred the slow-database fault test as "covered by the in-JVM timeouts only" (KNOWN-ISSUES.md:890).
- **Validation:** hold `LOCK TABLE aggregates IN ACCESS EXCLUSIVE MODE` during a command: no answer at `commandTimeout`. With 33 blocked aggregates, a `GetAggregate` on a 34th hangs.
- **Fix:**
  - Document and recommend `socketTimeout`, `tcpKeepAlive=true`, and role-level `statement_timeout`, `lock_timeout` and `idle_in_transaction_session_timeout`.
  - Set `queryTimeout` on the owner's statements.
  - Check the command's deadline right before COMMIT.
  - Optionally, a bus-side watchdog paired with that pre-commit check.
- **Regression test:** a store whose `persist` blocks: the caller is answered at the deadline and nothing commits. Add the slow-database fault to E2.

### OWN-6 — A failed version lookup in `caughtUp()` strands the command until its timeout and drops the owner's state (Local types, multi-node)
- **Severity:** Medium · **Likelihood:** Possible · **Confidence:** Confirmed (code)
- **Location:**
  - `caughtUp` 318: `eventStore.storedVersion(aggregateId)` is outside any `Try`.
  - Callers: `attempt` 407, `GetState` 272, min-version 815/835, Squash 280, Rewrite 282.
  - `guarded` 300–307.
- **Scenario (pool exhausted, a 5 s wait then a failure, SCALING §1):**
  1. `begin` marks command C started (363). `caughtUp` throws. `guarded` sets `become(unrestored)`.
  2. C stays started but not running, and nothing re-attempts it.
  3. In serial mode, `startQueued` (355) waits up to 60 s behind C. Under F99, a stranded `rerunning` command blocks the contended queue.
  4. GetState, Squash and Rewrite callers get no answer. At a min-version deadline, the expired queries are neither answered nor re-armed (843).
  5. The next message triggers a full restore, adding load during the overload.
- **Impact:** 60 s stalls, and "timed out and is not applied" in place of a fast error; restore storms amplify the database problem.
- **Fix:** catch the lookup failure in `caughtUp`. Answer the one request (`pendingFailed`, or `Failure` for queries), keep the state, and do not become unrestored.
- **Regression test:** a store whose `storedVersion` throws once, with the node not alone: the command is answered with `CommandHandlingError` within 1 s, and the queue moves on.

### OWN-7 — Personal data from commands, events and states reaches logs and error replies through exception messages
- **Severity:** Medium · **Likelihood:** Possible · **Confidence:** Confirmed (code)
- **Location:**
  - `persist` 488–489 and `finishWithError` 626–628 log the cause.
  - `restore` 237–239 logs at every restore attempt.
  - `AggregateRestoreFailedException` puts `cause.getMessage` in its message (Exceptions.scala:15–16) and is sent to callers (252–256, 272) and across nodes.
  - `AggregateRestorer.squashEvent` 103–105 builds `"... fails on the squash event X: " + e`.
  - That reason is logged by the SquashScheduler (90) and by the command bus at ERROR (`... is not deleted: " + reason`, AggregateCommandBusActor 437, repeated at each re-delivery).
  - `rewriteHistory` 790–791.
  - mpjsons parse errors quote 80 characters of the JSON.
- **Scenario:**
  - A partial-function handler missing a case: `scala.MatchError`'s message is the unmatched value's `toString`, with every field. For a squash event, that is the whole state.
  - The outbox poller, gap requests and the scheduler re-trigger restores, so the line repeats every few minutes. A REST layer may show the `AggregateRestoreFailedException` text to another user.
- **Impact:** personal data in logs (review 8 lists logs as out of the erasure's reach) and in error replies. ARCHITECTURE §2.5 documents the MatchError log, but not its content.
- **Fix:**
  - Log and forward the exception class, and for a `MatchError` only the class of the value. Keep the full text behind an opt-in debug flag.
  - Use the cause's class, not its message, in `AggregateRestoreFailedException` and in the Squash, Rewrite and Materialize refusals.
- **Regression test:** a missing event case: neither the log nor the reply contains any field value.

### OWN-8 — A `Command` expecting a version above the owner's forces a full restore on every attempt (a client-controlled cost)
- **Severity:** Medium · **Likelihood:** Possible · **Confidence:** Confirmed (code)
- **Location:** `attempt` 406–407: `if (expected.exists(_ > version)) restore() else caughtUp()`.
- **Scenario:**
  - The host passes the client's version (optimistic locking). A client sends `expectedVersion = Int.MaxValue` at 50 per second to an aggregate of 10 000 events.
  - Each attempt replays the whole history (392 ms measured for 50 000 events, SCALING §3; about 80 ms for 10 000, estimated), then answers a conflict.
  - Across 32 aggregates, the node's blocking threads and connections are saturated. A failed restore also puts the owner in the failed-restore state for 5 s.
- **Fix:** compare `storedVersion` first (one key lookup). Restore, incrementally (OWN-9), only when the store really is ahead; otherwise answer the conflict at once.
- **Regression test:** N such commands produce 0 restores (`reactivecqrs.restore`) and N conflicts.

### OWN-9 — A `Local` owner in a multi-node cluster replays the whole aggregate before each read or command that follows another node's write
- **Severity:** Medium · **Likelihood:** Possible (Likely for Local types without snapshots that are used through several nodes) · **Confidence:** Confirmed (code)
- **Location:** `caughtUp` 317–324 calls `restore()`, a full replay (`AggregateRestorer.restore` 50–58) even though the owner already holds the state at its version.
- **Scenario:** node A writes aggregate X every second while node B serves its reads. Every read on B is a full replay. A 10 000-event history costs about 80 ms per read (estimated), and B's owner of X saturates.
- **Mechanism:** H10 and SCALING record F131's cost as "a key lookup each". The replay that follows a foreign write is not counted.
- **Fix:**
  - Catch up incrementally: if the generation is unchanged and there is no squash point above the owner's version, apply only `readAndProcessOwnEvents(id, version, None)` to `root`, as the snapshot path does (45–48). Otherwise do a full restore.
  - Document snapshots or `Sharded` for such types.
- **Regression test:** two nodes alternating 100 times: at most one replay from events on the reading node, and the states equal a replay (property test with undo and squash on the other node).

### OWN-10 — Squashing a base with many duplicates: one replay per duplicate on the owner's thread, then one transaction that locks every duplicate's chain
- **Severity:** Medium · **Likelihood:** Possible · **Confidence:** Confirmed (code; the magnitudes are estimated)
- **Location:**
  - `squashHistory` 711 calls `detachEvents`, which calls `squashEvent(dup, 1)`, which calls `restore(dup, Some(Left(1)))` (AggregateRestorer 102, 113–119): the base's history is replayed once per duplicate.
  - `squash` → `lockChain` 470–472 takes `FOR UPDATE` on every chain row, including each duplicate's own row, which `add_events` updates.
  - `SquashScheduler` 92–94 moves on after `answerTimeout` while the squash continues.
- **Scenario:**
  - A template of 5 000 events with 2 000 duplicates.
  - The owner does about 2 000 replays of roughly 40 ms each, about 80 s (estimated).
  - Then one transaction rewrites 2 000 first events with full-state squash events, a large WAL burst.
  - Meanwhile every command on any duplicate blocks on its row, holding a thread and a connection (which leads to OWN-5).
  - After 1 min the scheduler starts the next squash, so long squashes overlap.
- **Mechanism:** ARCHITECTURE §4.6 records only the replay cost, as "a risk to measure".
- **Fix:**
  - Compute the base's state once per distinct base version.
  - Cap the detaches per squash, or detach in batches in earlier transactions.
  - Set `lock_timeout`.
  - Have the scheduler wait for the running squash instead of starting another.
- **Regression test:** with K duplicates, base replays ≤ distinct base versions, and commands on duplicates finish within their timeout.

### OWN-11 — A commit whose acknowledgement is lost is answered "nothing was stored"; a sharded or lone owner then stays behind the store and does not re-publish
- **Severity:** Medium · **Likelihood:** Unlikely · **Confidence:** Confirmed (code; the trigger is a known JDBC behaviour)
- **Location:**
  - `persist` 537–540: the generic failure becomes `EventHandlingError` with no reload (its doc says "nothing was stored", CustomCommandResponse.scala:34–36).
  - `caughtUp` 318: sharded and lone owners never compare.
  - `republish` 868–870: `if (start <= version)`.
  - `LocalOwners.send` 55: every re-publication refreshes the owner's activity.
- **Scenario:**
  1. COMMIT succeeds on the server but the client gets an I/O error. The caller is told the command failed, so a non-idempotent retry applies it twice.
  2. GetAggregate keeps answering the old version.
  3. The poller's `RepublishOutbox` arrives with `from > version`, so nothing is sent. "Undelivered" is logged at 10 min, then every 30 min.
  4. On a lone node with ≤ 200 owners, those retries keep the owner alive, so the 12 h idle stop never comes: projections miss the events until the aggregate's next command.
- **Fix:**
  - After an unclassified failure, `restore()` first. For a sole writer, a stored version of exactly `version + n` means committed. Otherwise answer an "outcome unknown" error.
  - In `republish`, when `from > version`, restore once.
- **Regression test:** a store that commits and then throws: the next read shows the commit, and the outbox is delivered.

### OWN-12 — Materialization: a late commit is answered "refused", a later retry returns the older copy, and the key is unique neither across types nor across starts
- **Severity:** Medium · **Likelihood:** Unlikely · **Confidence:** Confirmed (code)
- **Location:**
  - `materialize` 747–749 asks with `Timeout(commandTimeout)` and `recover`.
  - `materializeAnswered` 760 turns a failure into `MaterializeRefused` and accepts commands again.
  - `MaterializationKey.asDbKey = "materialize|" + ephemeralId` (AggregateContext.scala:90–92).
  - Token at AggregateCommandBusActor 147. Each start's id block is fixed (170).
- **Scenario A:**
  1. The persistent bus waits for an id pool, so the ask times out and the caller gets `MaterializeRefused`.
  2. The event commits later, as P1 with state S1.
  3. The user edits to S2 and materializes again.
  4. The stored response returns `Materialized(id, P1)` and the draft is dropped: the edits after S1 are lost.
- **Scenario B:**
  - Two ephemeral buses (any type, node or start, within the 30-day retention) draw the same 29-bit token. Their first aggregates then share an id and the same key.
  - The second caller is pointed at the other aggregate, possibly another organization's.
  - Probability: about 1/5.4×10⁸ per pair of starts (computed); about 9 % for 10 000 starts in 30 days (estimated).
- **Fix:**
  - Stay "materializing" after a timeout and re-send the same idempotent request until a definite answer.
  - Put a per-start UUID and the type name in the key.
- **Regression test:** a late persistent answer leaves the draft read-only, and the final persistent state equals the last state the caller saw.

### OWN-13 — Fatal errors in handlers or reads are not contained; a poison aggregate can crash-loop nodes
- **Severity:** Medium · **Likelihood:** Unlikely · **Confidence:** Strongly suspected (relies on Pekko's default `jvm-exit-on-fatal-error = on`)
- **Location:**
  - `nextRoot` 560–561 catches `Exception`.
  - `guarded` 302–303 and `restore` 227 catch `NonFatal` (via `Try`).
  - Scala's `Future` rethrows fatal errors (`runHandler` 429–442).
  - mpjsons `maxDepth` is 0 (unlimited), and no library instance sets it (e.g. ClusterSerializer.scala:34).
- **Scenario:**
  - A recursive handler over a deep tree, or an event with deep JSON, throws `StackOverflowError`, and the JVM exits.
  - If the event is stored, every automatic restore kills the node that runs it: the poller at 3, 6 and 30 min, gap requests, the squash scheduler and rebuilds.
- **Fix:**
  - Map `StackOverflowError` (only that one, not OOM) to `CommandHandlingError` or a failed restore.
  - Set `maxDepth` on the library's mpjsons instances and recommend it for the host's.
- **Regression test:** a 10⁶-deep recursion: the command is answered and the system stays up.

### OWN-14 — A rewrite without a new event that the store refuses strands its caller until the timeout, and the cause is never logged
- **Severity:** Low · **Likelihood:** Possible · **Confidence:** Confirmed (code)
- **Location:**
  - `persist` 538: `EventHandlingError(events.head.getClass…)` with `events` empty, a case `persistOrAnswer` (465) allows for F146.
  - `overwriteEvents` 147 throws `RewriteChangesClassException` (D17), or the size check fails (F154).
- **Mechanism:**
  - `events.head` throws `NoSuchElementException`, which `guarded` logs, so the real cause is never logged; the owner becomes unrestored.
  - The command is answered at `commandTimeout` with "not applied", where D17 promises an immediate `EventHandlingError`.
- **Fix:** take the event name from `events.headOption.orElse(rewritten.headOption.map(_.event))`, and log `e` first.
- **Regression test:** the error comes within 1 s, and the log names the D17 exception.

### OWN-15 — Waiting min-version queries are unbounded in number and duration, and each publish scans them all
- **Severity:** Low · **Likelihood:** Unlikely · **Confidence:** Confirmed (code)
- **Location:** `minVersionQuery` 821–822, `scheduleMinVersionDeadline` 843–847 (O(n) on each insert), `answerReachedMinVersionQueries` 826–831 (O(n) per publish). `maxMillis` is never capped: up to `Int.MaxValue` ms, 24.8 days.
- **Scenario:** clients wait for versions that never come (after a `CommandFailure`) with long `maxMillis`. 10⁵ such queries cost about 5×10⁹ operations to build (estimated), and every command scans them.
- **Fix:** cap the queries per owner and cap `maxMillis`; index them by deadline and by version.
- **Regression test:** 10⁴ unreachable queries: publish cost stays bounded, and the cap is enforced.

## 3. Lesser findings
- **Permanent delete at the version limit:** `PermanentDeleteEvent` takes a version, so once an aggregate reaches `aggregateVersionLimit` it cannot be deleted for good (`serializedChecked` 91–92); the `TooManyEventsException` doc (Exceptions.scala:44–46) mentions only growth.
- **Min-version failed although reached:**
  - The reloads at 407 and 533 do not answer the queries they now satisfy.
  - `answerExpiredMinVersionQueries` 836–838 fails expired queries without checking `q.version <= version`.
- **Handler `Error`s:** `nextRoot` 560 catches `Exception` only, so a `NotImplementedError` or `AssertionError` from an event handler strands the command until the timeout.
- **Metadata outside the `Try`:** `metadataOf` (493) runs outside the persist `Try`, so a throwing `WithMetadata.metadata` strands the command.
- **Throwing metrics after the commit:** a throwing host `Metrics` at 508–509 runs after the commit; the command is later answered "timed out, not applied" although it was stored. The contract says metrics must not throw.
- **`postStop` answers nothing:** at 198, a forced stop at the hand-off timeout answers neither pending commands nor min-version queries.
- **Stale stops drain commands:** `StopOwnerStale` (288–291) still runs started, queued and contended commands on a state it knows is stale.
- **Handlers run while the restore is failed:** in `failedRestore`, `runContended` (376–387) still runs handlers, so their side effects happen although nothing can be stored.
- **Squash events built from a stale history:** squash and detach events are built before the transaction (704–713), and `squash` does not compare the generation. On a `Local` type, another node's erasure committed in between can be overwritten.
- **New duplicates missed by a state-changing squash:** `reading` (710) is read before the transaction, so a duplicate made in between is not stopped.
- **Materialization lost on stop:** an owner stopped while `materializing` never answers the caller.
- **Negative versions:** `AggregateVersion.apply` (AggregateVersion.scala:11–17) throws `ArrayIndexOutOfBoundsException` for a negative value.
- **Long idempotency keys:** a key over 256 characters (`VARCHAR(256)`, CommandResponseState 81) fails every time with a generic error. A key built from personal data stays 30 days in a table erasure does not reach.
- **Contended queue scan:** `leftItsTurn` 391 scans the whole contended queue on every finish, O(n²) for large bursts.
- **Snapshot bursts at the sweep:** the local sweep (Owners 104–116) stops all idle owners at once, each writing a passivation snapshot.
- **Kept messages are unbounded:** `keptForStopping` (Owners 48, 57) has no bound, and the kept commands' timeouts start late.
- **Hidden squash refusals:** the scheduler logs refusals at DEBUG only (90). A permanent D85 refusal (an `Array` or `NaN` in the state, where `==` fails, AggregateRestorer 106) is retried every run, at one or two restores each.
- **`StoreFirstEvent` bypass:** any persistent bus accepts it and stores any first event without the command handlers (AggregateContext.scala:96–97), a defence-in-depth gap.
- **Clock skew in event times:** times come from the writing node's clock (484), so after a hand-over they may go backwards and blur `GetAggregateAtInstant`.
- **Scheduler stuck on a hung read:** `SquashScheduler.reading` stays set while a read hangs, and runs are skipped cluster-wide (see OWN-5).

## 4. Known, re-checked
- **F131 / H10 / SCALING:** "a key lookup each" understates the cost. After a foreign write, each read is a full replay (OWN-9).
- **F145:** it covers other nodes' changes before a command starts, not changes that keep the version made by the same owner while a handler runs (OWN-4). The M6 design line 472 assumes rewrites run "between commands"; that is wrong.
- **D76 / F8:** "lineage that every detach records" is false for a squash of a duplicate (OWN-2). Attached duplicates are reported unchanged and never rebuilt (OWN-3).
- **F20 / R5 / D56:** the timeout answer needs a free owner thread (OWN-5). Review 9's deferral of the slow-database test relies on this wrong assumption.
- **F81:** two edge paths answer wrongly or not at all (lesser findings, and OWN-6).
- **D17 / F8:** the promised `EventHandlingError` fails for rewrites without a new event (OWN-14).
- **D77:** "a retry stores nothing twice" holds, but the retry returns the first copy (OWN-12).
- **D30:** a forgotten `schemaVersion` bump leads to silent corruption (OWN-1).

## 5. Checked and safe
- **Timeout against completion:** exactly one answer. A late result is dropped (446), and a running persist only delays the timer's message.
- **Stray messages:** `RunAgain`, `CommandTimedOut` and `HandlerFinished` for commands already answered do nothing in every state.
- **Futures:** callbacks touch no actor state, and no owner callback calls `sender()`.
- **Idempotency:** a taken key rolls back and returns the stored response (525–530); conflicts return the stored response; copies on two nodes are serialized by the unique index; one key per command means no deadlock.
- **F99 bookkeeping:** `rerunning` is cleared on finish, timeout, back-off and stop, and waiting does not count as an attempt.
- **Serial queue:** the bound counts waiting commands only, and a timeout removes the queued entry.
- **F119 snapshot guard:** it holds, even for an owner that is behind the store.
- **Bad snapshots:** an unreadable or unfitting snapshot falls back to a full replay.
- **Integer arithmetic:** done in `Long` (871, 821).
- **Crash after commit:** the outbox and the poller cover undo, duplication, rewrite and plain events.
- **Permanent delete:** one transaction, with no snapshot of a deleted root.
- **Squash safety:** preconditions are re-checked under the chain lock; a rewrite racing a squash fails safely; an undo past the squash point is refused.
- **Scheduler on two nodes:** the second squash is refused.
- **Passivation:** the bus or the shard keeps messages, and an owner started only to be stopped does not read.
- **Failed restore (D11):** the owner fails fast, and a new owner tries again later.
- **SQL:** parameters are bound throughout these paths.
- **Times:** microsecond precision and UTC.

## 6. Open questions
- **Database timeouts:** Neula's JDBC URL settings (`socketTimeout`, `tcpKeepAlive`) and the role's `statement_timeout` and `lock_timeout`; how its database fails over (OWN-5).
- **Snapshots:**
  - Which contexts use them, and is there a release check that bumps `schemaVersion`?
  - Comparing snapshot restores with full replays on a production copy would show existing damage (OWN-1).
- **Duplicates:**
  - Which types are duplicated, and how many duplicates per base?
  - Is automatic squash on for those types?
  - Do bases hold personal data that state projections of the duplicates show? (OWN-2, OWN-3, OWN-10)
- **Client versions:** does Neula pass client versions into `expectedVersion` (OWN-8)?
- **Handlers:** do handlers copy state into events, and do rewrite-only commands run alongside `RewriteEverywhere`? (OWN-4)
- **Local types across nodes:** are `Local` types read and written through different nodes, and with what snapshot coverage? (OWN-9)
- **Logs:** the retention of error logs, and whether error replies reach end users (OWN-7).
- **Ephemeral buses:** ephemeral types and bus starts per 30 days (OWN-12).
- **Recursion:** handler recursion over deep trees, and whether `jvm-exit-on-fatal-error` is left on (OWN-13).

# Working notes: command bus and ids

> The notes of one area of the risk assessment of 18/10/2026, kept as written. Their ids (BUS-1, …) map to the
> F-numbers of [RISK-ASSESSMENT.md](../RISK-ASSESSMENT.md) (§14), which holds the consolidated findings: where the two
> differ, the report holds. Nothing was run; these are traces from the code, with lesser items not triaged there.

## 1. Examined

- **Read in full:** `core/.../commandhandler/{AggregateCommandBusActor,Owners,Idempotency,CommandResponseState}.scala`, `core/.../uid/{IdPool,IdRange,PostgresUidGenerator,UidGeneratorActor,UidGeneratorSchemaInitializer}.scala`, `api/.../id/*.scala`, `api/.../{Command,CustomCommandResponse}.scala`, and `core/.../aggregate/{AggregateOwner,AggregateRestorer,OutboxWriter}.scala`.
- **Paths followed into other files:**
  - `AggregateContext.scala`: Materialize, MaterializationKey, StoreFirstEvent, SnapshotPolicy, Placement.
  - `PostgresEventStoreState`: persist, deletePublished, deleteAggregatePermanently, read/writeSnapshot, historyGeneration, processEvents.
  - `add_events` in `PostgresEventStoreSchemaInitializer`; `OutboxDeletes`.
  - `EphemeralEventStoreState`, and `MemoryEventStoreState` (persist, localTx).
  - `MessageSize`, `ClusterSerializer` (limit, sizeOf), `Members`, `TypesNamesState.idFromDB`.
  - `SagaRunner`/`SagaDefinition` (how step keys are made).
  - util (`ActorLogging`, `BlockingDispatcher`, `SchemaLock`), metrics, core `reference.conf`.
  - The v0.12.47 uid initializer, generator and response state (`git show`).
- **Docs:**
  - CLAUDE.md; ARCHITECTURE §2.4–2.7, §3, §4.1–4.4, §4.7–4.10 and §13.5.
  - KNOWN-ISSUES: the titles, D39 and D77. The archive: F9, F23, F46, F64, F70, F74, F75, F78, F100, F101, F107, F120, F131, F143–F145 and F167.
  - SCALING.md; book chapters 5, 12, 17 and 20; design M6 (the ephemeral part).
- **Not examined:**
  - EventsBusActor beyond its ack contract, and the projection version caches beyond their entry points.
  - The cluster serializer's codec, and SpaceImport/Export.
  - Nothing was run (no sbt, no tests).

## 2. Findings

### BUS-1 — An aggregate id of another type is restored by the wrong context, which then writes into it: another type's history and snapshot are corrupted
- **Severity:** High. **Likelihood:** Possible. **Confidence:** Confirmed (code) that no layer checks the type. That the restore succeeds through a snapshot is Strongly suspected: it depends on mpjsons tolerating unknown fields.
- **Location:**
  - `AggregateCommandBusActor.scala:197-203,212`: the bus routes any id.
  - `AggregateRestorer.scala:40-49`.
  - `PostgresEventStoreState.scala:225-228`: `readSnapshot` reads the stored JSON as the class of the type asking. The snapshot row records no type.
  - `PostgresEventStoreSchemaInitializer.scala:281`: `UPDATE aggregates ... WHERE id = _aggregate_id AND base_id = _aggregate_id`, with no `type_id` check.
  - `AggregateOwner.scala:425-426,647`: the owner checks only that the version is not 0 and the root is not null.
- **Preconditions:**
  - The id of type B's aggregate X reaches type A's bus. Causes: an unvalidated end-user id, or a host or saga bug that mixes ids up.
  - A's restore of X succeeds, in one of two ways:
    - (a) A and B take snapshots with equal context `version` and `schemaVersion` (default 1), and X has no event after its snapshot. That is the usual case after a passivation, as `onPassivation = true` by default.
    - (b) A's event handlers are total (they have a default case).
- **Triggering scenario:** an A `ConcurrentCommand` with `aggregateId = X`, which needs no version. A `Command` works as well with X's version, which `AggregateConcurrentModificationError.was` reveals.
- **Failure mechanism:**
  1. A's owner restores X from B's snapshot with `mpjsons.deserialize[A](B's JSON)`. Missing fields read as null or zero (book 17-cluster.md:391-394), so the restore succeeds at X's version.
  2. A's handler runs, and `add_events(..., X, v, typeId(A), …)` appends A's events to X. The `event_keys` rows say type A, and the events are published as type A.
  3. At passivation the A owner writes A's state as X's snapshot: `writeSnapshot` checks only the version and the generation.
- **Expected impact:**
  - B's X either never restores again, or restores silently from the A-shaped snapshot to a state of nulls, and keeps taking commands on it.
  - A's projections ask for X's gap and receive B's events as type A. Their listeners fail; the failures are recorded and then block X's squash and permanent delete (D51).
  - `GetAggregate(X)` through A's bus returns B's fields that share a name with A's: a read across types.
  - Nothing fails when the bad events are written, so it is hard to detect.
- **Validation:**
  1. Take two contexts with snapshots and `version = 1`. Create B's X and passivate it.
  2. Send A's bus `GetAggregate(X)`: expected a failure, observed a Success.
  3. Send an A `ConcurrentCommand(X)`: it is stored.
  4. Restore X through B: it fails, or comes back as nulls.
- **Recommended fix:**
  1. The restore reads `type_id` with `history_generation` (the same row) and refuses a mismatch, naming both types.
  2. `add_events` and `add_undo_event` add `AND type_id = _aggregate_type_id`, with a distinct error when the row exists under another type.
  3. Snapshots store the state class and refuse a mismatch.
  - **Trade-off:** existing rows' type ids must match `AggregateType.of[R]` (the D75 renames). Check that first with `StoredDataCheck`, or run the check log-only for a while.
- **Regression test:** `WrongTypeIdSpec`. A query, a concurrent command and a squash request sent to the wrong bus are all refused, and X is unchanged.

### BUS-2 — A materialization that times out lets the draft change again; the next Materialize returns the older stored state and drops the draft with its newer edits
- **Severity:** High. **Likelihood:** Possible. **Confidence:** Confirmed (code).
- **Location:**
  - `AggregateOwner.scala:747`: `Timeout(commandTimeout)`.
  - `AggregateOwner.scala:754,760`: `materializing = None`, and a `Throwable` is answered `MaterializeRefused`.
  - `AggregateOwner.scala:330`: commands are rejected only while `materializing` is set.
  - `AggregateOwner.scala:402-404`: a stored response is answered before anything runs.
  - `AggregateContext.scala:90-92`: the key is the ephemeral id only.
- **Preconditions:** the persistent side stores `StoreFirstEvent` more than `commandTimeout` (60 s) after it was sent. Causes:
  - a Sharded persistent type's hand-over during a rolling restart: up to 90 s (SCALING §1–2);
  - a late id pool;
  - a saturated blocking dispatcher;
  - a slow database.
- **Triggering scenario:**
  1. Materialize the draft at version 5. No answer comes within 60 s, so the caller gets `MaterializeRefused` and the draft takes commands again.
  2. The user edits it to version 7.
  3. The first `StoreFirstEvent` commits later as P1, with the state of version 5.
  4. The user materializes again.
- **Failure mechanism:**
  - The second `StoreFirstEvent` finds the key's stored `SuccessResponse(P1)`, or loses the race on the key.
  - The owner then sends `EphemeralMaterialized`, which drops the draft, and answers `Materialized(id, P1)`.
- **Expected impact:**
  - The edits made after the first attempt are silently lost.
  - The book promises the opposite (20-ephemeral-aggregates.md:76-77): "what is stored is exactly what the caller saw".
  - The user cannot see it, and nothing is logged beyond the earlier refusal.
- **Validation:**
  1. Make `persistentBus` a probe that holds the first `StoreFirstEvent` past the timeout, then forwards it to the real bus.
  2. Add an item to the draft.
  3. Materialize again, forwarding to the real bus.
  4. The stored aggregate lacks the item, and the draft is gone.
- **Recommended fix:**
  - After an unknown outcome, keep the draft frozen (commands get `CommandRejected`). The caller retries Materialize with the same key until a definite answer comes.
  - Alternatively, key by (id, version), and refuse without dropping the draft when the stored response is for another version.
  - **Trade-off:** the draft stays read-only while the persistent side is unreachable.
- **Regression test:** the validation above, plus the race where both `StoreFirstEvent`s are in flight at once.

### BUS-3 — Ephemeral buses call the context's OutboxWriter with NoSession, on an in-memory store that never rolls back
- **Severity:** High where the writer has side effects; Medium where the drafts only get stuck. **Likelihood:** Possible. **Confidence:** Confirmed (code). The exact exception NoSession raises is Strongly suspected.
- **Location:**
  - `AggregateCommandBusActor.scala:81-84,150-151`: the ephemeral bus builds `OwnerSettings` from the same context.
  - `AggregateOwner.scala:121-124`: the writer comes from any context that mixes in `WithOutboxWriter`.
  - `AggregateOwner.scala:494-505,537-540`.
  - `MemoryEventStoreState.scala:455-457`: `localTx` is just `block(NoSession)`.
  - `MemoryEventStoreState.scala:45-47`: a version mismatch is an `IllegalStateException`, which never triggers a reload.
  - The design says "no outbox writer" (`docs/design/M6-organizations.md:489,532`).
- **Preconditions:** a `WithOutboxWriter` context (R12) that also runs on `AggregateCommandBusActor.ephemeral` (R16).
- **Triggering scenario:** any draft command whose events the writer handles.
- **Failure mechanism:** `persist` applies the events in memory, then `write(...)(NoSession)` runs:
  - **(a) The writer uses the session, as it should:**
    - it fails, and the caller gets `EventHandlingError`;
    - the events stay in the draft, and the owner's version does not move;
    - every later command fails with "Incorrect version" → `EventHandlingError`. This applies on a node that is alone, or for a Sharded context, until the owner is passivated; then the "failed" changes appear.
  - **(b) The writer ignores the session:** host rows (jobs, timers, messages to send) are written for drafts with negative ids.
  - **The same missing rollback for idempotency keys:** a keyed copy that loses the key race also keeps its events. That leaves orphan drafts that count toward `maxAggregates` for the idle time.
- **Expected impact:** drafts that cannot be used, answers that lie about what happened, or real side effects for drafts.
- **Validation:**
  1. Run `ephemeral(new CartsWithJobs, …)` (the context from OutboxHookSpec).
  2. Create a draft, then AddItem twice: each gets `EventHandlingError`.
  3. `PassivateAggregate`, then `GetAggregate`: the first item is there.
- **Recommended fix:**
  - Give ephemeral owners `outboxWriter = None`.
  - Make the in-memory `localTx` restore the aggregate's entries on an exception, or record the idempotency key before persisting.
- **Regression test:** the validation above, with both writer styles; a race of keyed creations leaves one draft.

### BUS-4 — One node with a too-high installation number moves the shared aggregate sequence; every correct node then fails all creations and refuses to restart
- **Severity:** High. **Likelihood:** Unlikely. **Confidence:** Confirmed (code).
- **Location:**
  - `PostgresUidGenerator.scala:46-54`: `if (next < r.from) SELECT setval(..., r.from, false)`.
  - `PostgresUidGenerator.scala:40-41`: a pool past the range's end is refused.
  - `AggregateCommandBusActor.scala:368,374-391`.
- **Preconditions:** a node starts against the same schema with an `installation-number` higher than the installation's, for example from a copied config.
- **Failure mechanism:**
  1. The wrong node moves the sequence to N×10^12.
  2. On every correct node, the next `NEXTVAL` lies past `until`. The result is `IllegalStateException` → `IdsNotReceived`, and every unchosen-id FirstCommand and every `ReserveAggregateId` fails.
  3. Each retry still consumes 10 000 ids, so the sequence runs on through range N.
  4. A correct node that restarts refuses to start, with "…past installation 0's range…: is the installation number right?". The message blames the correct configuration.
  - ARCHITECTURE §13.5 says "a wrong number fails the host as it starts". That holds only for a number that is too low.
- **Expected impact:**
  - Aggregate creation stops across the installation.
  - Nodes cannot restart until a DBA runs `setval`.
  - The wrong node's aggregates carry another installation's ids.
- **Validation:**
  1. Make generator(0) and draw a pool.
  2. Construct generator(3): it succeeds.
  3. `generator(0).nextIdsPool` now throws, and a new generator(0) throws as well.
- **Recommended fix:**
  - Record the installation number in the database at the first move, and refuse any node whose number differs.
  - Move the sequence only once, out of the legacy range.
  - **Trade-off:** renumbering an installation becomes an explicit operation.
- **Regression test:** a wrong node fails at start, and the sequence is left as it was.

### BUS-5 — Ephemeral id tokens repeat across starts, so materialization keys collide within the response retention
- **Severity:** High. **Likelihood:** Unlikely. **Confidence:** Confirmed (code); the probability below is estimated.
- **Location:**
  - `AggregateCommandBusActor.scala:147`: `1 + Random.nextInt(2^29-1)`.
  - `AggregateCommandBusActor.scala:170`: ids follow the token from the block's top, so for a given token the first draft of every start gets the same id.
  - `AggregateContext.scala:90-92`: the key is `"materialize|" + id`, with no type and no start.
  - Retention is 30 days; the stored response is answered at `AggregateOwner.scala:402-404,753-759`.
- **Failure mechanism:**
  - Any two starts (any node, any type) that draw the same token within the retention hand out the same ids.
  - Materializing id X in the later start returns the earlier start's `SuccessResponse(P_old)`. The draft is dropped, and the caller is told `Materialized(X, P_old)`: an aggregate of another draft, possibly another type or another organization.
  - Two nodes running at once with the same token also accept each other's misrouted draft commands.
- **Probability (estimated, birthday bound):** P ≈ S²/(2·2^29) for S starts within 30 days:
  - S = 1 000 → about 0.09 %;
  - S = 5 000 → about 2.3 %;
  - S = 10 000 → about 9 %.
  - A host that calls `Random.setSeed` makes a collision certain.
- **Expected impact:** a silently lost draft, and a pointer to someone else's data.
- **Validation:** force the same token on two ephemeral buses (test hook), create a draft on each, and materialize both. The second is answered with the first's id.
- **Recommended fix:** draw a 128-bit random per start (SecureRandom or a UUID), and put it and the type in `MaterializationKey`. There is no compatibility cost, as drafts never survive a restart.
- **Regression test:** the validation above.

### BUS-6 — The pool size is cached at start, so any lowering of the sequence step hands out the same ids twice
- **Severity:** High when triggered. **Likelihood:** Unlikely (latent). **Confidence:** Confirmed (code).
- **Location:**
  - `PostgresUidGenerator.scala:27`: `poolSize` is read once.
  - `PostgresUidGenerator.scala:34-43`.
  - `UidGeneratorSchemaInitializer.scala:26-29`: `ALTER … INCREMENT BY 10000` runs unconditionally at every start.
- **Failure mechanism:**
  - A pool is [v, v + cachedSize). Once the step is lowered to s < cachedSize, another node's `NEXTVAL` returns v + s, inside that pool.
  - F23's claim "no id is given twice, also with a node still on the old size" holds only when the step grows.
- **Triggers:**
  - a DBA lowers the step;
  - a later version with larger pools is rolled back to 1.0;
  - a later version with smaller pools runs next to 1.0 nodes.
- **Expected impact:**
  - **Duplicate aggregate ids:** creations fail with conflicts.
  - **Duplicate command ids:** the owner's `pending` map collides (`AggregateOwner.scala:341,446`). One caller gets another command's answer, and the other gets none.
  - **Duplicate saga ids:** `"sagaId|step"` keys collide, so a step's command is answered with another saga's response and not applied.
- **Validation:** take two generators sized 10 000, run `ALTER … INCREMENT BY 5000`, draw pools alternately, and check for overlap.
- **Recommended fix:**
  - The initializer never lowers the step (`GREATEST(current, 10000)`).
  - Read the step with each `NEXTVAL` and use the smaller of it and the cached size.
  - **Trade-off:** one catalog read per pool.
- **Regression test:** the validation above, expecting disjoint pools.

### BUS-7 — The Local sweep passivates owners in unpaced bursts, and each stop writes a snapshot on the shared blocking dispatcher, ahead of every owner's work
- **Severity:** Medium. **Likelihood:** Possible: Local types with snapshots (Local is the default placement, and `onPassivation = true` is the default). **Confidence:** Strongly suspected; the mechanism is confirmed, the size is estimated.
- **Location:**
  - `Owners.scala:104-116`: every 5 minutes, everything idle for more than 2 minutes beyond the 200 most recent owners is stopped in one loop.
  - `AggregateOwner.scala:676-679`: a stopping owner writes a snapshot when `version > snapshotVersion`. That includes owners only read: after a restore from events, `snapshotVersion` is 0.
  - Owners run on the 32-thread blocking dispatcher, whose queue is unbounded and first-in first-out.
- **Triggering scenario:** about 20 000 aggregates of one type touched within 5 minutes on a node.
- **Failure mechanism:**
  - One message triggers the sweep, which sends N `StopOwner` messages, which queue N snapshot upserts (serialize, then write on a pooled connection).
  - Every command of every type on the node waits behind them.
  - Estimated: N × 1–3 ms / 32 ≈ 0.6–1.9 s for N = 20 000 (not measured). Id-pool refills queue behind them too (lesser finding 2).
- **Expected impact:** a latency spike every 5 minutes, client timeouts at large N, and snapshot churn when `keepAliveLimit` is small.
- **Validation:** a bench with a Local snapshot type and 20 000 owners. Trigger the sweep and measure the p99 of another type's commands for the next 10 s.
- **Recommended fix:**
  - Pace the sweep: stop at most K owners per tick.
  - Skip passivation snapshots below a delta of events, or write them on a separate small pool.
  - Raise the default `keepAliveLimit`.
- **Regression test:** a sweep stops at most K owners at once, plus the bench's p99 bound.

### BUS-8 — An ephemeral bus keeps every idempotent response in memory for 30 days, whatever happens to its drafts
- **Severity:** Medium. **Likelihood:** Possible, when draft commands carry keys. **Confidence:** Confirmed (code).
- **Location:**
  - `AggregateCommandBusActor.scala:82`: `new MemoryCommandResponseState()`, with the default 30 days.
  - `CommandResponseState.scala:33-53`: an immutable map; `aggregateId` is ignored; every hour a `partition` of the whole map runs under the lock.
  - `AggregateCommandBusActor.scala:299-304`: dropping a draft leaves its responses.
- **Failure mechanism:**
  - Each keyed draft command adds an entry kept for 30 days, though the response is useless once the draft is dropped or materialized.
  - `maxAggregates` does not bound these entries.
- **Expected impact (estimated, about 250 bytes per entry):**
  - 1 keyed command/s → about 650 MB after 30 days; 10/s → about 6.5 GB.
  - The hourly O(n) partition holds the lock that every keyed draft command needs.
- **Validation:** send N keyed draft commands, drop the drafts, and check the state's size after the idle time.
- **Recommended fix:**
  - Set the retention to the bus's `idleTime`, or remove a draft's entries in `dropEphemeral`.
  - Use a mutable map with an expiry queue.
- **Regression test:** the entries go when their draft goes.

### BUS-9 — With `large-message-destinations` set, the command size check uses the large frame for every destination
- **Severity:** Medium. **Likelihood:** Possible: SCALING §6–7 suggests that setting. **Confidence:** Strongly suspected: the drop of oversized ordinary messages is Pekko's, as recorded in F121.
- **Location:**
  - `MessageSize.scala:16-21`: `max(frame, largeFrame) − 8 KiB`.
  - `AggregateCommandBusActor.scala:326-334`.
  - `ClusterSerializer.scala:72,80`: the same limit for the too-large marker.
- **Failure mechanism:**
  - A Sharded command between about 248 KiB and about 2 MiB passes the check and is written whole.
  - Sent to an owner path that is not a large destination, it goes over the ordinary stream, and Artery drops it. F121's symptom returns: no answer, and every retry is dropped too.
- **Expected impact:** the command can never run while its owner is on another node. It looks like a hang.
- **Validation:** in a TestCluster with large destinations set to another path, send a 400 KiB command to a remote owner. It times out, and Artery logs "Discarding oversized payload".
- **Recommended fix:** check commands, and the serializer's marker, against `maximum-frame-size`; allow the large frame only for listed paths.
- **Regression test:** the validation above.

### BUS-10 — On an ephemeral bus, a FirstCommand queued while ids were left, but served after they ran out, blocks every later command for good
- **Severity:** Medium. **Likelihood:** Unlikely. **Confidence:** Confirmed (code).
- **Location:** `AggregateCommandBusActor.scala`:
  - 242-247 and 258-259: ids are checked on arrival only;
  - 336-337 and 347: `serveWaiting` stops at the head of the queue;
  - 368: an ephemeral bus never asks for an aggregate pool, so no `IdsNotReceived(true)` ever comes.
- **Triggering scenario:** k ids are left in the 2^24 block, the command-id pool is momentarily empty, and more than k FirstCommands arrive.
- **Failure mechanism:**
  - FirstCommand k+1 sits at the head of `waitingForIds` forever.
  - All later commands queue behind it.
- **Expected impact:** every command on every draft of the bus goes unanswered until the node restarts, and the queue grows meanwhile.
- **Validation:** with a small block (test hook) and a delayed command pool, send k+1 FirstCommands, then a command to an existing draft.
- **Recommended fix:** answer such a FirstCommand `CommandRejected` and drop it from the queue.
- **Regression test:** the validation above.

### BUS-11 — An ephemeral bus that restarts loses its drafts and then refuses every new draft until the node restarts
- **Severity:** Medium. **Likelihood:** Unlikely. **Confidence:** Confirmed (code).
- **Location:** `AggregateCommandBusActor.scala`:
  - 81-84: `new EphemeralEventStoreState` inside the Props creator;
  - 147: a new token;
  - 133: an empty `IdPool`;
  - 169-172: the id block is received only in `preStart`;
  - 187-189: `postRestart` skips `preStart`;
  - 258-259: the refusal "ids are spent; a restart brings new ones".
- **Triggering scenario:** any exception in the bus's receive under default supervision. Examples: a host `Metrics` implementation that throws in `dropEphemeral` (line 303), or `InvalidActorNameException`.
- **Expected impact:**
  - Every draft is answered `EphemeralAggregateLost`.
  - Every new draft is rejected, with a message that blames a restart, though a restart caused this.
- **Validation:** use a metrics stub that throws once on `reactivecqrs.ephemeral.dropped`, then try to create a draft.
- **Recommended fix:**
  - Never let the bus throw: catch inside receive, in the style of F70.
  - Redo the ephemeral setup in `postRestart`.
- **Regression test:** the validation above.

## 3. Lesser findings

1. `Owners.scala:104-116`, `AggregateOwner.scala:684`: an owner with a command at work longer than 2 minutes (SCALING §8 advises long `commandTimeout`s) is passivated as idle, and every message for it, queries included, is held until the command ends. Any passivation also fails waiting min-version queries before their deadline. Pekko's idle passivation does the same for Sharded types.
2. `UidGeneratorActor.scala:53-54`, `IdPool.scala:34-38`, `AggregateCommandBusActor.scala:245`: pool refills run on the shared blocking dispatcher. Under load, the command pool (fetched with 2 000 left) can run dry before its `NEXTVAL` runs, and then every command of the type on the node waits in line (Strongly suspected).
3. `CommandResponseState.scala:172-175`, `AggregateCommandBusActor.scala:176`: each bus deletes expired responses of every type with its own state's retention, so the shortest retention among the states wins. It is one unbatched DELETE.
4. `CommandResponseState.scala:81,136-141`: a key longer than 256 characters fails at the INSERT, after the handler ran, as an `EventHandlingError` naming the event class. Nothing checks the key earlier.
5. `CommandResponseState.scala:144-170`: the response info is checked when written, never read back. Info that cannot be read makes every later copy fail ("key taken but no stored response") although the command succeeded (Hypothesis).
6. `AggregateOwner.scala:572`: when the response lookup in `conflict()` fails transiently, a copy whose original succeeded is answered `AggregateConcurrentModificationError`.
7. `AggregateCommandBusActor.scala:206,394-404`: historical reads have no bound. Each holds a blocking thread, a connection and its whole result in memory, and `GetArchivedEvents` is missing from SCALING.
8. `Owners.scala:83`: a root class whose simple name has a non-ASCII letter makes the actor name invalid. `InvalidActorNameException` is thrown in the bus at every new owner, so the bus restarts each time.
9. `ActorLogging.scala:25`: if the bus throws, the error log carries the message's full `toString`, which can hold a command's personal data (GDPR).
10. `AggregateCommandBusActor.scala:148,361,308-314`: every routed ephemeral FirstCommand, failed ones included, takes an id and a `lastUsed` entry for 12 h. At 10 creations/s the 2^24 block lasts about 19 days (estimate: 2^24 / 10 / 86 400).
11. `AggregateOwner.scala:117,196,265`: owners of an ephemeral bus of a `Sharded` context believe they are sharded. After a failed restore they send `ShardRegion.Passivate` to the bus, which answers `Status.Failure`, so the owner stays failed while it is used.
12. `AggregateCommandBusActor.scala:427-447` with `PostgresEventStoreState.scala:490-494`: a permanent delete that is blocked restores every duplicate to build its detach event before the cheap failed-subscription check, and does so at every re-publication of the delete event.
13. `IdRange.scala:24-28`: a chosen legacy id ahead of the sequence is allowed, so a later pool id collides with it and that creation gets a conflict. A conflict also reveals another organization's aggregate version (`was`).
14. `AggregateCommandBusActor.scala:318-322`: the chosen id of a permanently deleted aggregate is accepted; no tombstone exists. Projection caches may then take the new aggregate's first versions for duplicates (Hypothesis).
15. `AggregateCommandBusActor.scala:171,176`, `Members.scala:40-43`: the schedules are never cancelled, and `track` appends the context again at every bus construction. These leak only for buses created many times.
16. `Owners.scala:153`: the first `ClusterSharding.start` blocks the bus constructor on the default dispatcher while Pekko waits for its guardian.
17. `TypesNamesState.scala:82-93`, reached through `CommandResponseState.scala:131-133,146`: a new response or info class registers its name on a second pooled connection while the persist transaction holds one. With the pool saturated, it waits out the pool's borrow timeout (another area's code).
18. `AggregateOwner.scala:612-615`: a `CustomSuccessResponse` too large for the frame reaches the caller as `MessageTooLargeException` although the command was applied. A retry without a key applies it twice.

## 4. Known, re-checked

- **F46** (archive, "Fixed in M5 for sharded types"): Local is still the default placement. The full sort remains, and the stop it triggers is not paced (BUS-7).
- **F145 / F100 / F101:** on a node that is alone, and for sharded owners, `caughtUp()` is skipped (`AggregateOwner.scala:318`). The stale owner is stopped only by an asynchronous `HistoryChanged`/`DeletedPermanently` (`AggregateCommandBusActor.scala:222-225`). Commands already in a duplicate owner's mailbox still run on the state from before a state-changing squash, detach or rewrite of its base, and the version check misses it because the version did not change.
- **F23:** "no id is given twice, also with a node on the old size" holds only when the step grows (BUS-6).
- **F121:** the limit is `max(frame, large frame)` for every destination once large destinations are set (BUS-9).
- **F167:** deleting a deleted aggregate's responses also removes their deduplication. A later copy of the keyed creating command (a client retry, or a saga step run again) re-creates the aggregate. A tombstone that keeps the key and clears the info would keep both properties.
- **D39 / ARCHITECTURE §13.5:** "a wrong number fails the host as it starts" holds only for a number that is too low (BUS-4).
- **D77, design M6 and book chapter 20:** "no outbox writer" is not implemented (BUS-3). "What is stored is exactly what the caller saw" fails after a timeout (BUS-2). Token collisions are considered for routing only, not for the response keys (BUS-5).

## 5. Checked and safe

- **Watched actors:** every `Terminated` the bus can get is handled (`Owners.scala:79,139`), so no `DeathPactException`.
- **Actor state off the actor thread:** `sender()` is captured before every Future. Callbacks touch only immutable vals, `self` and the thread-safe log, and `context` is used only on the actor thread.
- **IdPool:** one request per kind is outstanding at a time. A reply after the 60 s ask timeout goes to dead letters: a gap in ids, never reuse. The spare-pool logic is correct.
- **Number limits:** the IdRange arithmetic and ephemeral ids stay at or below 2^53−1. Command and saga sequences refuse to pass 2^53. No overflow.
- **A chosen id that already exists** (any type or space): `add_events` with expected version 0 raises, so nothing is written. F120 covers the race on the primary key.
- **The same key on two nodes at once:** `ON CONFLICT DO NOTHING` waits for the other transaction; the loser rolls back and returns the stored response.
- **Indexes:** key lookups use the unique index, and expiry uses the timestamp index (a timestamp compared with a timestamptz can still use it).
- **Outbox deletes:** a failed delete, or an ack sent to a node that restarted, leaves the row for the poller to re-publish (at least once).
- **Order across passivation:** per-aggregate order is kept, and messages kept for a stopping owner go to the next one after `Terminated`.
- **Command routing:** wrong-type commands end in `CommandHandlingError`. `spawnedCount` overflow is harmless, since actor names include the id.
- **Persistent bus restart:** the pools are asked for again on the next message, and the shard region is reused.
- **SQL interpolation:** only constant sequence names are interpolated; responses use bound parameters.
- **Ephemeral sweep:** it is never shorter than the command timeout.
- **Saga keys:** revert steps use new step numbers (`SagaRunner.scala:191,195,200`). One command per step and handlers without side effects are both documented.

## 6. Open questions

- Does mpjsons 0.7 ignore unknown JSON fields? The answer decides how likely BUS-1's snapshot route is.
- Which Neula types take snapshots, and with which `version` and `schemaVersion`? Do host endpoints and sagas check an id's type before they send a command?
- Does any Neula `WithOutboxWriter` context also run on an ephemeral bus? Do draft commands carry keys?
- How many ephemeral bus starts happen within 30 days (BUS-5)? What retention does each response state use (lesser finding 3)?
- How does Pekko 1.7's Artery behave with an oversized ordinary message when large destinations are set? A cluster test would settle BUS-9.
- How many owners per type and node does one sweep stop, and how long does a snapshot write take? A bench would settle BUS-7's size. Which types run Local in production?

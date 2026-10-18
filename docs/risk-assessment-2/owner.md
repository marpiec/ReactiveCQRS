# Working notes: aggregate owner (second risk assessment)

| | |
|---|---|
| Area | The aggregate owner (prefix R2-OWN) |
| Scope | `core/.../aggregate/*` (`AggregateOwner`, `AggregateRestorer`, `StateShape`, `SquashScheduler`, `OutboxWriter`), `api/.../AggregateContext.scala`, and the owner's calls into the event store (`PostgresEventStoreState`, the procedures, `MemoryEventStoreState`), the command bus (`AggregateCommandBusActor`, `Owners`, `Idempotency`) and `Members`; mpjsons 0.7.0 and Pekko 1.7.1 sources read where a finding depends on them |
| Date | 18/10/2026 |
| Commit | `2453711` (master, 1.0.1-SNAPSHOT); the fixes since `v1.0.0` read with `git diff v1.0.0..HEAD` |
| Method | Static reading only: nothing was run. Numbers are estimated unless a source is named. |

Already known and not repeated: the first assessment's OWN-1…OWN-15 (F196, F193, F194, F195, F186, F231, F191, F213,
F214, F215, F253, F208/F209, F250, F265, F251) and its lesser items, and the open F215, F270, F274.

## Findings

### R2-OWN-1 — A command whose events put an undo before another event always fails since F195
- **Severity** High (where a context does it) · **Likelihood** Possible · **Confidence** Confirmed (code)
- **Where:** `AggregateOwner.persist` `AggregateOwner.scala:539-540` (one `expectedGeneration` for the whole command);
  `PostgresEventStoreState.persist` 78-84 and `persistOne` 141-143 (each event after an undo goes through
  `persistPlain` with that generation); `add_undo_event` `PostgresEventStoreSchemaInitializer.scala:391` (raises the
  generation); `add_events` 334-336 (compares it). The memory store checks once before and raises after
  (`MemoryEventStoreState.scala:51-53, 82`), so it accepts what PostgreSQL refuses.
- **Scenario:** a command handler returns `CommandSuccess(Seq(ChangesUndone(2), NoteAdded(...)))` ("undo the last two
  changes and record why", or "undo and apply the corrected value"). This worked in 1.0.0.
- **Mechanism:** in one transaction `add_undo_event` raises `history_generation` to g+1; the next event's `add_events`
  call carries the command's g and raises "Concurrent aggregate modification exception … its history changed". The owner
  takes it as another writer (`movedOnWithoutThisOwner`, 573-579), restores, and answers a conflict; a
  `ConcurrentCommand` runs again up to 10 times, each time with a full restore, and fails the same way every time.
- **Impact:** a whole class of commands never succeeds after the upgrade to 1.0.1; each attempt costs a restore. No data
  is damaged. Tests pass: no spec emits an undo followed by another event, and the memory store does not reproduce it.
- **Validation:** on PostgreSQL, a command returning `[UndoEvent(1), PlainEvent]`: 1.0.0 stores both; `2453711`
  answers `AggregateConcurrentModificationError` (a `ConcurrentCommand`: `CommandHandlingError`/conflict after 10 tries).
- **Suggested fix:** pass the expected generation only to the first procedure call of the command (or to none after an
  undo: `persistOne` clears it once an undo is stored), or compare the generation once, before the first event, in
  SQL. Make `MemoryEventStoreState` check per event as the procedures do (F274).
- **Regression test:** `PostgresEventStoreStateSpec` and an owner spec on PostgreSQL: `[Undo, Plain]` and
  `[Plain, Undo, Plain]` are stored with consecutive versions; a rewrite on another connection between read and commit
  still refuses them.
- **Continues:** F195 (the fix introduced it).

### R2-OWN-2 — A materialization answered `CommandHandlingError` is taken as refused, so the F208 data loss comes back through F253
- **Severity** Medium · **Likelihood** Unlikely · **Confidence** Confirmed (code)
- **Where:** `AggregateOwner.materializeAnswered` `AggregateOwner.scala:842-844` (`case other`: the draft is unfrozen);
  the persistent owner answers `CommandHandlingError` for a commit of unknown outcome (F253, 580-587).
- **Scenario:** the persistent owner's connection drops around the COMMIT of the `StoreFirstEvent`; the commit lands.
  The ephemeral owner receives `CommandHandlingError`, sets `materializing = None` and tells the caller
  `MaterializeRefused`. The user edits the draft to S2 and materializes again: the same key returns the stored
  `SuccessResponse(P1)`, so the caller gets `Materialized(P1)` with the state S1, and the draft with S2 is dropped.
- **Mechanism:** F208's fix treats only an ask failure (a `Throwable`) as "outcome unknown"; since F253 the persistent
  side says "unknown" with `CommandHandlingError`, which the ephemeral owner treats as a definite answer.
- **Impact:** silent loss of the edits made after the first attempt, the case F208 closed.
- **Validation:** a persistent store that commits and then throws a `SQLException` with SQLState 08006: the draft takes
  commands again, and the second `Materialize` returns the first state.
- **Suggested fix:** treat `CommandHandlingError` like a missing answer (keep frozen, re-send with the same key; the key
  makes it safe); only `CommandRejected`, `FailureResponse` and `EventHandlingError` end the materialization as refused.
- **Regression test:** as Validation: the draft stays frozen and the final persistent state is the frozen one.
- **Continues:** F208, F253.

### R2-OWN-3 — The snapshot shape ignores the subclasses a state holds through a trait, so a field added to one is read as zero, false or null
- **Severity** High · **Likelihood** Possible · **Confidence** Confirmed (code; mpjsons 0.7.0 source)
- **Where:** `StateShape.describe` `StateShape.scala:33-47`: a field is visited by its declared type; an interface or
  abstract class has no fields, so `Discount{}` is all a `discount: Discount` contributes. mpjsons serializes such a
  field through `markTypedClass` / `registerSuperclassConverter` with the concrete class (`MPJsons.scala:355-365`), and
  a missing key of that concrete class reads as zero/false/null (`BeanDeserializer`, first assessment OWN-1).
- **Scenario:** the state holds `status: OrderStatus` (a sealed trait marked typed) with `case class Cancelled(reason:
  String)`. A release adds `Cancelled.refund: BigDecimal`. The digest of the root is unchanged, every snapshot is still
  used, and restored states carry `refund = null`; handlers decide on it and the next snapshot keeps it.
- **Mechanism:** F196's fix covers the declared type graph only; ADTs are common in states, and are exactly where
  fields get added.
- **Impact:** as F196: wrong decisions stored as events, silently, only on the snapshot path.
- **Validation:** `StateShapeSpec`: change a field of a subclass of a trait-typed field; the shape stays equal.
- **Suggested fix:** compute the shape from what was written: walk the state object at `writeSnapshot`, collect the
  runtime classes reached, and store their names with the digest; at read, recompute the digest of those classes. Or,
  for sealed traits, include `knownDirectSubclasses` (Scala reflection, D75 permits it until mpjsons goes).
- **Regression test:** a snapshot holding a subclass that later gains a field is not used; the state equals a replay.
- **Continues:** F196.

### R2-OWN-4 — A plain squash keeps the history generation: an owner or snapshot below the squash point is brought forward through the squash event, and historical reads below it are answered from a stale snapshot
- **Severity** Medium · **Likelihood** Possible (stale snapshots and reads); Unlikely (wrong state) · **Confidence** Confirmed (code)
- **Where:** `AggregateOwner.caughtUp` 343-345 (incremental only on the generation; F214's recommended "no squash point
  above the owner's version" check is missing); `PostgresEventStoreState.squash` 846-847 (a plain squash deletes the
  snapshots ≤ n and leaves the generation); `writeSnapshot` 255-260 (guard: `base_version >= v` and the generation, no
  squash point, and `ON CONFLICT DO UPDATE` without `version < EXCLUDED.version`); `AggregateRestorer.usableSnapshot`
  93-94 and `applyAfter` 77-85 (`readAndProcessOwnEvents` does not check the squash point, unlike `processEvents` 218-222).
- **Scenario (Local type, several nodes, squash on):** node B's owner holds X at v = 40. The squash scheduler squashes X
  to n = 100 on node A. (a) A read on B: same generation, so B applies the own events after 40: the squash event at
  100, then 101…, to its state at 40. (b) B's owner is swept and writes its passivation snapshot at 40 (the guard
  passes; it also overwrites A's newer snapshot). Every later restore starts from 40 the same way, and
  `GetAggregateForVersion(70)` returns the snapshot's state labelled version 40 instead of `AggregateSquashedException`.
- **Mechanism:** D85 checks the squash event's handler on the initial state only; the code assumes it ignores its input.
  A handler written as `state.copy(...)` or that accumulates (`count = state.count + e.count`) passes D85 and gives a
  wrong state here.
- **Impact:** (a)/(b) with such a handler: a wrong state used for decisions and snapshots; with any handler: D51
  violated for historical reads, and a newer snapshot replaced by an older one (more replay after every sweep).
- **Validation:** two `Local` nodes: B reads at 40, A writes to 120 and squashes to 100, B reads again; with a
  `state.copy` squash handler the states differ from a replay. Stop B's owner and read version 70.
- **Suggested fix:** return `squashed_version` with `storedVersion`; catch up incrementally only when it is ≤ the
  owner's version, else restore. In `writeSnapshot`, require `coalesce(squashed_version, 0) <= v` and update only when
  `snapshots.version < EXCLUDED.version`; in `usableSnapshot`, refuse a snapshot below the squash point.
- **Regression test:** as Validation, with an accumulating squash handler: B's state equals a replay; the version-70
  read fails with `AggregateSquashedException`; a stale passivation snapshot does not replace a newer one.
- **Continues:** F214, F237.

### R2-OWN-5 — Conflicts with another node and transient failures still replay the whole aggregate, and a pool timeout doubles the wait
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code); the pool's null SQLState Strongly suspected (commons-dbcp2 behaviour)
- **Where:** `AggregateOwner.persist` 573-579 (`movedOnWithoutThisOwner` → `restore()`) and 580-587 (F253: transient →
  `restore()`); `Errors.transient` `Errors.scala:55` (a null SQLState counts as transient).
- **Scenario A:** two nodes write one hot `Local` aggregate of 10 000 events with `ConcurrentCommand`s. Every lost race
  is a full replay (about 80 ms each, estimated from SCALING §3's 392 ms for 50 000 events, measured), then a back-off
  and an attempt whose catch-up is incremental. F214 fixed the read path; the conflict path was left.
- **Scenario B:** the pool is exhausted. `localTx` fails after the checkout wait (5 s, SCALING §1) with a pool
  `SQLException` without SQLState: "transient", so the owner restores at once, waiting for another checkout; if it gets
  one it replays the whole aggregate, if not it fails its restore and refuses its aggregate for 5 s. Nothing was sent to
  the database, yet the caller is told "outcome unknown".
- **Impact:** throughput collapse under cross-node contention; under pool exhaustion every failing command holds its
  thread twice as long and adds replays, feeding the overload (the restore storm F231 removed from look-ups).
- **Validation:** count `reactivecqrs.restore` (`from=events`) for N cross-node conflicts (N today, expected 0 with
  snapshots off); a pool of 1 held by another transaction: one command waits two checkout timeouts.
- **Suggested fix:** use `caughtUp(always = true)` (incremental) after a conflict and after a transient failure; treat
  a failed checkout (no connection obtained) as a definite "not stored" without a restore.
- **Regression test:** as Validation: no replay from events after a conflict; a checkout failure is answered after one
  wait, with no restore.
- **Continues:** F214, F253 (and F231's restore-storm concern).

### R2-OWN-6 — F250's containment covers command handlers and restores only; other owner paths still let a `StackOverflowError` end the JVM
- **Severity** Medium · **Likelihood** Unlikely · **Confidence** Confirmed (code; Pekko 1.7.1 source: `FaultHandling.handleNonFatalOrInterruptedException` catches `NonFatal` only, and the system's uncaught-exception handler exits the JVM by default)
- **Where:** uncontained calls on the owner's thread: `caughtUp` incremental `AggregateOwner.scala:344` (`Try` does not
  catch it); `squashHistory` 769-779 (`squashEvent`: a restore at `upTo`, the handler on the squash event, its
  serialization in `squash`); `materialize` 810; `persist` 533-546 (mpjsons serializes the events inside `Try`);
  `writeSnapshot` (`AggregateRestorer.scala:110`: serializing the state); `rewriteHistory` 858-863 (the context's
  `historyChanges`). Off the owner: the bus's historical reads in a `Future` (`AggregateCommandBusActor.scala:416-425`).
- **Scenario:** a deeply nested state (a linked structure of case classes) passes event handling but overflows mpjsons'
  recursive serializer: the snapshot written every `every` events, or at passivation, ends the JVM; after the restart
  the sweep's passivation snapshot ends it again. On `Local` types, the incremental catch-up of another node reads the
  poison event uncontained. The squash scheduler, a singleton, repeats a poison squash on each node it moves to.
- **Impact:** node crash loops, the F250 scenario through paths its fix did not reach.
- **Validation:** a state nested 10⁵ deep with snapshots `every = 1`: the JVM exits.
- **Suggested fix:** wrap these calls in `contained` (move it to a shared utility for the bus); set mpjsons' `maxDepth`
  on the library's instances (left open by F250).
- **Regression test:** as Validation, and a squash and a catch-up of such an aggregate: each fails as an ordinary error.
- **Continues:** F250.

## Lesser items
- `StateShape.Unknown` is written into snapshots and matches itself (`StateShape.scala:18,22`, `AggregateRestorer.scala:110`):
  a state class whose shape cannot be computed (a `TypeNotPresentException` from a field's generic type) silently turns the
  F196 guard off; a `GenericSignatureFormatError` (a `LinkageError`) escapes the `Try` and fails every restore uncontained.
- Rewriting an aggregate whose owner is not running restores it twice: the first message's restore
  (`AggregateOwner.scala:224`) and F194's unconditional one (871). An erasure walk pays two full replays per aggregate.
- F194's "tell the duplicates every time" starts, on a `Sharded` type, every duplicate's entity only to passivate it
  (`Owners.scala:129`, `AggregateOwner.scala:870,881-884`): K entity starts per base rewrite, also when nothing changed.
- `Errors.transient` (`Errors.scala:55`) also counts deadlocks (40P01), statement timeouts (57014) and resource errors
  (53xxx) as "outcome unknown", though they rolled back: callers are told to read before retrying, and the owner restores.
- `members.alone` follows gossip (`Members.scala:37,66-72`): a `Local` owner skips the F131 look-up before it learns
  of a joining node, and after a downed node is removed while its process still writes; it then serves stale reads,
  erased values included, until a command conflicts.
- A commit that becomes visible after F253's restore read leaves a sharded or lone owner behind the store;
  `RepublishOutbox` then sends nothing (`AggregateOwner.scala:288, 959`) and the rows are reported undelivered until the
  owner stops (F253's tail; one `caughtUp(always = true)` when the outbox starts above `version` closes it).
- `historyChangedFor` updates the duplicates' rows without an order (`PostgresEventStoreState.scala:157,282`):
  `RewriteHistoryCommand`s on a base and on its duplicate at once can deadlock (40P01), then answered "outcome unknown".
- `RepublishRange` after a failed look-up answers nothing (`AggregateOwner.scala:289`): the subscriber waits for its gap.
- `rewriteEverywhere` holds every changed event in memory (`PostgresEventStoreState.scala:723,749`): up to the version
  limit × the size limit, 10 000 × 100 000 characters ≈ 2 GB of strings (estimated worst case), on the owner's thread.
- A generation-only conflict is answered `AggregateConcurrentModificationError(expected = v, current = v)`
  (`AggregateOwner.scala:502`): a client cannot tell it from a bug.

## Fixes checked and found sound
- F193: `squash` records the lineage before the chain rows go (`PostgresEventStoreState.scala:848`).
- F194: duplicates told and the owner restored after every rewrite (cost: lesser items above).
- F197: type checked at every restore through `generationAndType`; a mismatch refuses, snapshots of another class unused.
- F213: a higher expected version looks up the store first; no restore when it is not ahead.
- F229: `pastDeadline` before the reads and before the transaction; a late timer finds nothing.
- F231: a failed look-up fails only its request; the state and the receive stay.
- F237: snapshot guard `FOR SHARE`; every change locks the aggregate row before deleting snapshots (lock order checked: no deadlock).
- F244: the version limit applies only to events that add a version.
- F245: rewrites run in `failedRestore`; unreadable rows changed as text or refused.
- F251: at most 1 000 waiting queries, 5 minutes each.
- F252 (owner side): long idempotency keys refused before the handler runs; owner names URL-encoded.
- F265: the error names the first new or rewritten event.
- F209, F242: keys carry a per-start UUID; a bare `StoreFirstEvent` is refused.
- F218: at most 200 stops per sweep, the rest a second later.
- Not sound: F195 (R2-OWN-1), F196 (R2-OWN-3), F208 (R2-OWN-2), F214 (R2-OWN-4, R2-OWN-5), F250 (R2-OWN-6), F253 (R2-OWN-2, R2-OWN-5).

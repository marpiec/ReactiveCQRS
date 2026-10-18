# Working notes: event store (second assessment)

- **Area:** the event store and its SQL (prefix R2-STORE).
- **Scope:** `core/.../eventstore/*` (`PostgresEventStoreState`, `PostgresEventStoreSchemaInitializer` with its
  procedures and `installProcedures`, `MemoryEventStoreState`, `EventStoreState`), `util/Ddl`, `util/SchemaLock`,
  `rewrite/HistoryRewriter`; callers followed into `AggregateOwner`, `AggregateRestorer`, `AggregateCommandBusActor`.
  Focus: `git diff v1.0.0..HEAD` of those files (the F185–F274 fixes).
- **Date:** 18/10/2026. **Commit:** 2453711 (1.0.1-SNAPSHOT).
- **Method:** read only; nothing was run. PostgreSQL behaviour cited is its documented semantics. Every number is an
  estimate unless a source is named.

## Findings

### R2-STORE-1 — A command whose events put an undo before another event is now always refused on PostgreSQL
- **Severity** Medium · **Likelihood** Possible · **Confidence** Confirmed (code). Continues F195.
- **Where:** `PostgresEventStoreState.scala:78-84` (a command with an undo or a duplication event stores each event
  through `persistOne`), `:141-143` (a plain event of it goes through `persistPlain` with the command's
  `expectedGeneration`), `:119-121`; `PostgresEventStoreSchemaInitializer.scala:391` (`add_undo_event` raises the
  generation), `:334-336` (`add_events` compares it); `AggregateOwner.scala:539-540` (passes
  `Some(command.baseGeneration)`), `:573-579`, `:602-606`, `:633-642`; `MemoryEventStoreState.scala:51-53` (checks
  once, before any event).
- **Scenario:** a handler answers `CommandSuccess(ChangesUndone(1), NoteAdded(...))` (undo, then a plain event).
  It worked in 1.0.0.
- **Mechanism:**
  1. `add_undo_event` raises the aggregate's `history_generation` from g to g+1 in the transaction.
  2. The next event's `add_events` call is given the expected generation g, reads g+1 from the row it just updated,
     and raises "Concurrent aggregate modification exception … its history changed".
  3. The owner takes it for another writer (`movedOnWithoutThisOwner`), restores the aggregate, and answers a
     conflict. A `ConcurrentCommand` runs again with back-off and fails the same way every time.
- **Impact:** the command can never be stored; a `Command` is answered `AggregateConcurrentModificationError`, which
  invites retries that fail the same way; each attempt costs a full restore. No data is damaged. The memory store
  checks the generation once before all events, so specs on it pass (F274's pattern).
- **Validation:** `PostgresEventStoreStateSpec`: `persist(id, NewEvents(u, v, t, Seq(Undo(1), Plain), None,
  Some(generation)))` throws; the same with `MemoryEventStoreState` succeeds. End to end: a `ConcurrentCommand` that
  emits [undo, event] is answered a failure after the retries.
- **Suggested fix:** check the generation once per command: pass the expected generation only to the first procedure
  call of `persist` (later calls get -1, the row is locked by then), or raise the expected value by one after each
  undo.
- **Regression test:** the validation, on both stores; plus [event, undo, event].

### R2-STORE-2 — During the rolling upgrade from 1.0.0, a 1.0.0 node's snapshot keeps the 1.0.1 shape on the row: F196's guard accepts an old-shape state
- **Severity** High · **Likelihood** Possible (a state class changes in the deploy that brings 1.0.1, with
  snapshots on) · **Confidence** Confirmed (code). Continues F196.
- **Where:** 1.0.0's `writeSnapshot` (`git show v1.0.0:…/PostgresEventStoreState.scala`, lines 236-241):
  `ON CONFLICT (aggregate_id) DO UPDATE SET version, context_version, schema_version, state, created`, no `shape`;
  1.0.1's reader `PostgresEventStoreState.scala:242-246` and `AggregateRestorer.scala:93-94` (uses a snapshot whose
  `shape` equals the class's); book 13-snapshots.md:166-168 ("a snapshot written by 1.0.0 has no shape, so each
  aggregate is replayed once").
- **Scenario:** the host's release bumps the library to 1.0.1 and adds a field to a state class without raising
  `schemaVersion` (F196's case). Rolling restart: a 1.0.1 node writes X's snapshot (new shape S2); X's shard moves to,
  or a `Local` owner of it passivates on, a node still on 1.0.0 (old class), which writes its state over the row.
- **Mechanism:** the upsert replaces `state`, `version`, `context_version`, `schema_version` and keeps `shape = S2`.
  A 1.0.1 restore finds `shape == S2`, uses the old-shape JSON, and mpjsons gives the new field zero, false or null.
  Only rows first inserted by 1.0.0 carry no shape; rows a 1.0.1 node wrote first do.
- **Impact:** F196's silent wrong state comes back in the very window the shape was meant to cover: handlers decide on
  it and store the decisions as events (I6), and the next snapshot keeps it. The book promises the opposite.
- **Validation:** with 1.0.1 write X's snapshot; run 1.0.0's `INSERT … ON CONFLICT` with an old-shape state; restore
  X with 1.0.1: the snapshot is used.
- **Suggested fix:** bind the shape to the state: store `shape` as digest plus `md5(state)` (computed in the insert)
  and compare `md5(state)` on read, so a writer that does not set `shape` invalidates it. Or, simpler, state in the
  upgrade notes: no state-class change in the deploy that brings 1.0.1, or `DELETE FROM snapshots` after the rollout
  (they are disposable).
- **Regression test:** the validation: an upsert without `shape` makes the snapshot unusable.

### R2-STORE-3 — The event size limit applies to detach and squash events: a base whose copy's state is larger than the limit can never be permanently deleted or purged
- **Severity** Medium (erasure, Regulation (EU) 2016/679 Art. 17) · **Likelihood** Possible · **Confidence**
  Confirmed (code). Continues F244 (the same pattern: a guard on what users write applied to what the library writes).
- **Where:** `PostgresEventStoreState.scala:94-103` (`serializedChecked` checks the size whatever `adds`), `:809`
  (detach event), `:839` (squash event); `:526-560` and `:562-575` (permanent delete and the purge's detach run
  `detachDuplicate` in their transaction); `AggregateCommandBusActor.scala:448-468` (a failure is logged, the
  delete's outbox row and the other acknowledged rows stay, `:460` runs only for `Left`), `:471-491`;
  `AggregateRestorer.scala:125-135` (the event is built without a size check).
- **Scenario:** a template B (a price list, a document) whose state at the duplication point serializes to more than
  the squash event class's limit (100 000 characters by default; the events that built it were each under it). Copies
  of B exist. B is permanently deleted (erasure) or its organization purged.
- **Mechanism:** each copy's detach event is the copy's whole state at version 1; `serializedChecked` throws
  `EventTooLargeException` inside the delete's transaction, which rolls back. The bus logs an error and keeps the
  outbox rows; the outbox poller re-publishes them (3 and 6 min, then every 30 min, F123), and every ack restores every
  copy again and fails again. The purge gets `DetachRefused` for good.
- **Impact:** the aggregate reads as deleted, but its events (with the personal data) stay, forever, with an error
  line every 30 minutes; the organization purge cannot finish. The aggregate's own squash fails the same way at every
  scheduler run, each after a restore.
- **Validation:** a context with squash; B with a state of 150 000 characters; duplicate it; `PermanentDeleteEvent`
  on B: B's events remain, and the log repeats the failure at each re-delivery.
- **Suggested fix:** do not size-check events the library writes in place of stored ones (detach, squash), or give
  them their own, larger limit; check it in `AggregateRestorer.squashEvent` so the refusal is a reason, not an
  exception; in `deletePermanently` delete the other acknowledged rows on failure too.
- **Regression test:** the validation: the delete completes, or is refused once with a reason that names the limit.

### R2-STORE-4 — Event-history reads are not type-checked: another type's events, with their envelopes, are returned through any type's bus
- **Severity** Medium (disclosure) · **Likelihood** Possible (an unvalidated id from a client) · **Confidence**
  Confirmed (code). Continues F197 (its fix covers restores, not these reads).
- **Where:** `AggregateCommandBusActor.scala:222-226` (`GetEventsForAggregate`, `GetEventsForAggregateForVersion`
  through `restorer.events`, `GetArchivedEvents` through `readArchive`); `AggregateRestorer.scala:149-153` (no
  `generationAndType` check, unlike `restore` at `:49-53`); `PostgresEventStoreState.scala:184-224`, `:702-710`
  (select by id only; each row deserialized by its own stored class name, so nothing fails).
- **Scenario:** type B's aggregate id X reaches type A's bus in `GetEventsForAggregate(X)`.
- **Mechanism:** the read restores nothing, so the type check of F197 never runs; the events of X deserialize as B's
  classes and are answered as `EventInfo[A]`, with their metadata envelopes and user ids.
- **Impact:** a client allowed to read A's histories reads B's (personal data included); the `asInstanceOf` fails only
  later, if at all. `GetAggregate(X)` is refused since F197, so the gap is inconsistent.
- **Validation:** create B's X; ask A's bus `GetEventsForAggregate(X)` and `GetArchivedEvents(X)`: both answer B's
  events.
- **Suggested fix:** in `historicalRead`, read `generationAndType` first and refuse a type mismatch as `restore`
  does (one key lookup), honouring `aggregate-type-check = "log"`.
- **Regression test:** the validation: both reads are refused, naming both types.

### R2-STORE-5 — An erasure changes an unreadable event's JSON but not its metadata envelope, and reports it rewritten
- **Severity** Medium (erasure) · **Likelihood** Unlikely (an unreadable row whose envelope holds a value the change
  erases) · **Confidence** Confirmed (code). Continues F245.
- **Where:** `PostgresEventStoreState.scala:726-740` (an unreadable row goes to `changedUnread` with the event text
  only; `metadataOf` and `alter`, which apply `metadataChanges`, run only in `checkRead`, `:741-754`), `:764-765`
  (`UPDATE … SET event = ?` only); `AggregateOwner.scala:855-857`.
- **Scenario:** a context's `metadataChanges` erases a person's id from `EventMetadata.custom` or `actor`; one of the
  aggregate's events cannot be read (F52-style rows), and the change has `inUnreadableEvent`.
- **Mechanism:** the event text is changed; the envelope column of that row is never passed to the change.
- **Impact:** the value stays in `events.metadata` (and in the archive's), while `HistoryRewritten` counts the row and
  `RewriteReport.complete` is true.
- **Validation:** corrupt one event's JSON, give it an envelope with the value, rewrite with a change that erases it
  in envelopes and has a text form: the envelope still holds the value.
- **Suggested fix:** for unreadable rows, also read and alter the envelope (it is readable on its own), and write both.
- **Regression test:** the validation, for `events` and `events_archive`.

## Lesser items

- A plain squash raises no generation, so a stale `Local` owner on another node, waiting on F237's `FOR SHARE`, still
  writes a snapshot below the squash point after it (`PostgresEventStoreState.scala:255-258`, `:847`); a restore then
  applies the squash event to a non-initial state, right only if its handler ignores the state. Add
  `AND coalesce(squashed_version, 0) <= version` to the guard. Hypothesis for the impact.
- `historyChangedFor` updates the copies' own rows in plan order (`:282`) while `lockChain` locks in key order
  (`:514-516`): a rewrite command on a base (`overwriteEvents`) concurrent with a complete rewrite, squash or delete of
  a copy that has copies, or with a squash of the base when a copy's id is below the base's (ids from per-node pools),
  can deadlock; PostgreSQL aborts one, and `Errors.transient` (`Errors.scala:55`, class 40) reports it as an unknown
  outcome.
- `versionsRepeat()` is a `GROUP BY` over all of `events` (`PostgresEventStoreSchemaInitializer.scala:18-19`): run by
  `buildIndexesConcurrently` before the concurrent build that would find duplicates anyway (`:293`), and at start when
  `small()` misjudges a large table (`reltuples = -1` after `pg_upgrade` or a restore, before `ANALYZE`, `:211-213`),
  under the schema lock and followed by a non-concurrent unique build that blocks writes (first assessment's lesser
  item, amplified).
- `Ddl.has` finds an index through the search path and also when it is INVALID (`Ddl.scala:33`); `hasColumn` looks in
  `current_schema()` only (`:29-30`). With an INVALID `events_aggregate_version_uidx` (an interrupted concurrent build),
  a start that judges the table small drops the valid plain index and builds nothing (`SchemaInitializer:190-196`).
- Each of `Ddl`'s 30 attempts queues every statement on the table for up to 2 s behind the waiting `ALTER`
  (`Ddl.scala:22-25`, `:62-73`): about 60 s of stalls on a hot table before the start fails (estimate).
- `add_undo_event` and `add_duplication_event` take no expected generation (`PostgresEventStoreState.scala:129-140`):
  an undo event class that copies values from a stale state escapes F195's check (cross-node only).
- F269: a node older than the recorded procedures skips the whole install (`SchemaInitializer:59-66`), so rolling back a
  release with a faulty procedure needs a hand-deleted `procedures:<n>` row; not documented.
- `deletePermanently` deletes the other acknowledged outbox rows only when the store answers `Left`, not when it throws
  (`AggregateCommandBusActor.scala:457-466`): they are re-published until the delete succeeds.

## Fixes checked and found sound

- F185: `Ddl` reads the catalog first and changes under `lock_timeout`; an unchanged schema takes no table lock (see the
  lesser items for its limits).
- F193: `squash` records lineage before deleting the chain rows, in both stores.
- F195: the owner, `add_events` (10-argument form plus the 1.0.0 wrapper) and `overwriteEvents` compare the generation;
  the restore reads it before the history (apart from R2-STORE-1).
- F196: shape stored and compared (apart from R2-STORE-2's mixed-version gap).
- F197: restores, commands and squashes check the type (apart from R2-STORE-4).
- F198 (store part): unique index at start on a small store and by `buildIndexesConcurrently`; plain index kept while
  duplicates exist.
- F220: id-range walk, pauses, retried batches; no whole-table scan.
- F237: `FOR SHARE` in the guard, and every change locks the row (UPDATE or `lockChain`) before deleting snapshots; lock
  order with snapshot upserts is the same, so no deadlock.
- F244: the version limit only on events that add a version, never on a permanent delete (size limit: R2-STORE-3).
- F245: unreadable rows changed as text or the rewrite refused (envelopes: R2-STORE-5).
- F246: per-aggregate `min(version)` by its own events' times; bounded by its unsquashed old events through the
  `(aggregate_id, version)` index.
- F252: constructor values checked.
- F269: `procedures:<n>` recorded; older nodes of 1.0.1+ leave newer bodies.
- F192, F194: `HistoryRewriter` rebuilds every visited aggregate, and none when all were refused.

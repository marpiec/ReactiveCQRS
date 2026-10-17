# M6 design: organizations and the data lifecycle

How milestone M6 implements D38, D40, D41, D52 (R11), D56 (R17), D76 (R15) and D77 (R16) of
`KNOWN-ISSUES.md`, and fixes F8 (complete) and F124. Written at the start of the work, 17/10/2026;
`ARCHITECTURE.md` describes the result.

## What the milestone must give Neula

- **An organization is a space** (D38): every aggregate belongs to the space of its first event,
  space 0 being the platform; ids below 1001 are constants every installation shares.
- **Purge** (R17): an organization's rows go from every table the library writes and from every
  document store; other organizations are untouched.
- **Move** (D40, D41): an organization is exported from one installation and imported into
  another, with fresh ids only where its legacy ids collide there, and its projections rebuilt.
- **Complete rewrite** (R15, D76): a value is replaced wherever the library stored it (events,
  squash events, snapshots, detached copies, outbox-hook rows on request), the projections that
  read it are rebuilt, and an audit entry records who, when and which classes and fields.
- **Ephemeral aggregates** (R16, D77): a context runs an aggregate only in memory on one node,
  never written or published, until `Materialize` persists it under a new id.
- **Retention squash** (R11, D52): audit events stay readable in an archive while technical
  events are squashed away, and a report says which aggregates still use a class.

## Constraints carried from the reviews

- M1: purge and rewrite need `commands_responses` rows tied to their aggregate and saga orders
  tied to a space; purge detaches duplicates first (F76); ephemeral aggregates need a store of
  their own (the memory store is a test double); the import round trip can use two schemas of one
  database (F71).
- M3: export, import and the rewrite use the context's squash event (`SquashPolicy.toEvent`) and
  take the chain lock of F96 when they change a base's history; retention squash builds on the
  squash scheduler and writes its archive in the squash transaction.
- M4: imports and purges rebuild per organization through the targeted rebuild of one space,
  which needs every aggregate's space (the backfill, F49, first); a purge clears the
  organization's documents in every build the registry lists.
- M5: in a cluster these operations change aggregates under owners on other nodes: `Local` owners
  catch up through the stored version and generation (F131), sharded ones are stopped through
  their shard; each operation runs once (a singleton or the schema lock); new event classes pass
  the write switch (D67).
- D28: stored events are changed only by the rewrite and squash; schema changes are additive,
  heavy work runs as online jobs (D69).

## What the store holds of an organization

From a survey of every table the library writes (17/10/2026):

| Table | Rows of space S | Notes |
|---|---|---|
| `aggregates` | `space_id = S`, own rows (`id = base_id`) and their chain rows | the only reliable record of an aggregate's space: a first event's `spaceId` may be a `def`, not stored; no index on `space_id` |
| `events`, `noop_events`, `events_to_publish`, `snapshots`, `subscriptions`, `subscription_failures` | through the aggregates' ids | a duplicate in S reads events stored under bases in other spaces, and duplicates in other spaces read S's |
| `commands_responses` | none | the aggregate id is only inside the response's JSON; rows expire after 30 days |
| `sagas` | none | an order may hold ids; rows go when a saga ends |
| `projection_<t>` | `space_id = S`, as the projection wrote it | written on insert only, never indexed, read by space only by `clearSpace`; a projection may write any space (the test domain writes 0) |
| projections' own tables (`build.table`), the host's outbox rows (R12) | unknown to the library | the host's schema |
| `types_names` | not applicable | ids differ per database: an export carries class names |

So a purge or an export finds an organization through `aggregates.space_id`; the rows the library
cannot tie to it (responses, sagas) get links (below); the rest is the projections' and the
host's, reached through hooks.

## Order of the work

1. **Required space** (done) and **the commit-order pass by type and space** (F124): an export reads
   one organization in commit order, and every operation ends with an organization's rebuild.
2. **The archive and retention squash** (R11): before the others, which must each cover archived
   events.
3. **Purge** (R17).
4. **Export and import** (D40).
5. **The complete rewrite** (R15, D76; F8, F146), with the lineage of detached copies.
6. **Ephemeral aggregates** (R16, D77).
7. **Close**: the suites (also as a cluster of one), the bench, the review, the tag.

Each step ends with the full suites and a commit; the failure circumstances below are written
before each step's code and revised with it.


## Required space (D38)

`add_events` already refused to create an aggregate without a space (`space_id = -1`), but with
an exception the caller saw as `EventHandlingError`, and the memory store took it. The owner now
refuses a first event without a space before anything is written, with a `FailureResponse` that
names the event, for both stores (done, `AggregateOwnerSpec`). Rows from before the space column
keep `-1` until the backfill (F49, M4) gives them one; they belong to no organization, so a purge
or an export of a space never sees them, and `StoredDataCheck` counts them.

## The commit-order pass by type and space (F124)

`readCommittedPage` walks `events` by id and keeps the rows of its types (and, scoped, of its space)
through a join with `aggregates`. Its cost is the store up to the page's last row, not the build's
share of it: a projection over a tenth of 100 million events, or one organization's rebuild after
an import, reads the whole store.

Options considered:

1. Columns `type_id` and `space_id` on `events`, indexed. New rows get them from the procedures,
   but every old row needs an `UPDATE`, which D28 (1) rules out and which doubles the table until
   VACUUM. Rejected.
2. **A narrow table of keys** `event_keys (id PK → events.id ON DELETE CASCADE, type_id, space_id)`
   indexed on `(type_id, id)` and `(space_id, type_id, id)`, written with each event by the
   procedures and deleted with it by the cascade, and backfilled online for the rows from before
   (D69). Additive (D28). Chosen.
3. Reading each aggregate of the type or space by version and merging in memory: commit order
   across many aggregates needs every cursor open. Rejected.
4. Leaving it, and documenting that a build reads the whole store. Rejected: an organization's
   rebuild after every import would read every organization's events.

**Measured** (bench, `scenarios=commands`, two single runs each, the same day): with the extra
insert, 9 182 and 9 406 commands/s without subscribers and 4 300 and 5 104 with two projections;
without it, 9 076 and 9 846, and 5 185 and 5 001; p50 1.4–1.5 ms either way. The difference is
within the runs' spread. Size, estimated: about 40 bytes a row before its two indexes, so about
4 GB per 100 million events with them.

The design:
- **Writes.** The three procedures insert the key with the event: the type and space are the
  aggregate's own row's (a duplicate's events are under the duplicate). A squash and a detach
  replace an event in place (its id stays), and the events they delete take their keys with them
  (the cascade); so do a permanent delete and a purge. The space backfill (F49) updates the keys
  of the aggregates it gives a space. An import writes keys with its events.
- **Backfill.** `PostgresEventStoreState.backfillEventKeys(batchSize)` inserts the keys of the
  rows from before, in id order, a batch per transaction (`ON CONFLICT DO NOTHING`), and, once a
  final check finds no event without a key, records it in `schema_steps` (a new table of
  completed online steps). It is run once in the background after the first node of the new
  version started, as `buildIndexesConcurrently` is (H11).
- **Reads.** Until that record exists, `readCommittedPage` keeps its join. Then it reads one
  subquery per type, by `(type_id, id)` or, in a space, by `(space_id, type_id, id)`, each ordered
  by id and limited to the page, merged by id; a build of named aggregates reads them by
  `events (aggregate_id, id)`. The store looks for the record again at most once a minute until
  it finds it.

| Circumstance | What happens | Handled by |
|---|---|---|
| A node of the old version writes events after the new one replaced the procedures | the procedures are the database's: every node calls the new ones | `initSchema` creates the table before it replaces the procedures |
| The backfill runs while events are written | rows it has not reached yet, and new ones | new rows get keys from the procedures; the backfill takes ids up to the last at its start, `ON CONFLICT DO NOTHING`; its final check runs after |
| A path writes an event without a key after the switch | a build would never see that event | every insert is in the three procedures and the import, each tested; the final check covers what was there before |
| The backfill is stopped midway | the switch never happens | it starts again from the first id without a key; the join serves meanwhile |
| An aggregate's space changes (the backfill of F49) | its keys keep the old space | the space backfill updates them in its transaction |
| Many types in one build | one index scan each | one subquery per type, each at most a page, merged |
| A rollback to the previous version | its procedures do not write keys, and a later switch would miss rows | a version whose step completed is not rolled back below it (as D67's new classes); documented |


## The archive and retention squash (R11)

A squash deletes the events below its point (D47). An audit trail must outlive that: Neula's
business entities keep who changed what for years, while their technical events can go.

- **What is kept**: `SquashPolicy(toEvent, automatic, keep: Set[Class[_]] = Set.empty)`. Events of a
  `keep` class at or below the squash point are moved, in the squash's transaction, into
  `events_archive (id PK, aggregate_id, version, event_type_id, event_type_version, event, user_id,
  event_time, metadata, undone_from, archived)`, indexed on `(aggregate_id, version)`: the row as it was, its
  undo mark (`noop_events.from_version`), and when it was archived. The event at the squash point,
  which the squash event replaces, is archived first when it is of a kept class.
- **Reading it**: `GetArchivedEvents(id)` on the command bus, answered from the store like the
  historical reads: `ArchivedEvents(id, events)` with each event's version, user, time, metadata
  and undo mark. Restores, re-publications and rebuilds never read the archive: what a projection
  derives comes from the squash event on (D51).
- **Retention by age**: `AutomaticSquash(afterEvents, keepLast, keepNewerThan: Option[FiniteDuration])`:
  the scheduler squashes up to the newest version older than `keepNewerThan` (and at most
  `base_version - keepLast`), once `afterEvents` are squashable.
- **Who still uses a class**: `StoredDataCheck.usersOf(className, limit)` lists the aggregates with
  events or archived events of a class, and their counts; an empty answer means the class can go
  from the code (D29's new classes replace old ones).
- **Everything else covers the archive**: a purge and a permanent delete delete an aggregate's
  archived rows, an export carries them, and the complete rewrite rewrites them.

| Circumstance | What happens | Handled by |
|---|---|---|
| The event at the squash point is an audit event | the squash event replaces it in place | it is archived before the update, in the same transaction |
| An archived event was undone | its undo mark would go with `noop_events` | `undone_from` keeps it |
| A duplicate reads the base below the squash point | it is detached with a squash event of its own state (D50) | the base's archive keeps the base's audit events; the duplicate's own events stay in its history; a lineage row (step 5) points from the copy to the base |
| An archive grows for years | one row per audit event | indexed by aggregate; purged and exported with the aggregate |
| A class is removed from the code while archived rows use it | they could not be read | `usersOf` lists them; `StoredDataCheck` reads the archive too |
| A rewrite (R15) must reach audit events | they hold the values too | the rewrite reads archived events with the others |
| A large squash archives many events | the transaction holds them | as a squash today: the scheduler's per-run limit, one squash at a time |
| Retention by age on a busy type (found while building it) | walking back through each aggregate's newer events would read most of the type's recent events on every run | one boundary per run: the first event id not older, by a binary search of the primary key (about 40 reads), then one probe of `events (aggregate_id, id)` per aggregate; exact up to the longest command's duration and the nodes' clock skew; events written later with old times (an import) count as new, so they are kept longer |

Built as designed (17/10/2026), with the last row's change.

## Purge (R17)

`SpacePurge(...).purge(space)` is a host operation, run once from one node while the organization
is quiet (its users stopped: the library cannot hold back a command sent meanwhile, but a pass that
finds new aggregates of the space runs again). It answers a `PurgeReport` with the rows removed per
table, and refuses with the reasons when something blocks it.

1. **Plan**, reading only: the space's aggregates (own rows); the aggregates of other spaces that
   read one of them (chain rows with `base_id` in the space and `id` outside), whose types must
   detach them (a squash policy, D50) and whose detach must not wait (F143's checks); the
   projections that do not implement `clearForRebuild` for a space. Anything that blocks is
   reported before a row changes.
2. **Detach** the other spaces' duplicates, each base in its own transaction under the chain lock
   (F96), as a permanent delete does, and stop their owners (`HistoryChanged`).
3. **Delete**, in batches of aggregates, each a transaction that locks their chains: `noop_events`,
   `events` (their keys by the cascade), `events_archive`, `events_to_publish`, `snapshots`,
   `subscriptions` and `subscription_failures` of every subscriber, the responses tied to them, and
   the `aggregates` rows; the host's `PurgeHook`s run in the same transaction (the outbox writer's
   rows, a projection's own tables). Listed again until a pass finds none.
4. **Owners**: each type's command bus on this node stops the purged aggregates' owners
   (`ForgetAggregates(ids)`): local ones it holds, sharded ones through their shard. A `Local` owner
   on another node finds no stored row at its next use (F131, F145) and fails; until then its
   state stays in that node's memory (documented).
5. **Sagas** tagged with the space are deleted (a running saga's next write finds no row and stops,
   F36).
6. **Projections**: each projection the host names (a managed one's supervisor passes it to every
   build it runs) gets `PurgeSpace(space, ids)`, and in its own process clears the space
   (`clearForRebuild(OneSpace(space))`, the targeted rebuild's hook), resets the purged aggregates'
   versions (which empties its version cache, F29) and, until it stops, acknowledges deliveries of
   those aggregates without applying them, so an event already on its way cannot write a document
   again. It answers `SpacePurged`.

New links, written from now on:
- `commands_responses.aggregate_id` (nullable, indexed): the aggregate of the response, from the
  response itself; older rows expire within the retention (30 days by default).
- `sagas.space_id` (nullable): `SagaActor.spaceOf(order): Option[SpaceId]` (default none), stored
  when the saga starts; untagged sagas are not purged (documented).

| Circumstance | What happens | Handled by |
|---|---|---|
| An aggregate of another organization was duplicated from one of the space | it reads the purged events | detached first; refused when its type has no squash event, or its detach must wait (reported) |
| An aggregate of the space was duplicated from another organization's | its chain rows point there | its rows go; the base stays |
| A command creates an aggregate in the space during the purge | a new row after the pass | passes repeat until one finds none (bounded; the report says if rows remain) |
| An event of the space is on its way to a projection | it could write a document after the clear | the projection drops deliveries of purged aggregates; a gap request finds no state to send |
| The projection's version cache holds a purged aggregate | a late delivery would pass the gate | the reset empties those entries in the projection's own process |
| A document cache holds the space's documents | a query could still answer them | the projection clears its own stores and caches |
| A projection writes documents without the right space | `clearSpace` misses them | documented: projections write the aggregate's space (H11); `StoredDataCheck` counts documents per space |
| A `Local` owner on another node holds a purged aggregate | its state stays in memory until its next use or its node's restart | documented; sharded types are stopped through their shard |
| The organization has millions of events | one transaction would hold them all | batches of aggregates, each a transaction; the passes resume where they left |
| `space_id` has no index | each pass scans `aggregates`, and each projection table scans for its space | an index on `aggregates (space_id)`, built with `buildIndexesConcurrently`; the projections' scans are documented (an index per projection on demand) |
| The purge stops midway | some rows gone, some not | it is run again: each step is idempotent |
| Legacy rows without a space (-1) | they belong to no organization | not purged; `StoredDataCheck` reports them |

### Refined before building (17/10/2026)

The interface, as a host sees it:

```scala
val purge = new SpacePurge(eventStore, commandBuses = Map(AggregateType.of[Cart] -> cartBus, ...),
  projections = Seq(cartsList, ...), hooks = Seq(myOutboxRowsHook))(system)
purge.purge(SpaceId(42)): Future[PurgeReport]
```

- **Plan** reads the space's aggregates by type and the duplicates of other spaces that read them
  (each chain row carries its aggregate's own space, so one query finds them), and refuses, before a
  row changes, when: a type of the space has no command bus given (its owners could not be stopped);
  a projection answers `PurgeCheck` with a refusal (it does not implement `clearForRebuild`, or a
  managed one is building or rebuilding a target).
- **Detach** sends `DetachDuplicates(base, duplicates)` to the base's command bus, which builds the
  detach events as a permanent delete does (`detachEvents`, D85, `notWrittenYet`) and calls
  `EventStoreState.detachDuplicates`: under the chain lock, only those duplicates, F143's checks for
  them; answered `DuplicatesDetached` or `DetachRefused(reason)`. One refusal ends the purge before a
  row is deleted (the detaches done stay: a detached copy is whole on its own).
- **Delete**, in batches of at most 1 000 aggregates and 100 000 events (by `base_version`; a larger
  aggregate alone), in descending id order (a copy usually goes before its base): one transaction
  locks the batch's chains, leaves out the aggregates a copy of another space reads (a duplication
  made after the plan: the next pass detaches it), runs the hooks, and deletes the rows
  (`PostgresEventStoreState.purgeAggregates`: `noop_events`, `events` with their keys,
  `events_archive`, `events_to_publish`, `snapshots`, `subscriptions`, `subscription_failures`,
  `commands_responses` by `aggregate_id`, `aggregates`). After each batch, each type's bus gets
  `ForgetAggregates(ids)` (owners stop without a snapshot) and each projection
  `PurgedAggregates(space, ids)`; both answer, which paces the batches.
- **Sagas** of the space go (`DELETE FROM sagas WHERE space_id = ?`).
- **Passes** repeat while a pass found aggregates, at most three; the report says what remains.
- **Projections** then get `PurgeSpace(space)`: `clearForRebuild(OneSpace(space))` in one
  transaction, answered `SpacePurged` (or a failure, reported). Run again, a purge that finds nothing
  still clears the projections, so a failed clear is repeated by running the purge again.
- **A projection after `PurgedAggregates`** forgets the aggregates (versions, queues, failures) and,
  for a window (`purgedDropWindow`, 30 minutes by default), acknowledges deliveries of them without
  applying them: deliveries already on their way (in a mailbox, or sent by another node's bus) cannot
  write a document again. The ids are kept as one sorted array of longs (8 bytes each); a targeted
  rebuild takes the aggregates it resets out of it.
- **`PurgeReport(space, passes, aggregates, rows, detached, projections, failed, refused, remaining)`**,
  `complete` when nothing was refused, failed or left.
- **`StoredDataCheck.spaceRows(space)`** counts what a space still has (aggregates, event keys,
  projection tables, sagas) and the rows whose aggregate is gone, to verify a purge.

| Circumstance | What happens | Handled by |
|---|---|---|
| A sharded type's owners | `ForgetAggregates` reaches each purged aggregate's shard, which starts an owner only to stop it (no restore) | batches pace it; documented |
| Deliveries already on their way when the rows go | a delivery applied after the clear would write a document again | the drop window; `StoredDataCheck.spaceRows` to verify; a second run clears again |
| A purge of a large organization | the drop window holds the purged ids in each projection | 8 bytes per aggregate per projection, for the window only (1 million aggregates: 8 MB, estimated) |
| A managed projection starts a build during the purge | the build may read events of the space before they go | refused at the plan; a build started later is documented: do not rebuild during a purge |
| The purge stops between batches | some aggregates gone; a copy in the space may read a base already gone | run it again: every step is idempotent, and the copy goes in the next run |
| Responses stored before M6, sagas started before M6 | no `aggregate_id`, no `space_id` | responses expire with the retention (30 days by default); untagged sagas are not purged (documented) |
| A host table with rows of the space | the library does not know it | a `PurgeHook`, in the batch's transaction |
| A copy of another organization reads two of the space's aggregates (a copy of a copy) | the plan lists it once per base, and a second detach would find it detached | each copy is detached once, through its first base |

Built as refined (17/10/2026). The index of an organization's aggregates became `(space_id, id)`, so
that pages of a space come in id order without a sort (the targeted rebuild of a space reads it too).

## Export and import (D40)

`SpaceExport.write(space, out)` writes one organization as JSON lines; `SpaceImport.read(in, settings)`
writes it into another installation. Its projections are then rebuilt for the space
(`RebuildAggregates(OneSpace)`, which step 1 makes cheap).

- **Format**: a header (format version, source installation, space, the time), the aggregates
  (id, type and event class names, space, creation time, squash point, the chain within the
  space), then the events in commit order (aggregate, version, class name, the stored JSON, user,
  time, metadata, undo mark) and the archived events. Class names, not type ids (they differ per
  database). Snapshots, outbox rows, subscriptions, responses and sagas are not exported: an
  organization moves quiet, and its projections are rebuilt.
- **Duplicates of other organizations' aggregates**: their first event is exported as the squash
  event of their state at version 1, as a detach makes it (D50, D85), so the import needs nothing
  outside the space; a type without a squash event refuses the export.
- **Ids**: an aggregate keeps its id unless the target has it (legacy ids below 10^12 can collide;
  installation ranges cannot, D39); a colliding one gets a fresh id from the target's pool. Ids
  below 1001 are constants and never move: the export refuses them in the space. Typed ids
  (`AggregateId`, `SpaceId`, `UserId` and types made of them) are remapped by reading each event
  with an `MPJsons` instance of the import's own, whose transforms map them; the `user_id` column
  and the metadata's typed fields too. Untyped references (a `Long`, a string) are the context's:
  an `ImportRewriter` per type gets each event, its metadata and the id map. References to
  aggregates the import does not have are reported, not changed.
- **Writing**: rows are inserted directly, in the export's commit order (new event ids keep the
  relative order), each aggregate's own row with its version and squash point, its chain within
  the space, its events, undo marks, keys and archived events; no outbox rows. The target's size
  and version limits are checked before anything is written.

| Circumstance | What happens | Handled by |
|---|---|---|
| An exported id exists in the target | two aggregates under one id | a fresh id from the target's pool; every typed reference follows |
| A reference is untyped (`Long`, `"u#123"`, encrypted) | the codec cannot see it | the context's `ImportRewriter`; documented as the host's part (the plan's risk) |
| A reference points outside the organization | the target may not have it | reported; left as it is |
| The space id itself collides | the organization's aggregate gets a new id | `SpaceId` values are remapped like aggregate ids |
| A squashed aggregate | its events start at the squash point | written at their versions, with the squash point |
| A duplicate within the organization | its chain points at its base | the base is written first; the chain rows copy the base's |
| The target's limits differ | an event or a history too large there | checked before writing; refused with the aggregate named |
| The organization is large | memory and one transaction | streamed both ways; the id map holds only the collisions; batches of aggregates per transaction |
| The import stops midway | part of the organization written | the report lists what was written; the host purges the space in the target and starts again |
| The organization has events in flight | the export misses them | documented: export a quiet organization; the export reports outbox rows of the space |
| Event classes unknown to the target | it cannot read them | checked from the header before writing |

### Refined before building (17/10/2026)

The interface, as a host sees it:

```scala
val exported: ExportReport = new SpaceExport(eventStore, contexts = Seq(cartContext, ...)).write(SpaceId(42), writer)
val imported: ImportReport = new SpaceImport(eventStore, uidGenerator, rewriters = Map(...)).read(path, into = SpaceId(7))
```

- **Format**, JSON lines written by mpjsons: an `ExportHeader` (format version, source installation,
  space, time, every class name the lines use), one `ExportedAggregate` per aggregate (id, root class,
  version, squash point, creation time, its chain within the space, and whether its first event is a
  detach), then one `ExportedEvent` per event in commit order (`events.id`; aggregate, version, class,
  the stored JSON, user, time, metadata JSON, undo mark), then one `ExportedArchived` per archived
  event. The events are read in one read-only transaction with a fetch size, so the export streams.
- **Detached on export**: an aggregate whose chain reaches a base outside the space (directly or
  through a copy in the space) is exported with its version-1 event replaced by the type's squash
  event of its state at version 1, checked (D85) as a detach is, and no chain; a type without a
  squash event, or a context not given, refuses the export before anything is written.
- **Refused on export**: ids below 1001 in the space (constants, D39); the report counts the space's
  outbox rows (an organization moves quiet).
- **Import, pass 1** (reads the file, writes nothing): the header's classes must all load; the
  target space must be empty; every exported id that the target already has gets a fresh id from
  the target's pool (`GetNewAggregatesIdsPool`); sizes and versions are checked against the target
  store's limits; a refusal lists every reason.
- **Import, pass 2**: the aggregates' rows (own and chain, bases mapped) in batches, then the events
  in the file's order, each read with an `MPJsons` of the import's own whose post-deserialization
  transforms map `AggregateId`, `UserId` (same map) and `SpaceId` (source space to target space),
  then the type's `ImportRewriter` (untyped references), then written with the store's serializer
  under a new id from `events_seq` (relative order kept), with its undo mark and its key; then the
  archived events. Transactions of at most 10 000 events. No outbox rows: the host rebuilds the
  space's projections (`RebuildAggregates(OneSpace(into))`).
- **`ImportReport`**: counts, the ids remapped, references to aggregates neither exported nor in the
  target (counted, the first 1 000 listed), refusals.

| Circumstance | What happens | Handled by |
|---|---|---|
| An organization of 10 million events | a file of about 4 GB (382 bytes per event measured in load-M5; estimated), read twice | streamed both ways; memory holds the aggregate ids and the remapped ones only |
| Events of 5 MB | long lines | read line by line; one event in memory at a time |
| The import stops in pass 2 | part of the organization written | the report says how far; the host purges the target space (it was empty) and imports again |
| A reference held as text ("user:5" in a metadata actor) | the codec cannot see it | the type's `ImportRewriter`; documented as the host's part |
| The two installations run different code | a class of the file is missing in the target | refused in pass 1 with the class names |
| The target space is not empty | two organizations would merge | refused in pass 1 |
| A copy of a copy reaches outside the space | its chain cannot be imported | detached on export, as above |

Built as refined (17/10/2026), with one change: the header lists no classes; pass 1 checks the class
of every line it reads, which it reads anyway for the sizes.

## The complete rewrite (R15, D76; F8, F146)

A rewrite replaces a value everywhere the library stored it, for an aggregate and every copy of
it: GDPR erasure. Today a `RewriteHistoryCommand` changes the aggregate's own events of named
classes, deletes snapshots and stops attached duplicates' owners; it misses detached copies, the
archive, the host's rows, rebuilds and an audit trail, and drops a rewrite without a new event
(F146).

- **The change is data, the context applies it**: a `HistoryChange` (a serializable request, such as
  `EraseUser(userId)`) and `AggregateContext.historyChanges: HistoryChange => PartialFunction[Event[R], Event[R]]`:
  the events it does not match stay. `RewriteEverywhere(userId, aggregateId, change)` on the
  command bus runs in the aggregate's owner: it applies the change to every own event (squash
  events included) and to the archive, in one transaction under the chain lock, deletes the
  snapshots of the aggregate and of its attached duplicates (which read those rows), keeps the
  version, publishes nothing, and records an audit row. A plain `RewriteHistoryCommand` without a
  new event is stored too (F146).
- **Detached copies**: every detach records `lineage (aggregate_id, copied_from, at_version, at)`
  (a squash's, a permanent delete's, a purge's). The rewrite follows it, from the aggregate to its
  copies and theirs, and applies the same change to each copy (same type, same context).
- **The host's rows**: `OutboxWriter.rewrite(aggregateId, change)(session)`, called in the same
  transaction (default: nothing).
- **Projections**: `HistoryRewriter.rewrite(aggregateId, change, userId, projections)`, the host's
  entry point, sends the change, follows the lineage, then has each named projection rebuild every
  aggregate that changed (`RebuildAggregates`), one target at a time, and answers a report.
- **Audit**: `history_rewrites (id, request, aggregate_id, user_id, at, change_class, classes, fields)`:
  who, when, which change, which event classes, and which fields changed (the JSON paths whose
  values differ between the old and the new event), never values.

| Circumstance | What happens | Handled by |
|---|---|---|
| A change applied twice (a retry) | the second finds nothing to change | changes are written to be idempotent (documented); the audit row records no fields |
| A copy's type has no change for it | its events keep the value | the report names it |
| A `Local` owner of the aggregate on another node | its state holds the old value | the generation moves, and it catches up before reads and commands (F131, F145) |
| A sharded owner | — | stopped through its shard (`HistoryChanged`) |
| The change alters an event's class | refused (D17) | as today |
| An undo or a duplication event matched by the change | their meaning lives in `noop_events` and the chain rows | refused: the change may not alter them |
| A projection cannot rebuild an aggregate (unmanaged, or no `clearForRebuild`) | its documents keep the value | the report names it; the host repairs it |
| The value is in metadata | events keep their envelopes | the change gets the metadata too (optional) |
| The value is in a saga order or a response | rows the library cannot read for meaning | documented: responses expire; sagas end; Neula erases on its side |
| Many copies (a template duplicated thousands of times) | a long fan-out | one owner at a time, resumable from the report |

### Refined before building (17/10/2026)

The interface, as a host sees it:

```scala
case class EraseCustomer(customer: CustomerId) extends HistoryChange          // the change is data
// in the context: which events it alters, and how (the others stay)
override def historyChanges = {
  case EraseCustomer(c) => { case e: OrderPlaced if e.customer == c => e.copy(customerName = "erased", email = None) }
}
val report: RewriteReport = new HistoryRewriter(commandBus, eventStore, projections).rewrite(orderId, EraseCustomer(c), userId)
```

- **One aggregate, in its owner**: `RewriteEverywhere(userId, aggregateId, change)`, a library message
  to the command bus routed like `SquashAggregate`, runs in the aggregate's owner between commands.
  The owner reads the aggregate's own events and its archived ones, applies `historyChanges(change)`
  (and `metadataChanges(change)` to their envelopes: `actor` and `custom` may hold what the change
  removes), and keeps what the change altered (`!=`). Refused, with nothing written, when the change
  alters an event's class (D17), alters an undo or a duplication event, throws, or makes an event
  too large. Otherwise, in one transaction under the chain lock: the changed events and archived
  events written again in batches; the history generation of the aggregate and of the duplicates
  reading it raised (their snapshots are dropped, F119); the host's rows (`OutboxWriter.rewrite`,
  nothing by default); an audit row. After the commit the owner reads its state again and has the
  bus stop the duplicates' owners, as a rewrite does today. The version does not change and nothing
  is published. Answered `HistoryRewritten(aggregateId, events, archived)` or
  `RewriteRefused(aggregateId, reason)`.
- **F146**: a `RewriteHistoryCommand` whose handler rewrites events and adds none stores the
  rewrites, keeps the version, and is answered with it.
- **Copies**: `HistoryRewriter` walks from the aggregate to its attached duplicates
  (`aggregates.base_id`) and its detached copies (a new table `aggregate_lineage (aggregate_id,
  copied_from, at_version, detached)`, written by every detach: a squash's, a permanent delete's,
  a purge's), and on to theirs, breadth first, each once, and sends `RewriteEverywhere` to each,
  one at a time: a copy's own events may hold the value too. The bases the aggregate reads are not
  changed; the report names them (the value may have come from there, and the host decides).
- **Projections**: then each managed projection given rebuilds the aggregates that changed, one
  `RebuildAggregates(OneAggregate(id))` after another (D63), one projection at a time. A new target
  for many aggregates would need a new case in every host's `clearForRebuild`, so none is added. A
  projection that refuses (no `clearForRebuild`, or a build in progress) is named in the report.
- **Audit**: `history_rewrites (id, aggregate_id, user_id, at, change_class, classes, fields,
  events, archived)`: who, when, which change, the event classes and the fields it altered (the
  case class fields whose values differ), how many rows; never a value.
- **`RewriteReport`**: the aggregates changed (with counts), those the change left as they were,
  refusals with their reasons, bases not followed, projections rebuilt and not, and where it
  stopped. A second run does the rest: changes are idempotent, so it finds nothing to change where
  the first run changed it.

| Circumstance | What happens | Handled by |
|---|---|---|
| An aggregate of 50 000 events | read, changed and written in one transaction | batched writes; the owner holds its commands meanwhile (seconds); the rewriter's ask timeout (5 minutes by default) |
| A template copied 10 000 times | 10 000 owners, then the projections | one owner at a time, resumable from the report; each projection rebuilds them one after another (tens of milliseconds each, estimated) |
| A copy detached before the lineage table existed | not found | none in practice: detaching (D50) is new in 1.0; documented |
| An old event still in a projection's mailbox | applied with the old value | the rebuild comes after every rewrite; the old delivery, if later, is a duplicate and acknowledged without applying |
| An event waiting in the outbox | delivered from the store | it reads the new value |
| A command on the aggregate meanwhile | — | the owner runs the rewrite between commands |
| A squash, a detach or a purge on the chain meanwhile | — | the chain lock |
| A `Local` owner on another node, a sharded owner | holds the old state | the history generation (F131); the bus stops the duplicates' owners, a shard passivates them |
| The change throws, or makes an event too large | that aggregate's rewrite fails | refused, nothing written; the report names it and the rewriter goes on |
| User ids (`events.user_id`) | identifiers, not personal data | not changed; documented (personal data lives in the user's own aggregate, which its own change erases) |
| Saga orders, command responses, exported files | rows the library cannot read for meaning | documented: responses expire, sagas end, files are the host's |

Built as refined (17/10/2026), with two additions: an export carries a detached copy's lineage when
its base is in the organization (and the base of a copy the export detaches), so an erasure after a
move still finds the copy; and a permanent delete removes the lineage rows of the aggregate, as copy
and as base, keeping its audit rows.

## Ephemeral aggregates (R16, D77)

- **Where they run**: `AggregateCommandBusActor.ephemeral(context, ...)`, a second bus for the
  same context on a node, with an in-memory store of its own (thread-safe, no outbox, no
  publication: an event bus of its own that acknowledges), a response state in memory, and no
  outbox writer. Commands for an ephemeral aggregate go to that bus; the host routes them (they
  carry negative ids).
- **Ids**: negative, `-(startSecond × 10^6 + n)` for the node's start (at most a million per
  second of start), so an id of an earlier start is recognised: a command for it is answered
  `EphemeralAggregateLost` (the node restarted), one for an id of this start that is not held
  `EphemeralAggregateUnknown`.
- **`Materialize(id, userId)`**: the ephemeral owner makes the squash event of its state
  (`SquashPolicy.toEvent`, checked as D85), asks the persistent bus to store it as the first event
  of a new aggregate (a library first command with a reserved id), answers the new id, and drops
  the ephemeral aggregate. Without a squash policy, `Materialize` is refused.
- **Bounds**: the bus holds at most `maxAggregates` (configurable) and refuses more; an ephemeral
  aggregate unused for its idle time is dropped (reported in the log and metrics).

| Circumstance | What happens | Handled by |
|---|---|---|
| The node restarts | the aggregates are gone | `EphemeralAggregateLost` for their ids |
| A command reaches another node | it holds nothing there | `EphemeralAggregateUnknown` or lost; documented: the host keeps the node with the id |
| The materialized event breaks the target's rules (size, space, serialization) | the persistent store refuses it | `Materialize` answers the refusal; the ephemeral aggregate stays |
| A memory store accepts what PostgreSQL would not | events that cannot be persisted | the ephemeral bus applies the same checks (space, size of the serialized JSON) |
| Many ephemeral aggregates | memory | `maxAggregates` and the idle time |

### Refined before building (17/10/2026)

The interface, as a host sees it:

```scala
val drafts = system.actorOf(AggregateCommandBusActor.ephemeral(orderContext, persistentBus = orders, uidGenerator, mpjsons), "orderDrafts")
drafts ? CreateOrder(user, ...)              // SuccessResponse(Id(-38654705663), Version(1)): a negative id
drafts ? AddLine(user, draftId, version, ...) // as on a persistent order
drafts ? Materialize(draftId, user)          // Materialized(draftId, Id(1000000001234)): stored, the draft dropped
```

- **Ids** (changed from the first sketch): `-(token × 2^24 + n)`, with a random 29-bit `token` that the
  ephemeral bus picks when it starts and `n < 2^24` (16 777 216 aggregates per start). So |id| stays
  below 2^53, as D39 requires for Neula's UI, and two starts, or two nodes started in the same second,
  share a token once in 2^29 pairs (estimated), where the start-second form shared every id. An id
  of another token is answered `EphemeralAggregateLost` (another start, or another node); an id of
  this token that the bus does not hold (never made, dropped while idle, materialized)
  `EphemeralAggregateUnknown`; a query for either fails with `EphemeralAggregateLostException` or
  `NoEventsForAggregateException`. The pool is one block: the bus refuses new aggregates once it
  is spent.
- **The bus**: `AggregateCommandBusActor.ephemeral(context, persistentBus, uidGenerator, mpjsons, ...)`
  is the same bus with its owners on this node whatever the context's placement, an
  `EphemeralEventStoreState`, responses in memory, no outbox writer, and an `EventsBusActor` of its
  own without subscribers, which acknowledges every publication at once. Command ids come from the
  uid generator, as for any bus.
- **The store** keeps each aggregate's events in memory, under one lock, without the commit-order
  list and the outbox of the test double (reads by version and instant work; rebuilds never read
  it), and checks what PostgreSQL would: a first event's space, each event's JSON size through the
  host's `MPJsons` (the same limits), the version limit. Squash, duplication, archive and history
  rewrites are refused for ephemeral aggregates.
- **Materialize(id, userId)**: the owner refuses without a squash policy; otherwise it builds its
  state's squash event, checked as a squash checks it (D85), stops taking commands (they are
  answered `CommandRejected`, which may be sent again), and sends the persistent bus
  `StoreFirstEvent(userId, event, MaterializationKey(id))`: a library first command written as
  given, without the context's handlers, idempotent under the ephemeral id, so that a retry never
  stores two copies. On `SuccessResponse(newId, _)` the ephemeral aggregate is dropped and the caller
  gets `Materialized(id, newId)`; on a refusal or no answer within the context's command timeout
  the owner takes commands again and the caller gets `MaterializeRefused(id, reason)`.
- **Bounds**: `maxAggregates` (100 000 by default): new ones are answered `CommandRejected`; an
  aggregate unused for `idleTime` (12 hours by default) is dropped by a sweep every five minutes,
  logged and counted (`reactivecqrs.ephemeral.dropped`).

| Circumstance | What happens | Handled by |
|---|---|---|
| The node restarts | the aggregates are gone | `EphemeralAggregateLost` for their ids (another token) |
| A command reaches another node | that node holds nothing of it | `EphemeralAggregateLost` (another token); the host routes by node |
| `Materialize` while commands arrive | a command could change the state being stored | commands are rejected (retryable) until the answer |
| The persistent bus answers late | the materialization may have been stored after all | `StoreFirstEvent` is idempotent under the ephemeral id: sent again, it answers the same new id |
| The squash event breaks the persistent store's rules | refused | `MaterializeRefused` with the reason; the ephemeral aggregate stays |
| 10 000 commands at once on ephemeral aggregates | one lock over the store | each section is short and in memory (no I/O under the lock) |
| An ephemeral aggregate of 10 000 events | memory, restores | the version limit (10 000 by default); its owner keeps it like any other |

Built as refined (17/10/2026), with one addition found by its test: the sweep drops an aggregate only
once it has been unused for the context's command timeout too, as a command routed before could
still write into it until then (R5).

# M4 design: the projection rebuild

How milestone M4 implements D57–D63 and D70 of `KNOWN-ISSUES.md`, and fixes F3, F43, F49
(backfill), F54–F62, F82, F93, F94 and F98. Written at the start of the work, 17/10/2026,
after the first two steps; `ARCHITECTURE.md` describes the result.

## What the host's code needs

A read-only survey of Neula's code (17/10/2026; counts measured in the source) shaped the
interfaces below:

- About 60 projection actors are created in one wiring file with fixed actor names. Their
  document stores (57, one per projection except one projection with 8) are created there too,
  with literal table names, and passed into the constructors; creating a store runs its DDL.
  About a dozen projections also keep hand-written state classes with tables of their own,
  some created per organization at run time, one with a shared sequence.
- Callers reach projections through their `ActorRef`: query APIs (some bound late) and the
  REST layer ask them with arbitrary messages, answered with `pipeTo`. 18 projections park
  queries with `delayIfNotAvailable*`, which only an instance that applies live events
  answers. Three subscribable projections keep push subscriptions in memory.
- 10 of the subscribers are reactors with side effects (emails, commands, DDL), not read
  models; today a rebuild-mode constructor flag suppresses them, and three read models switch
  to another algorithm under the same flag. Rebuild is an offline run mode of the server.
- Three pairs share state across projections (one writes a table another reads; one store is
  also read by a plain actor). Four listeners query other projections while they apply events.
- Half the projections (31 of 60) have state listeners only; 21 listen to more than one type.
- `projectionName` and `version` are set by every projection; subscriptions are keyed by class
  name; one projection was renamed (class, name and table together) without clean-up.

## Order of the work

1. **Subscription state: one mode (F3), batches of many aggregates (D61).** Done
   (`49f9024`): the in-memory mode and `dump()` are gone; one transaction may move a
   subscription several times.
2. **The projection takes chunks; the gate starts at a squash event (D58, D61, F98).** Done
   (`47731cc`): `ApplyChunk`, `BuildFromStore`, `GoLive`; `startsAtSquash` on re-publications.
3. **The builder (D59, D60, D62; F57, F58, F59, F62, F82).** Commit-order replay with bounded
   replay states, final states for state-only projections, gap fills, reconciliation.
4. **Registry, builds, the projection supervisor and the switch (D57, D70, D63 triggers;
   F54, F55, F60, F61, F94, F43).**
5. **Targeted rebuild (D63, R14) and the space backfill (F49).**
6. **Throughput (F62, F93, H1):** many aggregates per transaction on the live path too; the
   bench's scenarios on stored subscriptions and the new rebuild; the old replayer removed
   (F82; `VersionsState` went too, as the registry reads `components_versions` itself to
   adopt). Done. See "Live groups" below.

Each step ends with `sbt test` green, the docs updated and a commit; the plan's M4
verification follows.

## Builds and the registry (D57, D70)

A **build** is one materialization of a projection version: its own tables and its own
subscription identity. The registry keeps, per projection name (the one stable key, D70),
the active build and the build in progress, if any:

- `projections(name PRIMARY KEY, active_build, building_build, last_build, …)`: the switch is
  `UPDATE projections SET active_build = building_build, building_build = NULL WHERE name = ?
  AND building_build = ?`, one row.
- `projection_builds(name, build, version, suffix, subscriber, contexts, phase, position,
  created)`: what each build is, and how far a build in progress got (D61).
- `projection_build_tables(name, build, table_name)`: every table and sequence a build named,
  so a retired build is dropped whole (also the state classes' tables, and tables made per
  organization at run time).

**Names.** A build's suffix is `_v<version>` for the first build of a version and
`_v<version>_<n>` for the n-th (a rebuild of the same version, D63). `build.table("tasks")`
gives `tasks_v4`, which the document store prefixes as before (`projection_tasks_v4`); a
state class uses the same call for its own tables. Its subscriber identity is
`projection:<name><suffix>`, independent of the class name, so renaming a class no longer
resets a projection.

**Adoption (D81).** A projection the registry does not know is adopted as build 0 with the
suffix `""` (the tables it has, `projection_<table>`) and its class name as subscriber (the
subscription rows it has), at the version and context versions `components_versions`
recorded (the 0.12 rebuild bookkeeping, read once); when that differs from the code, a build
starts at once. A projection with no trace at all gets its first build.

**Retirement (F94).** A retired build's tables, subscription rows and failure rows are
dropped once nothing uses it: at the switch on a single node. `RetireProjection(name)` drops
every build of a projection removed from the code. (As built: `Projections.retire(name)`, a call
at start, not a message.)

## The projection supervisor

The host creates each managed projection through the runtime, which returns the
supervisor's `ActorRef`, named as the projection was; the host passes that ref where it
passed the projection's before (the REST layer, query APIs, `RegisterSubscribers`):

```scala
val projections = new Projections(system, ProjectionsSettings(eventStore, subscriptionsState, registry, contexts))
// As built: no event bus (the supervisor is registered on the bus by the host); also builder, buildsAtOnce, restartAfter.
val carts: ActorRef = projections.start[ShoppingCartsListProjection]("ShoppingCartsList", version = 2, "shoppingCartsList") { build =>
  new ShoppingCartsListProjection(subscriptionsState, new PostgresDocumentStore[CartView](build.documentStore("carts"), mpjsons, new CartsCache))
}
```

Each build gets stores, and caches, of its own: a cache keyed by document id, shared by two builds,
would give one build the other's documents (review 6).

The supervisor reads the registry, creates one projection instance per build it runs (on the
blocking dispatcher, F27), answers the bus's `GetSubscriptionRequests` with their
subscriptions, and forwards everything else to the instance that **serves**: the active build,
or the first build of a new projection. `forward` keeps the asker, so `pipeTo` answers reach
it. Status (`GetProjectionStatus`) adds the build's progress. A projection that has no reason
to be rebuilt from history (a reactor with side effects) is not managed: created as today, it
behaves as today, and since the builder delivers only to its target (D58) it never sees a
rebuild.

**What a projection sees.** `rebuilding` is true while the instance is built and does not
serve yet: a listener with side effects (messages to other actors, push updates) checks it,
which replaces the hosts' rebuild-mode flags. `onActivated()` is called when a built instance
starts to serve. Listeners that keep state in memory between calls must not rely on it while
built: a build commits chunk by chunk and resumes after a crash from the stored versions.

## A build (D57–D62)

The builder (step 3) feeds the building instance directly from the store:

1. **Bulk pass.** Commit order (`events.id`) across the projection's types, consecutive events
   of one aggregate as one run, every event as live delivery sent it, undone events and undo
   events too (D60), with the state after each run from bounded replay states (D59): an undo
   or a duplication takes the state the store gives at its version, as the owner did; a
   duplicate starts from its base's open state when the base is at the duplicated version; an
   evicted state is restored from snapshot plus tail. A projection whose listeners all take
   state only gets each aggregate's final state instead, one item per aggregate (D62). Chunks
   of about 1 000 events go to the instance, which applies each in one transaction (D61);
   two are in flight, so the next page is read while the instance applies. The bus's
   deliveries meanwhile are acknowledged unapplied: their events are in the store.
2. **GoLive.** The instance applies the bus's deliveries from now on.
3. **Reconciliation.** Every aggregate whose stored version is above the build's subscription
   version gets what it lacks, read by version: the events committed after the bulk pass read
   past them, and rows that became visible late (row ids are not commit order between
   concurrent transactions). After it, nothing committed before GoLive is missing, and
   everything after GoLive came live.
4. **Switch.** Once caught up and with no failed subscription (else it waits for a retry or a
   skip, shown in the status), the registry switches in one update; the instance starts to
   serve (`onActivated`); the old instance hands its push subscriptions over, answers its
   parked queries, leaves the bus (`RemoveSubscriber`, so no outbox row waits for it) and
   stops; its build is retired.

Missing versions the instance reports are read again by version. The bulk position is saved
after each applied chunk; the subscription versions, committed with each chunk, make a resumed
build apply nothing twice, and a resume repeats at most the chunks in flight. A row that
cannot be read fails its aggregate's subscription (D20a) and the build goes on (F82).

**While a version is rebuilt on one node** the code of the old version is gone, so the
active build keeps applying live events with the new code to its old tables: queries, parked
ones too, keep working with fresh data, unless the new code cannot run on the old documents;
`whileBuilding = ServeStale` keeps them unchanged instead. In M5's rolling upgrade the old
version runs on old nodes until the rollout ends (D70).

**When builds start (D63).** A projection version that differs from the active build's, or a
context version that differs from the one a build recorded for a type whose state the
projection receives, starts a build at start; `RebuildProjection` starts one on request. At
most two builds run at once (configurable); a build waits for the projections it is declared
`after` (listeners that read other projections). (As built: no `after`; builds of projections that
read others are ordered by the host.)

## Targeted rebuild (D63, R14)

`RebuildAggregates(target)` for one aggregate, one type or one organization (space): in
place, on the active build. The projection clears what it derived from the target in
`clearForRebuild(target)(session)` (a projection that does not implement it refuses), its
subscription rows of those aggregates are reset in the same transaction, and the builder
delivers their events from the store, in commit order. Organizations need `aggregates.space_id`:
the backfill (F49) reads each legacy aggregate's first event (or asks a per-context function)
in batches, online (D69).

## Live groups (D61 on the live path, F93)

A projection applied each live batch in a transaction of its own, after reading its version
alone: one commit and one read per aggregate per delivery. Without parallel processing it now
groups them. A queue that gets a delivery is marked due; a `FlushGroup` message sent after the
deliveries already in the mailbox, or `maxBatchesPerTransaction` (100) due queues, or any
message other than a delivery, drains the due queues together: their versions in one read per
listener kind (cached, as single reads cache them), their batches in transactions of up to
100. Under no load a group is one batch, so nothing waits; under load the groups grow with the
backlog.

- *Order and isolation:* a batch stays its queue's batch at work until its group commits, so a
  queue applies one batch at a time as before; other messages are handled after the group,
  so a query sees what was delivered before it.
- *Failures:* a group that fails runs again one batch at a time (each in its own
  transaction), so only the failing aggregate retries and is recorded (D20a); the others are
  applied once, as the group's writes were rolled back. A `noDBSession` listener runs alone:
  another aggregate's failure must not repeat its side effects.
- *Listeners:* a listener's session may now carry other aggregates' writes, so it must never
  commit or roll it back (H9).
- *Not done:* flow control (a window per subscription, F93). Groups raise the rate at which a
  projection keeps up; a projection slower than the commands still lets the bus's deliveries
  and its mailbox grow.

## Refinements of the decisions

- **D57:** builds, not versions, own tables (a context version bump or a repair rebuilds the
  same version); the old tables are dropped when the build is retired.
- **D61:** the offsets that matter, each aggregate's subscription version, commit with the
  chunk; the bulk pass's position is saved after the chunk, so a resume repeats at most the
  chunks in flight, which the versions make harmless.
- **D57, D70 on one node:** the active build applies live events with the new code during a
  version's build (or serves unchanged, per projection).
- **D58:** reactors are not managed and are never rebuilt; a subscriber that must not act on old
  events can start each aggregate at its first delivery (`skipHistory`). (As built: not provided;
such subscribers stay unmanaged and see only live events.)
- **D60:** listeners that ignore undo events (Neula's two, H6) give the same result rebuilt as
  live; whether they should handle undo is the host's decision.

## Looking ahead

- **M5:** the supervisor is the unit that becomes the projection's cluster singleton (D53); a
  version's build and the active build may then run on different nodes (old and new code
  during a rollout), which the registry already separates. Chunk and delivery messages carry
  `Any` states, so AR4's serializable protocol covers them; push subscriptions cannot cross
  nodes and are renewed by their clients after a switch there.
- **M6:** an imported organization's projections are rebuilt with the targeted rebuild of one
  space; purge clears the organization's rows from the tables the registry lists for every
  active build; the complete rewrite (D76) triggers targeted rebuilds.
- **M7:** the migration guide carries the host steps (H9): wiring through the runtime, stores
  and state tables named by the build, `rebuilding` instead of rebuild-mode flags, one owner
  per shared state, reactors unmanaged, no offline rebuild mode.

## Tests

The plan's M4 verification, plus: the builder against both stores (undo, duplication,
squash, a late-committed row, an unreadable row); resume after a stop in each phase; the
switch under live traffic with parked queries and push subscriptions; adoption of 0.12 tables
and rows; retirement drops every named table; the targeted rebuild changes only its target.

As built: `ProjectionBuilderSpec` (the builder), `ProjectionsSpec` (builds, the switch, resume,
adoption, retirement, targeted rebuilds, a failing registry), `ProjectionDeliverySpec` and
`ProjectionGroupsSpec` (the projection's chunks and live groups), and the equivalence property
test `RebuildEquivalenceSpec`: a seeded random history of two aggregate types with undo,
duplication, deletion and squash, applied live by a reference projection, gives the same rows
(JSONB documents, compared row by row) in the build applied live, in a build made while the
active one serves and events keep coming, and in a build stopped midway and resumed. Each spec
was red under mutations of what it checks (the undo and duplication replay, the squash start,
the reconciliation, the version reset, the groups).

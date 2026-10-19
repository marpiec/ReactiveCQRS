# Changelog

Notable changes of ReactiveCQRS, newest first. Dates are DD/MM/YYYY.

## 1.0.1 (not released)

**1.0.1-RC1 (19/10/2026, tag `v1.0.1-RC1`):** a release candidate of everything below, for Neula's migration branch and
its rehearsal on a copy of production; the final 1.0.1 follows them.

Fixes from [the risk assessment of 1.0.0](docs/RISK-ASSESSMENT.md), in the order of its plan (§13), and the regressions
that [the second assessment](docs/RISK-ASSESSMENT-2.md) found in them.

- **Regressions of the fixes below, fixed** (F275, F279, F282, F304, F312, F314, chapters 9 and 17):
  - A command whose events put an undo before another event is stored again on PostgreSQL (refused since F195).
  - Idle `Local` owners are stopped as fast as new ones come (since F218, beyond 200 a second they grew without bound).
  - After a rollback the active build catches up also when the version rolled back to does not use `ServeStale`.
  - A transient database error during a reconciliation is retried in place, not by restarting the reconciliation.
  - **New:** `SkipFailedVersion(id, pastMissing = true)` moves past missing versions that exist but cannot arrive.
  - `java.time` values sent between nodes on their own are read again (refused since F240). **Changed:** a value no
    node reads (a plain class) is refused by the sending node, with an exception naming its class.
- **Sagas written as a `SagaActor`** run a failed step once and revert, as in 0.12: their commands carry no idempotency
  key, so a step run again could apply one twice (F285, chapter 12). A `SagaDefinition` runs a failed step again, so
  its commands must carry the step as their key.
- **E2 ends a node whose start fails:** it leaves the cluster and its process ends, to be started again, instead of
  staying a member that is never ready (F287, chapter 17; copy it in your host).
- **For the move from 0.12** (F278, F309, F336–F341, chapters 16 and 18, the runbook), found by the analysis of
  Neula's migration:
  - **Changed:** `Nothing` is back in `io.reactivecqrs.api`, a deprecated alias of `NoInfo`: code written for 0.12
    that says `Nothing` after `import io.reactivecqrs.api._` keeps its meaning, where it silently became
    `scala.Nothing`; the compiler names each use to rename. Say `scala.Nothing` where the bottom type is meant.
  - Many document stores set up at once no longer exhaust the pool: the threads of a node wait for the schema lock
    holding no connection.
  - A query condition on an empty list of values matches nothing, as in the memory store, instead of invalid SQL.
  - `backfillSpaces` leaves an aggregate whose first event's space throws (an old row without its field), instead of
    stopping; run it after the first start of 1.0, as on a 0.12 schema it does nothing.
  - `StoredDataCheck.ProjectionTable` reads a generic document class (`Wrapper[Item]`) as its store does.
  - A restored database whose statistics are missing no longer makes the first start build large indexes in a way that
    blocks writes.
  - **Release:** published to `neula-snapshots` for a snapshot version and `neula-releases` otherwise (`-DreleasesRepo`
    and `-DsnapshotsRepo` override them); only `api`, `core` and `testkit` are published.

- **No values in logs** (F190, F191). The library logs no SQL statements and describes a failure without the event, state,
  order or document it was about: by its classes, SQLState, constraint, table and column. The same text goes into
  `subscription_failures` and answers. `-Dreactivecqrs.log-values=true` shows the details again, for diagnosis. Add
  `logServerErrorDetail=false` to the JDBC URL (chapter 18).
- **Idempotency keys** longer than the 256 characters stored are refused before the handler runs (F252).
- **Start-up takes no table locks** when the schema is as it should be; a change it must make waits at most 2 s for its
  lock, tried again, then fails naming `pg_stat_activity` (F185).
- **Hung reads**: the outbox poller and the squash scheduler give up a read after `readTimeout` (5 minutes) and read
  again; a warning at start when the connection pool is smaller than the blocking dispatcher; the book and E2 set the
  driver's `socketTimeout`, `tcpKeepAlive` and a `validationQuery` (F186, F272).
- **The event bus starts without a subscriber that does not answer** within `DeliveryOptions.registrationTimeout`
  (2 minutes) or stops before it answers, logging an error; every event keeps its outbox row until it answers, then is
  published again. A publication that comes before the bus starts is published again from the outbox, not kept in
  memory. A release that adds a projection or saga no longer hangs a rolling upgrade (F187, F188, F255).
- **A new projection version's indexes** go to its own build's tables only; the active build keeps its indexes (F189).
- **Purges of space 0 and of negative spaces** are refused (F200); a squashed copy keeps its link to its base, so an
  erasure of the base still reaches it (F193).
- **Erasure reaches every copy of a value** (F192, F194, F195, F237, F244, F245, chapter 15):
  - `HistoryRewriter` has the projections rebuild every aggregate it visited, changed or not, so a copy that reads an
    erased base is rebuilt, and a run again repairs what an earlier run could not.
  - `RebuildAggregates` is no longer refused while a build or another targeted rebuild runs: it waits its turn, and a
    build in progress does it before it replaces the active build. Waiting rebuilds survive a stop.
  - A command whose handler ran across a rewrite runs again on the new state instead of writing the old value back,
    also when the rewrite ran on another node (a new form of the `add_events` procedure; the old one stays for nodes
    of 1.0.0).
  - `HistoryChange.inUnreadableEvent` erases a value from an event that can no longer be read.
  - An aggregate at `aggregateVersionLimit` can still be rewritten, squashed and deleted for good.
  - A snapshot is never written over a history that changed meanwhile.
- **Moving an organization** (F198, F199, F225, F226, F248, chapter 16):
  - The export reads the organization as one snapshot of the database, and its file (format 2) ends with a trailer
    of counts and a digest. **Source change:** `SpaceImport` takes the contexts of the space's types, as the export does.
  - The import refuses, before writing anything, a file cut short, changed or inconsistent (versions, chains, classes),
    and one of format 1 unless `acceptFormat1 = true`; it checks every event's size after its ids are mapped.
  - An exported id in the target installation's own range gets a new one, as its generator may hand it out; an import
    from another installation keeps its ids. Write batches end at 16 MB too.
  - `events (aggregate_id, version)` is unique: at start on a small store; run `buildIndexesConcurrently()` again on a
    large one. `StoredDataCheck` reports versions stored twice.
- **Projection failures** (F201, F254, F259, F261, chapters 6 and 9):
  - A database that is away (a restart, a failover) no longer leaves failure records: such errors are retried without
    counting.
  - A failure record left behind by a successful retry is cleared, not turned into a false "versions missing"
    failure; an owner asked for versions it does not have says so; a skip of a "versions missing" failure marks
    nothing; failures survive `ServeUnchanged`.
  - The history an owner gives to a projection that had nothing of an aggregate is applied as history: no pushes.
  - A permanent delete keeps the keys of the aggregate's stored responses (without their info) until they expire, so
    a retried keyed creation does not create the aggregate again.
- **Snapshots and types** (F196, F197, chapters 13 and 18):
  - A snapshot records the shape of its state class; one of another shape is not used, so a field added to the state
    no longer reads as zero or null. Snapshots written by 1.0.0 have no shape: each aggregate is replayed once.
  - An aggregate sent to another type's command bus is refused there (queries, commands, squashes), naming both
    types. `reactivecqrs.aggregate-type-check = "log"` only logs it; `StoredDataCheck.run(..., contexts = ...)`
    reports aggregate types none of the contexts has.
- **Sagas under faults** (F205, F206, F207, F232, chapter 12):
  - A step whose future fails runs again with its `SagaStep` key (`stepRetries`: 1 s doubling to 30 s, five attempts)
    before the saga reverts. A step throws `SagaStepNotDone` for an answer that may change and returns `SagaFailed`
    only for a refusal; the book's sagas do so.
  - A caller hears a saga's outcome only once it is stored; a progress write that keeps failing is tried again for
    as long as the runner runs (`reactivecqrs.saga.write.failed`), never dropped. `GetSagaStatus` reports running
    sagas and waiting orders.
  - Orders extending `IdempotentSagaOrder` carry an `orderKey`: a repeat joins the saga under way or gets its stored
    answer, kept `answersKeptFor` (30 days). New columns and a unique index on `sagas`, added at start.
  - A load does not start a running saga again (nor one just created), and writes compare the
    stored phase and step.
  - **Source change** for a host with its own `SagaState`: `createSaga` takes the order key and returns
    `SagaCreation`, writes take the phase and step they move from, new `finishSaga` and `removeFinished`,
    `loadAllSagas` gives a `StoredSaga`.
- **Drafts** (F208, F209, F236, F256, F257, chapter 20):
  - A materialization the persistent bus does not answer in time leaves the draft frozen, sending the same request
    again until an answer comes; `Materialize` then gets the outcome. Edits made after a timeout are no longer lost.
  - A materialization's key carries a random value of the bus's start (`MaterializationKey(id, start)`), so drafts of
    two starts with the same ids are never mixed up.
  - Ephemeral buses refuse new drafts once their ids are spent, also ones that waited, and no longer hang.
  - Drafts do not call the context's `WithOutboxWriter`; the in-memory stores roll back a failed transaction.
  - A command bus no longer restarts over a message whose handling throws (it logs and drops it).
- **Document store** (F210, F211, F273, chapters 8 and 19):
  - JSON paths are written into SQL so that no name can change the query: a map key holding a comma, a quote or a
    brace is one key, in conditions, sorts, parts and indexes.
  - **Changed:** a query without a limit (`-1`, `DocumentStoreQuery.basic`, `findDocumentByPath` and the other
    finders) fails with `TooManyDocumentsException` when it finds more than the store's `maxRows` (100 000, a new
    constructor parameter), instead of reading them all into memory. Page with `limit` and `offset`; for a small read
    model, `DocumentStoreQuery.Unbounded` or `findAll()`.
  - `MemoryDocumentStore` sorts, pages and limits as the PostgreSQL store does.
- **Delivery under load** (F212, F221, F222, F223, F228, F258, F260, F264, F268, chapter 9 and appendix B):
  - A bus tick visits only the deliveries that expire or stall, not every one waiting.
  - A subscriber that answers nothing past the deadline gets one message per deadline, not a new window.
  - In a cluster, a state listener's window counts the state each message carries.
  - The reconciliation and gap fills read the aggregates behind as they send them.
  - Status answers list at most 100 items, with totals (`gaveUpTotal`, `failuresTotal`); the bus keeps its latest 100
    undelivered.
  - **Changed:** managed projection instances are named `<projection>-build-<n>-<k>`: the `bus.behind` gauge's tag
    and the log lines name the projection (metric series are renamed).
  - The outbox poller reads the newest waiting aggregates too when more than `maxTracked` wait; one it first sees
    late is no longer reported undelivered 30 s after its re-publication; a full publication acknowledges events a
    re-publication to one subscriber left waiting.
- **Owners and projections under load** (F213, F214, F216, F218, F219, F220, F229, F230, F231, F234, F235, chapters 5,
  11 and 17):
  - An owner checks the stored version before it restores; a `Local` owner behind another node reads only the new
    events. A command expecting a version far ahead is a conflict at once.
  - A command past its timeout is never read for nor stored, however busy its owner; a failed version lookup fails
    only the request that asked.
  - Push subscriptions: subscribers that stop lose their subscriptions; at most 100 000 subscriptions and 10 000 cached
    updates; cancels and expiry no longer slow with their number.
  - Delayed queries are answered at their deadline while updates keep coming; a search that throws fails its own query.
  - The idle-owner sweep stops at most 200 owners at once; `AfterCommit` and type names take no JVM-wide lock;
    `backfillEventKeys` scans no whole table, pauses between batches (`pause`) and retries a failed batch; a projection
    switch whose answer was lost completes; a retired build's subscription rows go in batches.
- **Deployment and start-up** (F203, F204, F233, F240, F247, F249, F250, F269, chapters 17 and 18):
  - A managed build's table or sequence name past 63 characters is refused before it is created.
  - The aggregate-id generator refuses a node whose installation number does not fit ids given already, never moves
    a used sequence, and draws pools of the sequence's current step; a start never lowers the step.
  - **Changed:** a node is not ready before its first event bus exists, nor once its shutdown begins.
  - The cluster serializer reads only the library's classes and plain values or case classes, and refuses lengths
    past the frame.
  - A type name another node stored takes no id; commands are checked against the ordinary frame; a stack overflow in
    a handler or a restore fails that command or restore instead of the JVM.
  - The event store's procedures are versioned (`schema_steps`): an older node leaves newer ones as they are.
  - The book covers clocks and time zones (run the JVM on UTC, keep clocks in sync) and new message classes during a
    rolling upgrade (F270, F271, left open).
- **Remaining findings** (F224, F246, F251, F252, F253, F263, F265, F266, F267, chapters 4, 5, 7, 10, 14 and 20):
  - **Changed:** a write whose database connection fails is answered `CommandHandlingError` (outcome unknown), not
    `EventHandlingError` (nothing stored); read the aggregate, or use an idempotency key, before sending it again.
  - `keepNewerThan` squashes judge each aggregate by its own events' times, so an import no longer moves the boundary.
  - At most 1 000 version queries wait per aggregate, each at most 5 minutes; a negative `AggregateVersion`, store
    limits that are not positive, and saga caller paths over 256 characters are refused where they are made.
  - Smaller: draft responses kept for `idleTime`; size markers answered by the saga runner and projections; a refused
    rewrite without events answered at once; document-cache clears also after the commit; cached push updates replayed
    by type.
  - Left open, each with its reason in KNOWN-ISSUES: F238, F239, F243, F274.
- **Quick wins among the findings left open** (F202, F217, F227, F241, F242, F262, chapters 10, 11 and 16):
  - After a rollback that abandons a `ServeStale` build, the active build catches up by itself with what was committed
    meanwhile; `RebuildAggregates` is no longer needed.
  - **Upgrade step:** run `PostgresDocumentStore.buildSpaceIndexesConcurrently()` once, in the background, so a purge
    or rebuild of one organization no longer scans whole document tables; new tables get the index as they are made.
  - **Changed:** only a push subscription's own listener renews or cancels it; a persistent command bus refuses a
    `StoreFirstEvent` that does not come from a materialization.
  - `afterCommit` in a listener's first part waits for the commit of its second; an oversized message between nodes
    is written no further than the limit.

## 1.0.0 (18/10/2026)

The first release after 0.12.47. It rewrites how events reach their subscribers, how an aggregate lives
in memory, how read models are rebuilt and how the library runs on several nodes, and it adds
organizations, erasure, squash and a test kit. **Stored data stays**: 1.0 reads every table 0.12 wrote,
makes its schema changes at start (small, idempotent steps under a lock) or in background steps you run
once, and never needs a rewrite of the events table. The milestone builds `1.0.0-M2` to `1.0.0-M6`
led here.

Upgrading from 0.12: [the runbook](docs/UPGRADE-0.12-TO-1.0.md) goes through the move, a planned stop, step by
step; every change a host makes is listed, release by release, in [KNOWN-ISSUES.md, section 4](docs/KNOWN-ISSUES.md)
(H1–H12). The book, [*ReactiveCQRS, Step by Step*](docs/book/README.md), shows every feature
below with running code.

### Requirements

- Scala 2.13 (built with 2.13.18) and Java 21 or later; the library is compiled for Java 21, and its suites
  pass on Java 25 too.
- Apache Pekko 1.7.1, its cluster modules included; ScalikeJDBC 4.3.5; mpjsons 0.7.0.
- PostgreSQL; the library's suites and benches run on PostgreSQL 17.

### New

- **Delivery through an outbox** (chapters 8 and 9). Each committed event waits in `events_to_publish`
  until every subscriber acknowledged it; the event bus keeps no cursor, waits for every registered
  subscriber before it publishes, limits what a slow subscriber is sent (flow control) and
  `OutboxPollerActor` alone re-delivers what waits. A listener that keeps failing is recorded in
  `subscription_failures`, shown by `GetProjectionStatus`, and retried or skipped on request.
- **Aggregate owners** (chapters 4, 5 and 13). One actor holds an aggregate's state and version and runs its
  commands; idle owners are passivated. New: snapshots (`snapshots`), serial command processing
  (`commandProcessing = Serial(...)`, `CommandRejected` when the queue is full), a metadata envelope per
  event (`WithMetadata`: actor, correlation, trace context), rows of your own written with the events
  (`WithOutboxWriter`), handlers on a dispatcher of their own, and a command timeout.
- **Squash** (chapter 14). A squash event replaces an aggregate's oldest events, on request
  (`SquashAggregate`) or by rule (`SquashScheduler`); it must give back the state it replaces, and events
  of chosen classes move to an archive (`GetArchivedEvents`).
- **Managed projections** (chapter 10). `Projections.start` runs each read model in tables of its own per
  build: a new version is built from the store while the old one serves, then takes over in one update;
  `RebuildProjection`, `GetBuilds`, `GetBuildStatus`, targeted rebuilds of an aggregate, a type or an
  organization (`RebuildAggregates`), and `retire` for a projection removed from the code.
- **Subscribable projections** (chapter 11) push updates after their transaction commits.
- **Sagas as definitions** (chapter 12). A `SagaDefinition` holds a saga's steps and `SagaRunner` runs it,
  storing every step's order; a saga stored by a `SagaActor` resumes under the definition of its name.
- **The cluster** (chapter 17). Every installation is a Pekko cluster, of one node or many. Owners are
  `Local` (the default) or `Sharded` (one per aggregate in the cluster); what must run once (the outbox
  poller, the squash scheduler, saga runners, managed projections) runs as a singleton; every message
  between nodes goes through the library's serializer, with size limits; `Readiness` and
  `ReadinessCheck` for load balancers; rolling upgrades with app-versions and `writtenFrom` for new event
  classes.
- **Organizations** (chapter 16). Every aggregate belongs to a space, given by its first event; an
  organization can be exported, imported into another installation under new ids (`SpaceExport`,
  `SpaceImport`) and purged (`SpacePurge`).
- **Erasure** (chapter 15). A complete rewrite of an aggregate's history, its copies included
  (`HistoryRewriter`).
- **Ephemeral aggregates** (chapter 20). Drafts kept in one node's memory, materialized into stored
  aggregates when they matter.
- **Operations** (chapter 18). Metrics through `ReactiveMetrics` (no-op by default; `RecordingMetrics` in
  tests), `StoredDataCheck` for stored data, `buildIndexesConcurrently()`, `backfillSpaces()` and
  `backfillEventKeys()` as background steps, and the delivery, outbox and projection statuses.
- **A published test kit**, `reactivecqrs-testkit` (chapter 19): `TestDatabase` (a schema per test run),
  `TestSystems` (a cluster of one on request), `TestCluster` (several nodes in one JVM),
  `SchemaReference` (the golden test of stored formats), `TestClock`, `TestIds` and `askFor`.
- **The book and its examples.** Twenty chapters and five appendices in `docs/book`, whose code comes
  from the `examples` module (E1–E11, each runnable and tested), with a three-node deployment for Docker
  Compose and Kubernetes in `examples/deploy/e2`, and fault tests that kill, stop and cut off its nodes
  under load (`examples/deploy/e2/faults`). The book is also in Polish (`docs/book_pl`), with the same
  code.

### Changed (a host's code or configuration changes)

- **Wiring**: the host sends the event bus one `RegisterSubscribers` with every projection, saga and
  listener, creates one `OutboxPollerActor`, and calls each Postgres state's `initSchema()`.
  `EventBusSubscriptionsManagerApi` and `PostgresEventBusState` are gone, and so are the `event_bus` and
  `events_to_route` tables' uses.
- **Configuration**: `pekko.actor.provider = cluster` with the split brain resolver, on every node, also a
  single one, and `pekko.actor.serialization-bindings { "java.io.Serializable" = reactivecqrs }`: a node
  without the library's serializer for its messages stays not ready. On Kubernetes, nodes join through
  Pekko Management's Cluster Bootstrap rather than static seed nodes (the book's chapter 17).
- **Events**: no event versioning; `event_type_version` is always 0, and a changed event shape is a new
  class (`...V2`). A first event gives its organization's space. Undo events need no case in event
  handlers.
- **Messages between nodes** must be classes mpjsons writes and reads by their class: no fields typed
  `Any`, a trait, `ActorRef` or `Throwable`; a field added later is an `Option`.
- **Read models**: a read model to be rebuilt starts through `Projections`, naming its tables through the
  build; side effects stay in unmanaged projections or check `rebuilding`.
- **API**: `io.reactivecqrs.api.Nothing` is `NoInfo`; `OnConcurrentModification`, `SimulateEvent`,
  `EventHandler`, `IdentifiableEvent`, `EventIdentifier`, `EventsIdentifiers`, `EventWithIdentifier`,
  `AggregateVersion.upTo` and `isJustAfter`, and `LRUCache` are gone; `SubscriptionsState`'s per-kind version methods are `lastVersion` and `newVersion` with a
  `SubscriptionType`; `SagaActor` is deprecated in favour of `SagaDefinition`.
- **Tests**: `testutils` is the published `reactivecqrs-testkit`; its ask is `askFor[T](message)`.
- **Behaviour** a host may notice: a projection with two listeners of one kind for one type refuses to
  start; a `RegisterSubscribers` after the bus's start registers with a warning; finders inside a listener
  read in its transaction; the bus logs a stopped subscriber as an error only when nothing replaces it.

### Fixed

171 issues recorded and fixed, each with the test that showed it, are in
[the archive of closed issues](docs/KNOWN-ISSUES-ARCHIVE.md): among them events that were never re-delivered
while their aggregate stayed in memory (F2), a history rewrite left half-applied (F8), idempotent
commands that could run twice (F9), memory that grew with uptime in the bus (F1), and rebuild reads
that slowed down with the store's size (F172).

### Performance

Each milestone's bench run is in [docs/perf](docs/perf/README.md) (`baseline-M1` to `baseline-M6`, and
the high-load runs of M5); [docs/SCALING.md](docs/SCALING.md) reads them and gives limits and
recommendations for high volume and load.

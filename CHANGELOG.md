# Changelog

Notable changes of ReactiveCQRS, newest first. Dates are DD/MM/YYYY.

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

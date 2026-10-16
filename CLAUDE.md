# CLAUDE.md

Guidance for working in this repository. This file is intended as a deep map of the
codebase so it can be used to hunt for **correctness bugs** and **performance
problems**. Known problems, decisions and the plan of the 1.0 line live in
`docs/KNOWN-ISSUES.md` and `docs/IMPLEMENTATION-PLAN.md` (§7 below points there);
`docs/ARCHITECTURE.md` describes how the library works in detail.

> Note: written from a static read of 0.12.44 and updated for the 1.0 line at the end of
> milestone M1 (16/10/2026). Line numbers drift as files change — re-grep before quoting.

---

## 1. What this is

**ReactiveCQRS** is a Scala library implementing CQRS + Event Sourcing on top of
**Apache Pekko** actors (the project migrated from Akka; `org.apache.pekko.*`),
with **PostgreSQL** as the durable store accessed via **ScalikeJDBC**, and
**mpjsons** (`io.mpjsons`) for JSON (de)serialization of events and documents.

It is a *framework/library*, not a runnable application. The `testdomain` module
(a shopping-cart domain) is the canonical worked example and the main integration
test surface.

- Scala **2.13.18**, compiled with `-release 17`.
- Pekko **1.7.1**, ScalikeJDBC **4.3.5**, postgresql **42.7.14**, mpjsons **0.7.0**.
- Uses the **default ScalikeJDBC connection pool** (`DB.autoCommit`, `DB.readOnly`,
  `DB.localTx`). The connection pool **must be initialized by the host application
  before** any framework code runs (see README).

### Modules (`build.sbt`)
| Module | Path | Purpose |
|--------|------|---------|
| `api` | `api/` | Public domain API: `Aggregate`, `Event`, `Command`, results, ids. Depends only on Pekko. |
| `core` | `core/` | All the machinery: actors, event store, event bus, projections, sagas, document store, uid generator. Depends on `api`. |
| `testdomain` | `testdomain/` | Example shopping-cart domain + integration specs. Depends on `api`, `core`. |
| `testutils` | `testutils/` | `CommonSpec`, `TestDatabase` (a new schema per test run), actor-ask helpers. |
| `bench` | `bench/` | Performance bench and load driver (`sbt "bench/run <name>"`, reports in `docs/perf/`). Not published. |
| `utils` | `utils/` | Present in tree; commented out / mostly unused in `build.sbt`. |

`memory`/`postgres` modules are referenced as commented-out lines in `build.sbt` —
ignore them.

---

## 2. Build & test

SBT project. Common commands (run from repo root):

```bash
sbt compile              # compile all modules
sbt test                 # run all tests (ScalaTest)
sbt core/test            # tests for one module
sbt "testdomain/testOnly *ReactiveTestDomainSpec"
sbt "project core" "~compile"   # incremental
```

- Test framework: **ScalaTest 3.2.19**.
- **Postgres-backed tests require a running PostgreSQL**: database `reactivecqrs`, user and
  password `reactivecqrs` on localhost. `TestDatabase.init()` (testutils) gives each test run
  its own schema `test_run_<epoch>_<n>` (dropped after six hours); suites run one at a time.
  Memory-backed variants (`Memory*State`, `MemoryDocumentStore`) are test doubles.
- Publishing: `publishMavenStyle`, target `https://nexus.neula.in/...`
  (overridable via `-DsnapshotsRepo=`). Version lives in `project/Common.scala`
  (`version := "1.0.0-SNAPSHOT"`) — bump it there, not per-module.

---

## 3. Core domain concepts (the `api` module)

- **Aggregate root** — a plain immutable case class (e.g. `ShoppingCart`). The
  framework holds it as `Option[AGGREGATE_ROOT]` (`None`/`null` = deleted).
- **`AggregateContext[AGGREGATE_ROOT]`** (`api/.../AggregateContext.scala`) — the
  unit a domain author implements. Supplies:
  - `initialAggregateRoot`
  - `commandHandlers: AGGREGATE_ROOT => PartialFunction[Any, GenericCommandResult]`
  - `eventHandlers: (UserId, Instant, AGGREGATE_ROOT) => PartialFunction[Any, AGGREGATE_ROOT]`
  - `version` (no event versioning since 1.0, D29: a changed event shape is a new class).
  - Optional features: `snapshots` (D30), `commandProcessing` (R8), `squash` (D47: a squash
    event that replaces events 1..N at version N, on request with `SquashAggregate` or by the
    `SquashScheduler`; reads, undos and duplications below the squash point are refused).
  - See `testdomain/.../ShoppingCartAggregateContext.scala` for the pattern.
- **Commands** (`api/.../Command.scala`) come in flavors that drive routing:
  - `FirstCommand` — creates a new aggregate (no id yet).
  - `Command` — has `expectedVersion` → strict optimistic-lock check.
  - `ConcurrentCommand` — no expected version; uses current version and
    **auto-retries** on concurrent-modification.
  - `RewriteHistoryCommand` / `RewriteHistoryConcurrentCommand` — rewrite past events.
  - `IdempotentCommand[IID]` with optional `idempotencyId` → response dedup.
    `SagaStep(sagaId, step)` is the canonical idempotency key.
- **Command results** (`api/.../CommandResult.scala`): `CommandSuccess`,
  `CommandFailure`, `RewriteCommandSuccess`, and `AsyncCommandResult` (wraps a
  `Future`) — async handlers return the latter.
- **Events** (`api/.../Event.scala`):
  - `Event[AGGREGATE_ROOT]` — base, `Serializable`, carries a `ClassTag` of its root (D75);
    type keys come from `AggregateType.of[R]`.
  - `FirstEvent` — must define `spaceId` (events that create an aggregate).
  - `UndoEvent(eventsCount)` — logically cancels the last N events (see "undo").
  - `DuplicationEvent` — creates a new aggregate as a copy of another at a version
    (the "duplication chain" — see schema below).
  - `PermanentDeleteEvent` — hard-deletes aggregate + events.
- **Versioning**: `AggregateVersion` is an `Int` starting at `ZERO`. Each persisted
  event increments it by one (the owner's `version`; on restore the version is read from the
  stored events, which a squash leaves in place).

---

## 4. End-to-end flow (the actor pipeline)

Wiring example: `testdomain/.../ReactiveTestDomainSpec.scala:52-87` shows how a
host assembles the system (uid generator, event bus, command bus, projections,
sagas).

### Write path (command → events)
```
caller
  → AggregateCommandBusActor            (per aggregate TYPE; top-level router)
      routes by command flavor, allocates AggregateId/CommandId from pools
  → AggregateOwner                      (per aggregate INSTANCE; owns state+version; M3, AR1)
      idempotency dedup; version check (reload when a newer version is expected)
      runs the domain commandHandler as a Future → CommandResult (events);
      a timer answers CommandHandlingError after commandTimeout (the result is then dropped)
      eventStore.persistEvents(...) inside a localTx  (optimistic lock in SQL fn)
      the new state (handlers run once, on a copy) becomes the owner's after commit
      → EventsBusActor  ! PublishEvents   → answers the caller
```

### Read path (state)
The owner answers `GetAggregate`, `GetEventsForAggregate` and `GetAggregateMinVersion` (held
until the aggregate reaches the version, or answered with a failure at its deadline, F81).
The command bus answers historical reads (`GetAggregateForVersion`, `GetAggregateAtInstant`,
`GetEventsForAggregateForVersion`) itself with `AggregateRestorer`, in a `Future`.

### Event publication / projections
```
AggregateOwner → EventsBusActor → subscribers (ProjectionActor, sagas,
                                                          AggregateListenerActor)
  At start the host sends the bus RegisterSubscribers; the bus asks each subscriber
  for its subscriptions (GetSubscriptionRequests) and publishes nothing until all
  have answered. Command buses register as their type's publishers (RegisterPublisher).
  EventsBusActor keeps one delivery per event until every subscription (subscriber and
  DeliveryKind) has answered MessageAck or MessageFailed, the deadline passes, or the
  subscriber stops; only acks and failures acknowledge the publication (PublishEventsAck)
  to the owner's command bus, which deletes the outbox rows (the owner may be reaped).
  No cursor (D7). Re-publications go in parts of 1 000 events.
  OutboxPollerActor (one per system) alone re-delivers: every 30 s it reads the
  outbox and has owners re-publish (Republish via the bus) at 3 and 6 minutes, then
  reports "undelivered" at 10; rows from before the start go out at the first poll.
  ProjectionActor updates a DocumentStore and records progress in subscriptions.
```
Design of the delivery work in M2: `docs/design/M2-delivery.md`.

### Key actors
- **`AggregateCommandBusActor`** (`core/.../commandhandler/`) — one per aggregate
  type. Keeps one `AggregateOwner` per aggregate; stops idle ones (default keep-alive 200,
  sweep every 5 min) by passivation: `StopOwner`, keeping the messages that arrive meanwhile
  for a new owner. Deletes delivered outbox rows (acks come to it, F91).
  Allocates ids from pools of 10 000 fetched ahead (`IdPool`, F23: no `Await`; a command waits
  for a late pool, and a failing generator is answered with `CommandHandlingError`, F75).
- **`AggregateOwner`** (`core/.../aggregate/`) — the source of truth for one aggregate
  instance (replaced the command handler, the per-command executors and the repository
  actor in M3). On first access it restores the root and version through `AggregateRestorer`:
  from the snapshot and the events after it when the context opts in (D30), otherwise by
  replaying the *entire* event stream; then it keeps them updated. With `commandProcessing = Serial(...)`
  (R8) it runs one command at a time from a bounded queue. A context mixing in
  `WithOutboxWriter` writes rows of its own in the transaction of the events (R12). `aggregateVersionLimit` defaults to
  10 000 events per aggregate. Design: `docs/design/M3-aggregates.md`.
- **`EventsBusActor`** — in-memory routing and delivery tracking (deadline, stall
  logging, `GetDeliveryStatus`); routes re-publish requests to the type's command bus;
  back-pressure for the replayer (`DeliveryOptions.maxReplayBuffer`, 10 000).
- **`OutboxPollerActor`** (`core/.../delivery/`) — the single owner of re-deliveries
  (D8); its timing lives in the pure `RedeliveryPolicy`. The host creates one per system.
- **`SquashScheduler`** (`core/.../aggregate/`) — squashes aggregates by their contexts'
  `AutomaticSquash` rule (D52), a limited number per run, one at a time. The host creates one
  per system.
- **`ProjectionActor` / `SubscribableProjectionActor`** — apply events to read
  models. One queue per (listener kind, aggregate) through the `SequenceGate`:
  duplicates are acked, new versions applied in one transaction with the new
  subscription version, gaps asked from the owner (`RequestRepublish`). A failing
  listener is retried about 2 minutes, then recorded in `subscription_failures` and
  answered `MessageFailed` (D20a); `GetProjectionStatus`, `RetryFailedSubscription`,
  `SkipFailedVersion`; failures are retried at start, one per second.
- **`SagaActor`** — process managers; `CONTINUES`/`REVERTING`/`ERROR` phases.
- **`UidGeneratorActor`** — hands out id pools (`PostgresUidGenerator` reads
  `NEXTVAL`/`increment_by` from a sequence; pool size = sequence step).

---

## 5. PostgreSQL schema

Schema is created in code by `*SchemaInitializer` classes (idempotent
`CREATE ... IF NOT EXISTS`). Concurrency control for events lives in **stored
procedures** (`add_event`, `add_undo_event`, `add_duplication_event`) defined in
`PostgresEventStoreSchemaInitializer.scala`.

### Event store (`PostgresEventStoreSchemaInitializer.scala`)
- **`events`** — `id (events_seq) PK, user_id, aggregate_id, event_time, version,
  event_type_id, event_type_version, event TEXT(json), metadata JSONB` (the
  envelope of D32; null when none).
  Indices: `events(aggregate_id)`, `events(aggregate_id, id)`.
- **`aggregates`** — PK `(id, base_id)`, plus `space_id, creation_time, type_id,
  base_order, base_id, base_version, squashed_version` (the squash point, D49, D51). The `base_id`/`base_order`/`base_version`
  columns implement **duplication chains**: a duplicated aggregate shares history
  with its base up to a version, then diverges. `base_version` is the current
  version and is what the stored procedures compare for optimistic locking.
  Indices: `aggregates(type_id)`, `aggregates(base_id)`.
- **`noop_events`** — `id (→events.id) PK, from_version`. Marks events that became
  no-ops (e.g. due to undo) from a given version.
- **`snapshots`** (M3, D30) — `aggregate_id PK, version, context_version, schema_version,
  state, created`: the latest performance snapshot of an aggregate; disposable.
- **`events_to_publish`** — `event_id PK, aggregate_id, version, user_id,
  event_time`. The outbox: owners publish what they committed, delivered rows are
  deleted in batches (F26), the outbox poller re-publishes waiting ones. Indexed on
  `(aggregate_id, version)`.

### Event bus tables (unused since 1.0)
- **`event_bus`** (the 0.12 cursor of the last published version per aggregate) and
  **`events_to_route`** are no longer read or written: the bus keeps no cursor (D7).
  Existing databases keep them until a later contract step (D24).

### Projections / subscriptions / sagas / commands / documents
- **`subscriptions`** (`SubscriptionsState.scala`) — per (subscriber type,
  subscription type, aggregate) last-processed version. Optimistic locking by
  version compare. Index on `aggregate_id`; unique on
  `(subscriber_type_id, subscription_type, aggregate_id)`. Live mode caches the versions
  of at most 10 000 aggregates (LRU); the database decides.
- **`subscription_failures`** (since 1.0, D20a) — per subscription, the aggregate it gave
  up on: failed version, error, since. A permanent delete waits while the aggregate has
  a row here.
- **`sagas`** (`PostgresSagaSchemaInitializer.scala:12`) — saga progress. The table
  *is* auto-created (`CREATE TABLE IF NOT EXISTS sagas`), with a unique index on
  `(name, saga_id)` (since 1.0, F34).
- **`commands_responses`** (`CommandResponseState.scala`) — idempotent command
  responses keyed by idempotency key: unique index on `key` (since 1.0, F9), response info
  in `info`/`info_type_id` (F64), rows removed after a retention period.
- **`projection_<name>`** (`PostgresDocumentStore.scala`) — read models stored as
  `JSONB` with `space_id, id PK, version, document`. Supports dynamically created
  GIN/expression indices over JSON paths. Auto-migrates (adds `space_id`, drops
  legacy `metadata`).

---

## 6. Conventions & gotchas for editing

- **Pekko, not Akka.** Imports are `org.apache.pekko.*`. Don't add `akka` deps.
- **Actor state is `var` + mutable collections**, relying on single-threaded actor
  semantics. This is safe *only* inside `receive`. **Be very careful with
  `Future {...}` blocks and `onComplete` callbacks** inside actors: they run on a
  dispatcher thread, not the actor thread, so they must **not** touch the actor's
  `var`s/mutable maps. Several places use fire-and-forget Futures — check whether
  they close over actor state.
- **Blocking work** (JDBC) runs on `reactivecqrs.blocking-dispatcher` (`BlockingDispatcher`, F27): owners
  run on it; other actors run their JDBC in futures on it and get the result back as a message.
  Command handlers run on the default dispatcher. Never touch `context` from a callback.
- **`sender()` capture**: never call `sender()` inside a `Future`/callback; capture
  it into a val first.
- **SQL** is written with ScalikeJDBC. Two styles coexist: the `sql"..."`
  interpolator (binds params safely) and `SQL(s"...")` with manual string building
  (used for dynamic IN-lists, index/table names, JSON paths). The latter is where
  injection risk lives — table/index/JSON-path fragments are interpolated, not
  bound. Keep identifiers validated (see `PostgresDocumentStore` uniqueId checks).
- **Serialization**: events are stored as JSON text via mpjsons keyed by
  `event_type_id` (the class name); `event_type_version` is always 0 (D29). Changing an
  event class shape breaks replay: add a new class (`...V2`) and keep the old one.
- **Snapshots are opt-in** (D30, `AggregateContext.snapshots`): without them an aggregate is
  rebuilt by replaying all its events. A change that alters the state behind a snapshot
  (undo, rewrite, squash, permanent delete) must delete it in its own transaction.
- **Logging**: use `MyActorLogging` / slf4j. There are stray `println`s in
  production paths (saga / uid generator) — prefer the logger when touching them.
- **`asInstanceOf`** is used liberally to recover generic event types after
  deserialization; type mismatches fail at runtime, not compile time.

---

## 7. Known problems and planned work

The catalogue of known problems that used to live here moved to `docs/KNOWN-ISSUES.md`
(decision D5): every issue (F-numbers) with severity, confidence, the fix milestone and its
status, the decisions (D-numbers), the invariants (I1–I9) and the architecture reviews
(AR-numbers). `docs/IMPLEMENTATION-PLAN.md` orders the work into milestones M1–M7 and keeps
a progress log. Before changing behaviour, check both: a problem may already be fixed, or
planned in a way that the change must follow. Measured performance is in `docs/perf/`.

---

## 8. Where to look first

| You want to understand… | Start here |
|---|---|
| Public API a domain author implements | `api/.../AggregateContext.scala`, `Command.scala`, `Event.scala`, `CommandResult.scala`; example `testdomain/.../shoppingcart/` |
| How a command becomes events | `core/.../commandhandler/AggregateCommandBusActor.scala`, `core/.../aggregate/AggregateOwner.scala` |
| Aggregate state & persistence | `core/.../aggregate/{AggregateOwner,AggregateRestorer}.scala` |
| Squash and duplication chains | `ARCHITECTURE.md` §4.6, `AggregateOwner.squashHistory`, `PostgresEventStoreState.squash`, `add_duplication_event` |
| Durable events + optimistic lock SQL | `core/.../eventstore/Postgres*` |
| Event delivery, acks, back-pressure | `core/.../eventbus/EventsBusActor.scala`, `core/.../delivery/*`, `docs/design/M2-delivery.md` |
| Read models | `core/.../projection/*`, `core/.../documentstore/*` |
| Process managers | `core/.../saga/*` |
| Full wiring of a system | `testdomain/.../spec/ReactiveTestDomainSpec.scala` |
| Replay / rebuild | `core/.../eventsreplayer/*`, `ReplayAggregateRepositoryActor.scala` |

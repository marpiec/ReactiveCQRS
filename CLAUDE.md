# CLAUDE.md

Guidance for working in this repository. This file is intended as a deep map of the
codebase so it can be used to hunt for **correctness bugs** and **performance
problems**. The final section ("Known hotspots / risk areas") catalogs concrete
concerns with `file:line` references — treat those as leads, not confirmed bugs;
verify each against current code before acting.

> Note: this document records observations from a static read of the code (as of
> version 0.12.40). Line numbers drift as files change — re-grep before quoting.

---

## 1. What this is

**ReactiveCQRS** is a Scala library implementing CQRS + Event Sourcing on top of
**Apache Pekko** actors (the project migrated from Akka; `org.apache.pekko.*`),
with **PostgreSQL** as the durable store accessed via **ScalikeJDBC**, and
**mpjsons** (`io.mpjsons`) for JSON (de)serialization of events and documents.

It is a *framework/library*, not a runnable application. The `testdomain` module
(a shopping-cart domain) is the canonical worked example and the main integration
test surface.

- Scala **2.13.18**, compiled with `-target:jvm-1.8`.
- Pekko **1.4.0**, ScalikeJDBC **3.5.0**, postgresql **42.7.10**, mpjsons **0.6.50**.
- Uses the **default ScalikeJDBC connection pool** (`DB.autoCommit`, `DB.readOnly`,
  `DB.localTx`). The connection pool **must be initialized by the host application
  before** any framework code runs (see README).

### Modules (`build.sbt`)
| Module | Path | Purpose |
|--------|------|---------|
| `api` | `api/` | Public domain API: `Aggregate`, `Event`, `Command`, results, ids. Depends only on Pekko. |
| `core` | `core/` | All the machinery: actors, event store, event bus, projections, sagas, document store, uid generator. Depends on `api`. |
| `testdomain` | `testdomain/` | Example shopping-cart domain + integration specs. Depends on `api`, `core`. |
| `testutils` | `testutils/` | `CommonSpec`, actor-ask helpers for tests. |
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
- **Postgres-backed tests require a running PostgreSQL** and an initialized
  ScalikeJDBC pool (see `testdomain/src/test/resources/application.conf` and
  `testutils`). Memory-backed variants (`Memory*State`, `MemoryDocumentStore`)
  exist for tests that don't need a DB.
- Publishing: `publishMavenStyle`, target `https://nexus.neula.in/...`
  (overridable via `-DsnapshotsRepo=`). Version lives in `project/Common.scala`
  (`version := "0.12.40"`) — bump it there, not per-module.

---

## 3. Core domain concepts (the `api` module)

- **Aggregate root** — a plain immutable case class (e.g. `ShoppingCart`). The
  framework holds it as `Option[AGGREGATE_ROOT]` (`None`/`null` = deleted).
- **`AggregateContext[AGGREGATE_ROOT]`** (`api/.../AggregateContext.scala`) — the
  unit a domain author implements. Supplies:
  - `initialAggregateRoot`
  - `commandHandlers: AGGREGATE_ROOT => PartialFunction[Any, GenericCommandResult]`
  - `eventHandlers: (UserId, Instant, AGGREGATE_ROOT) => PartialFunction[Any, AGGREGATE_ROOT]`
  - `eventsVersions` (event schema versioning via `EV(...)`) and `version`.
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
  - `Event[AGGREGATE_ROOT]` — base, `Serializable`, carries a `TypeTag`.
  - `FirstEvent` — must define `spaceId` (events that create an aggregate).
  - `UndoEvent(eventsCount)` — logically cancels the last N events (see "undo").
  - `DuplicationEvent` — creates a new aggregate as a copy of another at a version
    (the "duplication chain" — see schema below).
  - `PermanentDeleteEvent` — hard-deletes aggregate + events.
- **Versioning**: `AggregateVersion` is an `Int` starting at `ZERO`. Each persisted
  event increments it by one (`AggregateRepositoryActor` `version.increment`).

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
  → CommandHandlerActor                 (per aggregate INSTANCE; idempotency dedup)
  → CommandExecutorActor                (ephemeral, one per command; 60s self-PoisonPill)
      asks AggregateRepositoryActor for current Aggregate state
      runs the domain commandHandler → CommandResult (events)
  → AggregateRepositoryActor            (per aggregate INSTANCE; owns state+version)
      eventStore.persistEvents(...) inside a localTx  (optimistic lock in SQL fn)
      applies eventHandlers to update in-memory aggregateRoot + version
      → EventsBusActor  ! PublishEvents
  → response flows back to the original caller
```

### Read path (state)
`AggregateRepositoryActor` answers `GetAggregate`, `GetAggregateForVersion`,
`GetAggregateAtInstant`, `GetAggregateMinVersion` (the last one can be *delayed*
until the aggregate reaches a version — see `delayedQueries`).

### Event publication / projections
```
AggregateRepositoryActor → EventsBusActor → subscribers (ProjectionActor, sagas,
                                                          AggregateListenerActor)
  EventsBusActor tracks in-flight messages, waits for MessageAck, then marks the
  event published in the event_bus table (PostgresEventBusState).
  ProjectionActor updates a DocumentStore and records progress in subscriptions.
```

### Key actors
- **`AggregateCommandBusActor`** (`core/.../commandhandler/`) — one per aggregate
  type. Caches child `CommandHandlerActor`/`AggregateRepositoryActor` refs in
  mutable maps, reaps idle ones (default keep-alive 200, sweep every 5 min).
  Allocates ids from pools via the UID generator (currently with a blocking
  `Await` — see hotspots).
- **`AggregateRepositoryActor`** — the source of truth for one aggregate instance.
  **No snapshotting**: on first access it replays the *entire* event stream
  (`restoreState` → `eventStore.readAndProcessEvents`) to rebuild
  `aggregateRoot`/`version` in memory, then keeps them updated. `aggregateVersionLimit`
  defaults to 10 000 events per aggregate.
- **`EventsBusActor`** — in-memory routing + ack tracking with a DB-backed
  `event_bus` cursor; implements consumer back-pressure (`MAX_BUFFER_SIZE` 1000).
- **`ProjectionActor` / `SubscribableProjectionActor`** — apply events to read
  models; handle out-of-order delivery via delayed/merge buffers.
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
  event_type_id, event_type_version, event TEXT(json)`.
  Indices: `events(aggregate_id)`, `events(aggregate_id, id)`.
- **`aggregates`** — PK `(id, base_id)`, plus `space_id, creation_time, type_id,
  base_order, base_id, base_version`. The `base_id`/`base_order`/`base_version`
  columns implement **duplication chains**: a duplicated aggregate shares history
  with its base up to a version, then diverges. `base_version` is the current
  version and is what the stored procedures compare for optimistic locking.
  Indices: `aggregates(type_id)`, `aggregates(base_id)`.
- **`noop_events`** — `id (→events.id) PK, from_version`. Marks events that became
  no-ops (e.g. due to undo) from a given version.
- **`events_to_publish`** — `event_id PK, aggregate_id, version, user_id,
  event_time`. The outbox consumed by the event bus. **No secondary index defined**
  even though it is queried by `aggregate_id` and joined frequently.

### Event bus (`EventBusState.scala`, `EventBusSchemaInitializer.scala`)
- **`event_bus`** — `id (event_bus_seq) PK, aggregate_id, aggregate_version` —
  cursor of "last published version per aggregate". Unique indices on
  `(aggregate_id)` and `(aggregate_id, aggregate_version)`.
- **`events_to_route`** — `id PK, aggregate_id, version, message_time, subscriber,
  message_type, message bytea`. Unique index `(aggregate_id, version, subscriber)`.

### Projections / subscriptions / sagas / commands / documents
- **`subscriptions`** (`SubscriptionsState.scala`) — per (subscriber type,
  subscription type, aggregate) last-processed version. Optimistic locking by
  version compare. Index on `aggregate_id`; unique on
  `(subscriber_type_id, subscription_type, aggregate_id)`.
- **`sagas`** (`PostgresSagaState.scala`) — saga progress. ⚠ schema is **not
  auto-created** here the way other tables are; verify it exists.
- **`commands_responses`** (`CommandResponseState.scala`) — idempotent command
  responses keyed by idempotency key (see hotspot about a mismatched index).
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
- **`sender()` capture**: never call `sender()` inside a `Future`/callback; capture
  it into a val first.
- **SQL** is written with ScalikeJDBC. Two styles coexist: the `sql"..."`
  interpolator (binds params safely) and `SQL(s"...")` with manual string building
  (used for dynamic IN-lists, index/table names, JSON paths). The latter is where
  injection risk lives — table/index/JSON-path fragments are interpolated, not
  bound. Keep identifiers validated (see `PostgresDocumentStore` uniqueId checks).
- **Serialization**: events are stored as JSON text via mpjsons keyed by
  `event_type_id` + `event_type_version`. Changing an event class shape without a
  version mapping in `eventsVersions` breaks replay/deserialization.
- **No aggregate snapshots**: aggregates are rebuilt by replaying all their events.
  Long-lived/high-version aggregates are a latency and memory concern by design.
- **Logging**: use `MyActorLogging` / slf4j. There are stray `println`s in
  production paths (saga / uid generator) — prefer the logger when touching them.
- **`asInstanceOf`** is used liberally to recover generic event types after
  deserialization; type mismatches fail at runtime, not compile time.

---

## 7. Known hotspots / risk areas

These are leads gathered from a deep read, grouped by theme. **Each needs
verification** against current code before you treat it as a real bug. Line numbers
are approximate.

### A. Blocking calls inside actors (thread-starvation risk)
- `AggregateCommandBusActor.scala:~318-337` — `Await.result(uidGenerator ? ..., 60s)`
  when an id pool runs out. Blocks the whole command bus for that type. TODO
  "get rid of ask pattern" is in-code.
- `SagaActor.scala:~136-152` — same `Await.result` pattern in `takeNextSagaId`.
- `EventsBusActor.scala:~189-191` — `Await.result(subscriptionsManager.getSubscriptions, 180s)`
  during init; freezes the bus if the subscriptions manager is slow.
- `AggregateRepositoryActor` persistence runs a synchronous `eventStore.localTx`
  on the actor thread (by design, but it serializes the aggregate and blocks it
  during DB I/O).

### B. Optimistic-locking / correctness
- `EventBusState.scala:~126-149` (`flushUpdates`) — batch `UPDATE event_bus ...
  WHERE aggregate_version = ?` does **not check affected row count** (in-code TODO
  "handle optimistic locking!!!!"). A lost update leaves the cursor stale →
  events skipped or re-published.
- `CommandHandlerActor.scala:~95-106` — idempotency check is read-then-act, not
  atomic; two concurrent identical commands can both miss the cached response and
  execute. Confirm whether the DB unique key / version check downstream saves it.
- `CommandExecutorActor.scala:~99` — in-code TODO "handling concurrent command is
  not thread safe".
- `EventsBusActor.scala:~290` — in-code TODO "filter out events received again if
  sender resent them": no dedup of re-published events → duplicate delivery to
  subscribers.
- `ProjectionActor.scala:~249-274` — complex `&&`/`||` ordering condition gating
  whether an update is delayed; worth checking precedence and the
  "already processed but below minimumDelayVersion" branch for gap-detection
  skips.

### C. Memory growth / unbounded collections
- `CommandResponseState.scala:~16` — `MemoryCommandResponseState` cache grows
  forever (no eviction). Test/in-memory only, but watch it.
- `EventsBusActor.scala:~91-95` — `messagesSent`, `eventSenders`,
  `eventsPropagatedNotPersisted`, `publishedEvents` HashMaps: entries removed only
  on full ack. A dead/slow subscriber leaks these indefinitely; old unacked
  entries are logged but never evicted.
- `AggregateCommandBusActor.scala:~84-86` — child-actor caches; reaped on a timer
  but can hold up to keep-alive limit continuously under churn.
- `ProjectionActor.scala:~730` and `AggregateRepositoryActor` `delayedQueries` —
  unbounded lists of pending min-version queries, only drained when timeouts fire.
- `EventBusSubscriptionsManager` — no unsubscribe path; subscriptions only grow
  and are lost on restart (not persisted).
- `MemoryDocumentStore.scala:~13` — `ParHashMap` store, never evicted; also
  `insertDocuments` batch overloads are `???` (unimplemented).

### D. SQL / query performance
- **Missing index on `events_to_publish`** (no `(aggregate_id)` index) — the outbox
  is scanned/joined frequently; full scans as it grows. (`EventBusSchemaInitializer`,
  `PostgresEventStoreState` outbox queries.)
- `PostgresEventStoreState.scala:~158` — in-code TODO: version/instant-filtered
  reads fetch **all** events then filter in app code instead of in SQL.
- `PostgresEventStoreState.scala:~303-318` — `countAllEvents` uses `pg_class.reltuples`
  (approx) but falls back to `COUNT(*)` (O(n)); `countEventsForAggregateTypes` joins
  without an ideal `(type_id, base_order)` index.
- `PostgresDocumentStore.scala:~423` — `getDocuments` does `loaded.find(_._1 == id)`
  inside a per-key loop → O(n²); build a `Map` first.
- `AggregateRepositoryActor.scala:~95,136` — `.distinct` on the pending-publish
  `List` is O(n²) per publish.
- `PostgresEventStoreState.scala:~241-248` (`PermanentDeleteEvent`) — deletes
  `events` **before** the `DELETE FROM noop_events WHERE id IN (SELECT id FROM
  events WHERE aggregate_id = ?)` subquery runs, so the subquery matches nothing
  and **noop_events rows are never deleted** (leak + inconsistency). Verify order.

### E. Schema / migration
- `CommandResponseState.scala:~37-52` — the unique index references an
  `aggregate_id` column that the `commands_responses` table definition doesn't
  appear to declare; index creation may fail. Verify the actual `CREATE TABLE`.
- `PostgresSagaState.scala` — no `CREATE TABLE IF NOT EXISTS` for `sagas` (unlike
  the document store); confirm the table is created somewhere.
- `PostgresDocumentStore` migration (add `space_id`, drop `metadata`, rebuild
  indices) is **not wrapped in one transaction** and can lock tables on large
  read models.

### F. Cross-instance cache coherency
- `PostgresDocumentStore` and `PostgresSubscriptionsState` keep **in-memory caches**
  with no cross-instance invalidation. In a multi-node deployment, one node's
  writes won't invalidate another's cache → stale reads. Single-node assumption is
  implicit.

### G. Error handling
- `UidGeneratorActor` and `SagaActor` use `println(... , e)` / throw inside
  `Future.onComplete` — exceptions there are **not** caught by actor supervision
  and may be silently lost. Saga creation has no retry on persistence failure.

---

## 8. Where to look first

| You want to understand… | Start here |
|---|---|
| Public API a domain author implements | `api/.../AggregateContext.scala`, `Command.scala`, `Event.scala`, `CommandResult.scala`; example `testdomain/.../shoppingcart/` |
| How a command becomes events | `core/.../commandhandler/{AggregateCommandBusActor,CommandHandlerActor,CommandExecutorActor}.scala` |
| Aggregate state & persistence | `core/.../aggregaterepository/AggregateRepositoryActor.scala` |
| Durable events + optimistic lock SQL | `core/.../eventstore/Postgres*` |
| Event delivery, acks, back-pressure | `core/.../eventbus/EventsBusActor.scala`, `EventBusState.scala` |
| Read models | `core/.../projection/*`, `core/.../documentstore/*` |
| Process managers | `core/.../saga/*` |
| Full wiring of a system | `testdomain/.../spec/ReactiveTestDomainSpec.scala` |
| Replay / rebuild | `core/.../eventsreplayer/*`, `ReplayAggregateRepositoryActor.scala` |

# CLAUDE.md

Guidance for working in this repository. This file is intended as a deep map of the
codebase so it can be used to hunt for **correctness bugs** and **performance
problems**. The final section ("Known hotspots / risk areas") catalogs concrete
concerns with `file:line` references — treat those as leads, not confirmed bugs;
verify each against current code before acting.

> Note: this document records observations from a static read of the code (as of
> version 0.12.44). Line numbers drift as files change — re-grep before quoting.

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
  (`version := "0.12.44"`) — bump it there, not per-module.

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
- **`sagas`** (`PostgresSagaSchemaInitializer.scala:12`) — saga progress. The table
  *is* auto-created (`CREATE TABLE IF NOT EXISTS sagas`), but it has **no primary key
  and no index** → `loadAllSagas` (`WHERE name = ?`) and per-saga update/delete
  full-scan. See §7.D.
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

## 7. Known hotspots / risk areas (performance & integrity)

This section is the **performance map**: concrete latency, throughput, memory, and
integrity concerns found by a deep read, **re-verified against v0.12.44**. Treat each
as a *lead, not a confirmed bug* — line numbers still drift, so re-grep before acting.
**Severity is the author's estimate** (not load-tested): it weighs how hot the path
is, whether it blocks the actor thread, and blast radius. Suggested fixes are
starting points, not vetted designs.

### Severity-ranked summary

| Sev | Area | Concern | Where |
|-----|------|---------|-------|
| 🔴 Critical | Integrity | `event_bus` batch UPDATE ignores rowcount, cache cleared regardless → stale cursor / lost update | `EventBusState.scala:127-140` |
| 🔴 Critical | Latency | No snapshots: every repository actor start replays the **entire** event stream on the actor thread | `AggregateRepositoryActor` restore; `PostgresEventStoreState.scala:117-176` |
| 🟠 High | Throughput | `Await.result(…, 60s)` blocks the command bus when an id pool drains | `AggregateCommandBusActor.scala:315-348` |
| 🟠 High | Throughput | `Await.result(…, 60s)` blocks the saga actor per new saga when its id pool drains | `SagaActor.scala:136-152` |
| 🟠 High | Latency | Synchronous `eventStore.localTx` runs on the actor thread during persist | `AggregateRepositoryActor.scala:284-320` |
| 🟠 High | SQL | **Missing index** on `events_to_publish(aggregate_id)` → outbox full-scans | `PostgresEventStoreSchemaInitializer.scala:83` |
| 🟠 High | SQL | **No PK/index** on `sagas` → `loadAllSagas` full-scans on every saga-actor start | `PostgresSagaSchemaInitializer.scala:12` |
| 🟠 High | SQL | Version/instant reads fetch **all** events then filter + deserialize in app | `PostgresEventStoreState.scala:117-176` (TODO L161) |
| 🟠 High | Integrity | `PermanentDeleteEvent` deletes `events` before the noop_events subquery → **noop_events never deleted** | `PostgresEventStoreState.scala:244-250` |
| 🟠 High | Memory | EventBus ACK-tracking HashMaps leak on dead/slow subscribers; never evicted | `EventsBusActor.scala:91-95` |
| 🟡 Medium | CPU | `getDocuments` does `loaded.find` per key → O(m·n) | `PostgresDocumentStore.scala:423` |
| 🟡 Medium | CPU | `.distinct` on `pendingPublish` List → O(n²) per publish | `AggregateRepositoryActor.scala:95,136` |
| 🟡 Medium | CPU | Child-cache reap sorts the whole map O(n log n) on the actor thread | `AggregateCommandBusActor.scala:168-184` |
| 🟡 Medium | Memory | Unbounded projection delay buffers & `delayedQueries`; merge re-sorts each retry | `ProjectionActor.scala:105-107,748` |
| 🟡 Medium | Memory | Subscription caches (`perAggregate`/`dumped`) never evicted | `SubscriptionsState.scala:131-132` |
| 🟡 Medium | Integrity | Saga `updateSaga`/`deleteSaga` ignore rowcount → silent state divergence | `PostgresSagaState.scala:23-33` |
| 🟡 Medium | Reliability | `throw` inside saga `onComplete` runs off the actor thread (unsupervised); no persist retry | `SagaActor.scala:68-132` |
| 🟢 Low | Schema | Document-store migration not wrapped in one tx; can lock large tables | `PostgresDocumentStore.scala:30-33` |
| 🟢 Low | Ops | `println` in hot/recovery paths bypasses SLF4J | `UidGeneratorActor.scala:39,50,61`; `SagaActor.scala:47` |
| 🟢 Low | Multi-node | In-memory caches have no cross-instance invalidation → stale reads | `PostgresDocumentStore`, `PostgresSubscriptionsState` |

### A. Blocking calls inside actors (thread-starvation risk)
- 🟠 `AggregateCommandBusActor.scala:315-348` — `takeNextAggregateId`/`takeNextCommandId`
  do `Await.result(uidGenerator ? …, 60s)` when an id pool runs out (in-code TODO
  "get rid of ask pattern"). The **whole command bus for that type stalls** up to 60s.
  *Fix:* prefetch the next pool asynchronously before the current one drains (request
  at a low-water mark); reply via `pipeTo` instead of ask+`Await`.
- 🟠 `SagaActor.scala:136-152` (`takeNextSagaId`) — same `Await.result(…, 60s)`; blocks
  the saga actor on pool exhaustion (every new saga when the pool is small).
  *Fix:* same async low-water-mark prefetch; buffer id requests.
- 🟡 `EventsBusActor.scala:187-191` (`initSubscriptions`) —
  `Await.result(subscriptionsManager.getSubscriptions, 180s)` during init. One-shot,
  10s after start, so low risk, but it freezes a dispatcher thread if the manager is
  slow. *Fix:* `pipeTo self` / `onComplete` and stay non-blocking.
- 🟠 `AggregateRepositoryActor.scala:284-320` (`persist`) — `eventStore.localTx { … }`
  is a **synchronous DB transaction on the actor thread**; the aggregate is blocked for
  the full round-trip (one `add_event` PL/pgSQL call per event). By design, but it is
  the per-aggregate write ceiling. *Fix:* if it becomes a bottleneck, move persistence
  to a dedicated blocking dispatcher and re-enter the actor with the result by message.

### B. Optimistic-locking / correctness (integrity, perf-adjacent)
- 🔴 `EventBusState.scala:127-140` (`flushUpdates`) — batch
  `UPDATE event_bus SET aggregate_version=? WHERE aggregate_id=? AND aggregate_version=?`
  **ignores affected-row count** (in-code TODO L140 "check if all updates occured") and
  the in-memory dirty map is cleared (L135) regardless. A concurrent update makes the
  WHERE match 0 rows → the cursor silently stays stale → events skipped or re-published.
  *Fix:* inspect `batch(...)` per-row counts, or `INSERT … ON CONFLICT … DO UPDATE`
  with a version guard; on mismatch re-read and retry.
- 🟡 `PostgresSagaState.scala:23-33` — `updateSaga`/`deleteSaga` run
  `…update().apply()` and **discard the rowcount**. A missing/concurrently-deleted saga
  fails silently → state divergence (saga stuck in `CONTINUES` or re-run).
  *Fix:* assert `rowsUpdated == 1`, else log/raise.
- 🟡 `CommandHandlerActor.scala:95-106` — idempotency check is read-then-act, not atomic;
  two concurrent identical commands can both miss the cached response and execute.
  Downstream the optimistic version check usually rejects the second, but confirm.
  *Fix:* rely on a unique constraint on the idempotency key, or serialize per key.
- `CommandExecutorActor.scala` — in-code TODO "handling concurrent command is not
  thread safe"; verify the `ConcurrentCommand` retry path.
- `EventsBusActor.scala` — in-code TODO "filter out events received again if sender
  resent them": no dedup of re-published events → possible duplicate delivery. Projections
  are expected to be idempotent, but confirm per subscriber.
- `ProjectionActor.scala:~226-290` — `&&`/`||` ordering condition gating whether an
  update is delayed vs. applied; check operator precedence and the
  "already processed but below minimum delay version" branch for gap-detection skips.

### C. Memory growth / unbounded collections
- 🟠 `EventsBusActor.scala:91-95` — `messagesSent`, `eventSenders`,
  `eventsPropagatedNotPersisted`, `publishedEvents` HashMaps. Entries are removed only
  on **full ACK**; a dead/slow subscriber leaks them indefinitely (the 120s scheduler
  at L116 only *logs* old entries — never evicts). `publishedEvents` (L95) is never
  evicted and `postRestart` (L178) deliberately does **not** clear these maps.
  *Fix:* `DeathWatch` subscribers and drop their pending entries on `Terminated`; cap or
  TTL-evict `messagesSent`; bound/LRU `publishedEvents`.
- 🟡 `ProjectionActor.scala:105-107` — `delayedAggregateUpdates`,
  `delayedAggregateWithEventsUpdate`, `delayedEventsUpdate` maps are unbounded; the merge
  path re-`sortBy`s the whole per-aggregate list on each retry. If an aggregate's events
  never order, the list grows. *Fix:* cap per-aggregate buffer (drop/alert past a
  threshold); insert in order instead of re-sorting.
- 🟡 `ProjectionActor.scala:748` (`delayedQueries`) + `AggregateRepositoryActor`
  delayed min-version queries — unbounded lists; `scheduleNearest` (L824) scans the whole
  list O(n) to find the nearest deadline. *Fix:* timeout/evict abandoned queries; use a
  priority queue keyed by deadline.
- 🟡 `SubscriptionsState.scala:131-132` — `perAggregate`/`dumped` are lazy caches that
  grow per aggregate touched, with no eviction. Long-lived projections over many
  aggregates accumulate unbounded RAM. *Fix:* LRU/TTL, or `dump()` on a size threshold.
- `SubscribableProjectionActor.scala:49,86-91` — `updatesCache` queue is TTL-cleaned only
  on each `sendUpdate` (`dequeueAll`), no hard cap; bursts faster than the TTL grow it.
  *Fix:* add a max-size bound alongside the TTL.
- `AggregateCommandBusActor.scala:86` — child-actor caches (`childrenActivity`); reaped on
  a timer but hold up to `keepAliveLimit` (200) continuously under churn (see §D).
- `EventBusSubscriptionsManager` — no unsubscribe path; subscriptions only grow and are
  re-derived on restart (not persisted). *Fix:* add `Unsubscribe` + `DeathWatch` cleanup.
- `CommandResponseState.scala` (`MemoryCommandResponseState`) — cache grows forever
  (no eviction). Test/in-memory only, but watch it.
- `MemoryDocumentStore.scala` — `ParHashMap` store, never evicted; some batch
  `insertDocuments` overloads are `???` (unimplemented). Test-only.

### D. SQL / query performance
- 🟠 **Missing index on `events_to_publish(aggregate_id)`** —
  `PostgresEventStoreSchemaInitializer.scala:83` creates the table with **no secondary
  index** (verified: the file's only `CREATE INDEX`es are on `events`/`aggregates`).
  The outbox is queried/joined by `aggregate_id` on every projection fetch
  (`PostgresEventStoreState` outbox reads) → full scans as it grows.
  *Fix:* `CREATE INDEX IF NOT EXISTS events_to_publish_aggregate_idx ON events_to_publish (aggregate_id)`.
- 🟠 **No PK/index on `sagas`** — `PostgresSagaSchemaInitializer.scala:12` creates the
  table with no primary key and no index. `loadAllSagas` (`WHERE name = ?`) full-scans on
  every saga-actor `preStart`; `updateSaga`/`deleteSaga` (`WHERE name=? AND saga_id=?`)
  scan too. *Fix:* `CREATE UNIQUE INDEX … sagas (name, saga_id)` and `CREATE INDEX … sagas (name)`.
- 🟠 `PostgresEventStoreState.scala:117-176` (`readAndProcessEvents`) — when filtering by
  version or instant, **all** matching events are pulled from PG and discarded in app code
  after deserialization (in-code TODO L161). Wasted I/O + JSON parsing scaling with stream
  length. *Fix:* push the version/instant predicate into the SQL `WHERE`/join.
- 🟡 `PostgresDocumentStore.scala:423` (`getDocuments`) — `cache.put(id, loaded.find(_._1 == id)…)`
  inside a per-key loop → O(m·n) over a `List`. *Fix:* `val byId = loaded.toMap` once, then
  `cache.put(id, byId.get(id))`.
- 🟡 `AggregateRepositoryActor.scala:95,136` — `(… ::: pendingPublish).distinct` rebuilds
  and de-dups the pending-publish `List` each persist → O(n²). *Fix:* track pending in a
  `Set`/keyed structure, or dedup by `(aggregateId, version)` key.
- `PostgresEventStoreState.scala:302-323` — `countAllEvents` uses `pg_class.reltuples`
  (fast estimate, can be stale after VACUUM) with a `COUNT(*)` fallback when 0 — acceptable
  by design; `countEventsForAggregateTypes` joins `events`↔`aggregates` where querying
  `aggregates` (with `base_order = 1`) alone would suffice.

### E. PermanentDelete ordering (data leak)
- 🟠 `PostgresEventStoreState.scala:244-250` — inside the `PermanentDeleteEvent` localTx,
  `DELETE FROM events WHERE aggregate_id=?` (L247) runs **before**
  `DELETE FROM noop_events WHERE id IN (SELECT id FROM events WHERE aggregate_id=?)` (L248).
  By the time the subquery runs, the matching `events` rows are gone, so it deletes
  nothing → **orphaned `noop_events` rows accumulate** (storage leak + LEFT-JOIN cost in
  `readAndProcessEvents`). *Fix:* delete `noop_events` **before** `events` (or capture the
  ids first).

### F. Schema / migration
- 🟢 `PostgresDocumentStore.scala:30-33` (`init`) — `createTableIfNotExists` /
  `addSpaceColumn` / `dropMetadataColumn` (and index rebuilds) each run in their **own**
  `DB.autoCommit`, not one transaction. A mid-migration failure leaves an inconsistent
  schema; on large read models the separate DDL statements lock tables longer.
  *Fix:* wrap the migration in a single `DB.localTx`.
- `CommandResponseState.scala` — confirm the `commands_responses` `CREATE TABLE` declares
  every column its unique index references (historic mismatch on `aggregate_id`).

### G. Cross-instance cache coherency
- 🟢 `PostgresDocumentStore` and `PostgresSubscriptionsState` keep **in-memory caches** with
  no cross-instance invalidation. In a multi-node deployment one node's writes won't
  invalidate another's cache → stale reads. The single-node assumption is implicit;
  document it for operators or add an invalidation channel (e.g. PG `LISTEN/NOTIFY`).

### H. Error handling (reliability / hidden latency)
- 🟢 `UidGeneratorActor.scala:39,50,61` — `println(…, e)` in `Future.onComplete`; on failure
  the requester is **never answered**, so the §A `Await` waits the full 60s before failing.
  *Fix:* use the logger and reply a failure message so the waiter fails fast.
- 🟡 `SagaActor.scala:68-132` — `throw new IllegalStateException(e)` inside `onComplete`
  runs on the dispatcher thread, **not** the actor thread, so supervision never sees it and
  the `SagaPersisted` message is never sent → the saga is silently lost, with no retry.
  Plus `println` on recovery (L47). *Fix:* handle failures with `recover`, log, and retry
  with backoff; never `throw` from a callback.

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

# ReactiveCQRS — internals reference

A working map of how the library defines aggregates, persists them, publishes their
events and feeds projections/listeners. It is meant as the baseline for future changes
to the library's behaviour.

> **Provenance.** Static read of `master` @ `4b5060f` (version 0.12.47), 16/10/2026, updated for
> the changes of milestone M1 (to `cc8925c`) and checked against that code at the M1 review.
> Where M1 changed the code, references name methods; the remaining line numbers are from
> `4b5060f` and may have drifted by a few lines. Defaults and limits quoted below are the
> literal values in the code; measured performance is in `docs/perf/`.
>
> **Naming.** There is no `events_queue` table or class. The publication queue is the
> **`events_to_publish`** table (a transactional outbox); the bus-side cursor is the
> **`event_bus`** table or its in-memory equivalent. This document uses those names.

---

## 0. File index

Short names used throughout (all under `core/src/main/scala/io/reactivecqrs/core/`
unless the path says `api/` or `testdomain/`).

| Short name | Path |
|---|---|
| `AggregateContext.scala`, `Command.scala`, `Event.scala`, `CommandResult.scala`, `CustomCommandResponse.scala`, `Aggregate.scala` | `api/src/main/scala/io/reactivecqrs/api/` |
| `AggregateCommandBusActor.scala`, `CommandHandlerActor.scala`, `CommandExecutorActor.scala`, `CommandResponseState.scala`, `Idempotency.scala` | `commandhandler/` |
| `AggregateRepositoryActor.scala`, `ReplayAggregateRepositoryActor.scala` | `aggregaterepository/` |
| `PostgresEventStoreState.scala`, `PostgresEventStoreSchemaInitializer.scala`, `EventStoreState.scala` | `eventstore/` |
| `EventsBusActor.scala`, `EventBusState.scala`, `EventBusSubscriptionsManager.scala`, `SubscriptionClassifier.scala` | `eventbus/` |
| `ProjectionActor.scala`, `SubscriptionsState.scala`, `SubscribableProjectionActor.scala`, `AggregateListenerActor.scala`, `VersionsState.scala` | `projection/` |
| `EventsReplayOrchestrator.scala`, `EventsReplayerActor.scala` | `eventsreplayer/` |
| `SagaActor.scala`, `PostgresSagaState.scala`, `PostgresSagaSchemaInitializer.scala` | `saga/` |
| `PostgresDocumentStore.scala`, `DocumentStoreCache.scala` | `documentstore/` |
| `AfterCommit.scala` (cache updates after commit) | `util/` |
| `StoredDataCheck.scala` (dry run of stored JSON, D78) | `tools/` |
| `Baseline.scala` (performance bench) | `bench/src/main/scala/io/reactivecqrs/bench/` |
| `ShoppingCartAggregateContext.scala`, `ShoppingCartsListProjection.scala` | `testdomain/src/main/scala/io/reactivecqrs/testdomain/shoppingcart/` |
| `ReactiveTestDomainSpec.scala` (full system wiring) | `testdomain/src/test/scala/io/reactivecqrs/testdomain/spec/` |

---

## 1. The big picture

```
caller ──command──▶ AggregateCommandBusActor          one per aggregate TYPE
                      │ takes CommandId / AggregateId from pools (UidGeneratorActor)
                      ▼
                    CommandHandlerActor                one per aggregate INSTANCE
                      │ idempotency lookup (commands_responses), spawns ▼
                    CommandExecutorActor               one per command, stops after its one reply
                      │ ① asks for state   ② runs command handler   ③ PersistEvents
                      ▼
                    AggregateRepositoryActor           one per aggregate INSTANCE, owns state+version
                      │ localTx: add_event(s) → events, aggregates, events_to_publish (+ command response)
                      │ PublishEvents(type, events, id, Option(root))
                      ▼
                    EventsBusActor                     one per system
                      │ IdentifiableEvents | AggregateWithType | AggregateWithTypeAndEvents
                      ▼
                    ProjectionActor / AggregateListenerActor  (N subscribers)
                      │ localTx: listener(...) + SubscriptionsState compare-and-set
                      │ MessageAck
                      ▼
                    EventsBusActor ──PublishEventsAck──▶ AggregateRepositoryActor
                                                          └─ DELETE FROM events_to_publish
```

Wiring reference: `ReactiveTestDomainSpec.scala:36-89`. The host application must
initialise the ScalikeJDBC singleton connection pool before creating any component
(`README.md`), and call `initSchema()` on `PostgresTypesNamesState`,
`PostgresEventStoreState`, `PostgresCommandResponseState`, `PostgresEventBusState`,
`PostgresSubscriptionsState` and `PostgresSagaState`; the uid generators and the document
stores create their own tables. Since 1.0 the saga index build refuses duplicate
`(name, saga_id)` rows (F34).

---

## 2. Defining an aggregate

An aggregate has two halves: the **model** (an immutable value) and an
**`AggregateContext`** that binds commands, command handlers, events and event handlers
to that model.

### 2.1 The model

- A plain immutable case class, e.g. `case class ShoppingCart(name: String, items: Vector[Item])`
  (`testdomain/.../Model.scala:3`).
- The framework exposes it as `Aggregate[R](id, version, aggregateRoot: Option[R])`
  (`Aggregate.scala:16`). `aggregateRoot` is `None` when the aggregate never existed
  (version 0) or when an event handler returned `null`, which means "deleted"
  (`Option(aggregateRoot)` in `AggregateRepositoryActor.publish`).
- **It must be immutable.** The same instance is held by the repository actor and sent
  inside `PublishEvents` to the bus and every subscriber, on other threads
  (`AggregateRepositoryActor.publish`).
- **It should be a top-level class (inferred invariant).** The type name is derived in
  three ways that agree only for top-level classes: `classTag.toString`
  (`AggregateContext.aggregateType`, gives `Outer$Inner`), `typeOf[R].toString` (projection
  listener keys, `ProjectionActor.scala:180-196`, gives `Outer.Inner`) and
  `typeSymbol.fullName` (persist path). A nested class would never be routed to
  projections.
- Identifiers (`api/.../id/`): `AggregateId` (Long, LRU-pooled), `AggregateVersion` (Int,
  starts at `ZERO`, +1 per persisted event, pooled below 1000), `SpaceId` (tenant/partition
  id written by the first event), `UserId`, `CommandId`, `SagaId`.

### 2.2 Events (`Event.scala`)

| Base class | Extra members | Persisted by (SQL function) | Repository behaviour after persist |
|---|---|---|---|
| `Event[R]` | — | `add_event` with `space_id = -1` | applied incrementally |
| `FirstEvent[R]` | `spaceId` | `add_event`; creates the `aggregates` row when expected version is 0 | applied incrementally |
| `UndoEvent[R]` | `eventsCount` | `add_undo_event`; marks the last N non-noop events and itself as noop | full state re-read |
| `DuplicationEvent[R]` (is a `FirstEvent`) | `baseAggregateId`, `baseAggregateVersion`, `spaceId` | `add_duplication_event`; copies the base's duplication chain | full state re-read; skipped in tryout |
| `PermanentDeleteEvent[R]` | — | `add_event` | rows hard-deleted once the bus confirms publication (§4.5) |

- An ordinary event on an aggregate that has no `aggregates` row fails inside
  `add_event` ("Maybe FirstEvent was not used",
  `PostgresEventStoreSchemaInitializer.scala:154-155`).
- Events carry a `TypeTag` of their aggregate root (`Event.scala:7-9`); the persist path
  uses it to resolve the aggregate type id.
- Storage format: JSON text (mpjsons), with the class identified by a `types_names` id
  plus `event_type_version`.

### 2.3 Event schema versioning (`EV`)

`AggregateContext.EV[Base](v -> classOf[Concrete], ...)` (`AggregateContext.scala:30-32`)
builds an `EventVersion(baseTypeName, List(EventTypeVersion(concreteClass, v)))`. Two maps
are derived (`AggregateCommandBusActor.scala:66-72`):

- **write:** concrete class → `(baseTypeName, v)`; the row stores
  `event_type_id = typeId(baseTypeName)`, `event_type_version = v`
  (`PostgresEventStoreState.scala:59-61`). Unmapped classes are stored as
  `(ownClassName, 0)`.
- **read:** `(baseTypeName, v)` → concrete class to deserialize into
  (`PostgresEventStoreState.scala:224-227`).

There is no upcasting step. To change an event's shape, keep the old class, map it to the
old version number, and handle **both** classes in event handlers and projections, e.g.
`EV[ItemAdded](0 -> classOf[ItemAddedV0], 1 -> classOf[ItemAdded])`. Renaming or moving
an event class without a mapping breaks replay.

### 2.4 Commands (`Command.scala`)

The five kinds are **abstract classes**, so a command is exactly one kind; the bus tells
them apart by class (`AggregateCommandBusActor.receive`).

| Kind | Members | State the executor requests | Version check | On concurrent modification |
|---|---|---|---|---|
| `FirstCommand` | `userId`; since 1.0 optionally `chosenAggregateId` | none; `initialAggregateRoot` is passed directly (`CommandHandlerActor.receive`) | expected = 0 | `AggregateConcurrentModificationError` (the chosen aggregate exists) |
| `Command` | `userId`, `aggregateId`, `expectedVersion` | `GetAggregateRootExactVersion`; since 1.0 the actor reloads first when the expected version is newer than its own (F19) | strict | `AggregateConcurrentModificationError`; also for an aggregate that does not exist (`was` = 0, F67) |
| `ConcurrentCommand` | `userId`, `aggregateId` | `GetAggregateRootCurrentVersion` | current version | state re-fetched and handler re-run, at most 10 more times, with jittered back-off from 5–10 ms up to 250–500 ms (`CommandExecutorActor.onConflict`) |
| `RewriteHistoryCommand` | + `expectedVersion`, `eventsTypes` | `GetAggregateRootWithEventsExactVersion` | strict | error |
| `RewriteHistoryConcurrentCommand` | + `eventsTypes` | `GetAggregateRootWithEventsCurrentVersion` | current version | re-run like `ConcurrentCommand`, again with the events (fixed F73) |

Mix-in `IdempotentCommand[IID]` with `idempotencyId: Option[IID]` makes a command take
effect once per key `IID.asDbKey` (`SagaStep(sagaId, step)` → `"sagaId|step"`); keys are
global across aggregate types. Since 1.0 (D12, fixed F9) the `Idempotency` module
(`core/commandhandler/Idempotency.scala`) owns the rules: the response is stored in the
persist transaction, a copy that finds the key taken rolls back and gets the stored
response, and so does a copy that meets a version conflict. Stored responses are removed
after the response state's retention period (default 30 days; each command bus runs the
clean-up hourly). A `FirstCommand` can name its aggregate in `chosenAggregateId` (an id
from `ReserveAggregateId`), so a resent creation cannot create a second aggregate.
Limits: only commands that changed state store a response, so a copy of a command that
failed or produced no events runs again; the info of a `CustomSuccessResponse` must be of a
class without type parameters; the host must call `PostgresCommandResponseState.initSchema()`
(its insert needs the unique index).

### 2.5 Command handlers and results

`commandHandlers: R => PartialFunction[Any, GenericCommandResult[Any]]`
(`AggregateContext.scala:36,44`). The function receives the current root (for a
`FirstCommand`, `initialAggregateRoot`) and returns one of (`CommandResult.scala`):

| Result | Effect | Response to caller (`CustomCommandResponse.scala`) |
|---|---|---|
| `CommandSuccess(events, info)` | events persisted | `SuccessResponse(id, version + n)`, or `CustomSuccessResponse(id, version + n, info)` when `info` is not `Nothing` |
| `CommandSuccess(Nil)` | nothing written at all, not even a statement (R9, `CommandExecutorActor.handleCommandResult`) | `SuccessResponse(id, version)` |
| `CommandFailure(msgs)` | nothing persisted | `FailureResponse(msgs)` |
| `RewriteCommandSuccess(rewritten, events, info)` | past events overwritten, then new events persisted | as success |
| `AsyncCommandResult(Future[...])` | the future's result is sent back to the executor | as above |
| exception or unhandled command (`MatchError`) | logged with an errorId | `CommandHandlingError(name, errorId, commandId)` |
| no result within `AggregateContext.commandTimeout` (60 s) | the executor stops; the command may still be applied | `CommandHandlingError` (F20) |
| the aggregate's restore failed (§4.4) | nothing persisted | `CommandHandlingError`; a `FirstCommand` gets `EventHandlingError` at persist |
| no id available (the uid generator fails) | nothing persisted, the bus keeps running (F75) | `CommandHandlingError` with command id 0 |
| event handler failed in tryout or persist | nothing persisted | `EventHandlingError(...)` |
| version mismatch (memory or SQL) | nothing persisted | `AggregateConcurrentModificationError(...)`, or the stored response for an idempotent command whose copy won |
| idempotency key taken, but its response already removed by retention | nothing persisted | `CommandHandlingError` |

`AggregateContext` provides an implicit `Future[CustomCommandResult] → AsyncCommandResult`
conversion (`AggregateContext.scala:26-28`). A command sent to a deleted aggregate throws
"Aggregate is already deleted" and yields `CommandHandlingError`
(`CommandExecutorActor.handleFollowingCommand`).

### 2.6 Event handlers

`eventHandlers: (UserId, Instant, R) => PartialFunction[Any, R]` (`AggregateContext.scala:38,42`).
They run in four places:

1. **Tryout** before persisting, on a copy of the state (`AggregateRepositoryActor.tryToApplyEvents`).
   A failure aborts the command with `EventHandlingError`; nothing is written.
2. **Apply** right after the transaction commits (`applyPersisted`).
3. **Restore** on the actor's first message, and again on every reload: after an SQL
   conflict, when a command expects a newer version than the actor holds (F19), and after an
   undo or duplication event (`assureRestoredState`, `handleEvent`).
4. **Replay** during projection rebuilds (`ReplayAggregateRepositoryActor.scala:45-59`).

Rules that follow:

- Handlers must be **pure and deterministic**. Restore and replay must reproduce live state;
  handlers get the same timestamp live, on restore and on replay (fixed F7: one timestamp
  per persist, truncated to microseconds).
- Return `null` to mark the aggregate deleted.
- Handle `UndoEvent` subclasses (they go through tryout; the test domain returns the root
  unchanged, `ShoppingCartAggregateContext.scala:32`).
- Handle `DuplicationEvent` subclasses by returning the given root unchanged: on restore,
  the base aggregate's events have already been applied (`:27`).
- Undone (noop) events are skipped on restore but still count towards the version
  (`AggregateRepositoryActor.handleEvent`).

### 2.7 `AggregateContext`: the binding unit

```scala
abstract class AggregateContext[R : ClassTag] {           // AggregateContext.scala:24
  def initialAggregateRoot: R                             // state before the first event
  def commandHandlers: R => PartialFunction[Any, GenericCommandResult[Any]]
  def eventHandlers: (UserId, Instant, R) => PartialFunction[Any, R]
  def rewriteHistoryCommandHandlers                       // optional; default throws
      : (Iterable[EventWithVersion[R]], R) => PartialFunction[Any, GenericCommandResult[Any]]
  val eventsVersions: List[EventVersion[R]] = List.empty  // EV(...) mappings
  val version: Int                                        // bump to force projection rebuild
  val commandTimeout: FiniteDuration = 60.seconds         // since 1.0 (R5): answer CommandHandlingError after it
  val aggregateType = AggregateType(classTag[R].toString) // FQCN; the routing key everywhere
}
```

- `aggregateType` keys the bus subscriptions and the `types_names` row. Renaming or moving
  the root class creates a **new** aggregate type; existing rows stay keyed by the old id.
- `version` is used only by `EventsReplayOrchestrator` and `VersionsState` to decide which
  projections to rebuild (§8).
- The context is turned into a running component with
  `AggregateCommandBusActor(context, uidGenerator, eventStoreState, commandResponseState, eventBus, eventsReplayMode, maxInactivityMillis = 12 h, keepAliveLimit = 200, failedRestoreRetryAfter = 5 s)`
  (`AggregateCommandBusActor.apply`).
- Example: `ShoppingCartAggregateContext.scala` (commands in `Commands.scala`, events in
  `Events.scala`, pure handler functions in `CommandsHandlers.scala` / `EventsHandlers.scala`).

---

## 3. Write path: command → events

1. **Routing** (`AggregateCommandBusActor.receive` and the `route*` methods). The bus takes a
   `CommandId` (and, for `FirstCommand` without `chosenAggregateId`, a new `AggregateId`) from
   in-memory pools. When a pool is empty it calls `UidGeneratorActor` with a **blocking**
   `Await` of up to 60 s (`takeNextCommandId`, `takeNextAggregateId`); since F75 a failing
   generator is answered with `CommandHandlingError` instead of restarting the bus, and since
   F70 so is a message the bus does not understand (`Status.Failure`). It then creates or
   reuses the `CommandHandlerActor` (and its `AggregateRepositoryActor`) and forwards an
   `Internal*CommandEnvelope(respondTo, commandId, command)`.
2. **Idempotency** (`CommandHandlerActor`). For an `IdempotentCommand` with a key, a
   synchronous `commands_responses` lookup returns the stored response instead of
   executing. Copies running at the same time are settled at persist (steps 6 and 7).
3. **Executor** (`CommandHandlerActor.receive`). A `CommandExecutorActor` is spawned
   per command; the handler actor asks the repository for the state the command kind
   needs (table in §2.4).
4. **Handler** (`CommandExecutorActor.handleFollowingCommand`, `handleFirstCommand`). The
   in-memory version check runs first, then the command handler; sync results and async
   results (via `onComplete → self !`) converge on `handleCommandResult`.
5. **PersistEvents** carries `expectedVersion`, one timestamp taken **in the executor** and
   truncated to microseconds (`eventsTimestamp`), the events and an optional
   `IdempotentCommandInfo`.
6. **Repository** (`AggregateRepositoryActor.handlePersistEvents`). Tryout of all events on
   the current root, then a second in-memory check `expectedVersion == version`, then
   `persist`: one `eventStore.localTx` on the actor thread that
   - optionally overwrites past events (rewrite commands),
   - calls `persistEvents` (`add_event` per event, or one `add_events` call for 2+ plain/first events),
   - stores the idempotent command response in the **same transaction**
     (`INSERT … ON CONFLICT (key) DO NOTHING`; nothing inserted means the key is taken).

   Any failure inside the transaction is thrown, so it rolls back whole (fixed F68: a
   later step used to fail after earlier statements had run, and they were committed).
7. **After commit** the repository applies the events to its in-memory root at once (or
   re-reads everything for undo/duplication events), sends `PublishEvents` to the bus,
   records the events in `pendingPublish`, answers any waiting `GetAggregateMinVersion`
   queries and replies `AggregateModified` to the executor, which forwards the prepared
   response to the caller. Every other outcome is one reply as well (AR3): "Concurrent
   aggregate modification exception" from PostgreSQL → reload, then
   `AggregateConcurrentModificationError`; a taken idempotency key → `IdempotencyKeyTaken`;
   anything else → `EventHandlingError`. For an idempotent command the executor answers a
   conflict or a taken key with the stored response. If the re-read after an undo or
   duplication event fails, nothing is published and the actor enters the failed-restore
   state (§4.4); the caller still gets success, because the events are committed, and the
   outbox re-sends them later.

Ordering note: the state changes before the next message is processed (fixed F69; it used
to change through a self-message, and a reload in between applied the events twice), so a
`PersistEvents` already queued behind a successful one is rejected by the in-memory version
check. The SQL lock (§4.2) still catches writers outside this actor.

---

## 4. Aggregate persistence (PostgreSQL)

Every table, sequence and function lives in the schema the connection's `search_path`
selects, and catalog lookups are limited to `current_schema()` (fixed F71), so one database
can hold several installations or copies in separate schemas. The test suite uses this: each
run works in a new schema (`TestDatabase`).

### 4.1 Event-store tables (`PostgresEventStoreSchemaInitializer.scala`)

| Table | Columns / keys | Role |
|---|---|---|
| `events` (`:29-41`) | `id` (from `events_seq`) PK, `user_id`, `aggregate_id`, `event_time TIMESTAMP`, `version`, `event_type_id`, `event_type_version`, `event TEXT`; indexes `(aggregate_id)`, `(aggregate_id, id)` | append-only event log |
| `aggregates` (`:103-115`) | PK `(id, base_id)`, `space_id`, `creation_time`, `type_id`, `base_order`, `base_id`, `base_version`; indexes `(type_id)`, `(base_id)` | one row per (aggregate, ancestor) in its duplication chain; the own row (`base_id = id`) holds the **current version** used for locking |
| `noop_events` (`:75-81`) | `id` (= `events.id`) PK, `from_version` | events cancelled by an undo, effective from that version |
| `events_to_publish` (`:83-101`) | `event_id` PK, `aggregate_id`, `version`, `user_id`, `event_time`; index `(aggregate_id, version)` | **outbox**: events not yet confirmed by every subscriber |
| `types_names` (`TypesNamesState.scala`) | `id SMALLINT`, unique `name` | class-name ↔ id dictionary (aggregates, events, subscribers, responses) |
| `commands_responses` (`CommandResponseState.scala`) | `id`, `key` (unique), `handling_timestamp` (indexed, for retention), `response`, `response_type_id`, and since 1.0 `info`, `info_type_id` (the info of a `CustomSuccessResponse`, F64) | idempotent responses (D12) |

### 4.2 Stored procedures and optimistic locking

`add_event(user, space, aggregate, expected_version, type ids…, time, json)` (`:139-172`):

1. `UPDATE aggregates SET base_version = base_version + 1 WHERE id = agg AND base_id = agg RETURNING base_version - 1`.
   The row lock serialises concurrent writers to the same aggregate.
2. No row: when `expected_version = 0` and `space_id >= 0` (a first event), insert the
   `aggregates` row; otherwise raise.
3. When `current_version != expected_version`, raise "Concurrent aggregate modification exception".
4. Insert into `events` **and** `events_to_publish` with `version = current + 1`.

Related functions:

- `add_events` (`:178-194`) loops `add_event` with `expected + i - 1`: one round trip for a
  multi-event command.
- `add_undo_event` (`:196-229`) inserts `noop_events` rows (`from_version = new version`)
  for the last `undo_count` not-yet-noop events and for itself.
- `add_duplication_event` (`:231-263`) copies the base's `aggregates` chain rows, caps the
  base row at `_base_version`, and appends its own row with `base_order = n + 1`.

Version checks happen twice: in memory (repository actor) and in SQL. The SQL check is the
real arbiter, for example when two repository actors exist for one aggregate during
reaping, or in a multi-node setup.

Limits (`PostgresEventStoreState.scala:15-17`): `eventSizeLimit = 100000` characters
(overridable per class), `aggregateVersionLimit = 10000`, `fetchSize = 1000`.

### 4.3 Reading state back

- **Restore** (`readAndProcessEvents`, `PostgresEventStoreState.scala:189-248`): join
  `events` to every `aggregates` chain row of the aggregate
  (`events.aggregate_id = aggregates.base_id AND events.version <= aggregates.base_version`),
  left-join `noop_events` to get the noop flag, order by `base_order, version`. Ancestor
  events rebuild the state but don't increment the version (`aggId == aggregateId` check).
- **There are no snapshots.** Every restore replays the whole chain on the actor thread:
  on the actor's first message and on every reload (§2.6). Measured: about 210k events/s,
  with peak heap growing with the history (452 MB at 500k events, `docs/perf/baseline-M1.md`).
- **Historical reads** (`GetAggregateForVersion`, `GetAggregateAtInstant`,
  `GetEventsForAggregateForVersion`) spawn a temporary repository actor that answers once
  and stops (`stopIfSingleRead`; the events read did not before F74)
  (`AggregateCommandBusActor.createAggregateRepositoryActorForVersion`, `…ForInstant`). Version reads filter the aggregate's own
  events in SQL (`PostgresEventStoreState.scala:204-211`); instant reads fetch **all**
  events and filter in Scala after deserialization (`:233`, TODO). Restore reads use
  `DB.readOnly` with autocommit on and no `fetchSize`, so the PostgreSQL driver loads the
  whole result set into memory before the first row is handled.
- `GetAggregateMinVersion(id, v, maxMillis)` parks the request in `delayedQueries` until
  the version is reached; an expired query is dropped without a reply (F81)
  (`AggregateRepositoryActor.receiveReturnAggregateRootMinVersion`).
- Queries never reload: a running actor does not see writes made around it (I1, one writer
  per aggregate). Only a command that expects a newer version reloads it (F19).

### 4.4 Repository-actor lifecycle

- Created lazily by `AggregateCommandBusActor` on the first command or query for an id
  (`getOrCreateAggregateRepositoryActor`). The first message runs `assureRestoredState()`,
  which also loads that aggregate's outbox rows; the outbox scan sends `EnsureRestored` so
  the re-send starts.
- **Failed restore** (fixed F10): the actor answers queries with
  `Failure(AggregateRestoreFailedException)` and persists with `EventHandlingError`, then stops
  after `failedRestoreRetryAfter` (5 s). The command bus watches its repository actors and on
  `Terminated` drops the actor and its command handler, so the next request restores again.
- **Reaping** (`AggregateCommandBusActor.clearOldAggregateRepositories`): at most every 5
  minutes, and only when a command or query arrives. When more than `keepAliveLimit` (200)
  actors are tracked, the least-recently used ones idle for more than 2 minutes are killed;
  any actor idle for more than `maxInactivityMillis` (12 h) is killed too. Kill =
  `PoisonPill` plus removal from the maps; a replacement may start while the old one drains
  (`spawnedCount` keeps names unique). Stopping a command handler also stops its executors,
  so a command still running then gets no reply (AR1 drains them, M3).
- No `supervisorStrategy` in `core`: Pekko defaults apply, so a throw inside `receive`
  restarts an actor and stops its children (AR7). The command bus's watch on its repository
  actors is the only `DeathWatch`.

### 4.5 History rewrite and permanent delete

- **Rewrite** (`RewriteCommandSuccess`): `overwriteEvents` runs `UPDATE events SET event = ?`
  per `(aggregate_id, version)` in the same transaction (`PostgresEventStoreState.scala:180-186`).
  Only new events are published, and the in-memory root is **not** rebuilt (F8). The test
  domain pairs each rewrite with a compensating event (`CartNameRewritten`).
- **Permanent delete**: the deletion runs when the bus acknowledges a published batch that
  contains a `PermanentDeleteEvent`. In one transaction it deletes the `aggregates`,
  `noop_events` (before `events`, so its sub-select still matches), `events`,
  `events_to_publish` and `subscriptions` rows (`PostgresEventStoreState.deletePublishedEventsToPublish`).
  The `event_bus` row and in-memory caches are not touched. Duplicates of the deleted
  aggregate keep chain rows that point at it and lose its history (F76).

---

## 5. Event publication

### 5.1 The outbox: `events_to_publish`

Lifecycle of one event:

1. Inserted by `add_event` in the command transaction, so it is never lost.
2. After commit it is published in memory (`PublishEvents`) and added to `pendingPublish`.
3. When every subscriber has acked it, the bus sends `PublishEventsAck(aggId, versions)` to
   the publishing repository actor (`EventsBusActor.scala:423-427`).
4. `markPublishedEvents` deletes the rows in a fire-and-forget `Future`
   (`AggregateRepositoryActor.markPublishedEvents`); a failed delete is only logged.

Re-send paths (the only retry mechanism in the system):

| Trigger | What is re-sent | Where |
|---|---|---|
| Every restore: the actor's first message and every reload (§2.6) | outbox rows loaded at that moment, 10 s later, then every 180 s while any remain; each restore starts one more such chain, so repeated reloads multiply the re-sends while rows remain (F72) | `AggregateRepositoryActor.assureRestoredState`, `resendEventsToPublish` |
| `EnsureEventsPublished(false)` 30 s after the command bus starts, then `(true)` every 60 s | creates the actor (and sends it `EnsureRestored`) for each aggregate whose outbox rows are older than 10 s / 1 min, which triggers the row above | `AggregateCommandBusActor.ensureEventsPublished`, `PostgresEventStoreState.readAggregatesWithEventsToPublish` |

`pendingPublish` is never re-sent, and `EnsureEventsPublished` is a no-op for an actor that
already exists. Events persisted during an actor's lifetime that are never acked are
therefore retried only after the actor restores again, or is reaped and recreated (F2).

### 5.2 `EventsBusActor`

**Subscription registry.** Projections send `SubscribeFor*` requests to
`EventBusSubscriptionsManager(minimumExpectedSubscriptions)` from `preStart`. The bus pulls
the list once, on its first message (an `InitEventBus` is self-scheduled at 10 s), with an
`Await` of up to 180 s (`EventsBusActor.scala:187-205`). The manager then closes; a later
subscription **throws** (`EventBusSubscriptionsManager.scala:62-72`).
`minimumExpectedSubscriptions` counts **requests**, which is one per listener per aggregate
type, not one per projection. There is no unsubscribe.

**Routing** (`handlePublishEvents`, `:282-369`). The bus drops the prefix
`version <= lastPublishedVersion` (`span`, `:294`), then builds one message per matching
subscription of the aggregate type:

| Subscription (request) | Classifier | Message sent | Listener kind (§6.2) |
|---|---|---|---|
| `SubscribeForEvents` | `SubscriptionClassifier` (aggregate id + event type) | `IdentifiableEvents(type, id, events, replayed)` | `EventsListener` |
| `SubscribeForAggregates` | `AggregateSubscriptionClassifier` (id only) | `AggregateWithType(type, id, lastVersion, versions, root, replayed)` | `AggregateListener` |
| `SubscribeForAggregatesWithEvents` | `SubscriptionClassifier` | `AggregateWithTypeAndEvents(type, id, root, events, replayed)` | `AggregateWithEventsListener` |

`root` is the aggregate state after the whole batch. `ProjectionActor` always subscribes
with accept-all classifiers (`ProjectionActor.scala:749-761`).

**Acknowledgement tracking** (`:91-95`, `handleMessageAck` `:382-459`):

- `messagesSent: EventIdentifier → receivers` and `eventSenders: EventIdentifier → publisher`.
- Each `MessageAck(subscriber, id, versions)` removes that subscriber from the event's
  receivers. When the set empties, the event is finished and `PublishEventsAck` goes to the
  publisher.
- When nobody subscribes, the bus acks itself (`:365-368`).

**Cursor** (`EventBusState`). After finishing events the bus advances "last published
version per aggregate" only when the first finished version is exactly `cursor + 1`;
otherwise it parks them in `eventsPropagatedNotPersisted` (`:431-449`). The cursor is cached
in `publishedEvents`.

- `MemoryEventBusState` (`EventBusState.scala:18-42`) starts at 0 for every aggregate.
  `claude-log/optimization.md` describes it as the production bus.
- `PostgresEventBusState` (`:48-149`) keeps the `event_bus` table and flushes batched
  updates every 5 s or every 20 aggregates, ignoring row counts (CLAUDE.md §7.B).

**Back-pressure** (`BackPressureActor.scala`). It is active only when a producer sends
`ConsumerStart`, which only the replayer does. The bus then grants up to `MAX_BUFFER_SIZE`
(10000, `EventsBusActor.scala:66`) in-flight events.

### 5.3 What subscribers can rely on

- **At-least-once** delivery, through the outbox and its re-send paths.
- **Per-aggregate order** on the live path (one publisher per aggregate, Pekko's
  per-sender ordering). Re-sends can arrive as duplicates, overlaps or after later events.
  Subscribers must deduplicate and detect gaps; `ProjectionActor` does this through
  `SubscriptionsState`.
- No ack → no retry from the bus itself; the retry comes only from the outbox.

---

## 6. Projections and listeners

### 6.1 The `ProjectionActor` contract (`ProjectionActor.scala:91-102`)

```scala
class MyProjection(val eventBusSubscriptionsManager: EventBusSubscriptionsManagerApi,
                   val subscriptionsState: SubscriptionsState, store: DocumentStore[Doc])
    extends ProjectionActor(ProjectionActorOptions.DEFAULT) {
  protected val projectionName = "MyProjection"   // used by VersionsState (rebuild decisions)
  protected val version = 1                       // bump to force rebuild
  protected val listeners = List(EventsListener(onEvents), AggregateListener(onAggregate))
  protected def receiveQuery: Receive = { case MyQuery(..) => sender() ! ... }
  protected def onClearProjectionData(): Unit = ()   // called on ClearProjectionData (rebuild)
}
```

Progress in `SubscriptionsState` is keyed by **`this.getClass.getName`**, not
`projectionName` (`:420, 503, 654`). Renaming or moving the class resets the projection's
progress.

### 6.2 Listener kinds

| Factory | Callback | Receives | Notes |
|---|---|---|---|
| `EventsListener(f)` | `(AggregateId, Seq[EventInfo[R]]) => DBSession => Unit` | new events of type `R`'s aggregate | `EventInfo(version, event, userId, timestamp)` |
| `AggregateListener(f)` | `(AggregateId, AggregateVersion, created: Boolean, Option[R]) => DBSession => Unit` | latest state; `created` is true when the first new version is 1 (`:694`) | `None` = deleted; cannot see individual events or undo |
| `AggregateWithEventsListener(f)` | `(AggregateId, AggregateVersion, Option[R], Seq[EventInfo[R]]) => DBSession => Unit` | state + new events | |
| `*.noDBSession(f)` | same, without a session | run outside a transaction; the subscription update uses `autoCommit` | for in-memory or side-effect-only listeners |

- Keys are the aggregate root type; **one listener per kind per aggregate type** (`toMap`
  silently keeps the last, `:180-196`). `Any`/`Nothing` types are rejected (`:200-204`).
- The callback's `DBSession` is the transaction that also writes the subscription version.
  Pass it to the document store so the read model and progress commit atomically
  (only when `keepInMemory = false`, §7).
- Event listeners receive `UndoEvent`s like any other event; undoing is the projection's
  job (the test domain does not, `ShoppingCartsListProjection.scala:59`). Aggregate
  listeners get the corrected state automatically.

### 6.3 Per-message algorithm (same for all three kinds)

For a message about aggregate `id`, with `last = subscriptionsState.lastVersionFor…(className, id)`
(`handleIdentifiableEventsUpdate` `:419-454`, `handleAggregateWithTypeAndEventsUpdate`
`:502-544`, `handleAggregateWithTypeUpdate` `:653-686`):

1. Split the message into `alreadyProcessed` (`version <= last`) and `newEvents`.
2. Only already-processed events → **ack immediately** (deduplication).
3. `newEvents.head == last + 1` → process:
   `subscriptionsState.localTx { s => listener(...)(s); newVersionFor…(className, id, last, newLast)(s) }`,
   then `MessageAck` to the bus. Any exception is **logged and not acked** (`:476-477, 623-626, 709-710`).
4. A gap (`head > last + 1`) → buffer the message in `delayed*Update(id)`. It is merged and
   retried when the next message for `id` arrives or when processing for `id` completes
   (`handleMessage*Processed`, `:489-500, 639-650, 723-733`).
5. Merging (`merge*Update`, `:309-416`) sorts the buffered messages by first version,
   concatenates contiguous runs, drops full overlaps, trims partial overlaps, and leaves the
   rest buffered.

### 6.4 Options (`ProjectionActorOptions`, `:41-76`)

| Option | Default | Effect |
|---|---|---|
| `groupUpdatesDelayMillis` | 0 | when > 0, non-replayed messages with first version ≥ `minimumDelayVersion` are buffered and processed together after the delay (`:249-274`) |
| `minimumDelayVersion` | 1 | lower bound for that grouping |
| `eventsToProcessImmediately` | ∅ | event classes that bypass grouping (event-carrying listeners only) |
| `parallelUpdateProcessing` | 0 | when > 1, listener work runs on a fixed thread pool, kept sequential **per aggregate** by a queue per aggregate (`runTaskAsyncPerAggregate`, `:564-598`). Listeners must be thread-safe; the connection pool must have at least that many connections. While an aggregate is being processed, its new non-replayed messages are buffered (`processedAggregates`). |
| `eventsLogger` | None | verbose per-message debug logging |

### 6.5 Delayed queries (read-your-writes for queries)

`delayIfNotAvailable[Async|AsyncCustom](respondTo, search, maxMillis)` (`:796-841`) answers
when `search` succeeds, otherwise parks the query and re-runs it after each processed
update (`ReplayQueries`) or at its deadline, then replies `None`. This lets a caller query
a document right after a command, before the projection has caught up.

### 6.6 `SubscribableProjectionActor`

Adds push subscriptions for clients: `handleSubscribe[DATA, UPDATE](id, listener, filter)`
and `sendUpdate[DATA](u)` deliver `SubscriptionUpdated(id, update)`. Subscriptions expire
after 10 minutes without `RenewSubscription`; an optional `updatesCacheTTL` replays recent
updates to new subscribers (`SubscribableProjectionActor.scala:33-140`).

### 6.7 `AggregateListenerActor`

A `ProjectionActor` with an empty query handler (`AggregateListenerActor.scala:3-13`), for
side-effect listeners such as integrations or notifications. It uses the same subscription,
ack and dedup machinery.

---

### 6.8 Document store (`PostgresDocumentStore.scala`)

Read models are stored as JSONB documents in `projection_<name>` tables (`space_id`, `id`,
`version`, `document`), with optional indexes on JSON paths created at start-up.

- **Optimistic updates.** `updateDocument` and `updateExistingDocument` read the document
  (from the cache or the table), apply `modify` and write the next version
  (`UPDATE … WHERE version = ?`). When another writer changed it first, they evict the cached
  copy and start again from fresh data, at most 10 times, then throw
  (`updateWithRetries`, fixed F21). So `modify` must be pure: it can run more than once.
- **Cache after commit.** Inside an `AfterCommit.localTx` (what `PostgresSubscriptionsState`
  uses for every listener transaction) cache entries are removed at once and stored only
  after the commit, so a rollback never leaves the cache ahead of the table (fixed F16).
  In any other session cache updates apply immediately.
- Writes need the listener's session: without one, the store opens a read-only session.

## 7. `SubscriptionsState` in depth (`SubscriptionsState.scala`)

**Stored data** (`:142-165`). Table `subscriptions(id, subscriber_type_id, subscription_type, aggregate_id, aggregate_version)`,
unique on `(subscriber_type_id, subscription_type, aggregate_id)`, plus an index on
`aggregate_id`. `subscriber_type_id = types_names id of the projection class name`;
`subscription_type`: 1 = AGGREGATES, 2 = EVENTS, 3 = AGGREGATES_WITH_EVENTS (`:15-33`).
One projection can therefore keep three independent cursors per aggregate.

**API.** `lastVersionFor…(subscriber, id)` and
`newVersionFor…(subscriber, id, expectedLast, newVersion)(session)`, which is a
**compare-and-set**: a mismatch throws `OptimisticLockingFailed` and rolls back the
listener transaction.

**Two modes**, selected by `PostgresSubscriptionsState(typesNamesState, keepInMemory)` (`:120`):

| | `keepInMemory = false` | `keepInMemory = true` |
|---|---|---|
| Source of truth | DB. INSERT when `last == 0`, otherwise `UPDATE … WHERE aggregate_version = last` with a rowcount check (`:397-418`); cache advanced only after the transaction commits (`AfterCommit`, since 1.0) | in-memory map; DB written only by `dump()` (`:303-350`) |
| Atomic with the listener's writes | yes (same `localTx`) | no |
| Intended for | normal operation | bulk replay (the replayer calls `dump()` periodically, `EventsReplayerActor.scala:179-182, 198`) |
| Risk | one DB round trip per update | progress lost on crash or restart unless the host calls `dump()` (F3); the test wiring uses `true` |

**Caches and concurrency.** `perAggregate` (and `dumped` in memory mode) cache one map per
touched aggregate (`:131-132`). Cache misses load from the DB outside the main monitor,
with 64 striped single-flight locks (`:134-140, 196-223`). The only eviction is in `dump()`,
which drops entries left unchanged during the write. `dump()` is meaningful only in memory
mode: `dumped` is never populated in DB mode, so a dump there would re-INSERT existing rows.
In DB mode the cache therefore grows with every aggregate touched (CLAUDE.md §7.C).

**Clearing.** `clearSubscriptionsInfo(subscriber)` deletes that subscriber's DB rows
(`:291-294`) but not the caches (F6). `MemorySubscriptionsState` is a test double with the
same compare-and-set contract (fixed F14). A failed compare-and-set in the database keeps
the cached version, so that aggregate stays stuck for the projection (F77).

**Related.** `VersionsState` (`components_versions`) stores the last-built `version` of each
aggregate context and projection; it is used only for rebuild decisions.

---

## 8. Replay / projection rebuild

`EventsReplayOrchestrator.replay(...)` (`EventsReplayOrchestrator.scala:25-100`):

1. Asks every projection for `SubscribedAggregates(name, version, aggregate types)`.
2. Compares them with `VersionsState`. Rebuilds the projections whose version changed and
   the projections that consume a changed aggregate type, and replays the aggregate types
   those projections need (`forceAll` bypasses this).
3. Sends `ClearProjectionData` (clears subscription rows, then calls `onClearProjectionData`).
4. Asks `EventsReplayerActor` to `ReplayAllEvents`. The replayer streams the events of each
   aggregate type (`readAndProcessAllEvents`, ordered by aggregate, noop events skipped),
   honours back-pressure through `BackPressureActor`, skips versions still in the outbox
   (`readNotYetPublishedEvents`), and feeds one `ReplayAggregateRepositoryActor` per
   aggregate. That actor rebuilds state, handles duplication by reading the base, and
   sends `PublishReplayedEvents(..., replayed = true)` (`ReplayAggregateRepositoryActor.scala:68-97`).
5. Saves the new versions to `VersionsState`.

Replayed messages go through the same bus routing, so the bus cursor must not already be
past them (F4). In practice the replay must run with a fresh in-memory bus.

---

## 9. Sagas (brief)

`SagaActor` (`SagaActor.scala`) is a process manager: `SagaOrder` → `handleOrder(step)`
returns `SagaContinues | SagaSucceded | SagaFailed` (failure switches to the `REVERTING`
phase and calls `handleRevert`). Progress is stored in `sagas` (`PostgresSagaState.scala`)
and reloaded at start. Each step passes `SagaStep(sagaId, step)` as the idempotency key of
the commands it sends, so a resumed saga doesn't repeat a command
(`MultipleCartCreatorSaga.scala`).

Since 1.0 (fixed F35, F17, F36, F34) every step result and every write of saga progress
comes back to the actor as a message, so all decisions run on the actor's thread;
`handleOrder` and `handleRevert` are called there too and must only start the work and
return a Future. A failed write is retried with back-off (from 100 ms, doubling, at most
`maxPersistAttempts` = 10 attempts; the last wait is 25.6 s). After that the saga stays at
its last stored step and resumes from it at the next start, which repeats that step; its
commands must therefore be idempotent. An order that cannot be stored at all, or gets no saga
id, is answered with `SagaFailureResponse` (F75). Pending sagas are read completely before
any is started, so a failed load is retried (without limit) without starting a saga twice,
and a restart of the actor does not load them again (F75). `updateSaga` and `deleteSaga` report whether the row was
there; a saga whose row vanished is stopped with an error log. The table has a unique
index on `(name, saga_id)`, built `CONCURRENTLY`. Still open: the blocking id-pool `Await`
(F23, M3).

---

## 10. Invariants to preserve when modifying the library

1. **One writer per aggregate, version strictly +1.** All persistence for an aggregate goes
   through its `AggregateRepositoryActor`, and the SQL procedures remain the final
   optimistic lock (`aggregates.base_version` on the own row).
2. **Event, aggregate-row and outbox writes share one transaction**, as do idempotent
   command responses. Publishing happens strictly after commit.
3. **Outbox rows are deleted only after every subscriber has acked.** Anything that
   finishes an event in the bus must also send `PublishEventsAck` to the publisher.
4. **Subscribers deduplicate by version** through `SubscriptionsState` and only advance on
   contiguous versions; the bus may deliver duplicates and out-of-order re-sends.
5. **Listener writes and subscription CAS share the `DBSession`** (DB mode). Code that runs
   listeners off the actor thread must not touch actor state; only messages to `self`
   (the existing async path is the pattern).
6. **Event handlers stay pure.** Restore, replay and live application must produce the
   same root; nothing may depend on wall-clock time or external I/O.
7. **Type names are stable identifiers.** Aggregate root, event, projection-class and
   response class names are persisted via `types_names`; renaming one is a data migration.
8. **Aggregate roots are immutable** and shared across threads by reference.
9. **The subscription set is fixed at bus start.** Adding a listener means changing
   `minimumExpectedSubscriptions` and creating the projection before the bus initialises.

---

## 11. Known problems

Problems found in this read, and every decision about fixing them, are tracked in
[KNOWN-ISSUES.md](KNOWN-ISSUES.md). The order of the work is in
[IMPLEMENTATION-PLAN.md](IMPLEMENTATION-PLAN.md). M1 fixed the most serious problems of
this read (lost document updates, caches ahead of the database, timestamps, lost sagas,
partial persists, idempotency). The most serious ones still open, at the end of M1:

- **F76:** permanently deleting an aggregate that duplicates read from breaks those
  duplicates (High if it happens; checked on a production copy).
- **F2, F5:** after a re-send, a projection can stall on an aggregate or apply events twice
  (M2).
- **F4:** a replay over a PostgreSQL bus cursor that moved past the events never finishes
  (M2; reproduced by `EventsReplaySpec`).
- **F44, F22:** no snapshots, and a restore holds the whole history in memory (M3).
- **F23:** the command bus blocks while it fetches an id pool (M3).
- **F79, F80:** the bus's debug timer changes actor state from another thread; a failing
  projection query restarts the projection actor (M2).

---

## 12. Where to change what

| Goal | Touch | Watch out for |
|---|---|---|
| New command kind / routing rule | `Command.scala`, `AggregateCommandBusActor.receive`, `CommandHandlerActor`, `CommandExecutorActor.handleFollowingCommand` | envelope types; the retry semantics in `CommandExecutorActor.onConflict` and `retryRequest` (a rewrite must fetch its events again, F73) |
| New special event type | `Event.scala`, `PostgresEventStoreState.persistEventsOneByOne` + a stored procedure, `AggregateRepositoryActor.applyPersisted`/`tryToApplyEvents`, `readAndProcessEvents` | batch eligibility (`:31-32`); restore semantics; replay path |
| Snapshots | `AggregateRepositoryActor.assureRestoredState`, `EventStoreState` (new read from version N), a new snapshot table | duplication chains (ancestor events), noop events whose `from_version` lies after the snapshot, the event-versioning map |
| Non-blocking persistence | `AggregateRepositoryActor.persist` → a dedicated dispatcher, re-entering via message | keep one in-flight persist per aggregate; apply the events before the next message is handled (F69: persist synchronously or stash); the in-memory version check |
| Outbox / re-send redesign | `AggregateRepositoryActor` (`eventsToPublish`, `pendingPublish`, `markPublishedEvents`), `AggregateCommandBusActor.ensureEventsPublished`, `EventStoreState` outbox methods | F2, F4; idempotency of re-send; PermanentDelete is triggered from the outbox delete |
| Bus cursor / dedup | `EventsBusActor.handlePublishEvents`/`handleMessageAck`, `EventBusState` | F1, F4, F11; back-pressure counters must stay balanced |
| New listener kind | `ProjectionActor` (listener class, map, `subscribe`, a handle/process/merge trio), `EventsBusActor` (subscription + message type), `SubscriptionType` | a new `subscription_type` id; dedup and gap logic; async path |
| Projection progress storage | `SubscriptionsState` | CAS semantics; same-session atomicity; `dump()` lifecycle (F3, F6) |
| Rebuild logic | `EventsReplayOrchestrator`, `EventsReplayerActor`, `ReplayAggregateRepositoryActor`, `VersionsState` | needs a bus cursor that doesn't filter replayed versions; back-pressure accounting |
| Idempotency rules | `commandhandler/Idempotency.scala`, `CommandResponseState` | the response is written in the persist transaction (I2); a taken key and a version conflict both answer with the stored response |
| Retries and timeouts | `CommandExecutorActor` (concurrent retries, command timeout), `SagaActor` (write retries), `PostgresDocumentStore.updateWithRetries` | bounded everywhere; the document-store retry runs inside the listener transaction and must stay immediate (AR6) |

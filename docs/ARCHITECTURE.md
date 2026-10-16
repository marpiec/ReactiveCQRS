# ReactiveCQRS — internals reference

A working map of how the library defines aggregates, persists them, publishes their
events and feeds projections/listeners. It is meant as the baseline for future changes
to the library's behaviour.

> **Provenance.** Static read of `master` @ `4b5060f` (version 0.12.47), 16/10/2026, updated for
> the changes of milestones M1 (to `cc8925c`), M2 (the delivery module, to `v1.0.0-M2`) and M3 as
> it goes (the aggregate owner), and checked against that code at each milestone review. Where a milestone changed the code,
> references name methods; the remaining line numbers are from `4b5060f` and may have drifted
> by a few lines. Defaults and limits quoted below are the literal values in the code;
> measured performance is in `docs/perf/`.
>
> **Naming.** There is no `events_queue` table or class. The publication queue is the
> **`events_to_publish`** table (a transactional outbox). The 0.12 bus cursor, the
> **`event_bus`** table, is no longer used since M2 (D7, D24). This document uses those names.

---

## 0. File index

Short names used throughout (all under `core/src/main/scala/io/reactivecqrs/core/`
unless the path says `api/` or `testdomain/`).

| Short name | Path |
|---|---|
| `AggregateContext.scala`, `Command.scala`, `Event.scala`, `CommandResult.scala`, `CustomCommandResponse.scala`, `Aggregate.scala` | `api/src/main/scala/io/reactivecqrs/api/` |
| `AggregateCommandBusActor.scala`, `CommandResponseState.scala`, `Idempotency.scala` | `commandhandler/` |
| `AggregateOwner.scala` (with `OwnerSettings`), `AggregateRestorer.scala` (M3) | `aggregate/` |
| `ReplayAggregateRepositoryActor.scala` (the 0.12 replayer's, until M4) | `aggregaterepository/` |
| `PostgresEventStoreState.scala`, `PostgresEventStoreSchemaInitializer.scala`, `EventStoreState.scala` | `eventstore/` |
| `EventsBusActor.scala`, `SubscriptionClassifier.scala` | `eventbus/` |
| `SequenceGate.scala`, `RedeliveryPolicy.scala`, `OutboxPollerActor.scala`, `Republish.scala` (the delivery module, M2) | `delivery/` |
| `OutboxDeletes.scala` (batched outbox deletes) | `eventstore/` |
| `Backoff.scala` (retry waits, AR6) | `util/` |
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
                      │ CommandEnvelope(respondTo, commandId, command)
                      ▼
                    AggregateOwner                     one per aggregate INSTANCE, owns state+version
                      │ idempotency lookup (commands_responses); runs the command handler as a Future
                      │ localTx: add_event(s) → events, aggregates, events_to_publish (+ command response)
                      │ answers the caller; PublishEvents(type, events, id, Option(root))
                      ▼
                    EventsBusActor                     one per system
                      │ IdentifiableEvents | AggregateWithType | AggregateWithTypeAndEvents
                      ▼
                    ProjectionActor / AggregateListenerActor  (N subscribers)
                      │ SequenceGate; localTx: listener(...) + SubscriptionsState compare-and-set
                      │ MessageAck | MessageFailed | RequestRepublish (a gap)
                      ▼
                    EventsBusActor ──PublishEventsAck──▶ AggregateCommandBusActor
                                                          ├─ deletes the outbox rows (batched)
                                                          └─ passes the ack to a live owner

OutboxPollerActor (one per system) ──Republish──▶ EventsBusActor ──▶ the type's command bus ──▶ owner
   every 30 s reads events_to_publish; re-publication at 3 and 6 minutes, "undelivered" at 10 (D8)
```

Wiring reference: `ReactiveTestDomainSpec.scala` (fixture). The host application must
initialise the ScalikeJDBC singleton connection pool before creating any component
(`README.md`), and call `initSchema()` on `PostgresTypesNamesState`,
`PostgresEventStoreState`, `PostgresCommandResponseState`, `PostgresSubscriptionsState` and
`PostgresSagaState`; the uid generators and the document stores create their own tables.
It creates the event bus, its subscribers and the command buses, sends the bus
`RegisterSubscribers(subscribers)` (D15), and creates one `OutboxPollerActor`. Since 1.0 the
saga index build refuses duplicate `(name, saga_id)` rows (F34).

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
  (`Option(root)` in `AggregateOwner.publish`).
- **It must be immutable.** The same instance is held by the owner, given to command handlers
  that run as futures, and sent inside `PublishEvents` to the bus and every subscriber, on
  other threads (`AggregateOwner.runHandler`, `publish`).
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
- Storage format: JSON text (mpjsons), with the class identified by a `types_names` id;
  `event_type_version` is always 0 (D29).

### 2.3 Changing an event class (D29)

There is no event versioning since 1.0: an event is stored under its own class name with
`event_type_version` 0. To change an event's shape, keep the old class (deprecated) and add a
new one (`ItemAddedV2`); event handlers and projections handle both. Renaming or moving a
stored event class breaks restore and replay. A stored row with another type version (written
through the removed `EV(...)` mappings) fails to read with `UnsupportedEventVersionException`;
`StoredDataCheck` counts such rows on a production copy before the upgrade.

### 2.4 Commands (`Command.scala`)

The five kinds are **abstract classes**, so a command is exactly one kind; the bus tells
them apart by class (`AggregateCommandBusActor.receive`).

| Kind | Members | State the handler runs on | Version check | On concurrent modification |
|---|---|---|---|---|
| `FirstCommand` | `userId`; since 1.0 optionally `chosenAggregateId` | `initialAggregateRoot` | expected = 0 | `AggregateConcurrentModificationError` (the chosen aggregate exists) |
| `Command` | `userId`, `aggregateId`, `expectedVersion` | the owner's state; the owner reloads first when the expected version is newer than its own (F19) | strict | `AggregateConcurrentModificationError` at once, without running the handler; also for an aggregate that does not exist (`was` = 0, F67) |
| `ConcurrentCommand` | `userId`, `aggregateId` | the owner's current state | current version | handler run again on the new state, at most 10 attempts, waiting 10 ms doubling to 500 ms, jittered (`AggregateOwner.conflict`, D37) |
| `RewriteHistoryCommand` | + `expectedVersion`, `eventsTypes` | the state and the events of `eventsTypes` | strict | error |
| `RewriteHistoryConcurrentCommand` | + `eventsTypes` | the current state and the events of `eventsTypes` | current version | run again like `ConcurrentCommand`, with the events read again (fixed F73) |

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
| `CommandSuccess(Nil)` | nothing written at all, not even a statement (R9, `AggregateOwner.persistOrAnswer`) | `SuccessResponse(id, version)` |
| `CommandFailure(msgs)` | nothing persisted | `FailureResponse(msgs)` |
| `RewriteCommandSuccess(rewritten, events, info)` | past events overwritten, then new events persisted | as success |
| `AsyncCommandResult(Future[...])` | the future's result comes back to the owner as a message | as above |
| exception or unhandled command (`MatchError`) | logged with an errorId | `CommandHandlingError(name, errorId, commandId)` |
| no result within `AggregateContext.commandTimeout` (60 s) | a result that comes later is dropped: the command is not applied (since M3) | `CommandHandlingError` (F20, R5) |
| the aggregate's restore failed (§4.4) | nothing persisted | `CommandHandlingError` |
| no id available (the uid generator fails) | nothing persisted, the bus keeps running (F75) | `CommandHandlingError` with command id 0 |
| event handler failed before persisting, or the persist failed | nothing persisted | `EventHandlingError(...)` |
| version mismatch (memory or SQL) | nothing persisted | `AggregateConcurrentModificationError(...)`, or the stored response for an idempotent command whose copy won |
| idempotency key taken, but its response already removed by retention | nothing persisted | `CommandHandlingError` |

`AggregateContext` provides an implicit `Future[CustomCommandResult] → AsyncCommandResult`
conversion (`AggregateContext.scala:26-28`). A command sent to a deleted aggregate, or to one that does not
exist, fails with "is already deleted" (or "does not exist") and yields
`CommandHandlingError` (`AggregateOwner.runHandler`).

### 2.6 Event handlers

`eventHandlers: (UserId, Instant, R) => PartialFunction[Any, R]` (`AggregateContext.scala:38,42`).
They run in three places:

1. **Before persisting**, on a copy of the state (`AggregateOwner.nextRoot`). A failure
   aborts the command with `EventHandlingError`; nothing is written. After the commit the
   copy becomes the owner's state, so they run once per event (fixed F25).
2. **Restore** on the owner's first message, and again on every reload: after an SQL
   conflict, when a command expects a newer version than the owner holds (F19), and after an
   undo, a duplication event or a rewrite (`AggregateRestorer.restore`).
3. **Replay** during projection rebuilds (`ReplayAggregateRepositoryActor.scala:45-59`).

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
  (`AggregateRestorer.restore`).

### 2.7 `AggregateContext`: the binding unit

```scala
abstract class AggregateContext[R : ClassTag] {           // AggregateContext.scala:24
  def initialAggregateRoot: R                             // state before the first event
  def commandHandlers: R => PartialFunction[Any, GenericCommandResult[Any]]
  def eventHandlers: (UserId, Instant, R) => PartialFunction[Any, R]
  def rewriteHistoryCommandHandlers                       // optional; default throws
      : (Iterable[EventWithVersion[R]], R) => PartialFunction[Any, GenericCommandResult[Any]]
  val version: Int                                        // bump to force projection rebuild
  val commandTimeout: FiniteDuration = 60.seconds         // since 1.0 (R5): answer CommandHandlingError after it
  val snapshots: Option[SnapshotPolicy] = None             // since 1.0 (D30): performance snapshots, opt-in
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

1. **Routing** (`AggregateCommandBusActor.routeCommand`). The bus takes a `CommandId` (and,
   for a `FirstCommand` without `chosenAggregateId`, a new `AggregateId`) from in-memory
   pools. When a pool is empty it calls `UidGeneratorActor` with a **blocking** `Await` of up
   to 60 s (F23, until M3's performance step); a failing generator is answered with
   `CommandHandlingError` (F75), a message the bus does not understand with `Status.Failure`
   (F70). It sends `CommandEnvelope(respondTo, commandId, command)` to the aggregate's
   `AggregateOwner`, creating it if needed; an id just taken from the pool makes a *fresh*
   owner, which skips the restore query.
2. **Idempotency** (`AggregateOwner.attempt`). For an `IdempotentCommand` with a key, a
   synchronous `commands_responses` lookup returns the stored response instead of
   executing. Copies running at the same time are settled at persist (step 5).
3. **Version check.** A `Command` or `RewriteHistoryCommand` expecting a version above the
   owner's reloads first (another writer moved the aggregate on, F19); a mismatch is
   `AggregateConcurrentModificationError` at once, without running the handler. A
   `FirstCommand` expects version 0; the concurrent kinds run on the current version.
4. **Handler** (`AggregateOwner.runHandler`). The handler runs as a `Future` on the owner's
   state (an immutable value; a rewrite also gets the events of the classes it names, read
   before); an `AsyncCommandResult` is flattened, and the result comes back as a message. A
   timer per command answers `CommandHandlingError` at the context's `commandTimeout` (R5); a
   result that arrives later is dropped, so a timed-out command is never persisted.
5. **Persist** (`AggregateOwner.persist`). A command without events writes nothing (R9).
   When another command persisted since the handler started, a `ConcurrentCommand` (or
   concurrent rewrite) runs again on the new state after a `Backoff` wait (10 ms doubling to
   500 ms, jittered, at most 10 attempts, D37), and any other command gets the conflict error
   (after the idempotency check). Otherwise the event handlers run once, on a copy of the state
   (F25), and one `eventStore.localTx` on the owner's thread
   - optionally overwrites past events (rewrite commands),
   - calls `persistEvents` (`add_event` per event, or one `add_events` call for 2+
     plain/first events), with one timestamp truncated to microseconds (D10),
   - stores the idempotent command response in the **same transaction**
     (`INSERT … ON CONFLICT (key) DO NOTHING`; nothing inserted means the key is taken).

   Any failure inside the transaction is thrown, so it rolls back whole (fixed F68).
6. **After commit** the copy of step 5 becomes the owner's state (undo and duplication
   events, and rewrites, re-read it from the store instead), the events are published,
   waiting `GetAggregateMinVersion` queries are answered, and the caller gets the response
   with the new version. Every other outcome is one answer as well: "Concurrent aggregate
   modification exception" from PostgreSQL → reload, then the conflict rule of step 5; a
   taken idempotency key → the stored response; anything else → `EventHandlingError`. If the
   re-read after an undo, duplication or rewrite fails, nothing is published and the owner
   enters the failed-restore state (§4.4); the caller still gets success, because the events
   are committed, and the outbox poller has them re-published later.

Ordering note: the state changes before the next message is processed (fixed F69), so a
command whose handler ran on the older state meets the version check of step 5. The SQL lock
(§4.2) still catches writers outside the owner.

---

## 4. Aggregate persistence (PostgreSQL)

Every table, sequence and function lives in the schema the connection's `search_path`
selects, and catalog lookups are limited to `current_schema()` (fixed F71), so one database
can hold several installations or copies in separate schemas. The test suite uses this: each
run works in a new schema (`TestDatabase`).

### 4.1 Event-store tables (`PostgresEventStoreSchemaInitializer.scala`)

| Table | Columns / keys | Role |
|---|---|---|
| `events` (`:29-41`) | `id` (from `events_seq`) PK, `user_id`, `aggregate_id`, `event_time TIMESTAMP`, `version`, `event_type_id`, `event_type_version`, `event TEXT`; indexes `(aggregate_id)`, `(aggregate_id, id)`, and `(aggregate_id, version)` built online by `buildIndexesConcurrently()` (F38) | append-only event log |
| `aggregates` (`:103-115`) | PK `(id, base_id)`, `space_id`, `creation_time`, `type_id`, `base_order`, `base_id`, `base_version`; indexes `(type_id)`, `(base_id)` | one row per (aggregate, ancestor) in its duplication chain; the own row (`base_id = id`) holds the **current version** used for locking |
| `noop_events` (`:75-81`) | `id` (= `events.id`) PK, `from_version` | events cancelled by an undo, effective from that version |
| `snapshots` (since M3) | `aggregate_id` PK, `version`, `context_version`, `schema_version`, `state TEXT`, `created` | the latest performance snapshot of an aggregate (D30, §4.3); disposable |
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

Version checks happen twice: in memory (the owner) and in SQL. The SQL check is the real
arbiter for writers outside the owner: another node (M5), or a write made outside the
library. Two owners of one aggregate never run at once in one command bus: a new one starts
only after the stopped one terminated (§4.4).

Limits (`PostgresEventStoreState.scala:15-17`): `eventSizeLimit = 100000` characters
(overridable per class), `aggregateVersionLimit = 10000`, `fetchSize = 1000`.

### 4.3 Reading state back

- **Restore** (`readAndProcessEvents`, `PostgresEventStoreState.scala:189-248`): join
  `events` to every `aggregates` chain row of the aggregate
  (`events.aggregate_id = aggregates.base_id AND events.version <= aggregates.base_version`),
  left-join `noop_events` to get the noop flag, order by `base_order, version`. Ancestor
  events rebuild the state but don't increment the version (`aggId == aggregateId` check).
- **The restore module** (`AggregateRestorer`): the state at the newest version, at a version
  or at an instant, and the events behind it; it hides the chains, the noop events and the
  snapshots. The owner restores through it, on its thread, on its first message and on every
  reload (§3); the command bus answers **historical reads** (`GetAggregateForVersion`,
  `GetAggregateAtInstant`, `GetEventsForAggregateForVersion`) with it in a `Future`, without
  an actor (since M3; before, each read started a temporary repository actor). Restore reads
  stream the rows (autocommit off and a fetch size, fixed F22). A read at an instant first
  finds the aggregate's version at that instant in SQL, then reads up to that version, with
  events undone later still counted as applied then (fixed F39; before, all events were read
  and filtered in Scala, with today's undo flags). Measured before snapshots: about 230k
  events/s, peak heap growing with the history (450 MB at 500k events, `baseline-M2.md`).
- **Snapshots** (D30, opt-in per context: `snapshots = Some(SnapshotPolicy(every, onPassivation,
  schemaVersion))`). Table `snapshots(aggregate_id PK, version, context_version, schema_version,
  state, created)`: the aggregate's latest state as its root's mpjsons JSON. The owner writes it
  right after a commit that reaches the next multiple of `every`, and when it stops if
  `onPassivation`, in a short transaction of its own on its thread; a failed write is only
  logged. A restore uses it when its context and schema versions match and it lies at or
  before the version asked for, then reads the aggregate's own events after it
  (`readAndProcessOwnEvents`; a duplicate's base events all come before its own); otherwise,
  and for reads at an instant, it reads every event, never failing because of the snapshot
  (R10). An undo deletes the aggregate's snapshot, a rewrite those of the aggregate and of its
  duplicates, a permanent delete its own, each in its transaction; the owner runs all of them,
  so a snapshot write cannot race them.
- `GetAggregateMinVersion(id, v, maxMillis)` waits in the owner until the version is reached
  and is answered then; when `maxMillis` passes first, it is answered with
  `Failure(AggregateInIncorrectVersionException)` (fixed F81).
- Queries never reload: a running owner does not see writes made around it (I1, one writer
  per aggregate). Only a command that expects a newer version reloads it (F19).

### 4.4 Owner lifecycle

- Created lazily by `AggregateCommandBusActor` on the first command, query or re-publish
  request for an id. The first message restores the state; what waits in the outbox is
  re-published when the outbox poller asks (D8, F72). An owner made for an id just taken from
  the pool starts at version 0 without reading.
- **Failed restore** (F10, D11): the owner answers commands with `CommandHandlingError` and
  queries with `Failure(AggregateRestoreFailedException)`, and after `failedRestoreRetryAfter`
  (5 s) asks the command bus to stop it (`OwnerPassivate`); the next request restores again in
  a new owner.
- **Stopping (passivation):** the command bus sends `StopOwner` and keeps the messages that
  arrive for that aggregate meanwhile; the owner answers the commands it started (and any
  waiting min-version queries) and stops; once it terminated, a new owner gets the kept
  messages. Reasons: reaping (below), a permanent delete, a failed restore, and
  `PassivateAggregate(id)` (operations, tests). It is Cluster Sharding's passivation (D43,
  D72), so M5 can put a shard in the bus's place without changing the owner.
- **Reaping** (`AggregateCommandBusActor.clearOldAggregateRepositories`): at most every 5
  minutes, and only when a message for some owner arrives. When more than `keepAliveLimit`
  (200) owners are tracked, the least recently used ones idle for more than 2 minutes are
  stopped; any owner idle for more than `maxInactivityMillis` (12 h) too.
- **Failures (AR7):** the owner never throws from `receive`: a failure that belongs to one
  command or query is answered to its caller; an unexpected failure of the owner itself is
  logged, and its state is read again from the store with the next message. The command bus's
  supervisor strategy stops a failed owner instead of restarting it (a restart would lose its
  pending callers). The event bus, the projections and the outbox poller (M2) also answer or
  log a failing message. `DeathWatch`: the command bus on its owners, the event bus on its
  subscribers.

### 4.5 History rewrite and permanent delete

- **Rewrite** (`RewriteCommandSuccess`): `overwriteEvents` runs `UPDATE events SET event = ?`
  per `(aggregate_id, version)` in the same transaction (`overwriteEvents`). Only new events
  are published; after the commit the owner reads its state again from the store (D17). A
  rewrite to another event class is not rejected yet (F8, M3's rewrite step). The test domain
  pairs each rewrite with a compensating event (`CartNameRewritten`).
- **Permanent delete**: the bus names the version of a `PermanentDeleteEvent` in its
  acknowledgement (`permanentDeleteAt`); the command bus then calls
  `EventStoreState.deleteAggregatePermanently` and stops the owner, whose state is gone. In one transaction it deletes the
  `aggregates`, `noop_events` (before `events`, so its sub-select still matches), `events`,
  `events_to_publish` and `subscriptions` rows. While a subscription of the aggregate is
  failed (`subscription_failures`, D20a) it deletes nothing and returns false; the event's
  outbox row stays and the next re-publication's ack tries again. In-memory caches are not
  touched. Duplicates of the deleted aggregate keep chain rows that point at it and lose its
  history (F76, M3).

---

## 5. Event publication

The delivery module of M2 (D3, D4, D7, D8, D15, D20a–c; design in `docs/design/M2-delivery.md`).
Contract: at least once, in order per aggregate, applied exactly once; failures and silence
are bounded, visible and repairable; the bookkeeping returns to zero when idle.

### 5.1 The outbox: `events_to_publish`

Lifecycle of one event:

1. Inserted by `add_event` in the command transaction, so it is never lost.
2. After commit the owner publishes it with its state (`PublishEvents`).
3. When every subscription has acknowledged or failed it, the bus sends
   `PublishEventsAck(aggregateId, versions, permanentDeleteAt)` to the actor the owner
   published on behalf of: its command bus (`publicationAcks`), which outlives the owner, so
   an owner reaped meanwhile loses nothing (F91).
4. The command bus hands the versions to `EventStoreState.deletePublished`; the PostgreSQL
   store deletes the rows of many acknowledgements in one statement (two arrays through
   `unnest`, up to 5 000 rows), on one background thread shared by every store, at most 20 ms
   late (`OutboxDeletes`, F26). A failed delete is logged and only means a later, harmless
   re-publication. When `permanentDeleteAt` is set it deletes the whole aggregate instead
   (§4.5). A live owner gets the acknowledgement too, to continue re-publishing its outbox.

**Re-delivery** has a single owner, the `OutboxPollerActor` (D8). Every 30 s (the first poll
10 s after start) it reads every aggregate with waiting rows in one query for all types
(`readWaitingInOutbox`, F37), and the pure `RedeliveryPolicy` decides:

| Rows | Re-publication | Then |
|---|---|---|
| written while the process runs (the live publication was the first attempt) | 3 and 6 minutes after it | at 10 minutes "undelivered", logged once; the rows stay for the next start |
| written before the process started | at the first poll, then 3 and 6 minutes later | the same, 10 minutes after the first poll |
| a newer oldest version (the older rows were delivered) | a fresh set of attempts | |

At most 1 000 aggregates are re-published per poll, the longest waiting first, so a backlog
after an outage does not start every owner at once. The poller sends `Republish(type, id)`
to the bus, which passes `RepublishOutbox(id)` to the type's command bus (registered with
`RegisterPublisher`), which passes it to the owner (creating it if needed; re-publications
count as activity for reaping). The owner re-publishes from its oldest outbox version to its
current version, with its current state, so a state-receiving subscriber always gets a
consistent range. A range longer than 1 000 events goes in parts (F83): the owner sends the
next part of its outbox when the bus acknowledges the previous one, and a subscriber filling a
gap asks for each next part itself; parts before the last carry the current state too.
`GetOutboxStatus` reports the waiting and the given-up aggregates; the log names at most 10 of
them per poll.

### 5.2 `EventsBusActor`

**Start-up (D15).** The host sends `RegisterSubscribers(subscribers)`. The bus watches each,
asks it for its subscriptions (`GetSubscriptionRequests` → `SubscriptionRequests`), and
stashes everything else until all have answered, logging the missing ones on each tick. A
subscriber that stops before answering is dropped. Subscriptions are fixed after start.

**Routing.** For each subscription of the event's aggregate type that accepts the event:

| Subscription request | Classifier | Message sent | Listener kind (§6.2) |
|---|---|---|---|
| `SubscribeForEvents` | `SubscriptionClassifier` (aggregate id + event type) | `IdentifiableEvents(type, id, events, replayed)` | `EventsListener` |
| `SubscribeForAggregates` | `AggregateSubscriptionClassifier` (id only) | `AggregateWithType(type, id, lastVersion, versions, root, replayed)` | `AggregateListener` |
| `SubscribeForAggregatesWithEvents` | `SubscriptionClassifier` | `AggregateWithTypeAndEvents(type, id, root, events, replayed)` | `AggregateWithEventsListener` |

`root` is the aggregate state after the delivered batch. `ProjectionActor` subscribes with
accept-all classifiers.

**Deliveries (D7, D20b).** The bus keeps no cursor. It keeps one delivery per event with the
subscriptions it waits for, one per (subscriber, `DeliveryKind`): a projection with an events
listener and a state listener of one type answers for each kind (F84). A delivery ends when
each has answered `MessageAck` or `MessageFailed` (both carry the kind), when the deadline
passes (10 minutes: "undelivered", no ack, logged and listed by `GetDeliveryStatus`), or when
the subscriber stops (watched). A stopped subscriber keeps its subscriptions, marked stopped:
the deliveries it would take end without an owner ack, so their rows stay in the outbox
until the next start (F87); `DeliveryStatus.stopped` lists it.
Only a delivery ended by acks and failures acknowledges the owner, grouped per owner and
aggregate; one ended by the deadline or a stop keeps its outbox rows. An event published
again while its delivery waits goes only to the subscribers it waits for. A re-publication
to one subscriber (`PublishEvents(onlyTo = Some(s))`) never acknowledges the owner, so it
cannot delete rows that others still wait for. Events nobody subscribed to are acknowledged
at once. A delivery waiting longer than a minute is logged once (at most 10 lines per tick),
and waiting deliveries are summed up every 5 minutes (F92). Timers are `Tick` messages to the
bus (F79).

**Re-publish requests.** `RequestRepublish(subscriber, type, id, from)` from a subscriber
with a gap or a retry goes to the type's command bus as `RepublishRange(id, from,
subscriber)`; the owner re-publishes `from` to its current version to that subscriber only
(D20c).

**Failures inside the bus (AR7).** A message the bus fails on (a classifier that throws) is
logged and dropped. A restart would lose every delivery and the subscriptions.

**Back-pressure.** Only for the replayer (until M4): `ConsumerStart`/`ConsumerStop` and
`ConsumerAllowedMore`, bounded by `DeliveryOptions.maxReplayBuffer` (10 000) replayed events
in flight.

### 5.3 What subscribers can rely on

- **At least once**, through the outbox, the live publication and the poller's
  re-publications; deliveries can repeat, overlap or arrive after later ones.
- **In order per aggregate on the live path** (one owner per aggregate, Pekko's per-sender
  ordering).
- **Applied exactly once** by `ProjectionActor`, through the sequence gate on the stored
  subscription version (§6.3); a gap is asked from the owner. A `noDBSession` listener runs
  before its version is written, so it sees a batch again if that write fails: at least once.
- **No back-pressure on the live path** (F93): when commands come faster than a projection
  applies them, its mailbox and the bus's deliveries grow with the lag.
- A delivery that nobody answers ends at the deadline; the poller tries again at the next
  start.

---

## 6. Projections and listeners

### 6.1 The `ProjectionActor` contract (`ProjectionActor.scala`)

```scala
class MyProjection(val subscriptionsState: SubscriptionsState, store: DocumentStore[Doc])
    extends ProjectionActor(ProjectionActorOptions.DEFAULT) {
  protected val projectionName = "MyProjection"   // used by VersionsState (rebuild decisions)
  protected val version = 1                       // bump to force rebuild
  protected val listeners = List(EventsListener(onEvents), AggregateListener(onAggregate))
  protected def receiveQuery: Receive = { case MyQuery(..) => sender() ! ... }
  protected def onClearProjectionData(): Unit = ()   // called on ClearProjectionData (rebuild)
}
```

The projection is passed to the bus in the host's `RegisterSubscribers` list; it answers the
bus's `GetSubscriptionRequests` with one request per listener (D15). Progress in
`SubscriptionsState` is keyed by **`this.getClass.getName`**, not `projectionName`.
Renaming or moving the class resets the projection's progress.

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

Since M2 every delivery goes through one path (`receiveDelivery`, `drain`, `afterBatch`).
Deliveries are queued per **(listener kind, aggregate)**; a queue exists only while it has
work and is removed when idle (F15). Acknowledgements and failures carry the kind (F84).
Draining a queue, with `applied` the subscription's stored version:

1. The waiting deliveries are taken in the order of their first versions. One at or below
   `applied` is acknowledged at once. For the others the `SequenceGate` (D4) decides against
   the running version: **apply from–to** → its new versions join one batch (a partly
   applied delivery adds only its new part, F5); **already applied** (all its versions lie
   in the batch) → it is acknowledged with the batch (F86); **gap** → it and everything after
   it wait.
2. The batch runs `listener(...)` and `newVersion(className, kind, id, applied, to)` in one
   `localTx` (a `noDBSession` listener runs first, then the version is written in
   `autoCommit`). State-receiving listeners get the state of the delivery holding the last
   version, and `created` when the batch starts at version 1. On success every delivery of
   the batch is acknowledged with all its versions. One batch per queue runs at a time; in
   parallel mode a scheduled drain during a batch is left to the batch's end (F89).
3. A gap is asked from the owner: `RequestRepublish(self, type, id, from)` to the bus. An
   unanswered request is repeated after `gapRequestRepeat` (10 s), doubling up to 5 minutes,
   however many deliveries arrive meanwhile (F83); a long gap comes in parts of 1 000 events,
   each next part asked at once. The re-publication arrives as a normal delivery; the waiting
   ones then become already applied (D20c, F45). A gap still open after `gapGiveUpAfter`
   (10 minutes) is recorded as a failure at its first version (F90).
4. A batch that throws is retried with `options.retries` (`Backoff` 1 s doubling to 30 s,
   nine attempts, about two minutes); deliveries arriving meanwhile wait. After the last
   attempt the failure is recorded in `subscription_failures` (first version not applied,
   error, since); only then are the waiting deliveries and every later one of that aggregate
   and kind answered `MessageFailed`, and other aggregates keep flowing (D20a, R1). While the
   failure cannot be stored, nothing is answered and the batch is tried again every 30 s (F85).
5. A store that cannot be read (the version) is retried every 30 s without becoming a
   failure.

**Failed subscriptions** (D20a). `GetProjectionStatus` → `ProjectionStatus(name, failures,
retrying, waitingForGap)`. `RetryFailedSubscription(id)` asks the owner for each failed
version again, repeated like a gap, and clears the failure once a batch reaches that version
(F88); `RetryAllFailedSubscriptions` does it for every failure, 20 per second
(`failureRetryInterval`); `SkipFailedVersion(id)` marks each failed version as applied without
applying it and asks once for the versions after it. Failures recorded before a start are
loaded when the bus registers the projection and retried the same way
(`retryFailuresOnStart`).

**Failures of the actor itself** (AR7, F80). A message the projection fails on, a query
included, is answered with `Status.Failure` and never rethrown, so the actor keeps its
queues. The parallel pool is shut down when the actor stops.

### 6.4 Options (`ProjectionActorOptions`)

| Option | Default | Effect |
|---|---|---|
| `groupUpdatesDelayMillis` | 0 | when > 0, live deliveries whose first version is ≥ `minimumDelayVersion` wait that long before their queue is drained, so they are applied together; a delivery that must not wait drains the queue at once |
| `minimumDelayVersion` | 1 | lower bound for that grouping |
| `eventsToProcessImmediately` | ∅ | event classes whose deliveries never wait (event-carrying listeners only) |
| `parallelUpdateProcessing` | 0 | when > 1, batches run on a fixed pool of that many threads, one batch per queue at a time, so each aggregate keeps its order. Listeners must be thread-safe; the connection pool must have at least that many connections |
| `eventsLogger` | None | verbose per-delivery debug logging |
| `retries` | `Backoff(1 s, 30 s, 9 attempts)` | the retries of a failing batch before the failure is recorded (D20a) |
| `retryFailuresOnStart` | true | retry the recorded failures at start, 20 per second |
| `gapRequestRepeat` | 10 s | how long a request for missing versions waits before it is sent again, doubling up to 5 minutes |
| `gapGiveUpAfter` | 10 minutes | how long missing versions are asked for before the gap is recorded as a failure |
| `failureRetryInterval` | 50 ms | the pace of retrying many failures (at start, `RetryAllFailedSubscriptions`) |

### 6.5 Delayed queries (read-your-writes for queries)

`delayIfNotAvailable[Async|AsyncCustom](respondTo, search, maxMillis)` answers when `search`
succeeds, otherwise parks the query and re-runs it after applied updates (once per burst,
F28) or at its deadline, then replies `None`. A search that throws is answered with the
failure (F80). This lets a caller query a document right after a command, before the
projection has caught up.

### 6.6 `SubscribableProjectionActor`

Adds push subscriptions for clients: `handleSubscribe[DATA, UPDATE](id, listener, filter)`
and `sendUpdate[DATA](u)` deliver `SubscriptionUpdated(id, update)`. Subscriptions expire
after 10 minutes without `RenewSubscription`; an optional `updatesCacheTTL` replays recent
updates to new subscribers. Listeners may call `sendUpdate` from pool threads (parallel mode):
the update reaches the subscriptions through a message to the actor. The one-minute tick that
expires subscriptions also trims the cache, and stops with the projection (F95).

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

**API.** `lastVersion(subscriber, subscriptionType, id)` and
`newVersion(subscriber, subscriptionType, id, expectedLast, newVersion)(session)`, which is a
**compare-and-set**: a mismatch throws `OptimisticLockingFailed` and rolls back the
listener transaction. An unknown subscription is at version zero.

**Two modes**, selected by `PostgresSubscriptionsState(typesNamesState, keepInMemory)` (`:120`):

| | `keepInMemory = false` | `keepInMemory = true` |
|---|---|---|
| Source of truth | DB. INSERT when `last == 0`, otherwise `UPDATE … WHERE aggregate_version = last` with a rowcount check (`:397-418`); cache advanced only after the transaction commits (`AfterCommit`, since 1.0) | in-memory map; DB written only by `dump()` (`:303-350`) |
| Atomic with the listener's writes | yes (same `localTx`) | no |
| Intended for | normal operation | bulk replay (the replayer calls `dump()` periodically, `EventsReplayerActor.scala:179-182, 198`) |
| Risk | one DB round trip per update | progress lost on crash or restart unless the host calls `dump()` (F3); the test wiring uses `true` |

**Caches and concurrency.** `perAggregate` (and `dumped` in memory mode) cache one map per
touched aggregate. Cache misses load from the DB outside the main monitor, with 64 striped
single-flight locks. In DB mode the cache is an LRU of at most `maxCachedAggregates`
(10 000) aggregates (F29); the database stays the source of truth, and a failed
compare-and-set in the database evicts the aggregate's entry, so the next attempt starts
from the stored version (F77). In memory mode nothing is evicted except by `dump()`, which
drops entries left unchanged during the write; `dump()` is meaningful only in memory mode.

**Clearing.** `clearSubscriptionsInfo(subscriber)` deletes that subscriber's version rows and
failures and removes its cached versions (F6). `MemorySubscriptionsState` is a test double
with the same contract (F14).

**Failures (D20a).** Table `subscription_failures(subscriber_type_id, subscription_type,
aggregate_id, aggregate_type_id, failed_version, error, failed_since)`, primary key on the
first three, index on `aggregate_id`: one row per subscription that gave up on an aggregate,
replaced by a later failure of the same subscription. `recordFailure`, `failures(subscriber)`
and `clearFailure(...)(session)`; a permanent delete waits while the aggregate has a row
here (§4.5).

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

Replayed messages go through the same bus routing; since M2 the bus keeps no cursor, so
events delivered before do not stall a replay (F4). A replay leaves out undone events, so a
projection rebuilding from cleared state sees gaps at those versions (F58, High, M4): it asks
the owner for them, which works only when a command bus of the type is registered on the
replay bus; otherwise the replay waits until the bus deadline ends those deliveries. M4
replaces this replayer.

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
   through its `AggregateOwner`, and the SQL procedures remain the final
   optimistic lock (`aggregates.base_version` on the own row).
2. **Event, aggregate-row and outbox writes share one transaction**, as do idempotent
   command responses. Publishing happens strictly after commit.
3. **Outbox rows are deleted only after every subscriber has acked or failed them.** A
   delivery that ends otherwise (deadline, a stopped subscriber, a re-publication to one
   subscriber) must not send `PublishEventsAck`; the poller re-publishes its rows.
4. **Subscribers apply each version once, in order**, through the `SequenceGate` on the
   stored `SubscriptionsState` version, and ask the owner for gaps; the bus may deliver
   duplicates, overlaps and out-of-order re-publications.
5. **Listener writes and subscription CAS share the `DBSession`** (DB mode). Code that runs
   listeners off the actor thread must not touch actor state; only messages to `self`
   (the existing async path is the pattern).
6. **Event handlers stay pure.** Restore, replay and live application must produce the
   same root; nothing may depend on wall-clock time or external I/O.
7. **Type names are stable identifiers.** Aggregate root, event, projection-class and
   response class names are persisted via `types_names`; renaming one is a data migration.
8. **Aggregate roots are immutable** and shared across threads by reference.
9. **The subscription set is fixed at bus start**: the host's `RegisterSubscribers` list,
   each subscriber answering `GetSubscriptionRequests`. Adding a projection means adding it
   to that list.

---

## 11. Known problems

Problems found in this read, and every decision about fixing them, are tracked in
[KNOWN-ISSUES.md](KNOWN-ISSUES.md). The order of the work is in
[IMPLEMENTATION-PLAN.md](IMPLEMENTATION-PLAN.md). M1 fixed the most serious problems of
this read (lost document updates, caches ahead of the database, timestamps, lost sagas,
partial persists, idempotency); M2 rebuilt delivery (no cursor, a single owner of
re-deliveries, the sequence gate, gap requests, failure records). The most serious ones
still open, at the end of M2:

- **F76:** permanently deleting an aggregate that duplicates read from breaks those
  duplicates (High if it happens; checked on a production copy; M3).
- **F58:** a rebuild leaves out undone events, and the gaps stall it unless the owner can be
  asked for them (High; M4 replaces the replayer).
- **F23, F27:** the command bus blocks while it fetches an id pool; JDBC work runs on the
  default dispatcher (M3).
- **F82:** the replayer stops the JVM when a replay stalls (M4).

---

## 12. Where to change what

| Goal | Touch | Watch out for |
|---|---|---|
| New command kind / routing rule | `Command.scala`, `AggregateCommandBusActor.receive`, `AggregateOwner` (`expectedVersion`, `runsAgainOnConflict`, `rewrittenClasses`, `userIdOf`) | the retry rule in `AggregateOwner.conflict` (a rewrite must read its events again, F73); a timed-out command must never be persisted |
| New special event type | `Event.scala`, `PostgresEventStoreState.persistEventsOneByOne` + a stored procedure, `AggregateOwner.persist`/`nextRoot` (`readAfterCommit`), `readAndProcessEvents` | batch eligibility (`:31-32`); restore semantics; replay path |
| Snapshots | `AggregateRestorer` (use), `AggregateOwner.snapshotIfDue`/`stopNow` (write), `EventStoreState.readSnapshot`/`writeSnapshot`/`readAndProcessOwnEvents`, the deletes in `add_undo_event`'s caller, `overwriteEvents` and `deleteAggregatePermanently` | every change that alters the state behind a snapshot must delete it in its own transaction; a snapshot is read only when its context and schema versions match; never fail a restore because of a snapshot |
| Non-blocking persistence | `AggregateOwner.persist` → a dedicated dispatcher, re-entering via message | keep one in-flight persist per aggregate; apply the events before the next message is handled (F69: persist synchronously or stash); the in-memory version check |
| Re-delivery timing | `delivery/RedeliveryPolicy` (pure), `OutboxPollerActor`, `EventStoreState.readWaitingInOutbox` | one owner of retries (D8); the attempts at 3 and 6 minutes come after a subscriber's own retries (about 2 minutes, D20a); the per-poll limit |
| Outbox deletes | `AggregateCommandBusActor.delivered`, `EventStoreState.deletePublished` / `deleteAggregatePermanently`, `OutboxDeletes` | only acked (or failed) versions; the acknowledgement goes to the command bus, never only to the owner, which may be reaped (F91); a permanent delete waits while a subscription of the aggregate is failed |
| Delivery tracking | `EventsBusActor` (`publish`, `finish`, `checkDeadlines`, `subscriberStopped`) | a delivery that ends without every ack or failure must not ack the owner; re-publications to one subscriber never do; back-pressure counters stay balanced; AR7 (no throws) |
| New listener kind | `ProjectionActor` (listener class, map, `subscriptionRequests`, the delivery case in `receiveUpdate`, `appliedVersion`, `advance`, `applyBatch`), `EventsBusActor` (request + message type), `SubscriptionType` | a new `subscription_type` id; everything else goes through the one gate path |
| Projection progress storage | `SubscriptionsState` | CAS semantics; same-session atomicity; the bounded cache and its eviction on a failed CAS (F29, F77); `dump()` lifecycle (F3); failures (D20a) |
| Rebuild logic | `EventsReplayOrchestrator`, `EventsReplayerActor`, `ReplayAggregateRepositoryActor`, `VersionsState` | undone events left out (F58); back-pressure accounting; replaced in M4 |
| Idempotency rules | `commandhandler/Idempotency.scala`, `CommandResponseState` | the response is written in the persist transaction (I2); a taken key and a version conflict both answer with the stored response |
| Retries and timeouts | `util/Backoff`; `AggregateOwner` (concurrent retries, command timeout), `SagaActor` (write retries), `ProjectionActor` (listener retries), `RedeliveryPolicy`, `PostgresDocumentStore.updateWithRetries` | bounded everywhere; the document-store retry runs inside the listener transaction and must stay immediate (AR6) |

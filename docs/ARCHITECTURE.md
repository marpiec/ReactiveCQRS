# ReactiveCQRS — internals reference

A working map of how the library defines aggregates, persists them, publishes their
events and feeds projections/listeners. It is meant as the baseline for future changes
to the library's behaviour.

> **Provenance.** Static read of `master` @ `4b5060f` (version 0.12.47), 16/10/2026, updated for
> the changes of milestones M1 (to `cc8925c`), M2 (the delivery module, to `v1.0.0-M2`), M3 (the
> aggregate owner, to `v1.0.0-M3`) and M4 as it goes (projection builds), and checked against
> that code at each milestone review. Where a milestone changed the code, references name
> methods; the remaining line numbers are from `4b5060f` and may have drifted
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
| `AggregateOwner.scala` (with `OwnerSettings`), `AggregateRestorer.scala`, `OutboxWriter.scala`, `SquashScheduler.scala` (M3) | `aggregate/` |
| `PostgresEventStoreState.scala`, `PostgresEventStoreSchemaInitializer.scala`, `EventStoreState.scala` | `eventstore/` |
| `EventsBusActor.scala` | `eventbus/` |
| `UidGeneratorActor.scala`, `IdPool.scala` (ids from pools fetched ahead, M3) | `uid/` |
| `BlockingDispatcher.scala` (F27) and `reference.conf` (`reactivecqrs.blocking-dispatcher`, the serializer's bindings) | `util/`, `core/src/main/resources/` |
| `ClusterSerializer.scala` (with `ClusterSerialization` and `RemoteFailure`, M5) | `cluster/` |
| `SchemaLock.scala` (one schema initialization at a time, F63) | `util/` |
| `SequenceGate.scala`, `RedeliveryPolicy.scala`, `OutboxPollerActor.scala`, `Republish.scala` (the delivery module, M2) | `delivery/` |
| `OutboxDeletes.scala` (batched outbox deletes) | `eventstore/` |
| `Backoff.scala` (retry waits, AR6) | `util/` |
| `ProjectionActor.scala`, `SubscriptionsState.scala`, `SubscribableProjectionActor.scala`, `AggregateListenerActor.scala` | `projection/` |
| `ProjectionBuilder.scala`, `ReplayStates.scala` (with `ReplaySource`), `Projections.scala` (the runtime and `ProjectionSupervisor`), `ProjectionRegistry.scala`, `ProjectionBuild.scala` (M4) | `rebuild/` |
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
- **Its type key** is `AggregateType.of[R]` everywhere (since M3, D75, F50): Scala's full name
  of the class (`Outer.Inner` for a nested one), as the store has always kept it, taken from
  the class without runtime reflection. Before M3 three derivations agreed only for top-level
  classes, so a nested root was never routed to projections.
- Identifiers (`api/.../id/`): `AggregateId` (Long; no instance pool since M3, F32), `AggregateVersion` (Int,
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
- Events carry a `ClassTag` of their aggregate root (`Event.aggregateRootClass`; a `TypeTag`
  before M3, D75); the persist path takes the aggregate type key from it.
- Storage format: JSON text (mpjsons), with the class identified by a `types_names` id;
  `event_type_version` is always 0 (D29).
- **Metadata envelope** (since M3, D32, R13): a command that mixes in `WithMetadata` gives each
  of its events an `EventMetadata(actor, correlationId, causationId, traceContext, spaceId,
  custom)`, stored in `events.metadata`. Event handlers do not see it; listeners get it as
  `EventInfo.metadata`, live, re-published and replayed, and event queries return it. An event
  stored without one has its user as the actor (`EventMetadata.ofUser`). A history rewrite
  keeps it.

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
| serial mode: `maxQueuePerAggregate` commands already wait | not run | `CommandRejected(id, reason)` (R8) |
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
3. **Replay** during projection builds: the builder's replay states restore an aggregate
   from its snapshot or history and apply the events after it (`ReplayStates`, §8.1).

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
  val commandProcessing: CommandProcessing = Concurrent    // since 1.0 (R8, D31): or Serial(maxQueuePerAggregate, Reject)
  val aggregateType = AggregateType(classTag[R].toString) // FQCN; the routing key everywhere
}
```

- `aggregateType` keys the bus subscriptions and the `types_names` row. Renaming or moving
  the root class creates a **new** aggregate type; existing rows stay keyed by the old id.
- `version` decides which managed projections are built again: one that receives the
  context's states is rebuilt when it changes (D63, §8.2).
- The context is turned into a running component with
  `AggregateCommandBusActor(context, uidGenerator, eventStoreState, commandResponseState, eventBus, maxInactivityMillis = 12 h, keepAliveLimit = 200, failedRestoreRetryAfter = 5 s)`
  (`AggregateCommandBusActor.apply`).
- Example: `ShoppingCartAggregateContext.scala` (commands in `Commands.scala`, events in
  `Events.scala`, pure handler functions in `CommandsHandlers.scala` / `EventsHandlers.scala`).

---

## 3. Write path: command → events

1. **Routing** (`AggregateCommandBusActor.route`). The bus takes a `CommandId` (and, for a
   `FirstCommand` without `chosenAggregateId`, a new `AggregateId`) from pools of 10 000 that it
   asks `UidGeneratorActor` for ahead, when a fifth is left (`IdPool`, F23: no `Await`). A command
   that arrives while no id is at hand waits, with the ones after it, until the pool comes; the
   bus keeps answering other messages meanwhile. When a pool does not come, the waiting
   commands get `CommandHandlingError` (F75), and the next command asks again; a message the
   bus does not understand gets `Status.Failure` (F70). It sends `CommandEnvelope(respondTo, commandId, command)` to the aggregate's
   `AggregateOwner`, creating it if needed; an id just taken from the pool makes a *fresh*
   owner, which skips the restore query.
2. **Idempotency** (`AggregateOwner.attempt`). For an `IdempotentCommand` with a key, a
   synchronous `commands_responses` lookup returns the stored response instead of
   executing. Copies running at the same time are settled at persist (step 5).
3. **Version check.** A `Command` or `RewriteHistoryCommand` expecting a version above the
   owner's reloads first (another writer moved the aggregate on, F19); a mismatch is
   `AggregateConcurrentModificationError` at once, without running the handler. A
   `FirstCommand` expects version 0; the concurrent kinds run on the current version.
   **Serial mode** (`commandProcessing = Serial(...)`, R8, D31): the owner starts one command
   at a time, in arrival order, the next when the current one is answered; so every handler
   sees the latest state and nothing conflicts. The idempotency check runs when a waiting
   command starts; a command beyond `maxQueuePerAggregate` waiting ones is answered
   `CommandRejected` at once; a command's timeout counts from its arrival.
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
   - calls the context's outbox writer, if it mixes in `WithOutboxWriter` (R12, D33), with the
     aggregate, its new version and the events: rows of the application's own in the same
     transaction, so a failure rolls the events back, and a copy whose idempotency key is
     taken never reaches it. The transaction is an `AfterCommit` one, so cache updates made in
     it apply only after the commit.

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
| `events` (`:29-41`) | `id` (from `events_seq`) PK, `user_id`, `aggregate_id`, `event_time TIMESTAMP`, `version`, `event_type_id`, `event_type_version` (always 0, D29), `event TEXT`, since M3 `metadata JSONB` (the envelope, D32; null when none); indexes `(aggregate_id)`, `(aggregate_id, id)`, and `(aggregate_id, version)` built online by `buildIndexesConcurrently()` (F38) | append-only event log |
| `aggregates` (`:103-115`) | PK `(id, base_id)`, `space_id`, `creation_time`, `type_id`, `base_order`, `base_id`, `base_version`, since M3 `squashed_version` (null unless squashed, D49); indexes `(type_id)`, `(base_id)` | one row per (aggregate, ancestor) in its duplication chain; the own row (`base_id = id`) holds the **current version** used for locking |
| `noop_events` (`:75-81`) | `id` (= `events.id`) PK, `from_version` | events cancelled by an undo, effective from that version |
| `snapshots` (since M3) | `aggregate_id` PK, `version`, `context_version`, `schema_version`, `state TEXT`, `created` | the latest performance snapshot of an aggregate (D30, §4.3); disposable |
| `events_to_publish` (`:83-101`) | `event_id` PK, `aggregate_id`, `version`, `user_id`, `event_time`; index `(aggregate_id, version)` | **outbox**: events not yet confirmed by every subscriber |
| `types_names` (`TypesNamesState.scala`) | `id SMALLINT`, unique `name` | class-name ↔ id dictionary (aggregates, events, subscribers, responses) |
| `commands_responses` (`CommandResponseState.scala`) | `id`, `key` (unique), `handling_timestamp` (indexed, for retention), `response`, `response_type_id`, and since 1.0 `info`, `info_type_id` (the info of a `CustomSuccessResponse`, F64) | idempotent responses (D12) |

### 4.2 Stored procedures and optimistic locking

`add_events(user, space ids[], aggregate, expected_version, aggregate type, event types[], time, events[], metadata)`,
used for the plain and first events of a command (since M3 one call per command, F31):

1. `UPDATE aggregates SET base_version = base_version + n WHERE id = agg AND base_id = agg RETURNING base_version - n`.
   The row lock serialises concurrent writers to the same aggregate.
2. No row: when `expected_version = 0` and the first event's space id is `>= 0` (a first
   event), insert the `aggregates` row; otherwise raise.
3. When `current_version != expected_version`, raise "Concurrent aggregate modification exception".
4. Insert each event into `events` (type version 0, the command's metadata) **and**
   `events_to_publish` with `version = current + i`; return the new version.

The event time is bound and read as UTC whatever the JVM's zone (fixed F65; the column stays
`TIMESTAMP`). Related functions:

- `add_event` is `add_events` with one element.
- `add_undo_event` inserts `noop_events` rows (`from_version = new version`) for the last
  `undo_count` not-yet-noop events (walking back the index on `(aggregate_id, version)`, F31)
  and for itself; refuses an undo that would reach `squashed_version` (D49); deletes the
  aggregate's snapshot (D30).
- `add_duplication_event` locks the base's chain rows (`FOR KEY SHARE`, in key order),
  refuses a base that does not exist or a `_base_version` below the base's
  `squashed_version` (D50), copies the base's `aggregates` chain rows, caps the base row at
  `_base_version`, and appends its own row with `base_order = n + 1`. The lock makes a
  duplication and a squash or permanent delete of its base wait for each other (§4.6).

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
  duplicates, a squash the one at or below its point, a permanent delete its own, each in its
  transaction; the owner runs all of them, so a snapshot write cannot race them. A snapshot is
  written only while its aggregate is stored at that version, and an owner whose history
  changed under it (a delete, a detach, a rewrite or state-changing squash of its base) is
  stopped without writing one (F100).
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
  `PassivateAggregate(id)` (operations, tests). When the store changed under the owner (a
  permanent delete; `HistoryChanged` from another owner after a rewrite or a state-changing
  squash of its base, or a detach) the bus sends `StopOwnerStale` instead, also to an owner
  stopping already: it writes no snapshot (F100). It is Cluster Sharding's passivation (D43,
  D72), so M5 can put a shard in the bus's place without changing the owner.
- **Dispatchers (F27, since M3):** owners run on `reactivecqrs.blocking-dispatcher` (the
  library's `reference.conf`: a fixed pool of 32 threads, throughput 1; size it near the JDBC
  pool), so persisting stays synchronous on the owner's thread (F69) without holding up the
  default dispatcher; command handlers, the application's code, run as futures on the default
  one. The command bus stays on the host's dispatcher and runs its JDBC (historical reads, the
  permanent delete, expired responses) in futures on the blocking dispatcher, as do the uid
  generator, the saga state, the outbox poller and the squash scheduler. Projections are made
  by the host: `Props(...).withDispatcher(BlockingDispatcher.id)`; one left on the default
  dispatcher logs a warning at start.
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
  per `(aggregate_id, version)` in the same transaction (`overwriteEvents`), and only where the
  stored event has the same class: a rewrite to another class is refused (D17, F8). The handler
  gets only the aggregate's own events, never its base's (F103). Only new events are published;
  after the commit the owner reads its state again from the store, and the owners of its
  duplicates are stopped to read theirs again (`HistoryChanged`, F100). The test domain pairs
  each rewrite with a compensating event (`CartNameRewritten`).
- **Permanent delete**: the bus names the version of a `PermanentDeleteEvent` in its
  acknowledgement (`permanentDeleteAt`); the command bus then calls
  `EventStoreState.deleteAggregatePermanently` and stops the owner, whose state is gone. In one transaction it locks
  the aggregate's chain as a squash does (§4.6), detaches its duplicates (F76, fixed in M3:
  each one's first event becomes the type's squash event of its state at version 1), and
  deletes the `aggregates`, `noop_events` (before `events`, so its sub-select still matches),
  `events`, `events_to_publish`, `snapshots` and `subscriptions` rows. It deletes nothing and
  gives the reason while a subscription of the aggregate is failed (`subscription_failures`,
  D20a) or when a duplicate was made after the command bus listed them; the event's outbox
  row stays and the next re-publication's ack tries again. A type with duplicates and no
  squash event cannot detach them: the command bus logs an error and the delete waits. In-memory caches are not
  touched.

### 4.6 Squash (D47–D52)

- **Opt-in per context:** `squash = Some(SquashPolicy(toEvent, automatic))`. `toEvent` makes
  the context's squash event from a state: an ordinary `FirstEvent` class of the context,
  handled by its event handlers and projections like any other; the history never holds raw
  state (D48).
- **Request:** `SquashAggregate(id, upTo, userId, metadata)` to the command bus, which passes
  it to the owner; the answer is `AggregateSquashed` or `SquashRefused(reason)`. The owner runs
  it on its thread between commands; its version and state stay as they are.
- **The check (D85):** the squash event, and each event that detaches a duplicate, is built
  and checked by the restore module (`AggregateRestorer.squashEvent`, `detachEvents`): the
  event handlers apply it to the initial state as a restore will (by the user it is stored by,
  at the time of the event it replaces), and the result must equal (`==`) the state it
  replaces, or the squash is refused (a permanent delete waits, F76). A class that mixes in
  `SquashMayChangeState` may differ on purpose: it is only applied, not compared; the store
  then deletes every snapshot of that aggregate and of the duplicates still reading it, the
  owner reads its state again, and it asks the command bus (`HistoryChanged`) to stop the
  owners of all its duplicates, which read theirs again (F101). Projections are not told (D51).
- **Preconditions (D51)**, checked by the owner first and again in the transaction
  (`squashBlocker`): not squashed that far already; no outbox rows at or below `upTo`; no
  failed subscription; no undo after `upTo` that undid events at or below it; and, for each
  duplicate the squash detaches, its first event not undone (F105), no outbox row at its
  version 1 and no failed subscription (the detach changes that event).
- **The transaction** (`EventStoreState.squash`): locks, in one statement and in key order,
  every chain row of the aggregate and of each aggregate that reads it (`ORDER BY id, base_id
  FOR UPDATE`; F104: so it cannot deadlock with a duplication, which locks the same way); checks the
  preconditions, and that the duplicates reading it below `upTo` are those the owner
  prepared (else "changed meanwhile", nothing changes); detaches them (D50: the version-1
  event becomes a squash event of the duplicate's state at version 1, its noop rows and its
  chain rows to other aggregates go); deletes the noop rows at or below `upTo`, the events
  below it, the outbox rows and snapshot at or below it and its own chain rows to its bases;
  replaces the event at `upTo` with the squash event (keeping its id and time, with the
  request's user and metadata); records `aggregates.squashed_version`.
- **After a squash:** an undo that would reach the squash point is refused by
  `add_undo_event` (D49), a duplication below it by `add_duplication_event`; historical reads
  below it fail with `AggregateSquashedException`, checked after the read so a squash during it
  is seen, and so does a read at an instant before the squash event's time (F102); a
  `RepublishRange` from below it starts at
  the squash event; a restore and a rebuild read the squash event first. Versions come from
  the stored events, so they stay as they were.
- **Scheduler (D52):** `SquashScheduler(eventStore, targets, options)`, started by the host,
  one per system (a cluster singleton in M5). Each run reads, per type, up to `perRun`
  aggregates that `AutomaticSquash(afterEvents, keepLast)` selects (`squashCandidates`: on the
  own row, `base_version - keepLast - coalesce(squashed_version, 0) >= afterEvents`), going
  on after the last id of the run before, and asks the command bus to squash them one at a
  time (`answerTimeout`); `GetSquashStatus` counts the answers. Its squash events carry the
  actor `system:squash-scheduler`.
- **Costs and operations:** each detached duplicate is restored at version 1 before the
  transaction, which reads its base's history; long duplication chains are an M3 risk to
  measure with release-freezing data. Disk space comes back through (auto)VACUUM; shrinking
  files needs `pg_repack` or `VACUUM FULL`. Contexts with audit duties should not opt in
  before R11's archive (M6).

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

**Routing.** Every subscription of the event's aggregate type gets it (since M3 there are no
subscription classifiers, AR8: the sequence gate needs every version):

| Subscription request | Message sent | Listener kind (§6.2) |
|---|---|---|
| `SubscribeForEvents` | `IdentifiableEvents(type, id, events, startsAtSquash)` | `EventsListener` |
| `SubscribeForAggregates` | `AggregateWithType(type, id, lastVersion, versions, root, startsAtSquash)` | `AggregateListener` |
| `SubscribeForAggregatesWithEvents` | `AggregateWithTypeAndEvents(type, id, root, events, startsAtSquash)` | `AggregateWithEventsListener` |

`root` is the aggregate state after the delivered batch. `startsAtSquash` is set when an
owner re-publishes from the aggregate's squash point (`PublishEvents(startsAtSquash = true)`)
and the subscription gets that first event: a subscriber that never saw the aggregate starts
there (F98, §6.3).

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

**Subscribers that come and go (M4).** After the start, a projection's supervisor adds the
subscriptions of a build it starts later (`AddSubscriptions`) and removes those of a build it
retires (`RemoveSubscriber`, answered with `SubscriberRemoved`): the deliveries that waited for
a removed subscriber end for it, and one it was the last to answer acknowledges its owner, so no
outbox row waits for a retired build (unlike a subscriber that stops, whose rows stay). An
instance that stopped by itself is removed only once its replacement is subscribed, and with
`keepOutbox = true`: its deliveries end as a stopped subscriber's do, so their rows stay in the
outbox and are published again to the replacement (F110).

**Re-publish requests.** `RequestRepublish(subscriber, type, id, from)` from a subscriber
with a gap or a retry goes to the type's command bus as `RepublishRange(id, from,
subscriber)`; the owner re-publishes `from` to its current version to that subscriber only
(D20c).

**Failures inside the bus (AR7).** A message the bus fails on (a malformed one) is logged and
dropped. A restart would lose every delivery and the subscriptions.

### 5.3 What subscribers can rely on

- **At least once**, through the outbox, the live publication and the poller's
  re-publications; deliveries can repeat, overlap or arrive after later ones.
- **In order per aggregate on the live path** (one owner per aggregate, Pekko's per-sender
  ordering).
- **Applied exactly once** by `ProjectionActor`, through the sequence gate on the stored
  subscription version (§6.3); a gap is asked from the owner. A `noDBSession` listener runs
  before its version is written, so it sees a batch again if that write fails: at least once.
- **No flow control on the live path** (F93): groups (§6.3) raise how fast a projection keeps
  up, but when commands come faster than it applies them, its mailbox and the bus's
  deliveries still grow with the lag.
- A delivery that nobody answers ends at the deadline; the poller tries again at the next
  start.

---

## 6. Projections and listeners

### 6.1 The `ProjectionActor` contract (`ProjectionActor.scala`)

```scala
class MyProjection(val subscriptionsState: SubscriptionsState, store: DocumentStore[Doc])
    extends ProjectionActor(ProjectionActorOptions.DEFAULT) {
  protected val projectionName = "MyProjection"   // in logs and the status
  protected val listeners = List(EventsListener(onEvents), AggregateListener(onAggregate))
  protected def receiveQuery: Receive = { case MyQuery(..) => sender() ! ... }
}
```

The projection is passed to the bus in the host's `RegisterSubscribers` list; it answers the
bus's `GetSubscriptionRequests` with one request per listener (D15). Progress in
`SubscriptionsState` is keyed by **`this.getClass.getName`**, not `projectionName`, so
renaming or moving the class resets an unmanaged projection's progress. A managed projection
(§8.2) is keyed by its build instead, and the version given to `Projections.start`
decides when it is built again.

### 6.2 Listener kinds

| Factory | Callback | Receives | Notes |
|---|---|---|---|
| `EventsListener(f)` | `(AggregateId, Seq[EventInfo[R]]) => DBSession => Unit` | new events of type `R`'s aggregate | `EventInfo(version, event, userId, timestamp)` |
| `AggregateListener(f)` | `(AggregateId, AggregateVersion, created: Boolean, Option[R]) => DBSession => Unit` | latest state; `created` is true when the first new version is 1 (`:694`) | `None` = deleted; cannot see individual events or undo |
| `AggregateWithEventsListener(f)` | `(AggregateId, AggregateVersion, Option[R], Seq[EventInfo[R]]) => DBSession => Unit` | state + new events | |
| `*.noDBSession(f)` | same, without a session | run outside a transaction; the subscription update uses `autoCommit` | for in-memory or side-effect-only listeners |

- Keys are the aggregate root type; **one listener per kind per aggregate type** (`toMap`
  silently keeps the last, `:180-196`). `Any`/`Nothing` types are rejected (`:200-204`).
- The callback's `DBSession` is the transaction that also writes the subscription version,
  and other aggregates' batches with it (§6.3). Pass it to the document store so the read
  model and progress commit atomically.
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
   it wait. A delivery that starts at the aggregate's squash event (`startsAtSquash`) is no gap
   for a subscription below it: nothing before it exists any more, so the subscription starts
   there (F98).
2. The batch runs `listener(...)` and `newVersion(subscriber, kind, id, applied, to)` in one
   `localTx` (a `noDBSession` listener runs first, then the version is written in
   `autoCommit`). State-receiving listeners get the state of the delivery holding the last
   version, and `created` when the subscription starts with the batch (at version 1, or at a
   squash event). On success every delivery of the batch is acknowledged with all its versions.
   One batch per queue runs at a time; in parallel mode a scheduled drain during a batch is
   left to the batch's end (F89).
   **Groups** (D61, F93): without parallel processing, the queues that get deliveries while
   deliveries keep arriving are drained together: once the deliveries already in the mailbox
   are taken (a `FlushGroup` sent after them), when `maxBatchesPerTransaction` (100) queues
   are due, or before any other message, so a query, a chunk, a reset or a retirement sees
   every delivery taken before it. Their applied versions are read at once, one read per
   listener kind (`lastVersions(..., cache = true)`, which caches them as `lastVersion`
   does), and their batches run in transactions of up to `maxBatchesPerTransaction`. A batch
   stays its queue's batch at work until its group runs. A group that fails runs again one
   batch at a time, so only the failing aggregate retries and fails (step 4). A `noDBSession`
   listener's batch runs alone: another aggregate's failure must not repeat its side effects.
   A gap check reads its version at once.
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
applying it, from the version the subscription has (below the one before the failure when it
failed at a squash start, F113), and asks once for the versions after it. Failures recorded before a start are
loaded when the bus registers the projection and retried the same way
(`retryFailuresOnStart`).

**Failures of the actor itself** (AR7, F80). A message the projection fails on, a query
included, is answered with `Status.Failure` and never rethrown, so the actor keeps its
queues. The parallel pool is shut down when the actor stops.

**Fed by a builder** (M4, D58, D61; the builder itself is described in §8). The projection
takes deliveries from any sender and answers that sender, so a builder that reads the event
store can feed it directly, through the same gate, failure records and status:

- `ApplyChunk(id, items)`: versions of many aggregates in commit order, one `ChunkItem` per
  subscription (kind) and run of versions, with the events and the state after the last
  version; `startsSequence` marks an item that needs nothing before it (a squash event, or a
  whole state, D62). The gate decides each item against the version the chunk left its
  subscription at, and every decided batch runs in **one transaction** with its versions
  (`SubscriptionsState` lets one transaction move a subscription several times). The answer,
  `ChunkApplied(id, gaps, handedOver)`, lists the subscriptions with missing versions (the rest
  of that aggregate in the chunk waits). A subscription with deliveries at work takes its items
  in its queue; a chunk that fails is handed to the queues whole, which apply, retry and record
  failures one aggregate at a time (D20a); each handed-over item is answered to the builder
  with `MessageAck` or `MessageFailed`. Items of a failed subscription are left out until a
  retry.
- `BuildFromStore(builder)`: while the projection is built (D57), deliveries from the bus are
  acknowledged without being applied, as the builder reads their events from the store, and
  missing versions are asked from the builder instead of the owner. A delivery that removes its
  aggregate (a permanent delete event, or no state for a state listener) is applied through the
  gate even then: a permanent delete takes the events out of the store once every subscriber
  has it, so no builder could give it later (F111). `GoLive` ends that, retries the failures
  recorded meanwhile, and is answered with `ProjectionLive`.

**Managed by a supervisor** (M4, §8.2). `BindSubscriber(subscriber)` gives the instance its
build's subscriber identity (otherwise its class name); `ConnectedTo(bus)` tells it the bus that
delivers to it (it then retries the failures recorded before, as at a start). `rebuilding` is
true from `BuildFromStore` until `Activate`, when the built instance starts to serve and
`onActivated()` runs, and whenever a listener applies what a builder gives a live instance (a
targeted rebuild's chunks and handed-over items, F114): a listener with side effects checks it,
as the events it is given then are history. `Retire(successor)` ends a retired build's instance: `onRetired(successor)` runs (a
`SubscribableProjectionActor` hands its push subscriptions over, with their filters, which must
not use the old build's stores), parked queries are answered with `None`, and it stops.

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
`version`, `document`), with optional indexes on JSON paths created at start-up. The table and
its column migrations are made in one transaction (F43). A name whose table name, or the name of
one of its indexes, would pass PostgreSQL's 63 characters (which it would cut silently) is
refused at once (F118); a managed projection's tables carry their build's suffix (§8.2).

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
`lastVersions(subscriber, subscriptionType, ids, cache)` reads many aggregates' versions in
one query for those not cached: a group of live deliveries caches them (`cache = true`, with
every subscriber's versions of each aggregate, as `lastVersion` does); a builder's chunks and
pages leave the cache alone, so a build does not push out what live delivery needs. A read
that fills the cache is not cached when a commit changed the aggregate's versions while it ran:
it may have missed that change (F115).

**One mode** (since M4, F3): the database is the source of truth. `newVersion` INSERTs when
`last == 0`, otherwise `UPDATE … WHERE aggregate_version = last` with a rowcount check, in the
listener's transaction, so the version and the listener's writes commit together; the cache
advances only after the commit (`AfterCommit`, F18). The 0.12 in-memory mode and its `dump()`
are gone (D61). One transaction may move a subscription several times (a batch of many
aggregates in commit order, D61): each move is checked against the version the transaction
gave it before (`AfterCommit.local`, values that live as long as the transaction), then the
committed cache.

**Caches and concurrency.** `perAggregate` caches one map per touched aggregate. Cache misses
load from the DB outside the main monitor, with 64 striped single-flight locks. The cache is
an LRU of at most `maxCachedAggregates` (10 000) aggregates (F29); the database stays the
source of truth, and a failed compare-and-set in the database evicts the aggregate's entry,
so the next attempt starts from the stored version (F77).

**Clearing.** `clearSubscriptionsInfo(subscriber)` deletes that subscriber's version rows and
failures and removes its cached versions (F6). `MemorySubscriptionsState` is a test double
with the same contract (F14).

**Failures (D20a).** Table `subscription_failures(subscriber_type_id, subscription_type,
aggregate_id, aggregate_type_id, failed_version, error, failed_since)`, primary key on the
first three, index on `aggregate_id`: one row per subscription that gave up on an aggregate,
replaced by a later failure of the same subscription. `recordFailure`, `failures(subscriber)`
and `clearFailure(...)(session)`; a permanent delete waits while the aggregate has a row
here (§4.5).

**Related.** `components_versions`, the 0.12 rebuild's record of the versions it built, is only
read, to adopt the projections 0.12 built (§8.2).

---

## 8. Replay / projection rebuild

M4 replaced the 0.12 replayer with managed projections (§8.2): each runs under a
supervisor that builds new versions from the event store with a builder that feeds one
projection directly (§8.1), while the active version serves, and switches in one update
(design: `docs/design/M4-rebuild.md`). One aggregate, type or organization is rebuilt in place.

### 8.1 The builder (`rebuild/ProjectionBuilder.scala`, M4)

`ProjectionBuilder(target, subscriberName, plan, eventStore, subscriptionsState, sources,
positions, reportTo, settings)` builds the projection `target` (D58: no bus, no other
subscriber). `BuildPlan` lists its subscriptions (the projection answers `GetListeners`);
`ReplaySource.of(contexts, eventStore)` gives each aggregate type's restorer and event
handlers. It runs on the blocking dispatcher and reads synchronously on its own thread.

1. **Bulk pass.** `readCommitted(types, after, limit)` pages through the events of the plan's
   types in commit order (`events.id`, D59), each under its own aggregate (a duplicate's own
   events, not its base's), undone events and undo events included (D60), with the aggregate's
   squash point. Consecutive rows of one aggregate with consecutive versions form a run; each
   run becomes one `ChunkItem` per listener kind, with the state after its last version for
   state listeners. `ReplayStates` keeps those states for at most `maxReplayStates` (10 000)
   aggregates, least recently used out: a run continues the state it holds, an evicted or
   unknown aggregate is restored at the version before the run (snapshot plus tail, D30), an
   undo or a duplication takes the state the store gives at its version (as the owner did), a
   duplicate starts from its base's state when the base is held at the duplicated version, and
   a run that starts at the aggregate's first event or squash event starts from the initial
   state. A projection whose listeners all take the state only (D62) gets one item per
   aggregate instead, its final state (`aggregatesOf` pages through the aggregates), which
   starts its subscription. Items go in chunks of about `chunkEvents` (1 000) events, two in
   flight, so the next page is read while the projection applies a chunk in one transaction
   (D61, §6.3). The projection acknowledges the bus's deliveries without applying them
   meanwhile (`BuildFromStore`).
2. **GoLive.** Once the pass is read and every chunk is answered, the builder sends `GoLive`;
   from the projection's `ProjectionLive` on, it applies the bus's deliveries.
3. **Reconciliation.** `aggregatesOf` pages through every aggregate of the plan's types, and
   `SubscriptionsState.lastVersions` reads the projection's versions of a page at once; an
   aggregate behind gets its missing versions, read by version (`readEventsRange`, a range from
   below the squash point starting at the squash event, F98), with the state at its version.
   This covers the events committed after the bulk pass read past them and rows that became
   visible late (row ids are not commit order between concurrent transactions). Then the
   builder sends `BuildCaughtUp` to `reportTo`.

**Missing versions.** A run ends where a version is missing, so the projection reports the
gap (`ChunkApplied.gaps`, or `RequestRepublish` from a queue while it is built), and the
builder reads it again by version, ahead of everything else.

**Resume (D61).** After each applied chunk of the bulk pass, its position (the last row id, or
aggregate id for final states) is saved through `BuildPositions`; once the pass ends, that it
ended. A builder started again continues from there (or goes straight to GoLive and the
reconciliation), and repeats at most the chunks that were in flight, which the projection's
stored versions make harmless.

**Failures.** A store that cannot be read is tried again after `retryAfter` (5 s); a chunk the
projection could not take is sent again. A row that cannot be read, or an aggregate whose state
cannot be restored, fails its subscription at that version (D20a, visible in the projection's
status) and the build goes on (F82).

### 8.2 Managed projections: the registry and the supervisor (`rebuild/Projections.scala`, M4)

**Wiring.** The host creates one `Projections(actorSystem, ProjectionsSettings(eventStore,
subscriptionsState, registry, eventBus, contexts))` and starts each managed projection with
`projections.start[P](name, version, actorName) { build => new P(...) }`. The result is the
projection's **supervisor**, whose `ActorRef` the host uses where it used the projection's:
queries, the REST layer, `RegisterSubscribers`. `create` makes an instance of the projection for
one build; it names the build's tables with `build.documentStore(table)` (the table name of a
document store), `build.table(name)` and `build.sequence(name)` (the projection's own state).
`contexts` are the aggregate contexts whose states projections receive. A projection that must
never be rebuilt from history (a listener with side effects) is not started this way: created
as before, it behaves as before, and no build delivers to it (D58).

**The registry** (`ProjectionRegistry`, `PostgresProjectionRegistry`, D70): `projections` holds
per projection name the active build and the build in progress; `projection_builds` holds each
build's number, version, table suffix (`_v<version>`, then `_v<version>_b<build>` for a later
build of the same version), subscriber identity (`projection:<name><suffix>`, independent of
the class), the context versions of the types whose states it receives (D63) and its position
(D61); `projection_build_objects` holds every table and sequence a build named. The switch is
one update of the projection's row. Build 0 is a projection 0.12 built, adopted as it is (no
suffix, its class name as subscriber, D81): at the version `components_versions` recorded (the
0.12 rebuild's bookkeeping), or at the code's version when only its subscription rows exist.

**The supervisor** (on the blocking dispatcher) reads the registry at start and runs one
instance (a child) per build it needs: the active build, and the build in progress. A build
starts when the code's version differs from the active build's, when the version of a context
whose states the projection receives differs from the one the active build recorded (D63), or
on `RebuildProjection`; a build of a version the code no longer has is abandoned and dropped.
It binds each instance to its build (`BindSubscriber`), subscribes the instances that take
deliveries (`ConnectedTo`, answering the bus's `GetSubscriptionRequests`, or `AddSubscriptions`
once the bus runs), puts the build in progress into `BuildFromStore`, and runs its
`ProjectionBuilder` when one of the runtime's build slots is free (`buildsAtOnce`, 2). Every
other message goes to the instance that **serves**: the active build, or a new projection's
first build, which serves while it is built. While a new version builds, the active build keeps
applying live events with the current code (`ServeLive`, so parked queries keep working) or
serves its tables unchanged (`ServeStale`).

**The switch.** When the builder reports `BuildCaughtUp` and the build has no failed
subscription (else it is checked again every 30 s; `ToBuildInProgress` reaches its instance for
a retry or a skip), the registry switches, the new instance gets `Activate` (`rebuilding` ends,
`onActivated()` runs), and the old instance leaves the bus (`RemoveSubscriber`: no delivery
waits for it any more, so no outbox row does), is retired (`Retire`: its push subscriptions go
to the new instance, its parked queries are answered with `None`) and stops; then its tables,
subscription versions and failures are dropped (F94). `GetBuilds` reports the builds.

**Failures.** A builder that stops before it caught up is started again after `restartAfter`
(10 s) and continues from the saved position; a store that fails for a moment (a lost
connection, a restart) is read again, and only what cannot be read at all fails a subscription
(F117). An instance that stops by itself is replaced after `restartAfter` (F110): the build
in progress alone, while the active build serves on (D57); the active one with every other
instance, the supervisor starting again. The bus keeps the stopped instances as stopped
subscribers until their replacements are subscribed, so what they did not take stays in the
outbox and is published again to the replacements; then they leave it without acknowledging.
Each child has a name of its own, and a supervisor holds or awaits one build slot at most. The
supervisor itself never restarts (AR7): a message it fails on (the registry
or the store unavailable) is logged and answered with `Status.Failure`, as a restart would
stop its instances and leave the new ones unsubscribed (the bus asks once). A switch that
fails in the registry is tried again after `restartAfter`; a context check or a targeted rebuild
the registry could not record waits for the next start. A retired build loses its subscription
versions and failures before its registry row, and a supervisor drops at its start every build
of its projection that is neither active nor in progress, so a stop between a switch and the
drop leaves nothing behind (F116). `Projections.retire(name)` drops every build the registry has
of a projection removed from the code.

**Targeted rebuild** (D63, R14). `RebuildAggregates(target)` rebuilds in place, on the active
build, what the projection derived from one aggregate (`OneAggregate`), one of its aggregate types
(`OneType`) or one organization (`OneSpace`, by `aggregates.space_id`). The supervisor records the
target in the registry (a rebuild a stop interrupted runs again at the next start), and sends
the active instance `ResetForRebuild`. The projection lists the target's aggregates then, once
it applied every delivery taken before, so one made meanwhile is either listed or delivered after
the reset (F112); in one transaction its `clearForRebuild(target)` removes what it derived (a
document, a space's documents) and its versions and failures of those aggregates are reset
(`SubscriptionsState.resetVersions`); the deliveries waiting for them are acknowledged, as the
builder gives their events again.
A builder scoped to the target (`AggregateScope.Only` or `InSpace`; no `BuildFromStore`, no
`GoLive`, as the instance stays live) then delivers their events in commit order and reconciles
them; live events of those aggregates meanwhile find a gap and are asked from their owners, and
the gate drops what comes twice. The answer is `AggregatesRebuilt(target, count)`. A projection
that does not implement `clearForRebuild` refuses (`RebuildRefused`), as does one with a build in
progress, and an organization is not rebuilt while aggregates of the projection's types have no
space yet.

**Spaces of legacy aggregates** (F49, D38). Rows of `aggregates` from before the `space_id`
column have `-1`. `PostgresEventStoreState.backfillSpaces(spaceOf)` gives them the space of their
first event (the squash event of a squashed one), or, when that event does not know it, the one
the host's `spaceOf` gives; it runs online, in batches of 1 000 aggregates, one transaction each
(D69), and reports the aggregates it could not place. A targeted rebuild of an organization needs
it first.

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
index on `(name, saga_id)`, built `CONCURRENTLY`. Saga ids come from pools fetched ahead
(F23); an order that arrives while none is at hand waits for the pool, and is refused if the
pool does not come. The saga state's JDBC calls run on the blocking dispatcher (F27).

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
10. **Nothing reads below a squash point.** Every path that reads or keeps an aggregate's
   old history (restore, undo, duplication, re-publication, a chain row of a duplicate)
   either starts at the squash event or is refused; a squash or a permanent delete detaches
   the duplicates that read below it first, under the chain lock that duplication also takes.
11. **A running owner never outlives a change of its stored history it cannot see.** A
   permanent delete, a detach, and a rewrite or a state-changing squash of its base stop it
   with `StopOwnerStale`, which writes no snapshot; snapshots are written only while their
   aggregate is stored at their version (F100, F101).

---

## 11. Known problems

Problems found in this read, and every decision about fixing them, are tracked in
[KNOWN-ISSUES.md](KNOWN-ISSUES.md). The order of the work is in
[IMPLEMENTATION-PLAN.md](IMPLEMENTATION-PLAN.md). M1 fixed the most serious problems of
this read (lost document updates, caches ahead of the database, timestamps, lost sagas,
partial persists, idempotency); M2 rebuilt delivery (no cursor, a single owner of
re-deliveries, the sequence gate, gap requests, failure records); M3 detaches duplicates
before a permanent delete (F76); M4 replaced the 0.12 replayer, which left out undone events
(F58) and could stop the JVM (F82), with builds from the event store. Still open: flow control
on the live path (F93), so a projection that cannot keep up lets its backlog grow in memory.

---

## 12. Where to change what

| Goal | Touch | Watch out for |
|---|---|---|
| New command kind / routing rule | `Command.scala`, `AggregateCommandBusActor.receive`, `AggregateOwner` (`expectedVersion`, `runsAgainOnConflict`, `rewrittenClasses`, `userIdOf`) | the retry rule in `AggregateOwner.conflict` (a rewrite must read its events again, F73); a timed-out command must never be persisted |
| New special event type | `Event.scala`, `PostgresEventStoreState.persistEventsOneByOne` + a stored procedure, `AggregateOwner.persist`/`nextRoot` (`readAfterCommit`), `readAndProcessEvents` | batch eligibility (`:31-32`); restore semantics; replay path |
| Snapshots | `AggregateRestorer` (use), `AggregateOwner.snapshotIfDue`/`stopNow` (write), `EventStoreState.readSnapshot`/`writeSnapshot`/`readAndProcessOwnEvents`, the deletes in `add_undo_event`, `overwriteEvents`, `squash` and `deleteAggregatePermanently` | every change that alters the state behind a snapshot must delete it in its own transaction; a snapshot is read only when its context and schema versions match; never fail a restore because of a snapshot |
| Squash | `AggregateOwner.squashHistory`, `EventStoreState.squashBlocker`/`duplicatesReading`/`squash`/`squashCandidates`, `add_undo_event` and `add_duplication_event` (the guards), `SquashScheduler` | the preconditions run again inside the transaction under the chain lock; duplicates reading below the point are detached first; every reader of old history must start at the squash event or be refused (§10, 10) |
| Non-blocking persistence | `AggregateOwner.persist` → a dedicated dispatcher, re-entering via message | keep one in-flight persist per aggregate; apply the events before the next message is handled (F69: persist synchronously or stash); the in-memory version check |
| Re-delivery timing | `delivery/RedeliveryPolicy` (pure), `OutboxPollerActor`, `EventStoreState.readWaitingInOutbox` | one owner of retries (D8); the attempts at 3 and 6 minutes come after a subscriber's own retries (about 2 minutes, D20a); the per-poll limit |
| Outbox deletes | `AggregateCommandBusActor.delivered`, `EventStoreState.deletePublished` / `deleteAggregatePermanently`, `OutboxDeletes` | only acked (or failed) versions; the acknowledgement goes to the command bus, never only to the owner, which may be reaped (F91); a permanent delete waits while a subscription of the aggregate is failed, or when duplicates changed after they were listed |
| Delivery tracking | `EventsBusActor` (`publish`, `finish`, `checkDeadlines`, `subscriberStopped`) | a delivery that ends without every ack or failure must not ack the owner; re-publications to one subscriber never do; AR7 (no throws) |
| New listener kind | `ProjectionActor` (listener class, map, `subscriptionRequests`, the delivery case in `receiveUpdate`, `appliedVersion`, `advance`, `workOf`), `EventsBusActor` (request + message type), `SubscriptionType` | a new `subscription_type` id; everything else goes through the one gate path |
| Projection progress storage | `SubscriptionsState` | CAS semantics; same-session atomicity; the bounded cache and its eviction on a failed CAS (F29, F77); one transaction may move a subscription several times (D61); failures (D20a) |
| Rebuild logic | `rebuild/ProjectionBuilder` (phases, chunks, reconciliation), `ReplayStates`, `Projections` (`ProjectionSupervisor`, `BuildSlots`), `ProjectionRegistry` | a build must deliver what live delivery delivers (undone events and undos included, D60); the switch is one update; a retired build's tables, versions and failures go together (F94) |
| Idempotency rules | `commandhandler/Idempotency.scala`, `CommandResponseState` | the response is written in the persist transaction (I2); a taken key and a version conflict both answer with the stored response |
| Retries and timeouts | `util/Backoff`; `AggregateOwner` (concurrent retries, command timeout), `SagaActor` (write retries), `ProjectionActor` (listener retries), `RedeliveryPolicy`, `PostgresDocumentStore.updateWithRetries` | bounded everywhere; the document-store retry runs inside the listener transaction and must stay immediate (AR6) |
| New message between nodes | `cluster/ClusterSerializer` (library classes by field name, the application's values through mpjsons, exceptions), the bindings in `reference.conf` | no functions, futures or mutable state in a message that leaves its node; a field added to a library message is an `Option` or has a default, a changed field type needs a new class (D68); a library exception keeps its class only with a field codec and a binding of its own |

---

## 13. The cluster (M5)

M5 runs the library on a Pekko cluster (design: `docs/design/M5-cluster.md`); this section grows
with that work.

### 13.1 Messages between nodes (D45, D68)

`cluster/ClusterSerializer` is the one serializer of the messages between nodes. `reference.conf`
binds it to `java.io.Serializable`, so every case class and Scala collection that Pekko has no
more specific serializer for comes to it (Pekko keeps `Some`, `None`, `Status`, `ActorRef` and its
own messages); Java serialization stays off. The manifest is the class name; the payload is one
value, each part written with a tag that says what it is:

- scalars, `Instant`, `UUID`, `BigDecimal`; an `ActorRef` as its serialized path with its node's
  address, resolved on the reading node;
- `Option`, `Try`, lists, sets, maps, other sequences (read back as vectors) and tuples, element
  by element;
- the library's case classes (`io.reactivecqrs.api.*`, `io.reactivecqrs.core.*`) field by field
  with the fields' names, rebuilt through their constructor, so a field typed `Any`, a type
  parameter or an `ActorRef` keeps its value, which mpjsons, typing by the declared type, could
  not (F64); case objects by name;
- the library's exceptions that callers match on (`AggregateInIncorrectVersionException` and the
  others) with their fields. Pekko binds exceptions to Java serialization, so they have bindings
  of their own and keep their class inside a `Status.Failure` too. Any other exception arrives as
  a `RemoteFailure` with its class name and message (inside a `Status.Failure`, as Pekko's
  `ThrowableNotSerializableException`);
- anything else, the application's commands, events, states, saga orders and responses, query
  messages and answers, as the JSON of its runtime class through mpjsons, as stored events are.
  `ClusterSerialization(system).mpjsons` is the instance; a host that registers converters of
  its own sets one configured the same way.

Reading is tolerant (D68), so nodes of two versions talk during a rolling upgrade. A library
message is matched by field name: a field the reader does not have is skipped, one it lacks takes
its Scala default, else `None`, zero or null. The application's JSON is read with
`ignoreNonExistingFields` (a missing field reads as zero, null or `None`). A field whose type
changed fails the message; such a change needs a new class. Stored events keep their strict
reader.

Messages that carry functions (`ResetForRebuild`, `SubscriptionsHandedOver`) stay inside one
singleton, and an actor's own messages, defined inside its class, hold its instance: neither can
cross nodes, and the serializer refuses them on the sending node. An exploratory run of the
testdomain suites with `pekko.actor.serialize-messages = on` found no other message that cannot
(M5); it is not a standing setting, as those local messages would need markers.

`ClusterSerializationSpec` (testdomain) round-trips the messages of each conversation and sends
some through a second node; `ClusterSerializerSpec` (core) reads messages that another version
wrote.

### 13.2 Schema initialization (F63, D69)

Every `initSchema()` and every document store's table creation runs under `util/SchemaLock`: a
PostgreSQL advisory lock (`pg_try_advisory_lock` on a number of the library's own) held on a
connection of its own, so processes that start together initialize the schema one at a time
instead of failing on concurrent DDL. It is reentrant on a thread, as one initialization may
start another, and gives up after five minutes. The heavy jobs (`buildIndexesConcurrently`,
`backfillSpaces`) stay separate calls.

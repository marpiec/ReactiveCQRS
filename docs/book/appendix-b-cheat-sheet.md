# Appendix B. Cheat Sheet: Messages and Answers

This appendix is the page you keep open next to your editor. It lists the messages you send to the
library's actors, grouped by who receives them, and exactly what comes back: the class, and whether
it arrives wrapped in a `Try`. At the end you will find the methods your application calls itself
(schema set-up, backfills, checks) and the defaults worth knowing by heart.

Every row was checked against the code of ReactiveCQRS 1.0 (`1.0.0`). Messages the library
sends only to itself are left out.

---

## How to ask, and the two kinds of "no"

All the examples use Pekko's `ask` with a timeout, and `mapTo` to name the type of the answer:

<!-- snippet: cart/CommandKindsSpec.scala#basics -->
```scala
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import io.reactivecqrs.api._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

implicit val timeout: Timeout = Timeout(5.seconds)

// A command: the answer is a command response.
val added: Future[CustomCommandResponse[_]] =
  (commandBus ? AddItem(userId, cartId, AggregateVersion(1), "milk")).mapTo[CustomCommandResponse[_]]

// A query: the answer is a Try.
val cart: Future[Try[Aggregate[ShoppingCart]]] =
  (commandBus ? GetAggregate(cartId)).mapTo[Try[Aggregate[ShoppingCart]]]
```

> **Remember:** there are two ways an answer can say "no". A `Failure(e)` inside a `Try` is an
> ordinary answer: your `Future` succeeds, and holds the failed `Try`. A `Status.Failure(e)` makes
> the `Future` itself fail with `e`. The tables below say which one each message uses.

---

## The command bus (`AggregateCommandBusActor`)

One per aggregate type (and per node). Messages from `io.reactivecqrs.api._`, except
`PassivateAggregate`, which lives in `io.reactivecqrs.core.commandhandler.AggregateCommandBusActor`.

| You send | You get back | Good to know |
|---|---|---|
| A `FirstCommand`, such as `CreateShoppingCart(None, userId, "Groceries")` | A command response (next section), for example `SuccessResponse(newId, AggregateVersion(1))` | The bus gives the new aggregate an id, unless the command's `chosenAggregateId` names one. A chosen id that is not positive, not below 2^53, or in another installation's range is answered `FailureResponse` before anything runs. |
| A `Command`, such as `AddItem(userId, cartId, AggregateVersion(1), "milk")` | A command response | The expected version must be the aggregate's current one, or the answer is `AggregateConcurrentModificationError`. |
| A `ConcurrentCommand` | A command response | Runs on the current version; after a clash it runs again, at most 10 attempts in all. |
| A `RewriteHistoryCommand` or `RewriteHistoryConcurrentCommand` | A command response | See [chapter 15](15-rewriting-history.md). |
| `ReserveAggregateId` | An `AggregateId` (not wrapped) | For `chosenAggregateId`. If no id pool can be had, the bus answers `Status.Failure`, so the ask fails. |
| `GetAggregate(id)` | `Try[Aggregate[R]]`: `Success(Aggregate(id, version, Some(root)))` | `aggregateRoot` is `None` at version 0 (the aggregate never existed) and after an event handler returned `null` (deleted). `Failure(AggregateRestoreFailedException)` when the aggregate cannot be read from the store. |
| `GetAggregateMinVersion(id, version, maxMillis)` | `Try[Aggregate[R]]`, as soon as the aggregate reaches `version` | `Failure(AggregateInIncorrectVersionException)` when it does not get there within `maxMillis`, or when its owner stops first; `Failure(AggregateRestoreFailedException)` when it cannot be read from the store. |
| `GetAggregateForVersion(id, version)` | `Try[Aggregate[R]]`: the state at that version | Read from the store, not from the owner. `Failure(AggregateSquashedException)` below the squash point. |
| `GetAggregateAtInstant(id, instant)` | `Try[Aggregate[R]]`: the state at that moment | `Failure(AggregateSquashedException)` for a moment before the squash event's time. |
| `GetEventsForAggregate(id)` | `Try[List[EventInfo[R]]]`: every event, oldest first | Undone events included; a duplicate's list starts with its base's events. The whole history comes in one message. |
| `GetEventsForAggregateForVersion(id, version)` | `Try[List[EventInfo[R]]]`: the events up to `version` | `Failure(AggregateSquashedException)` below the squash point. |
| `GetArchivedEvents(id)` | `Try[ArchivedEvents[R]]`: `ArchivedEvents(id, events)` | Each `ArchivedEvent(info, undoneFrom, archived)`: the event as it was stored, the version of the undo that cancelled it (if any), and when a squash archived it. Oldest first. |
| `SquashAggregate(id, upTo)`, optionally with `userId` and `metadata` | `AggregateSquashed(id, upTo)` or `SquashRefused(id, reason)`, not wrapped | Both extend `SquashResponse`. Appendix C explains every refusal. |
| `Materialize(id, userId)`, to an ephemeral bus | `Materialized(id, persistentId)` or `MaterializeRefused(id, reason)`, not wrapped | Both extend `MaterializeResponse`; the draft is dropped once materialized ([chapter 20](20-ephemeral-aggregates.md)). Commands for an id the bus does not hold get `EphemeralAggregateLost` or `EphemeralAggregateUnknown`. |
| `RewriteEverywhere(id, change, userId)` | `HistoryRewritten(id, events, archived, fields)` or `RewriteRefused(id, reason)`, not wrapped | One aggregate's complete rewrite, in its owner; both extend `RewriteResponse`. `HistoryRewriter` sends it to every copy too ([chapter 15](15-rewriting-history.md)). |
| `AggregateCommandBusActor.PassivateAggregate(id)` | Nothing | Stops the aggregate's owner once it has answered the commands it started; the next message starts a new owner. |
| Anything else | `Status.Failure(IllegalArgumentException)` | The ask fails with "Cannot handle this kind of message". |

The historical reads (`GetAggregateForVersion`, `GetAggregateAtInstant` and the event lists) wrap
any other trouble with the store in `Failure(AggregateRestoreFailedException)`.

> **Warning:** in a cluster, an answer that comes from an owner on another node must fit in one
> message between nodes (about 248 KiB with Pekko's default frame size). A larger one makes your
> ask fail with `MessageTooLargeException`. `GetEventsForAggregate` and the other historical reads
> are always answered on your own node, from the store. More in [chapter 17](17-cluster.md).

---

## Command responses (`CustomCommandResponse.scala`)

Every command gets exactly one of these. A copy of an idempotent command that was applied already
gets the response the first copy got.

| Response | What it means | Usual reasons |
|---|---|---|
| `SuccessResponse(aggregateId, aggregateVersion)` | The events are stored; `aggregateVersion` is the new version. | Your handler returned `CommandSuccess(...)`. With no events, nothing is stored and the version stays. |
| `CustomSuccessResponse(aggregateId, aggregateVersion, info)` | The same, with your `info`. | Your handler returned `CommandSuccess(event, info)`, as `AddItemWithReceipt` does with `ItemReceipt`. |
| `FailureResponse(exceptions)` | Refused; nothing stored. `exceptions` is a `List[String]` of messages. | Your handler returned `CommandFailure("...")`. The library refuses too: a chosen id it does not allow, a new aggregate whose first event has no space, a command of a sharded type too large to send between nodes. |
| `AggregateConcurrentModificationError(aggregateId, aggregateType, expected, was)` | A version clash; nothing stored. `expected` is what you said, `was` what the aggregate has. | A `Command` with an old expected version; an aggregate that does not exist (`was` is version 0); a `ConcurrentCommand` that clashed 10 times; a `FirstCommand` whose chosen id is taken already. |
| `CommandHandlingError(commandName, errorId, commandId)` | The command could not be handled; nothing stored. The log has the cause under `errorId`. | The command timed out; your handler threw, or did not handle the command; the aggregate cannot be restored; it does not exist or is deleted; no ids could be had (then `commandId` is `CommandId(0)`). |
| `EventHandlingError(eventName, errorId, commandId)` | The events could not be applied or stored; nothing stored. The log has the cause under `errorId`. | Your event handler threw; the event is too large; the aggregate has too many events; the database failed or had no free connection. |
| `CommandRejected(aggregateId, reason)` | Not run at all; sending it again later may work. | In serial mode, the aggregate's queue is full; during a rolling upgrade, an event class that not every node can read yet. |

Appendix C has a section for each error response: what causes it and what to do.

---

## The event bus (`EventsBusActor`)

One per node. Messages from `io.reactivecqrs.core.eventbus.EventsBusActor._`.

| You send | You get back | Good to know |
|---|---|---|
| `RegisterSubscribers(subscribers)` | Nothing | Send it once at start, with every projection, listener actor and managed projection's supervisor. The bus asks each for its subscriptions and publishes nothing until all have answered; meanwhile it logs every 5 seconds whom it waits for. `RegisterSubscribers(Nil)` starts a bus without subscribers. |
| `GetDeliveryStatus` | `DeliveryStatus(waiting, undelivered, stopped, held, behind)` | Answered even while the bus waits for its subscribers. See below. |

The fields of `DeliveryStatus`:

- `waiting: Int`: deliveries in flight (sent or held, not yet answered);
- `undelivered: Seq[Undelivered]`: the latest 100 deliveries that nobody answered within 10 minutes,
  each `Undelivered(aggregateType, aggregateId, version, subscriber, kind, at)`;
- `stopped: Seq[String]`: subscribers that stopped; their events stay in the outbox;
- `held: Int`: deliveries held back because a subscriber's window was full;
- `behind: Map[String, Int]`: for each subscriber that is behind, how many aggregates are held for it.

### If you write a subscriber by hand

`ProjectionActor` does all of this for you, version checks included. If you ever write a
subscriber from scratch, this is the conversation it must hold with the bus:

| The bus sends you | You answer |
|---|---|
| `GetSubscriptionRequests(registered)` | `SubscriptionRequests(requests, registered)`, with one `SubscribeForEvents(aggregateType, self)`, `SubscribeForAggregates(aggregateType, self)` or `SubscribeForAggregatesWithEvents(aggregateType, self)` per subscription |
| `IdentifiableEvents(aggregateType, aggregateId, events, startsAtSquash)` | `MessageAck(self, aggregateId, versions, EventsDelivery)` once applied (or applied before); `MessageFailed(self, aggregateId, versions, error, EventsDelivery)` when you give up |
| `AggregateWithType(aggregateType, id, version, events, aggregateRoot, startsAtSquash)` | The same, with `AggregatesDelivery` |
| `AggregateWithTypeAndEvents(aggregateType, id, aggregateRoot, events, startsAtSquash)` | The same, with `AggregatesWithEventsDelivery` |
| (nothing: you noticed missing versions) | `RequestRepublish(self, aggregateType, aggregateId, from)` to the bus that delivered |

Answer the actor that sent you the delivery.

---

## The outbox poller (`OutboxPollerActor`)

One per system. Messages from `io.reactivecqrs.core.delivery.OutboxPollerActor._`.

| You send | You get back | Good to know |
|---|---|---|
| `GetOutboxStatus` | `OutboxStatus(waiting, gaveUp, gaveUpTotal)` | `waiting`: how many aggregates had events waiting in the outbox at the poller's last poll (it reads at most 100 000, and the 50 000 newest beyond). `gaveUp`: the 100 reported undelivered that wait longest, each `WaitingInOutbox(aggregateType, aggregateId, oldestVersion, since)`; `gaveUpTotal`: all of them. |
| `Poll` | Nothing | Reads the outbox now. Its own timer sends it every 30 seconds, the first time 10 seconds after start. |

---

## Projections (`ProjectionActor`)

Messages from `io.reactivecqrs.core.projection._`. Send them to the projection, or to its
supervisor if it is managed: the supervisor passes them on.

| You send | You get back | Good to know |
|---|---|---|
| `GetProjectionStatus` | `ProjectionStatus(projectionName, failures, retrying, waitingForGap, failuresTotal)` | `failures`: the oldest 100, each `SubscriptionFailure(subscriptionType, aggregateType, aggregateId, version, error, since)` with its error cut to 500 characters; `failuresTotal`: all of them; `retrying` and `waitingForGap`: at most 1 000 aggregate ids each. |
| `RetryFailedSubscription(aggregateId)` | `FailedSubscriptionsHandled(aggregateId, count)` | Asks the owner for the failed versions again; each failure is cleared once its version is applied. `count`: the failed subscriptions of that aggregate (one per listener kind). |
| `RetryAllFailedSubscriptions` | `FailedSubscriptionsRetried(count)` | Retries every failure, 20 per second. |
| `SkipFailedVersion(aggregateId)` | `FailedSubscriptionsHandled(aggregateId, count)` | Marks each failed version as applied without applying it, then carries on from the next version. |
| Your own query, such as `GetAllCartsNames()` | Whatever your `receiveQuery` sends | A query that throws is answered `Status.Failure(e)`, and the projection carries on. |

### Delayed queries

Inside `receiveQuery` you can answer with a query that waits for its data. These are methods you
call, with `sender()` as `respondTo`:

| You call | The asker gets |
|---|---|
| `delayIfNotAvailable(respondTo, search, forMaximumMillis)`, with `search: () => Option[T]` | `Some(result)` as soon as `search` finds it (it runs again after each applied update); when `forMaximumMillis` has passed, the answer of one last search (`None` if still nothing); `Status.Failure(e)` if `search` throws |
| `delayIfNotAvailableAsync(respondTo, search, forMaximumMillis)`, with `search: () => Future[Option[T]]` | The same |
| `delayIfNotAvailableAsyncCustom(respondTo, search, found, forMaximumMillis)`, with `search: () => Future[T]` and `found: T => Boolean` | The result itself, a `T` (not wrapped), once `found(result)` is true; at the deadline the `T` of one last search; `Status.Failure(e)` on a failure |

E10's cart names answer their query this way:

<!-- snippet: e10/CartNames.scala#query -->
```scala
override protected def receiveQuery: Receive = {
  case GetCartName(id) => delayIfNotAvailable(sender(), () => store.getDocument(id.asLong).map(_.document), 2000)
}
```

A managed projection's build that is retired answers its waiting queries with `None`.

### Subscribable projections (`SubscribableProjectionActor`)

Messages from `io.reactivecqrs.core.projection.SubscribableProjectionActor._`.

| You send (or call) | You get back |
|---|---|
| Your own subscribe message, which the projection handles in `receiveSubscriptionRequest` by calling `handleSubscribe(subscriptionId, listener, filter)` | The listener gets `SubscribedForProjectionUpdates(subscriptionId)`, then `SubscriptionUpdated(subscriptionId, data)` for every update (`sendUpdate`) the filter turns into `Some(data)` |
| `RenewSubscription(subscriptionId)` | Nothing while the projection knows the subscription; `ProjectionSubscriptionsCancelled(List(subscriptionId))` when it does not (subscribe again) |
| `CancelProjectionSubscriptions(subscriptions)` | `ProjectionSubscriptionsCancelled(subscriptions)` |

A push subscription that is not renewed for 10 minutes expires.

---

## Managed projection supervisors (`ProjectionSupervisor`)

The `ActorRef` that `Projections.start` returns. Messages from
`io.reactivecqrs.core.rebuild.ProjectionSupervisor._`; `RebuildTarget` comes from
`io.reactivecqrs.core.projection`.

| You send | You get back | Good to know |
|---|---|---|
| `RebuildProjection` | `RebuildStarted(build)` or `RebuildRefused(reason)` | Starts a new build of the projection's current version; it replaces the active build once it has caught up. Refused while another build or a targeted rebuild runs. |
| `RebuildAggregates(target)`, with `RebuildTarget.OneAggregate(id)`, `RebuildTarget.OneType(aggregateType)` or `RebuildTarget.OneSpace(spaceId)` | `AggregatesRebuilt(target, aggregates)` when done, or `RebuildRefused(reason)` | Rebuilds in place, on the active build. The answer comes only when the rebuild has finished, so give the ask a generous timeout. Refused when the projection does not implement `clearForRebuild`, while a build is in progress, and for an organization while some of its aggregates have no space yet. |
| `GetBuilds` | `Builds(name, active, building, caughtUp)` | `active` and `building` are `Option[BuildRecord]`, each `BuildRecord(name, number, version, suffix, subscriber, contexts, position)`; the building one's `position` is the one its builder saved last. `caughtUp`: the build in progress has every event. |
| `GetBuildStatus` (from `ProjectionBuilder`) | `BuildStatus(phase, position, eventsSent, gapsFilled, handedOver, unreadable)` | The running builder's status: `phase` is `Bulk`, `GoingLive`, `Reconciling` or `CaughtUp`; without a builder at work, `Waiting` or `NoBuild`. Answers for a targeted rebuild's builder too. |
| `ToBuildInProgress(message)` | Whatever the build in progress answers, or `RebuildRefused("no build is running")` | For example `ToBuildInProgress(RetryAllFailedSubscriptions)`: a build with failed subscriptions does not replace the active one. |
| Anything else (your queries, `GetProjectionStatus`, the retry messages) | The answer of the instance that serves | The supervisor forwards it, keeping you as the sender. |

A message the supervisor itself fails on is answered `Status.Failure(e)`.

---

## The squash scheduler (`SquashScheduler`)

One per system. Messages from `io.reactivecqrs.core.aggregate.SquashScheduler._`.

| You send | You get back | Good to know |
|---|---|---|
| `GetSquashStatus` | `SquashStatus(squashed, refused, waiting)` | Counts since the scheduler started; `waiting` is what the current run has not finished yet. |
| `Run` | Nothing | Starts a run now, unless one is still going. Its timer sends it every hour, the first time a minute after start. |

---

## Saga runners (`SagaRunner`)

Messages from `io.reactivecqrs.core.saga._`.

| You send | You get back | Good to know |
|---|---|---|
| Your `SagaOrder`, such as `CreateMultipleCarts(userId, "Groceries", 3)` | The `SagaResponse` your step gives in `SagaSucceded(response)` or `SagaFailed(response)` | `SagaFailureResponse(exceptions)` when a step fails with an exception, when no saga id can be had, or when the saga cannot be stored. In a cluster, an answer can come twice: treat a second one for the same saga as a repeat. |

---

## Host operations (methods you call)

Not messages, but calls your application makes, mostly at start or as one-off jobs.

| You call | It returns | When |
|---|---|---|
| `new PostgresTypesNamesState().initSchema()` | The `PostgresTypesNamesState` | At every start, before the others. |
| `new PostgresEventStoreState(mpjsons, typesNames).initSchema()` | The `PostgresEventStoreState` | At every start. |
| `new PostgresCommandResponseState(mpjsons, typesNames).initSchema()` | The `PostgresCommandResponseState` | At every start. Without it, idempotent commands cannot store their responses and fail. |
| `new PostgresSubscriptionsState(typesNames).initSchema()` | The `PostgresSubscriptionsState` | At every start, when you have projections. |
| `new PostgresSagaState(mpjsons, typesNames).initSchema()` | `Unit` | At every start, when you have sagas. |
| `new PostgresProjectionRegistry().initSchema()` | The `PostgresProjectionRegistry` | At every start, when you have managed projections. |
| `eventStore.buildIndexesConcurrently()` | `Unit` | Once after an upgrade, in the background: it builds the newer indexes without blocking writes, and does nothing when they exist. |
| `eventStore.backfillEventKeys()` | `EventKeysBackfill(written, missing)` | Once, in the background, after every node runs a version with event keys. `missing` counts events whose aggregate row is missing. |
| `eventStore.backfillSpaces(spaceOf)` | `SpaceBackfill(filled, left)` | Once, for aggregates stored before spaces were required, before you rebuild an organization. `left` lists those it could not place. |
| `StoredDataCheck.run(mpjsons, typesNames, projections)` | `StoredDataCheck.Report` | Before an upgrade, on a copy of production. `report.text` is a readable table; `report.unreadable` counts the rows that do not read; `report.missingTables` names the tables it did not find. |
| `StoredDataCheck.usersOf(className)` | `StoredDataCheck.ClassUsers(className, events, archived, aggregates)` | Before removing an event class from the code: when `events` and `archived` are both 0, nothing uses it. |
| `projections.retire(name)` | `Unit` | At start, for each managed projection removed from the code; it drops its tables, versions and failures. |
| `subscriptionsState.clearSubscriptionsInfo(subscriberName)` | `Unit` | For an unmanaged projection removed from the code: forgets the versions and failures stored under its class name. |
| `ReactiveMetrics(system).use(metrics)` | `Unit` | Any time; until then nothing is measured. |
| `UidGeneratorActor.postgres(system)` | `Props` | At start, to create the uid generator. |
| `Singletons.start(system, name, props)` | The `ActorRef` of a proxy | In a cluster, for the outbox poller, the squash scheduler and each saga runner. |
| `Readiness(system).ready`, `Readiness(system).waitingFor` | `Boolean`, `Set[String]` | In health checks: whether the node is ready, and what it still waits for. |

`StoredDataCheck` lives in `io.reactivecqrs.core.tools`; `Singletons` and `Readiness` in
`io.reactivecqrs.core.cluster`; `ReactiveMetrics` in `io.reactivecqrs.core.metrics`. The uid
generator and the document stores create their own tables and sequences.

---

## Defaults worth knowing

These are the values in the code; the last column says where to change them.

| What | Default | Changed with |
|---|---|---|
| Command timeout | 60 s | `AggregateContext.commandTimeout` |
| Attempts of a concurrent command | 10, waiting 10 ms, doubling up to 500 ms | (fixed) |
| Serial mode queue | 1 000 waiting commands per aggregate | `CommandProcessing.Serial(maxQueuePerAggregate)` |
| Event size | 100 000 characters of JSON | `PostgresEventStoreState(eventSizeLimit, eventSizeLimitPerClassName)` |
| Events per aggregate | 10 000 | `PostgresEventStoreState(aggregateVersionLimit)` |
| Snapshots, once turned on | every 200 events, and when the owner stops | `SnapshotPolicy(every, onPassivation)` |
| Idle owners (`Local` types) | a sweep, at most every 5 minutes, stops those idle 2 minutes beyond the 200 most recently used, and any idle 12 hours | `AggregateCommandBusActor(..., maxInactivityMillis, keepAliveLimit)` |
| Idle owners (`Sharded` types) | passivated by Pekko after 120 s idle | `pekko.cluster.sharding.passivation` |
| Owner that could not restore | answers with an error, then is replaced after 5 s | `AggregateCommandBusActor(..., failedRestoreRetryAfter)` |
| Idempotent responses kept | 30 days | `PostgresCommandResponseState(mpjsons, typesNames, retention)` |
| Delivery deadline | 10 minutes | `DeliveryOptions(deadline)` |
| Window per subscriber | 10 000 events and 64 MB | `DeliveryOptions(window, windowBytes)` |
| Outbox poller | a poll every 30 s; publishes again at 3 and 6 minutes; "undelivered" at 10; then every 30 minutes; at most 1 000 aggregates per poll | `OutboxPollerOptions`, `RedeliveryPolicy.Options` |
| Projection retries of a failing listener | about 2 minutes (1 s doubling up to 30 s, 9 attempts) | `ProjectionActorOptions(retries)` |
| A gap is given up | after 10 minutes | `ProjectionActorOptions(gapGiveUpAfter)` |
| Aggregates per projection transaction | up to 100 | `ProjectionActorOptions(maxBatchesPerTransaction)` |
| Builds at the same time | 2 | `ProjectionsSettings(buildsAtOnce)` |
| Squash scheduler | first run after 1 minute, then every hour, up to 100 aggregates per type per run | `SquashSchedulerOptions` |
| Push subscriptions | expire after 10 minutes without renewal | `SubscribableProjectionActor(subscriptionTTL)` |
| Threads for owners and database work | 32 | `reactivecqrs.blocking-dispatcher` |
| Threads for command handlers | 32 | `reactivecqrs.handler-dispatcher` |
| Hand-over of sharded owners | 90 s | `pekko.cluster.sharding.handoff-timeout` |
| A message between nodes | 248 KiB (Pekko's 256 KiB frame, less 8 KiB) | `pekko.remote.artery.advanced.maximum-frame-size` |

---

Next: [Appendix C](appendix-c-troubleshooting.md), for when an answer is not the one you hoped for.

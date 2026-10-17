# Appendix C. Troubleshooting

Something answered with an error, a screen shows yesterday's data, or the log is shouting
"Undelivered". Don't panic: ReactiveCQRS is built so that nothing gets lost silently, and almost
every problem leaves a clear trail. This appendix goes from symptom to likely causes to what to do.
Find your symptom in the headings, read the causes in order (the most common come first), and work
down the steps.

---

## First, three places to look

1. **The log.** `CommandHandlingError` and `EventHandlingError` carry an `errorId`, a random string
   of 16 letters and digits. Search the log for it: the line with it names the cause, with the
   exception.
2. **The status messages.** Each part of the library answers a status question: the event bus
   (`GetDeliveryStatus`), the outbox poller (`GetOutboxStatus`), every projection
   (`GetProjectionStatus`), every managed projection's supervisor (`GetBuilds`) and the squash
   scheduler (`GetSquashStatus`). [Appendix B](appendix-b-cheat-sheet.md) shows what each answers.
3. **The metrics.** Once you call `ReactiveMetrics(system).use(...)`, you see command outcomes and
   times, the outbox's level, deliveries waiting and held, and projection failures
   ([chapter 18](18-production.md)).

---

## Commands

### A command answers `CommandHandlingError`

**What you see:** `CommandHandlingError(commandName, errorId, commandId)`. Nothing was stored.

**Likely causes**, by what the log says next to the `errorId`:

| The log says | What happened |
|---|---|
| `Command AddItem timed out after 60 seconds and is not applied, errorId: [...]` | The handler did not finish within the context's `commandTimeout`. In serial mode the time counts from the command's arrival, so time spent waiting in the queue counts too. |
| `Error handling command AddItem, errorId: [...]`, then an exception | Your command handler threw, or has no case for this command (a `MatchError`). Or the aggregate does not exist ("Aggregate 1234 does not exist") or was deleted ("is already deleted"). Or it could not be read from the store (an `AggregateRestoreFailedException`). |
| `Restoring aggregate 1234 of type ShoppingCart failed, retry after 5 seconds` | The owner could not read the aggregate: the database is down, or a stored event no longer reads (see [below](#a-stored-event-no-longer-reads-after-a-class-change)). For 5 seconds every command to it gets this error; then a new owner tries again. |
| `No pool of aggregate ids for ShoppingCart came` and `Cannot route command ... without ids` | The uid generator failed to hand out ids; `commandId` is then `CommandId(0)`. The next command asks again. |

Timeouts have a classic cause: handlers that hold their thread. Command handlers run on
`reactivecqrs.handler-dispatcher`, 32 threads. Measured on one machine
([docs/perf/heavy-M5.md](../perf/heavy-M5.md), 17/10/2026): 36 handlers of 5 seconds each, sent at
once, all succeeded, the last after 10.0 seconds, because 32 run at a time; a light command on
another aggregate waited 5.4 seconds meanwhile.

**What to do:**

1. Find the `errorId` in the log and read the exception.
2. For a slow handler, move the slow work out of the command: a saga or a worker does it, and a
   second command records the result ([chapter 12](12-sagas.md)). If the command must do it,
   return an `AsyncCommandResult` whose `Future` runs on a thread pool of your own, and raise
   `commandTimeout` for that context.
3. Retry safely: give commands an idempotency key ([chapter 5](05-command-flavors.md)), so sending
   one again can never apply it twice.
4. For restore failures, check the database first, then the section on stored events below.

> **Remember:** a command that timed out is never stored afterwards, even if its handler finishes
> later. Whatever the cause, a `CommandHandlingError` means this command stored nothing.

### A command answers `AggregateConcurrentModificationError`

**What you see:** `AggregateConcurrentModificationError(aggregateId, aggregateType, expected, was)`.
Nothing was stored. `expected` is the version your command named, `was` the version the aggregate
has.

**Likely causes:**

- **Someone else got there first.** Another command changed the aggregate after you read it. This
  is optimistic locking doing its job ([chapter 5](05-command-flavors.md)).
- **`was` is version 0:** the aggregate does not exist. Check the id, and the command bus you sent
  it to.
- **A `ConcurrentCommand` clashed 10 times in a row** with writes its owner did not make itself:
  owners of the same aggregate on other nodes (a `Local` type in a cluster), or writes made outside
  the library. Clashes between commands of one owner never use up those attempts.
- **A `FirstCommand` whose `chosenAggregateId` exists already:** the creation was sent twice
  without an idempotency key, or the id was used before.

**What to do:**

1. When a person is editing: read the aggregate again (`GetAggregate`), show the new state, and
   send the command again with the new `expectedVersion`.
2. When the order does not matter (adding an item to a cart), use a `ConcurrentCommand`: the
   library runs it again on the new state.
3. When many writers hit one aggregate at once, switch the context to serial mode
   (`override val commandProcessing: CommandProcessing = CommandProcessing.Serial()`), which never
   clashes. In a cluster, a `Sharded` type puts every command of an aggregate through one owner.
   Better still, split the hot aggregate.
4. For creations that may be sent twice, combine `chosenAggregateId` with an idempotency key: the
   second copy then gets the first copy's `SuccessResponse` instead of the error.

> **Tip:** concurrent commands cope with a lot. Measured on one machine
> ([docs/perf/load-M5.md](../perf/load-M5.md), 17/10/2026): 10 000 concurrent commands sent at
> once to a single aggregate all succeeded, 2 621 per second.

### A command answers `FailureResponse`, but not with your message

**What you see:** `FailureResponse(exceptions)`. Usually the message is one of your own, from a
`CommandFailure`. When it is not, the library refused the command before running it:

| The message starts with | Why | What to do |
|---|---|---|
| "A new aggregate needs a space" | The first event of a new aggregate has no space: its `spaceId` is negative, such as `SpaceId.unknown`. | Every `FirstEvent` names a space: `SpaceId(0)` for the platform, or the organization's ([chapter 16](16-organizations.md)). |
| "aggregate id ... is not positive", "... is not below 2^53", or "... is in the range of installation ..." | A `chosenAggregateId` the library does not allow. | Take chosen ids from `ReserveAggregateId` instead of making them up. Ids below 10^12 (system and legacy ids) are allowed too. |
| "Command ... is too large to be sent between nodes" | In a cluster, a command of a `Sharded` type that does not fit in one message between nodes (248 KiB by default). | Keep commands small: store large documents elsewhere first and send a reference. See [MessageTooLargeException](#messagetoolargeexception-in-a-cluster). |
| "Command ... cannot be sent between nodes" | The cluster serializer cannot write the command. | Make it a plain case class that mpjsons can write and read ([chapter 17](17-cluster.md)). |

### A command answers `EventHandlingError`

**What you see:** `EventHandlingError(eventName, errorId, commandId)`. Nothing was stored.

**Likely causes**, by the log line with the `errorId`:

| The log says | What happened |
|---|---|
| `Error handling event ItemAdded, errorId: [...]` | Your event handler threw on the new event, or has no case for it. |
| `Error persisting events, errorId: [...]` with `TooManyEventsException` | The aggregate is at its limit of events; see [below](#an-aggregate-stops-taking-commands-at-10-000-events). |
| ... with `EventTooLargeException` | An event's JSON is over the size limit; see [below](#an-event-is-too-large). |
| ... with a connection error | The database is down, or no connection was free in time (5 seconds with ScalikeJDBC's default pool). |
| ... "Undo cannot reach the squashed history" | An undo would cancel events below the squash point ([chapter 14](14-squash.md)). |
| ... "Duplication cannot read the squashed history" | A duplication of a version below the base's squash point. |
| ... "Maybe FirstEvent was not used" | A new aggregate's first event is a plain `Event`, not a `FirstEvent`. |
| ... `RewriteChangesClassException` | A history rewrite tried to change an event's class ([chapter 15](15-rewriting-history.md)). |
| ... an error about the `commands_responses` table | An idempotent command, but `PostgresCommandResponseState.initSchema()` was not called at start. |

**What to do:** fix what the log names. For connection errors, give the JDBC pool at least as many
connections as `reactivecqrs.blocking-dispatcher` has threads (32 by default), and make
PostgreSQL's `max_connections` cover the pools of every node.

### A command answers `CommandRejected`

**What you see:** `CommandRejected(aggregateId, reason)`. The command did not run at all, so
sending it again later is safe.

| The reason says | Why | What to do |
|---|---|---|
| "1000 commands of the aggregate wait already" | Serial mode, and the aggregate's queue is full. | Back off and send it again, with an idempotency key. If it happens often, the aggregate is a hot spot: split it. |
| "... is written once every node runs version ... or later" | A rolling upgrade is under way. | See [below](#a-rolling-upgrade-with-a-new-event-class). |

### The ask times out, but the command was applied later

**What you see:** your code gets an `AskTimeoutException`, yet later the change is there.

**Why:** your ask's timeout is shorter than the time the command took. Commands may wait: in a
serial queue, behind slow handlers, and in a cluster while their owner is handed over to another
node (up to 90 seconds). The library's own timeout is 60 seconds by default.

**What to do:**

1. Make your ask's timeout longer than the context's `commandTimeout`, or shorten
   `commandTimeout`, so the library's answer always comes first.
2. Give commands an idempotency key and retry with the same key: a command that went through late
   is then never applied twice.
3. Limit the commands in flight at the edge of your application, and answer "busy" (HTTP 429 or
   503) beyond that. [docs/SCALING.md](../SCALING.md), section 2, shows how to size the limit.

### `GetAggregateMinVersion` fails with `AggregateInIncorrectVersionException`

**What you see:** `Failure(AggregateInIncorrectVersionException)`: the aggregate did not reach the
version within `maxMillis`.

**Likely causes:** the command failed, or was never sent; you asked for a version the command does
not produce (a command with three events moves the version by three); or the owner stopped while
you waited.

**What to do:** take the version from the command's `SuccessResponse` and ask for exactly that one;
check what the command answered; allow more `maxMillis` if the aggregate is busy.

### An aggregate stops taking commands at 10 000 events

**What you see:** every command to one aggregate answers `EventHandlingError`, and the log names
`TooManyEventsException`: "Too many versions for aggregate 1234@..., max version is 10000".

**Why:** the event store's `aggregateVersionLimit`, 10 000 by default, guards against runaway
aggregates. Versions never go down: snapshots and squash make a long aggregate cheaper to load,
but neither lifts this limit.

**What to do:**

1. By design: split long-lived things along a natural boundary (a month, a season, a document
   version) into aggregates of their own.
2. If an aggregate really needs more, build the store with a higher limit, for example
   `new PostgresEventStoreState(mpjsons, typesNames, aggregateVersionLimit = 50000)`. It
   applies to every aggregate type of that store.
3. Turn on snapshots for such types ([chapter 13](13-snapshots.md)). Measured on one machine
   ([docs/perf/load-M5.md](../perf/load-M5.md), 17/10/2026): restoring an aggregate of 50 000
   events took 392 ms by replaying every event, and 12 ms from a snapshot.

### An event is too large

**What you see:** `EventHandlingError`, and the log names `EventTooLargeException`: "Event of type
... is too large: ... max size is 100000 bytes". The limit is counted in characters of the event's
JSON (the message calls them bytes).

**What to do:**

1. Keep documents out of events (the "claim check" pattern): store the file first, in an object
   store or a table of its own, then send a command that names it (an id, a hash, a size), and keep
   only that reference in the event and the state.
2. If an event class really must carry more, raise the limit for that class only, with
   `eventSizeLimitPerClassName = Map(classOf[ReportAttached].getName -> 1000000)` when you build
   the `PostgresEventStoreState` (`ReportAttached` stands for your own class).

> **Warning:** large events cost everywhere: in every restore, re-publication, rebuild and message
> between nodes. Measured on one machine ([docs/perf/large-M5.md](../perf/large-M5.md), 17/10/2026): restoring
> an aggregate of 20 events of 5 MB took 517 ms and the heap grew from 117 to 323 MB, and one such
> event serialized for another node is 4.8 MB, far over the 248 KiB a message may carry.
> [docs/SCALING.md](../SCALING.md), section 7, has the full list of what to tune.

---

## Delivery and projections

### The event bus publishes nothing

**What you see:** commands succeed, but no projection changes. Every 5 seconds the log says "Event
bus waits for RegisterSubscribers from the host; nothing is published until then", or "Event bus
waits for the subscriptions of: ..." with a list of names. The node does not report ready.

**Likely causes:**

- The host never sent `RegisterSubscribers` to the bus ([chapter 4](04-first-command.md)).
- A subscriber in the list never answers the bus's question about its subscriptions: an actor that
  is not a projection, or a managed projection whose supervisor cannot read its registry (the log
  says "Projection ... could not read its builds; trying again in 10 seconds", for example because
  `PostgresProjectionRegistry.initSchema()` was not called).

Nothing is lost meanwhile: the events are stored and wait in the outbox, and the bus keeps the
publications until it starts.

**What to do:** send `RegisterSubscribers` once at start, with every projection, listener actor and
supervisor (`RegisterSubscribers(Nil)` when there are none). The "waits for the subscriptions of"
line names the ones still missing.

> **Remember:** subscribers are fixed at start. A projection missing from the list simply gets
> nothing, and nobody complains: events of a type nobody subscribed to leave the outbox at once.

### A projection stops updating

**What you see:** a read model no longer changes, for every aggregate or only for some.

Ask the projection `GetProjectionStatus` and the bus `GetDeliveryStatus`, then:

| What you find | Likely cause | What to do |
|---|---|---|
| The aggregate is in `ProjectionStatus.failures` | The listener failed on it and gave up | [A subscription failed](#a-subscription-failed) |
| The aggregate is in `retrying` | The listener fails and is retried, for about two minutes | Read the warning "Projection ... failed on ..." in the log |
| The aggregate is in `waitingForGap` | The projection misses versions and asked the owner for them | Usually it resolves itself; after 10 minutes it becomes a failure. Check that the owner can restore the aggregate |
| `DeliveryStatus.stopped` names the projection | The projection actor stopped; its events wait in the outbox | Find why in the log. Unmanaged: restart the node. Managed: its supervisor starts it again after 10 seconds |
| `DeliveryStatus.behind` names it | It is slower than the writes; the bus holds what does not fit its window | Let it catch up; if it is far behind, rebuild it ([chapter 10](10-rebuilding-projections.md)); keep listeners cheap |
| Nothing at all | It is not registered on the bus, or the bus has not started | [The event bus publishes nothing](#the-event-bus-publishes-nothing) |
| A managed projection serves old data while `GetBuilds` shows a build in progress | It was started with `ServeStale` | By design: it switches once the new build has caught up |

> **Tip:** a projection created without `.withDispatcher(BlockingDispatcher.id)` says so at start:
> "Projection ... runs its listener transactions on the default dispatcher". Fix that first: it
> slows down everything else on the default dispatcher too.

### A subscription failed

**What you see:** an error in the log, "Projection ... gave up on ShoppingCart 1234 at version 7
(...); deliveries of it fail until it is retried or skipped", and the aggregate in
`ProjectionStatus.failures`. The other aggregates keep flowing.

**Likely causes:**

- The listener threw on that version for about two minutes of retries: a bug, a constraint of your
  own table, or a document that is not there ("Document for update does not exist").
- Missing versions were asked for during 10 minutes without an answer ("versions from ... are
  missing"): the owner cannot restore the aggregate, or, in a cluster, an event too large to reach
  the projection's node.
- During a build: an event that cannot be read from the store.

While a subscription of an aggregate is failed, that aggregate can neither be squashed nor deleted
permanently.

**What to do:**

1. Read `error` in the failure, and the log.
2. Fix the cause and deploy the fix.
3. Send `RetryFailedSubscription(aggregateId)`, or `RetryAllFailedSubscriptions`. Failures recorded
   before a start are retried at start anyway, 20 per second.
4. Only if that version can never be applied, send `SkipFailedVersion(aggregateId)`.
5. For a build in progress, wrap the message: `ToBuildInProgress(RetryAllFailedSubscriptions)`. A
   build with failed subscriptions does not replace the active one.

```scala
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import io.reactivecqrs.core.projection._
import scala.concurrent.Await
import scala.concurrent.duration._

implicit val timeout: Timeout = Timeout(5.seconds)

val status = Await.result((projection ? GetProjectionStatus).mapTo[ProjectionStatus], 5.seconds)
status.failures.foreach(f => println(s"${f.aggregateId} failed at ${f.version}: ${f.error}"))

// After the fix is deployed:
Await.result((projection ? RetryFailedSubscription(cartId)).mapTo[FailedSubscriptionsHandled], 5.seconds)
```

> **Warning:** `SkipFailedVersion` marks the version as applied without applying it, so the read
> model misses that change. Afterwards, rebuild what the projection made from that aggregate:
> `RebuildAggregates(RebuildTarget.OneAggregate(cartId))` for a managed projection that implements
> `clearForRebuild` ([chapter 10](10-rebuilding-projections.md)).

### Events stay in the outbox ("Undelivered" in the log)

**What you see:** errors from the outbox poller, "Undelivered: ShoppingCart 1234 from version 7
still waits in the outbox after 10 minutes; it is re-published again every 30 minutes", and from
the bus, "Undelivered after 10 minutes: ... it stays in the outbox until it is published again".
Earlier, warnings like "Delivery waits for 1 minute: ...". `GetOutboxStatus` lists them in
`gaveUp`.

**Likely causes:**

- **A subscriber that never answers:** a listener stuck in a long transaction (waiting for a
  database lock, calling a slow external service), or a hand-written subscriber that does not send
  `MessageAck`.
- **A subscriber that stopped:** its events stay in the outbox until the node starts again
  (`DeliveryStatus.stopped` names it).
- **The bus has not started:** see [above](#the-event-bus-publishes-nothing).
- **The owner cannot publish them again:** the aggregate cannot be restored; or the log says "No
  command bus registered for ..., cannot re-publish", because the type's command bus was built with
  another event bus than the one the poller uses.
- **A subscriber far behind:** what it has not taken waits its turn in the store.
- **In a cluster:** an event or state too large to reach the subscriber's node.

And one cause with no log at all: **no `OutboxPollerActor` was created.** Then events whose first
delivery did not complete (a crash, a restart, a subscriber that was down) are never published
again, and the outbox only grows.

**What to do:**

1. Ask `GetDeliveryStatus` (on every node's bus), `GetOutboxStatus` and `GetProjectionStatus`.
2. Fix or restart the subscriber; the waiting events follow by themselves.
3. Check that exactly one outbox poller runs per system (in a cluster, started with
   `Singletons.start`).
4. After a long outage, remember that the poller publishes at most 1 000 aggregates again per
   30-second poll: 1 million waiting aggregates take about 8.3 hours (estimated in
   [docs/SCALING.md](../SCALING.md), section 4). Rebuilding the projection may be faster; or raise
   the pace while the backlog drains:

   ```scala
   import org.apache.pekko.actor.Props
   import io.reactivecqrs.core.delivery.{OutboxPollerActor, OutboxPollerOptions, RedeliveryPolicy}

   val outboxPoller = system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus,
     OutboxPollerOptions(redelivery = RedeliveryPolicy.Options(maxRepublishPerPoll = 5000)))), "outboxPoller")
   ```

> **Under the hood:** the outbox poller is the single owner of re-deliveries (D8): every 30 seconds
> it reads which aggregates wait, and the pure `RedeliveryPolicy` decides who is due. Full story in
> [ARCHITECTURE.md, section 5.1](../ARCHITECTURE.md).

### A new projection misses old aggregates

**What you see:** you added a projection; it shows the aggregates that changed since, but not the
others.

**Why:** an unmanaged projection starts knowing nothing. When an aggregate gets a new event, the
projection notices the gap and asks for that aggregate's earlier events; an aggregate that never
changes again never reaches it.

**What to do:** start the projection as a managed one, with `Projections.start`: its first build
reads the whole event store before it serves ([chapter 10](10-rebuilding-projections.md)). For a
managed projection that went wrong, send its supervisor `RebuildProjection`.

### A query returns `None` (or old data) right after a command

**Why:** the read side is eventually consistent. The command succeeded; the projection applies its
events a moment later.

**What to do:** answer the query with `delayIfNotAvailable` and a deadline, so it waits for the data
([chapter 8](08-projections.md)); or ask the command bus `GetAggregateMinVersion` with the version
from the `SuccessResponse` ([chapter 7](07-reading-the-past.md)). If the answer stays `None`, check
the projection with the sections above.

---

## Long histories

### A squash is refused

**What you see:** `SquashRefused(id, reason)`, and nothing changed. The squash scheduler counts its
refusals (`SquashStatus.refused`) and tries those aggregates again in a later run.

| The reason says | What it means | What to do |
|---|---|---|
| "squash is not enabled for ..." | The context has no `squash` policy. | Give the context one: `override val squash: Option[SquashPolicy[ShoppingCart]] = Some(SquashPolicy(...))` ([chapter 14](14-squash.md)). |
| "version ... is not between 1 and ..." | `upTo` is 0, negative, or above the current version. | Pick a version the aggregate has. |
| "already squashed up to version ..." | It is squashed that far already. | Pick a later version, or leave it. |
| "events at or below version ... still wait in the outbox" | Some subscriber has not confirmed those events yet. | Wait, or see [Events stay in the outbox](#events-stay-in-the-outbox-undelivered-in-the-log). |
| "a subscription of the aggregate is failed" | A projection gave up on this aggregate. | [Repair the subscription](#a-subscription-failed) first. |
| "an undo after version ... undid events at or below it" | A later undo cancels events the squash would fold away. | Squash up to the undo's version or later, or below the first event it undid. |
| "the first event of duplicate ..., which the squash detaches, was undone" | A duplicate copied below the point cannot be detached. | Squash at or below the version that duplicate was copied at. |
| "the first event of duplicate ..., which the squash detaches, still waits in the outbox" | The duplicate's first event is not delivered yet. | Wait, and try again. |
| "a subscription of duplicate ..., which the squash detaches, is failed" | A projection gave up on that duplicate. | Repair that subscription first. |
| "the duplicates of the aggregate changed meanwhile" | Someone duplicated it during the squash. | Try again. |
| "the aggregate is deleted at version ..." | Its state is deleted at that version. | There is nothing to squash there. |
| "making the squash event failed: ..." | Your `toEvent` threw. | Fix `toEvent`. |
| "the event handlers fail on the squash event ..." | Your event handlers have no case for the squash event, or throw on it. | Handle the squash event in the event handlers. |
| "the squash event ... does not give back the state it replaces" | Applied to the initial state, the squash event gives a different state. | Fix `toEvent` or the handler so the state comes back equal. If leaving data out is intended, mix `SquashMayChangeState` into the event class. |
| "duplicate ...: " followed by one of the last three | The same check failed for a duplicate the squash would detach. | As above. |
| "... is written once every node runs version ... or later" | A rolling upgrade is under way, and the squash event class is new. | Finish the rollout first. |
| "the aggregate cannot be restored: ..." | The owner cannot read the aggregate. | See the restore causes [above](#a-command-answers-commandhandlingerror). |
| "the squash failed: ..." | The database failed. | Check the database, and try again. |

> **Tip:** the "gives back the state" check compares with `==`. Case classes holding `Vector`,
> `List`, `Map` or `Set` compare by content, as you would expect. An `Array` compares by reference,
> so a state holding one never equals its copy, and every squash is refused. Use a `Vector`.

### A historical read fails below a squash point

**What you see:** `Failure(AggregateSquashedException)`: "The history of aggregate 1234 is squashed
up to version 50; version 3 cannot be read" (or "its state at ... cannot be read").

**Why:** that is what a squash does: the events below the squash point are gone. Reading a version
below it (`GetAggregateForVersion`, `GetEventsForAggregateForVersion`), or a moment before the
squash event's time (`GetAggregateAtInstant`), fails instead of giving a wrong answer.
`GetEventsForAggregate` still works: the list starts at the squash event.

**What to do:**

- Read at the squash point or later: the exception tells you where it is (`squashedUpTo`).
- For audit, decide *before* squashing which event classes must stay readable, name them in
  `SquashPolicy.keep`, and read them with `GetArchivedEvents`. Events squashed before a class was
  added to `keep` are gone for good.
- Keep recent history readable with `AutomaticSquash(afterEvents, keepLast, keepNewerThan)`
  ([chapter 14](14-squash.md)).

```scala
import scala.util.{Failure, Success, Try}

(commandBus ? GetAggregateForVersion(cartId, AggregateVersion(3))).mapTo[Try[Aggregate[ShoppingCart]]].map {
  case Success(cart) => Some(cart)
  case Failure(e: AggregateSquashedException) => None // only versions from e.squashedUpTo on exist
  case Failure(e) => throw e
} // with an implicit ExecutionContext in scope
```

### A permanent delete waits

**What you see:** a `PermanentDeleteEvent` was stored, yet the aggregate's rows are still in the
database. The log has a warning, "Aggregate 1234 of type ShoppingCart is not deleted yet: ...", or
an error, "... is not deleted: ...".

**How it works:** the delete runs once every subscriber has acknowledged the event. When something
stands in the way, the event stays in the outbox, and the delete is tried again each time the
outbox poller has it published again (after 3 and 6 minutes, then every 30 minutes). A subscriber
that is stopped or far behind delays the delete too.

| The reason says | What to do |
|---|---|
| "a subscription of it is failed" | [Repair the subscription](#a-subscription-failed). A projection you removed from the code may have left failure records behind: for a managed one call `projections.retire(name)`, for an unmanaged one `subscriptionsState.clearSubscriptionsInfo(...)` with its class name. |
| "the first event of duplicate ..., which the delete detaches, still waits in the outbox" | Nothing: it is tried again once that event is delivered. |
| "a subscription of duplicate ..., which the delete detaches, is failed" | Repair that subscription. |
| "the first event of duplicate ..., which the delete detaches, was undone" | Such a duplicate cannot be detached; deal with it first, for example by deleting it permanently as well. |
| "the duplicates of the aggregate changed meanwhile" | Nothing: it is tried again. |
| "... is written once every node runs version ... or later" | Finish the rolling upgrade. |
| Error: "aggregates ... were duplicated from it, and the type has no squash event to detach them" | Duplicates read this aggregate's history, and only a squash event can detach them: give the context a `SquashPolicy`. Until then the delete never happens. |
| Error: "duplicate ...: " followed by a squash-event problem | The squash event fails its check for a duplicate's state; fix it as in [A squash is refused](#a-squash-is-refused). |

> **Remember:** the permanent delete removes the aggregate's events, archive, outbox rows, snapshot
> and subscription rows. Your read models are yours: projections remove their documents when they
> see the `PermanentDeleteEvent` ([chapter 6](06-events.md)).

### A history rewrite answers success, but nothing changed

**Why:** today, a rewrite whose handler returns rewritten events but no new event is answered
`SuccessResponse`, and nothing is stored. (A complete history rewrite is coming in 1.0.)

**What to do:** always add a compensating event, as the shopping cart does:
`RewriteCommandSuccess(rewritten, CartNameRewritten(name))`. It also tells your projections that
something changed ([chapter 15](15-rewriting-history.md)).

---

## Start-up, upgrades and clusters

### Start-up is slow

**Likely causes:**

- **Restores.** An owner replays its aggregate's whole history on the first message, unless the
  context takes snapshots. After a restart, every aggregate that is used pays this once. Estimated
  in [docs/SCALING.md](../SCALING.md), section 3: a node restores about 80 aggregates of 50 000
  events per second without snapshots, and about 2 700 with them.
- **The outbox after an outage.** At the first poll, 10 seconds after start, events written before
  the start are published again: up to 1 000 aggregates per poll, each starting and restoring an
  owner.
- **Projections retrying old failures,** 20 per second, at start.
- **The event bus waiting for its subscribers.** The node is not ready until every one has answered
  ("Event bus waits for the subscriptions of: ...").
- **Schema initialization waiting for another node.** Nodes take turns under the schema lock; one
  gives up after five minutes ("The schema lock was not free for 5 minutes: another process
  initializes the schema").
- **Heavy jobs in the start-up path.** `buildIndexesConcurrently()`, `backfillEventKeys()` and
  `backfillSpaces(...)` belong in the background, never before the actors start.

**What to do:** turn on snapshots for aggregate types with long histories, and squash old history
([chapter 13](13-snapshots.md), [chapter 14](14-squash.md)); run heavy jobs in the background; ask
`Readiness(system).waitingFor` what the node still waits for. A new build of a managed projection
does not slow the start: the active build serves meanwhile.

### A stored event no longer reads after a class change

**What you see:** after a deployment, commands to some aggregates answer `CommandHandlingError`,
and queries `Failure(AggregateRestoreFailedException)`; the log says "Restoring aggregate ... failed"
with a JSON or class error. Builds record failed subscriptions, and `GetEventsForAggregate` fails.
Or an `UnsupportedEventVersionException`: "... is stored with event_type_version ..., but event
versioning was removed in 1.0".

**Why:** events are stored as JSON under their class name, and read back into that class. Renaming
or moving an event class, changing a field's type, or deleting a class that stored events still use
breaks reading. Rows written with the event versions of releases before 1.0 do not read either.

**What to do:**

1. Never change a stored event class. Keep the old class (mark it `@deprecated`), add a new one
   (`ItemAddedV2`), and handle both in event handlers and projections ([chapter 6](06-events.md)).
2. Before every upgrade, run `StoredDataCheck` on a copy of production, with the new code:

   ```scala
   import io.reactivecqrs.core.tools.StoredDataCheck

   val report = StoredDataCheck.run(mpjsons, typesNames,
     Seq(StoredDataCheck.ProjectionTable[String]("storeA"))) // one per document store, with its document class
   println(report.text)
   if (report.unreadable > 0) sys.error("Fix the unreadable rows before the upgrade")
   ```

   It reads every stored event, archived event, saga order, idempotent response and the projection
   tables you list, and reports, per class, the rows that do not read.
3. Before deleting an old class from the code, ask `StoredDataCheck.usersOf(className)`: delete it
   only when both `events` and `archived` are 0.
4. When the aggregate root class changes shape, raise `SnapshotPolicy.schemaVersion`, so that old
   snapshots are set aside instead of being read into the new shape.

### A rolling upgrade with a new event class

**What you see:** during the rollout, commands answer `CommandRejected` with a reason like
"ItemAddedV2 is written once every node runs version 2.0.0 or later", and squashes are refused
with the same reason. (`ItemAddedV2(name, source)` is the new cart event of the test suite's rolling
upgrade, `TestCluster.scala`.)

**Why:** the context lists the class in `writtenFrom`. A node of the older version could not read
the event, so nothing writes it until every member of the cluster announces at least that
`pekko.cluster.app-version`. A node that does not know the cluster's members yet writes no new class
at all.

**What to do:**

1. Let handlers write the older event in the meantime, as the test suite's version 2.0.0 of the
   cart does:

   ```scala
   override val writtenFrom: Map[Class[_], String] = Map(classOf[ItemAddedV2] -> "2.0.0")

   override def commandHandlers = cart => {
     case c: AddItemConcurrently if everyNodeRuns("2.0.0") => CommandSuccess(ItemAddedV2(c.name, "version 2"))
     case other => super.commandHandlers(cart)(other) // version 1.0.0's handlers, which write ItemAdded
   }
   ```

   The same release handles `ItemAddedV2` in its event handlers and projections, so every node of
   that version can read it.

2. Set `pekko.cluster.app-version` to the release's version on every node. When no member sets
   one, the switch is off and every class is written at once, which an old node may not read.
3. Never roll back below a version whose new classes have been written.
4. Send a new command class only once every node runs the version that knows it.
5. A field you add to a class that travels between nodes (a command, a response, a query answer)
   must be an `Option`: a missing field reads as `None`, zero or `null`, never as its default value.
6. Changing a type's placement or `numberOfShards` needs a full stop, not a rolling upgrade
   ([chapter 17](17-cluster.md)).

### `MessageTooLargeException` in a cluster

**What you see:** an ask fails at once with `MessageTooLargeException`: "A ... of ... bytes is too
large to be sent between nodes; the limit is ... bytes". The sending node logs the class and the
size, at most once per class every 10 seconds. A command of a `Sharded` type is answered
`FailureResponse` instead. A delivery too large for a projection on another node leaves it behind
for that aggregate, until the gap becomes a failed subscription after 10 minutes.

**Why:** a message between nodes must fit Pekko's frame, 256 KiB by default; the library's limit is
8 KiB less, 248 KiB. On a single node nothing is serialized, so this shows up only in a cluster.

**What to do:**

- Keep commands, responses, states and query answers well under 248 KiB; keep documents out of
  events and commands (see [An event is too large](#an-event-is-too-large)).
- Read a large state on your own node with `GetAggregateAtInstant(id, Instant.now())`: it is
  restored from the store there, instead of being sent by its owner.
- Page large query answers.
- If larger messages cannot be avoided, raise `pekko.remote.artery.advanced.maximum-frame-size`, or
  route large messages through `large-message-destinations`. Mind the memory: Pekko keeps up to 128
  buffers of the frame size, so a 1 MiB frame can take up to 128 MiB of direct memory (estimated in
  [docs/SCALING.md](../SCALING.md), section 6).

---

## Habits that prevent most of this

- Give every command a client might send twice an idempotency key.
- Make the client's ask timeout longer than the command timeout.
- Keep events, commands and states small; store documents elsewhere and keep a reference.
- Turn on snapshots for long-lived aggregates; plan `SquashPolicy.keep` before the first squash.
- Never rename or reshape a stored event class: add a `V2` next to it.
- Run `StoredDataCheck` on a copy of production before every upgrade.
- Watch `GetProjectionStatus`, `GetDeliveryStatus`, `GetOutboxStatus` and the metrics, and alert on
  "Undelivered" and "gave up" in the log.

That is the end of the book. Back to the [table of contents](README.md) whenever you need it.

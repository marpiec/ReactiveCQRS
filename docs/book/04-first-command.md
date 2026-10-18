# Chapter 4. Wiring It Up and Sending Your First Command

Your cart from [chapter 3](03-first-aggregate.md) is a set of plain functions, waiting for someone to
call them. In this chapter we build that someone. By the end you will have a small program that
starts the library, creates a cart called "Groceries", puts apples and oranges in it, reads the cart
back and prints it. Along the way you will meet the actors that do the work, and learn what each
answer of the library means.

---

## Who is who

Picture a small town hall. A **ticket machine** hands out numbers. Each department has a **front
desk** that takes requests for its kind of case. Behind the desk, every case file has a **clerk**
who knows that file by heart and writes in it. A **notice board** announces what was decided to
whoever cares. And a **night porter** walks around checking that every announcement was read.

ReactiveCQRS has exactly these, as actors:

| Actor | In the town hall | Its job | How many |
|---|---|---|---|
| `UidGeneratorActor` | the ticket machine | hands out ids for new aggregates, for commands and for sagas | one per system |
| `AggregateCommandBusActor` | a front desk | takes the commands and questions for one aggregate type and passes each to the aggregate's owner | one per aggregate type |
| `AggregateOwner` | a clerk | looks after one aggregate: keeps its state, runs your handlers, stores the events | one per aggregate in use; the command bus makes them for you |
| `EventsBusActor` | the notice board | announces stored events to every subscriber and waits for each to say "got it" | one per system |
| `OutboxPollerActor` | the night porter | has events published again when a subscriber has not confirmed them | one per system |

You create all of them except the owners, each one after the pieces it needs. Let's go, one step at
a time. ("One per system" becomes "one per machine", or "one in the whole cluster", in
[chapter 17](17-cluster.md); for now we have one machine and one actor system.)

Each step's code is a piece of E1's `FirstProgram`
(`examples/src/main/scala/io/reactivecqrs/examples/e1/FirstProgram.scala`), which you will see whole at
the end of the chapter, with its imports.

## Step 1: the database

Exactly as in [chapter 2](02-setting-up.md): the connection pool first,

<!-- snippet: e1/FirstProgram.scala#pool -->
```scala
Class.forName("org.postgresql.Driver")
ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs",
  ConnectionPoolSettings(initialSize = 5, maxSize = 32, connectionTimeoutMillis = 3000L))
```

then the tables:

<!-- snippet: e1/FirstProgram.scala#tables -->
```scala
val mpjsons = new MPJsons
val typesNames = new PostgresTypesNamesState().initSchema()
val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
```

A cart with no projections and no sagas needs only these three. Once you add projections and sagas,
add the two other `initSchema()` calls of chapter 2.

## Step 2: the actor system

<!-- snippet: e1/FirstProgram.scala#system -->
```scala
val system = ActorSystem("shop")
```

Every actor of the library lives in this system, and it reads your `application.conf` (chapter 2)
when it starts.

## Step 3: the uid generator

New carts need ids, and every command gets one too (it comes back in the error answers you will meet
below). The uid generator hands them out:

<!-- snippet: e1/FirstProgram.scala#uids -->
```scala
val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
```

`UidGeneratorActor.postgres(system)` gives you the `Props` of a generator that takes its numbers
from three PostgreSQL sequences: `aggregates_uids_seq`, `commands_uids_seq` and `sagas_uids_seq`.
It creates them if they are missing, which is why chapter 2 had no `initSchema()` for them. It also
reads `reactivecqrs.installation-number` and keeps aggregate ids inside your installation's range.
And it sets all of this up right away, so a wrong installation number stops your program at start,
which is exactly when you want to hear about it.

Spelled out, the same generator looks like this. You will see this longer form in the library's
specs:

```scala
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import org.apache.pekko.actor.Props

val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
  new PostgresUidGenerator("aggregates_uids_seq"),
  new PostgresUidGenerator("commands_uids_seq"),
  new PostgresUidGenerator("sagas_uids_seq"))), "uidGenerator")
```

The one difference: the spelled-out form ignores the installation number. In an application,
prefer `UidGeneratorActor.postgres(system)`.

Ids come in **pools of 10 000**. A command bus asks for a pool when it starts, and for the next one
when only a fifth of the current pool is left, so a command hardly ever waits for an id.

## Step 4: the event bus

<!-- snippet: e1/FirstProgram.scala#eventBus -->
```scala
val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
eventBus ! EventsBusActor.RegisterSubscribers(Nil)
```

The second line matters more than it looks. The event bus **publishes nothing until you tell it who
its subscribers are**, even when the answer is "nobody". The subscribers are your projections, sagas
and listeners, and we have none yet, so the list is empty: `Nil`. From [chapter 8](08-projections.md)
on, this is where your projections go.

Why so strict? An event that nobody has subscribed to is acknowledged at once and leaves the outbox.
If the bus started publishing before your projections had subscribed, the first events after every
start would never reach them. So the bus waits, and holds what it receives in the meantime.

> **Warning:** forget `RegisterSubscribers` and nothing fails loudly. Your commands still succeed,
> and their events are safe in the database, but the bus holds every one of them and logs a warning
> every five seconds: `Event bus waits for RegisterSubscribers from the host; nothing is published
> until then`. Not a single projection hears about anything. Send it once, after you have created
> every subscriber.

## Step 5: the command bus

Now the front desk for carts. You give it your context from chapter 3 and the pieces of steps 1 to 4:

<!-- snippet: e1/FirstProgram.scala#carts -->
```scala
val carts = system.actorOf(
  AggregateCommandBusActor(new ShoppingCartContext, uidGenerator, eventStore, responses, eventBus), "carts")
```

`AggregateCommandBusActor(...)` returns the `Props` of a command bus for the context's aggregate
type; `system.actorOf` starts it. It has three more parameters with defaults that suit almost
everyone:

| Parameter | Default | What it does |
|---|---|---|
| `maxInactivityMillis` | 43 200 000 (12 hours) | An owner that has had nothing to do for this long is stopped. |
| `keepAliveLimit` | 200 | With more owners than this in memory, the least recently used ones that have been idle for over 2 minutes are stopped. |
| `failedRestoreRetryAfter` | 5 seconds | When an aggregate cannot be read from the store, how long its owner answers with that failure before the next request tries again. |

The bus looks for owners to stop at most every 5 minutes, as messages come in. A stopped owner is no
loss: the next command or question for that cart starts a new one, which replays the cart's events
(chapter 3's fold, for real).

A second aggregate type is one more line, sharing everything else:

```scala
val invoices = system.actorOf(
  AggregateCommandBusActor(new InvoiceContext, uidGenerator, eventStore, responses, eventBus),
  "invoices")
```

(`InvoiceContext` is a context you would write yourself, like the cart's.)

> **Remember:** one command bus per aggregate type. All of them share one uid generator, one event
> store, one response store and one event bus.

## Step 6: the outbox poller

<!-- snippet: e1/FirstProgram.scala#poller -->
```scala
system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus)), "outboxPoller")
```

Every event is written to the **outbox** (the `events_to_publish` table) in the same transaction as
the event itself, and leaves it once every subscriber has answered for it. The outbox poller is the
one that notices when that does not happen. Every 30 seconds it reads the outbox; for an event that
is still waiting, it has the aggregate's owner publish it again after 3 minutes and after 6; after
10 minutes it reports the event as undelivered in the log, and from then on tries again every 30
minutes.

Start exactly one per system, and start it after your command buses: its first look at the outbox
comes 10 seconds after it starts, and by then it needs the buses to pass its requests to. (On a
cluster there is one for the whole cluster; [chapter 17](17-cluster.md) shows how.) What it does
when things go wrong is the story of [chapter 9](09-projection-failures.md).

## Sending a command

Everything is in place. To send a command, use `ask` (chapter 1): it puts the command in the command
bus's mailbox and hands you a `Future` of the answer.

```scala
import com.example.shop.cart._
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.UserId
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._

implicit val timeout: Timeout = Timeout(70.seconds)

val alice = UserId(1)

val answer = (carts ? CreateShoppingCart(alice, "Groceries")).mapTo[CustomCommandResponse[_]]
val response = Await.result(answer, timeout.duration)
// response: SuccessResponse(Id(1001),Version(1))
```

`mapTo` tells the `Future` what kind of answer to expect: every command is answered with a
`CustomCommandResponse`. `Await.result` then blocks until the answer arrives. That is fine in a
small program's `main`, and nowhere else: never inside an actor, never in a web request. Real code
keeps the `Future` (more on that below).

Why 70 seconds? A command may take up to its context's `commandTimeout`, 60 seconds by default,
before the library itself answers with an error. Wait a little longer than that, and you always get
the library's answer instead of a timeout of your own. [docs/SCALING.md](../SCALING.md), section 2,
gives the same advice.

The answer to `CreateShoppingCart` is `SuccessResponse(aggregateId, aggregateVersion)`: the id the
new cart got, and its version, 1. The answer is the only place you learn the new cart's id, so keep
it. And the version is what your next `AddItem` expects:

```scala
response match {
  case SuccessResponse(cartId, version) =>
    val next = Await.result((carts ? AddItem(alice, cartId, version, "apples")).mapTo[CustomCommandResponse[_]],
      timeout.duration)
    // next: SuccessResponse(Id(1001),Version(2))
  case other =>
    println("No cart: " + other)
}
```

## What the library answers

Every command gets exactly one answer, always one of these (all in `io.reactivecqrs.api`):

| Answer | It means |
|---|---|
| `SuccessResponse(aggregateId, aggregateVersion)` | The events are stored; the aggregate is now at `aggregateVersion`. |
| `CustomSuccessResponse(aggregateId, aggregateVersion, info)` | The same, plus the `info` your handler returned with `CommandSuccess(event, info)`. |
| `FailureResponse(exceptions)` | Your handler said no (`CommandFailure`), with its reasons; or the library refused the command, as it does for a new aggregate whose first event names no space. |
| `AggregateConcurrentModificationError(aggregateId, aggregateType, expected, was)` | Your `expectedVersion` was not the aggregate's version: somebody changed it first, or it does not exist (`was` is then version 0). |
| `CommandRejected(aggregateId, reason)` | The command was not run at all, but sending it again later may work. |
| `CommandHandlingError(commandName, errorId, commandId)` | Something broke while running your handler: it threw, had no `case` for the command, or took longer than the command timeout; or the aggregate could not be read. |
| `EventHandlingError(eventName, errorId, commandId)` | Storing failed: an event handler threw, an event broke a limit, or the database failed. |

Only the two successes stored anything. For the two errors, the log has a line with the same
`errorId` and the full story. [Chapter 5](05-command-flavors.md) goes through every answer and what
to do about it, and [Appendix B](appendix-b-cheat-sheet.md) lists them all.

The `Future` itself can fail too, without any answer: with an `AskTimeoutException` when no answer
came within your timeout, or with a failure when you sent the bus something it does not understand
at all, which is not a command.

## Reading the cart back

To see a cart as it is now, ask its command bus with `GetAggregate`:

```scala
import scala.util.Try

// cartId: the id from the answer to CreateShoppingCart
val cart = Await.result((carts ? GetAggregate(cartId)).mapTo[Try[Aggregate[ShoppingCart]]], timeout.duration)
// cart: Success(Aggregate(Id(1001),Version(2),Some(ShoppingCart(Groceries,Vector(Item(1,apples))))))
```

The answer is a `Try`, inside a `Future` that succeeds: `Success` with the aggregate, or `Failure`
when the cart's events cannot be read (an `AggregateRestoreFailedException`). An `Aggregate` has
three fields:

| Field | Type | What it holds |
|---|---|---|
| `id` | `AggregateId` | which aggregate |
| `version` | `AggregateVersion` | how many events its history holds |
| `aggregateRoot` | `Option[ShoppingCart]` | the state; `None` when the aggregate does not exist (version 0) or was deleted |

The cart's owner answers from memory, and it is the same owner that stores the cart's events, so
the answer already includes your last command: no waiting, no "eventually". Use `GetAggregate`
when the write side needs a cart, for example to show it before changing it. For lists and searches
across many carts, use projections ([chapter 8](08-projections.md)). And for questions about the
past (an older version, the state at a moment, the list of events), see
[chapter 7](07-reading-the-past.md).

## The whole program

Here are all the steps in one place: E1's `FirstProgram`. It creates a cart, adds two items and
prints the cart. `main` opens what the program holds, the connection pool and the actor system, and
closes them at the end; `run` does the rest, and E1's test runs it against a test database of its
own. In your project, with the cart in a package of its own, add its import
(`import com.example.shop.cart._`).

<!-- snippet: e1/FirstProgram.scala#program -->
```scala
import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.UidGeneratorActor
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

object FirstProgram {

  def main(args: Array[String]): Unit = {
    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs",
      ConnectionPoolSettings(initialSize = 5, maxSize = 32, connectionTimeoutMillis = 3000L))
    val system = ActorSystem("shop")
    try run(system)
    finally {
      Await.result(system.terminate(), 30.seconds)
      ConnectionPool.closeAll()
    }
  }

  def run(system: ActorSystem): Unit = {
    // 1. The tables.
    val mpjsons = new MPJsons
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()

    // 2. The actors.
    val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
    val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
    eventBus ! EventsBusActor.RegisterSubscribers(Nil)
    val carts = system.actorOf(
      AggregateCommandBusActor(new ShoppingCartContext, uidGenerator, eventStore, responses, eventBus), "carts")
    system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus)), "outboxPoller")

    // 3. Talking to the carts.
    implicit val timeout: Timeout = Timeout(70.seconds)
    val alice = UserId(1)

    def send(command: AnyRef): SuccessResponse =
      Await.result((carts ? command).mapTo[CustomCommandResponse[_]], timeout.duration) match {
        case success: SuccessResponse => success
        case other => throw new IllegalStateException(command.getClass.getSimpleName + " failed: " + other)
      }

    val created = send(CreateShoppingCart(alice, "Groceries"))
    println("Created: " + created)

    val withApples = send(AddItem(alice, created.aggregateId, created.aggregateVersion, "apples"))
    val withOranges = send(AddItem(alice, created.aggregateId, withApples.aggregateVersion, "oranges"))
    println("Now at: " + withOranges.aggregateVersion)

    val cart = Await.result((carts ? GetAggregate(created.aggregateId)).mapTo[Try[Aggregate[ShoppingCart]]],
      timeout.duration).get
    println("The cart: " + cart)
  }
}
```

Notice how each `AddItem` carries the version from the answer before it. That is the optimistic
lock at work: each command says "I am changing the cart as it was after my last change".

## Running it

Start it with `sbt "examples/runMain io.reactivecqrs.examples.e1.FirstProgram"` (in your project,
`sbt "runMain ..."` with your object's full name). Among Pekko's start-up lines you will see the
event bus report `Event bus started with 0 subscriptions`, and then, on a fresh database:

```
Created: SuccessResponse(Id(1001),Version(1))
Now at: Version(3)
The cart: Aggregate(Id(1001),Version(3),Some(ShoppingCart(Groceries,Vector(Item(1,apples), Item(2,oranges)))))
```

Why 1001? The first 1 000 aggregate ids are kept for system ids, so the sequence starts at 1001.
Run the program again and the new cart gets 11001: every start of a command bus takes a fresh pool
of 10 000 ids, and what was left of the previous pool is skipped. Ids are unique, not consecutive;
never read meaning into them.

Now look at the diary itself. In `psql`:

```sql
SELECT e.aggregate_id, e.version, t.name AS event_class, e.event
  FROM events e JOIN types_names t ON t.id = e.event_type_id
 ORDER BY e.id;
```

```
 aggregate_id | version |                   event_class                   |        event
--------------+---------+-------------------------------------------------+----------------------
         1001 |       1 | io.reactivecqrs.examples.e1.ShoppingCartCreated | {"name":"Groceries"}
         1001 |       2 | io.reactivecqrs.examples.e1.ItemAdded           | {"name":"apples"}
         1001 |       3 | io.reactivecqrs.examples.e1.ItemAdded           | {"name":"oranges"}
```

Three lines, three facts. The cart itself, the `ShoppingCart` value you printed, is stored nowhere:
whenever an owner needs it, it replays these rows through your event handlers.

> **Under the hood:** here is the journey of `AddItem`. The command bus gives it a command id from
> its pool and passes it to the cart's owner (starting one, which replays the cart, if none is in
> memory). The owner checks `expectedVersion`, runs your command handler on the handler dispatcher,
> applies the new event to a copy of the state, and stores it in one transaction: a stored
> procedure checks the version once more and writes the `events` row, the `aggregates` row and the
> outbox row together. Then the owner publishes the event to the event bus and answers you. With no
> subscribers for carts, the bus acknowledges at once, and the outbox row is deleted a moment later.
> If the program stops before that, the row waits and is published again at the next start, which
> subscribers simply ignore as a repeat. [ARCHITECTURE.md, sections 3 and 5](../ARCHITECTURE.md)
> tell the full story.

## Try it: two ways to hear "no"

Make two small changes to the program and watch the answers.

1. **Send an old version.** Give the second `AddItem` the version of the first answer,
   `created.aggregateVersion`, instead of `withApples.aggregateVersion`. The cart is at version 2 by
   then, so the program stops with:

   ```
   AddItem failed: AggregateConcurrentModificationError(Id(...),AggregateType(io.reactivecqrs.examples.e1.ShoppingCart),Version(1),Version(2))
   ```

   Nothing was stored, and your handler did not even run.

2. **Break the house rule.** Add six items in a row, each with the version of the answer before it.
   The sixth comes back as `FailureResponse(List(Cannot have more than 5 items in your cart))`: your
   command handler said no.

## Real code composes futures

`Await` was a shortcut for a small program. In a real application, a command arrives in a web
request or a message handler, and blocking a thread per command would waste threads you need. Keep
the `Future`s (`scala.concurrent.Future`) and chain them instead, as E1's `ComposedCommands` does:

<!-- snippet: e1/ComposedCommands.scala#compose -->
```scala
import system.dispatcher   // an ExecutionContext for flatMap

def sendAsync(command: AnyRef): Future[SuccessResponse] =
  (carts ? command).mapTo[CustomCommandResponse[_]].flatMap {
    case success: SuccessResponse => Future.successful(success)
    case other => Future.failed(new IllegalStateException(command.getClass.getSimpleName + " failed: " + other))
  }

val groceries: Future[Aggregate[ShoppingCart]] = for {
  created <- sendAsync(CreateShoppingCart(alice, "Groceries"))
  apples  <- sendAsync(AddItem(alice, created.aggregateId, created.aggregateVersion, "apples"))
  _       <- sendAsync(AddItem(alice, created.aggregateId, apples.aggregateVersion, "oranges"))
  cart    <- (carts ? GetAggregate(created.aggregateId)).mapTo[Try[Aggregate[ShoppingCart]]].flatMap(Future.fromTry)
} yield cart
```

Nothing waits here: each step starts when the one before it has answered, and your web framework
turns the final `Future` into a response. A real application would also turn each kind of answer
into something better than an exception: a "please reload" for a concurrent modification, the
reasons of a `FailureResponse` for the user. [Chapter 5](05-command-flavors.md) shows how.

## When something is off

| You see | It means |
|---|---|
| `IllegalStateException: Connection pool is not yet initialized.(name:default)` | Library code ran before `ConnectionPool.singleton(...)`. Create the pool first. |
| `Event bus waits for RegisterSubscribers from the host; nothing is published until then`, every five seconds | You never sent `RegisterSubscribers` to the event bus. |
| `AggregateConcurrentModificationError(...)` | Your command carried an old `expectedVersion`. Read the aggregate again and decide again. |
| `CommandHandlingError(...)`, and `Error handling command ..., errorId: [...]` in the log | Your command handler threw, or has no `case` for that command. The log line with the same `errorId` has the stack trace. |

More symptoms and cures are in [Appendix C](appendix-c-troubleshooting.md).

> **Remember:** this chapter used a plain, local actor system, and everything in Part I works with
> one. In production, the library's design runs every deployment as a Pekko cluster, a cluster of a
> single node when there is only one machine, which behaves just like what you built here.
> [Chapter 17](17-cluster.md) shows how.

## From here on: the test domain's cart

The cart you built in chapter 3 is a slimmed-down copy of the test domain's cart
(`io.reactivecqrs.testdomain.shoppingcart`). From [chapter 5](05-command-flavors.md) on, the
examples use the test domain's cart itself, with its extra commands and events. Its
`CreateShoppingCart` has one more field in front, `idempotencyId`, so you will see it sent as
`CreateShoppingCart(None, alice, "Groceries")`; chapter 5 explains what that field is for.

## What you learned

- A running system needs, in this order: the connection pool and the tables, an `ActorSystem`, a
  uid generator (`UidGeneratorActor.postgres(system)`), an `EventsBusActor`, one
  `AggregateCommandBusActor` per aggregate type, and one `OutboxPollerActor`.
- The event bus publishes nothing until it gets `RegisterSubscribers`, even with an empty list.
- You send a command with `ask` and get exactly one answer, a `CustomCommandResponse`:
  `SuccessResponse(aggregateId, aggregateVersion)` when the events were stored, or one of the
  failures and errors.
- Each `Command` carries the version from the answer before it; that is the optimistic lock.
- `GetAggregate(id)` answers with a `Try[Aggregate[R]]`: the id, the version and the state, already
  including your last command.
- In real code, keep the `Future`s and compose them; `Await` belongs in small programs only.

Next: [chapter 5](05-command-flavors.md) serves the whole menu of command kinds.

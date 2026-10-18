# Chapter 5. Commands Come in Flavors

In chapter 4 you created a cart and put apples in it. Without noticing, you used two different kinds
of command. ReactiveCQRS has a few more, and each one answers a different question: "is this new?",
"what if someone changed it while I was looking?", "what if my request arrives twice?". By the end
of this chapter you will know every kind of command, which one to pick, what each answer from the
command bus means, and what to do when a command handler is slow. You will also meet the switch that
makes an aggregate take its commands one at a time, and a way to write rows of your own together
with the events.

---

## The menu

Think of a restaurant. "A table for two, please" asks for something new. "For table 12: the soup
instead of the salad, I'm looking at the order right now" changes an order you have seen. "Another
glass of water for table 12" changes it too, but nobody cares what else is on the bill. ReactiveCQRS
has a command kind for each of these:

| Kind | What it says | In the cart | If someone changed the aggregate first |
|---|---|---|---|
| `FirstCommand` | "make a new one" | `CreateShoppingCart` | (it is new) |
| `Command` | "change it; I last saw version 3" | `AddItem` | you are told, and nothing is stored |
| `ConcurrentCommand` | "change it, whatever its version is" | `AddItemConcurrently` | the library runs it again on the newer state |

Two more kinds correct the past instead of adding to it. They get a short section at the end of this
chapter, and chapter 15 to themselves.

Each kind is an abstract class in `io.reactivecqrs.api`, and every command case class extends exactly
one of them. The first type parameter is the aggregate's state class; the second names the kind of
response. The test domain always writes `CustomCommandResponse[_]` there, and so can you:

<!-- snippet: shoppingcart/Commands.scala#addItem -->
```scala
case class AddItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                   name: String) extends Command[ShoppingCart, CustomCommandResponse[_]]
```

From here on, the examples use the test domain's cart (`io.reactivecqrs.testdomain.shoppingcart`),
the bigger sibling of the one you wrote in chapter 3: the same state, plus the extra commands and
events that this chapter and the next are about. Its `CreateShoppingCart` has one more field,
explained below. Every block of this chapter that sends something runs in `CommandKindsSpec`
(`examples/src/test/scala/io/reactivecqrs/examples/cart/`), right before the lines that check its
answers; `sbt "examples/testOnly *CommandKindsSpec"` runs it. The examples import

<!-- snippet: cart/CommandKindsSpec.scala#imports -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._
```

and start from this setup, in a class that has the actor system (`system`) and the carts' command
bus of chapter 4 (`commandBus`):

<!-- snippet: cart/CommandKindsSpec.scala#setup -->
```scala
import system.dispatcher                             // runs the map and foreach callbacks below
implicit val timeout: Timeout = Timeout(70.seconds)  // longer than the command timeout; see "Slow handlers"

val alice = UserId(1)
val bob = UserId(2)
```

## FirstCommand: making something new

You met this one in chapters 3 and 4. The test domain's version has one more field:

<!-- snippet: shoppingcart/Commands.scala#create -->
```scala
case class CreateShoppingCart(idempotencyId: Option[SagaStep], userId: UserId, name: String)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]
```

(Ignore `idempotencyId` for now; it gets its own section below.)

A `FirstCommand` has no aggregate id, because the aggregate does not exist yet. The command bus takes
a new id from a pool of ids it fetches ahead, your handler runs on the context's
`initialAggregateRoot`, and the answer tells you which id the new cart got:

<!-- snippet: cart/CommandKindsSpec.scala#create -->
```scala
val answer = (commandBus ? CreateShoppingCart(None, alice, "Groceries")).mapTo[CustomCommandResponse[_]]
answer.foreach {
  case SuccessResponse(cartId, version) => println(s"New cart $cartId at $version")  // version: AggregateVersion(1)
  case other                            => println(s"No cart: $other")
}
```

The handler of a `FirstCommand` answers with a `FirstEvent`, the kind of event that starts a diary and
says which organization the new aggregate belongs to. Chapter 6 explains.

> **Tip:** the answer is the only place the new id appears. If an answer gets lost on the way (a
> network hiccup, a timeout) and the client sends the command again, you get two carts. "Choosing the
> id yourself", below, fixes that.

## Command: "I last saw version 3"

Alice and Bob both look at the Groceries cart at version 3. Alice removes the bread; the cart moves
to version 4. Bob, still looking at version 3, adds butter, because there is bread to put it on.
Should Bob's change go through? He decided based on something that is no longer true.

A `Command` carries `expectedVersion`: the version its sender based the decision on. Before the
cart's owner stores anything, it compares that version with the cart's:

- **the same:** your handler's events are stored as the next versions;
- **different:** nothing is stored, and you get `AggregateConcurrentModificationError`. Usually your
  handler does not even run.

<!-- snippet: cart/CommandKindsSpec.scala#addItem -->
```scala
def addItem(cartId: AggregateId, seen: AggregateVersion, item: String): Future[String] =
  (commandBus ? AddItem(bob, cartId, seen, item)).mapTo[CustomCommandResponse[_]].map {
    case SuccessResponse(_, version) =>
      s"Added $item; the cart is at version ${version.asInt} now"
    case AggregateConcurrentModificationError(_, _, expected, was) =>
      s"The cart changed while you looked (you saw version ${expected.asInt}, it is at ${was.asInt})"
    case FailureResponse(reasons) =>
      "Refused: " + reasons.mkString("; ")
    case other =>
      "Something went wrong: " + other
  }
```

The error carries four things: the aggregate's id, its type, the version you expected and the
version it was at. The usual reaction is to read the aggregate again (`GetAggregate`), show it, and
let the user (or your code) decide again.

A cart that does not exist gets the same error, with `was` equal to `AggregateVersion.ZERO`: version
0 is the version of nothing.

> **Remember:** take the version for your next `Command` from the latest answer.
> `SuccessResponse(cartId, AggregateVersion(4))` means "the cart is at version 4 now", so the next
> command expects version 4.

> **Under the hood:** the version is checked twice, by the owner in memory and again by the
> PostgreSQL function that stores the events, which also catches writers the owner cannot see. See
> [ARCHITECTURE.md, section 4.2](../ARCHITECTURE.md).

## ConcurrentCommand: "whatever the version is now"

A family shopping list on the fridge: you write "milk" at the bottom. You don't care what the others
added since you last looked; milk is needed either way.

<!-- snippet: shoppingcart/Commands.scala#concurrently -->
```scala
// No expected version: the framework uses the current one and retries on a concurrent change.
case class AddItemConcurrently(userId: UserId, aggregateId: AggregateId, name: String,
                               idempotencyId: Option[ClientKey] = None)
  extends ConcurrentCommand[ShoppingCart, CustomCommandResponse[_]] with IdempotentCommand[ClientKey]
```

A `ConcurrentCommand` has an aggregate id but no expected version. Your handler runs on the cart as
it is right now. If another command's events are stored while your handler runs, the library throws
your result away and runs your handler again, on the newer state. You get one answer: success, at
whatever version your events landed on.

<!-- snippet: cart/CommandKindsSpec.scala#concurrent -->
```scala
val milk = commandBus ? AddItemConcurrently(alice, cartId, "milk")
// answers SuccessResponse(cartId, AggregateVersion(n)), n being the version the milk landed on
```

How hard does it try?

- **Commands that reach the same owner** settle their clashes by queuing: the ones that clashed wait
  their turn and run again, one at a time, and waiting never counts as a failed attempt. Measured:
  10 000 commands sent at once to one aggregate all succeeded, the last one answered after 3.8 s (a
  single run on one development machine on 17/10/2026,
  [`docs/perf/load-M5.md`](../perf/load-M5.md)).
- **Clashes with writers the owner cannot see** (another node's owner of the same aggregate,
  chapter 17, or a write made outside the library) are retried: at most ten attempts in all, waiting
  10 ms before the second one and doubling up to 500 ms, with a random part so that rivals spread
  out. After the tenth, you get `AggregateConcurrentModificationError`.

> **Warning:** the handler of a `ConcurrentCommand` may run more than once for one command. Keep it
> free of side effects: no e-mails, no payments, no calls that change something elsewhere. That is
> good advice for every handler, since any handler's result can be thrown away (a clash, a timeout).
> Side effects belong in sagas and projections, which react to events once they are stored
> (chapters 8 and 12).

### Which one should I use?

- The decision depends on what the user saw ("butter, because there is bread", "pay the total I was
  shown"): use a `Command`.
- The change makes sense whatever else happened ("add milk", "count one more visit"): use a
  `ConcurrentCommand`.

The kind of the command decides how a clash is handled; there is no setting that changes it.

## Choosing the id yourself

Think of a paper order form with a printed number in the corner. If you post it twice by mistake,
the shop sees the same number twice and makes one order. A `FirstCommand` can carry such a number:
the id of the aggregate it creates.

1. Ask the command bus for a new id with `ReserveAggregateId`. The answer is an `AggregateId` from
   the same pool the bus uses itself.
2. Keep the id with the request (in the client, in your request log) before you send anything.
3. Send a `FirstCommand` that names the id in `chosenAggregateId`.
4. If you never got an answer, send the very same command again.

The test domain has such a command:

<!-- snippet: shoppingcart/Commands.scala#createWithId -->
```scala
/** Creates a cart under an id the caller reserved beforehand, so a retry cannot create a second cart. */
case class CreateShoppingCartWithId(idempotencyId: Option[ClientKey], userId: UserId, cartId: AggregateId, name: String)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]] with IdempotentCommand[ClientKey] {
  override def chosenAggregateId: Option[AggregateId] = Some(cartId)
}
```

<!-- snippet: cart/CommandKindsSpec.scala#reserve -->
```scala
val created: Future[CustomCommandResponse[_]] = for {
  cartId   <- (commandBus ? ReserveAggregateId).mapTo[AggregateId]
  response <- (commandBus ? CreateShoppingCartWithId(None, alice, cartId, "Groceries")).mapTo[CustomCommandResponse[_]]
} yield response   // SuccessResponse(cartId, AggregateVersion(1))
```

Sent a second time, the command cannot create a second cart. The cart exists, so the copy is
answered `AggregateConcurrentModificationError(cartId, AggregateType.of[ShoppingCart],
AggregateVersion.ZERO, AggregateVersion(1))`: "you expected nothing there, but it is at version 1".
With an idempotency key (next section) the copy gets the very same `SuccessResponse` instead, which
is friendlier.

### Which ids may I choose?

Take them from `ReserveAggregateId` and you never need to think about it. If you choose ids another
way, the command bus checks them before anything runs, and answers `FailureResponse` for an id that
is:

- not positive (zero or below; negative ids are kept for ephemeral, in-memory aggregates,
  [chapter 20](20-ephemeral-aggregates.md)),
- not below 2^53 (9 007 199 254 740 992), the largest whole number a JavaScript client reads exactly,
- in the range of another installation.

An *installation* is one deployment of your application with its own database: say, one for Europe
and one for Asia. Each sets `reactivecqrs.installation-number` (0 by default), and installation *n*
makes its ids from *n* × 10^12 up to where installation *n* + 1 starts, so the ids of two
installations never clash if their data ever meet. Every node of one installation sets the same
number:

```
# application.conf of installation 3: its ids start at 3 000 000 000 000
reactivecqrs.installation-number = 3
```

Ids below 10^12, the range where every id made before 1.0 lives, are allowed in every installation.

> **Under the hood:** the ranges live in `IdRange` (`core/.../uid/IdRange.scala`). See
> [ARCHITECTURE.md, section 13.5](../ARCHITECTURE.md).

## Idempotent commands: safe to send twice

You click "Pay" in an online shop and the page freezes. You click again. Did you pay twice? A good
shop gives both clicks the same order number, and the second one finds the order already placed.

A command that takes effect once per key, however often it is sent (and however many copies arrive
at the same moment), is called *idempotent*. You make one by mixing in `IdempotentCommand` and
giving it an `idempotencyId`:

```scala
trait IdempotentCommand[IID <: CommandIdempotencyId] {
  val idempotencyId: Option[IID]
}
```

The key is any class with an `asDbKey: String`. The test domain has one for clients:

<!-- snippet: shoppingcart/Commands.scala#clientKey -->
```scala
/** A key chosen by the caller, the way a client retrying a request would send it. */
case class ClientKey(value: String) extends CommandIdempotencyId {
  override def asDbKey: String = value
}
```

and the library has one for sagas: `SagaStep(sagaId, step)`, whose key is `"sagaId|step"`. Sagas
(chapter 12) give each step's command its step as the key, so a saga that resumes after a crash
cannot apply a step twice.

Here is a plum added twice with one key:

<!-- snippet: cart/CommandKindsSpec.scala#plum -->
```scala
val key = Some(ClientKey(java.util.UUID.randomUUID().toString))  // one key per user action
val addPlum = AddItemWithReceipt(key, alice, cartId, AggregateVersion(1), "plum")

val first  = commandBus ? addPlum  // CustomSuccessResponse(cartId, AggregateVersion(2), ItemReceipt("plum"))
val second = commandBus ? addPlum  // the very same answer; there is one plum in the cart
```

The first copy's response is stored in the same transaction as its events. Every later copy, and
every copy racing with it, is answered with that stored response, and its own events are never
stored. This holds after a restart too: the responses live in PostgreSQL. A key of `None` means "not
idempotent this time", like the `CreateShoppingCart(None, alice, "Groceries")` of the examples.

The rules:

- **Keys are global**, shared by every aggregate type. Use a fresh UUID per user action, never
  something like `"cart-1"`: another type's `"cart-1"` would get your cart's stored response. A key
  may have up to 256 characters.
- **Only commands that stored events keep a response.** A copy of a refused command
  (`FailureResponse`) or of one that produced no events simply runs again.
- **Responses are kept for 30 days** by default, and each command bus clears out older ones every
  hour. A copy that arrives later runs as a new command. Set another period when you create the
  response state:

  <!-- snippet: cart/CommandKindsSpec.scala#retention -->
  ```scala
  val responses = new PostgresCommandResponseState(mpjsons, typesNames, retention = 7.days).initSchema()
  ```

- **Custom info in a stored response** (see "Success with something extra" below) must be a class
  without type parameters, such as a case class like `ItemReceipt`.

> **Warning:** idempotency needs the response table and its unique index, which
> `PostgresCommandResponseState.initSchema()` creates. Forget `initSchema()` and every idempotent
> command fails.

> **Tip:** for creating things, combine both tricks: reserve the id, and give the creating command a
> key, as in `CreateShoppingCartWithId(Some(ClientKey(...)), alice, cartId, "Groceries")`. Two copies
> that race, even through two different nodes, both get `SuccessResponse(cartId, AggregateVersion(1))`.

## What a handler can answer

A command handler is a function from the current state to a result. Here are the results, from the
everyday ones to the more unusual.

### One event, or several

`CommandSuccess(ItemAdded(c.name))` stores one event. A handler may also return several, which get
consecutive versions:

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#addItems -->
```scala
case c: AddItems => CommandSuccess(c.names.map(ItemAdded))
```

`AddItems(alice, cartId, AggregateVersion(1), Seq("apple", "pear"))` is answered
`SuccessResponse(cartId, AggregateVersion(3))`. The events of one command are stored together or not
at all: if the second one cannot be stored, the first is not stored either.

### Success with something extra

Sometimes the caller needs more than the new version: a receipt, a computed number, a generated
code. Pass it as the second argument. The test domain's receipt is a case class of its own,
`ItemReceipt(itemName: String)`:

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#receipt -->
```scala
case c: AddItemWithReceipt => CommandSuccess(ItemAdded(c.name), ItemReceipt(c.name))
```

The answer is then `CustomSuccessResponse(cartId, AggregateVersion(2), ItemReceipt("plum"))` instead
of a plain `SuccessResponse`.

### No

A rule says no: return `CommandFailure` with a message. Nothing is stored, and the caller gets the
message back. Your cart from chapter 3 enforces its house rule this way:

<!-- snippet: e1/Carts.scala#addItemRule -->
```scala
case c: AddItem =>
  if (cart.items.size >= 5) CommandFailure("Cannot have more than 5 items in your cart")
  else CommandSuccess(ItemAdded(c.name))
```

The answer is `FailureResponse(List("Cannot have more than 5 items in your cart"))`. Use it for the
business rules; save exceptions for things that are really broken.

### Nothing to do

Some commands look and find nothing to change: a recurring check, or a request that is already
satisfied. Return a success without events:

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#checkCart -->
```scala
case _: CheckCart => CommandSuccess(Seq.empty[Event[ShoppingCart]])
```

The answer is `SuccessResponse(cartId, AggregateVersion(1))` with the cart's current version, and
nothing at all is written: no event, no new version, not even a database statement about the cart.
The test suite sends 10 000 of these and checks exactly that.

### Later: asynchronous handlers

A handler that has to wait for something, say a stock service, returns `AsyncCommandResult` around a
`Future` of one of the results above. The test domain's `AddItem` handler is written this way. Here
is a variation that asks a stock service of your own first:

<!-- snippet: cart/CommandKindsSpec.scala#stock -->
```scala
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

/** Your own service, not part of the library. */
trait Stock {
  def available(item: String): Future[Boolean]
}

class StockCheckingCarts(stock: Stock) extends ShoppingCartAggregateContext {
  // a bounded pool of your own for the follow-up work
  private implicit val stockPool: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))

  override def commandHandlers = cart => {
    case c: AddItem =>
      AsyncCommandResult(stock.available(c.name).map {
        case true  => CommandSuccess(ItemAdded(c.name))
        case false => CommandFailure(c.name + " is out of stock")
      })
    case other => super.commandHandlers(cart)(other)
  }
}
```

Your handler returns the moment it has the `Future`, which frees its thread; the owner picks up the
result when the future completes. Inside an `AggregateContext` you may even return the `Future`
itself: the context turns it into an `AsyncCommandResult` for you, which is what the test domain's
`createShoppingCart` relies on.

## Every answer at a glance

Whatever happens, a command gets exactly one answer:

| Answer | What happened | What you do |
|---|---|---|
| `SuccessResponse(id, version)` | Stored, or nothing to store. `version` is the aggregate's version now. | Carry on; use `version` for your next `Command`. |
| `CustomSuccessResponse(id, version, info)` | The same, with the `info` your handler returned. | Read `info`. |
| `FailureResponse(reasons)` | Your handler said no; or the bus refused the command before running it: a chosen id that is not allowed, a new aggregate whose first event names no space (chapter 6), or, in a cluster with sharded aggregates, a command too large to send between nodes (chapter 17). | Show the reasons. |
| `AggregateConcurrentModificationError(id, type, expected, was)` | The aggregate is not at the version you expected: someone changed it, it does not exist (`was` is 0), or a chosen id is already taken. A `ConcurrentCommand` gets it only after ten attempts. | Read the aggregate again and decide again. |
| `CommandRejected(id, reason)` | Not run: too many commands already wait for this aggregate (serial processing, below), or its events are of a class that not every node of a cluster can read yet (chapter 17). | Send it again a bit later. |
| `CommandHandlingError(command, errorId, commandId)` | Your handler threw an exception or had no case for the command; it took longer than the timeout; there was no aggregate to run on (deleted, or missing for a `ConcurrentCommand`); the aggregate could not be read from the store; or the bus had no ids. | Look up `errorId` in the log. |
| `EventHandlingError(event, errorId, commandId)` | Storing failed: an event handler threw, an event was too large or the aggregate has too many events (chapter 6), or the database failed. Nothing was stored. | Look up `errorId` in the log. |

A message that is not a command at all is not answered with any of these: the bus fails your ask's
`Future` with an `IllegalArgumentException`.

> **Tip:** show the `errorId` to whoever reports the problem. The library writes the same id to its
> log with the cause (for example "Error handling command AddItem, errorId: [...]"), so support can
> find it in seconds.

## Slow handlers

Most handlers take microseconds. Some must call a slow service or compute a lot. Two things matter
then: where the handler runs, and how long it may take.

### Where handlers run

Command handlers run on a thread pool of their own, `reactivecqrs.handler-dispatcher`, with 32
threads (the library's `reference.conf`). The buses, projections and sagas run elsewhere, so a slow
handler slows only commands. But a handler that *holds* its thread (a long computation,
`Thread.sleep`, a blocking HTTP or JDBC call) takes one of those 32, and when all 32 are taken, every
command on that node waits.

Measured on 17/10/2026 (a single run on one development machine with 12 CPU threads and PostgreSQL on
the same machine, [`docs/perf/heavy-M5.md`](../perf/heavy-M5.md)): 36 commands whose handlers each
held their thread for 5 seconds were all answered after 10.0 s, as 32 ran at a time, and meanwhile a
light command for another aggregate waited 5.4 s. With the same work on a pool of the application's
own, returned as `AsyncCommandResult`, the last answer came after 5.0 s and the light command took
3 ms.

You can size the pool in your `application.conf`:

```
reactivecqrs.handler-dispatcher.thread-pool-executor.fixed-pool-size = 64
```

More threads suit handlers that wait for I/O; for handlers that compute, about the number of CPU
cores is enough.

### The command timeout

Every context has a `commandTimeout`: 60 seconds unless you override it. It counts from the moment
the aggregate's owner takes the command. When it passes, the caller gets `CommandHandlingError`, and
the command is dead: if the handler finishes later, its result is thrown away and nothing is stored.
You can give a context more time:

<!-- snippet: cart/CommandKindsSpec.scala#patient -->
```scala
class PatientCarts extends ShoppingCartAggregateContext {
  override val commandTimeout: FiniteDuration = 2.minutes
}
```

> **Warning:** your own ask timeout is a separate clock. If it is shorter than the command timeout,
> your `Future` fails with an `AskTimeoutException` while the command carries on, and it may still be
> applied. Make the ask timeout longer than the command timeout (as in this chapter's setup), and
> give commands that clients retry an idempotency key.

### Long work, the comfortable way

For handlers that take minutes, the library's scaling notes recommend, best first
([`docs/SCALING.md`](../SCALING.md), section 8):

1. **Move the long work out of the command.** A command records the request (one event); a worker or
   a saga (chapter 12) does the work on its own threads, retried and resumable; a second command
   records the result. The aggregate stays available, and no timeout or clash ever touches the long
   work.
2. **If a command must do it:** return `AsyncCommandResult` with the work on a bounded pool of your
   own; raise `commandTimeout` above the longest run; use serial processing (next section) for that
   type, so the work is never run again after a clash; and give clients an idempotency key and a
   long enough timeout, or answer "accepted" at once and show the result later through a projection.

## One at a time: serial processing

By default an owner lets the commands of its aggregate run side by side, each on the state it found,
and settles clashes afterwards: with an error for a `Command`, with another run for a
`ConcurrentCommand`. For an aggregate that many people change at once, a single queue can be calmer,
like the one at a post office counter: everyone waits their turn, and each sees exactly what the one
before them left behind.

<!-- snippet: cart/CommandKindsSpec.scala#busy -->
```scala
class BusyCarts extends ShoppingCartAggregateContext {
  override val commandProcessing: CommandProcessing = CommandProcessing.Serial(maxQueuePerAggregate = 100)
}
```

With `Serial`:

- the commands of one aggregate run one at a time, in the order they arrived; each starts when the
  one before it was answered, so its handler sees the latest state;
- they cannot clash with each other, so a `ConcurrentCommand`'s handler runs once. (A `Command` whose
  expected version is out of date still gets `AggregateConcurrentModificationError`.)
- at most `maxQueuePerAggregate` commands wait behind the one that runs (1 000 if you write just
  `Serial()`); one more is answered at once with
  `CommandRejected(cartId, "100 commands of the aggregate wait already")`, and may be sent again
  later;
- a command's timeout counts from its arrival, time in the queue included. Ten commands of a minute
  each, in a row, need a timeout of over ten minutes for the last one.

The second parameter, `onOverflow`, says what to do with that one command too many; today it has a
single choice, `OverflowPolicy.Reject`.

Measured on 17/10/2026 (same machine, [`docs/perf/load-M5.md`](../perf/load-M5.md)): 10 000 commands
sent at once to one aggregate in serial mode gave 1 003 successes and 8 997 rejections, the last
answer after 0.6 s. In the default mode all 10 000 succeeded, but the last waited 3.8 s.

Pick `Serial` for aggregates that many clients write at the same time, for handlers with heavy work
that must not run twice, and when a quick "busy, try again" suits your clients better than a long
wait.

## Rows of your own, in the same transaction

Sometimes a command must leave something behind besides its events: a ticket to e-mail, a message
for another system, a job to run later. A projection ([chapter 8](08-projections.md)) writes such
things after the events are stored, so it may be a moment late. An **outbox writer** writes them in
the very transaction that stores the events, so both are stored or neither. A context mixes in
`WithOutboxWriter` and gives the writer. E6's concerts have one
(`class ConcertContext extends AggregateContext[Concert] with WithOutboxWriter[Concert]`); it writes
a ticket row for every booked seat:

<!-- snippet: e6/Concerts.scala#outboxWriter -->
```scala
// A ticket to email for each booking, in the transaction of its event: both are stored, or neither.
override def outboxWriter: OutboxWriter[Concert] = new OutboxWriter[Concert] {
  override def write(concertId: AggregateId, version: AggregateVersion, events: Seq[EventInfo[Concert]])(implicit session: DBSession): Unit =
    events.foreach(_.event match {
      case SeatBooked(customer) =>
        sql"INSERT INTO e6_tickets (concert_id, version, customer) VALUES (${concertId.asLong}, ${version.asInt}, $customer)".update()
      case _ => ()
    })
}
```

What the writer can count on:

- It gets the aggregate's id, its version after the events, the events with their versions, users
  and times (`EventInfo`), and the session of the transaction. Write with ScalikeJDBC in that
  session, never in a transaction of your own.
- If it throws, its rows and the events are rolled back together, and the caller gets
  `EventHandlingError`.
- A command that stores no events does not call it, and neither does a copy of an idempotent
  command that was applied already, so each row is written once.
- The rows are yours: your own code reads them and deletes them, say a job that sends the e-mails.
  When a history rewrite erases data ([chapter 15](15-rewriting-history.md)), the writer's `rewrite`
  method changes its rows too.

> **Tip:** E6 (`examples/src/main/scala/io/reactivecqrs/examples/e6/`) puts this chapter's tools
> together in one program: concerts whose seats are booked one at a time (`Serial`), scheduled with
> a key the client chooses (an idempotent command), each booking writing its ticket row. Run it with
> `sbt "examples/runMain io.reactivecqrs.examples.e6.CommandsSetup"`.

## Correcting the past

Sometimes the past itself has to change: a customer asks for their name to be removed from every
cart they ever made. For that there are two more kinds of command, `RewriteHistoryCommand` (with an
expected version) and `RewriteHistoryConcurrentCommand` (without). They name the event classes they
rewrite in `eventsTypes`; their handlers live in the context's `rewriteHistoryCommandHandlers`,
receive the stored events of those classes, and answer with `RewriteCommandSuccess`: the rewritten
events, plus new events that record the change. The cart's `RewriteCartName` renames the cart in its
`ShoppingCartCreated` event and adds a `CartNameRewritten`. It is a sharp tool, and
[chapter 15](15-rewriting-history.md) shows how to handle it safely.

## What you learned

- A `FirstCommand` creates an aggregate. The bus picks the id, unless `chosenAggregateId` names one
  you reserved with `ReserveAggregateId`, which makes a resent creation harmless.
- A `Command` carries `expectedVersion`; when the aggregate has moved on, nothing is stored and you
  get `AggregateConcurrentModificationError`.
- A `ConcurrentCommand` runs on the current version and is run again after a clash, so its handler
  must be free of side effects.
- `IdempotentCommand` with a key makes every copy of a command get the first copy's answer; keys are
  global and responses are kept for 30 days by default.
- Handlers answer with `CommandSuccess` (one event, several, none, or with extra info),
  `CommandFailure`, or `AsyncCommandResult`, and every command ends with exactly one answer.
- Slow handlers meet the 60-second command timeout and the 32-thread handler pool; long work belongs
  outside the command.
- `CommandProcessing.Serial` runs an aggregate's commands one at a time and answers `CommandRejected`
  when its queue is full.
- An outbox writer (`WithOutboxWriter`) writes rows of your own in the transaction of the events.

Next: [chapter 6](06-events.md) looks closely at what commands leave behind: events.

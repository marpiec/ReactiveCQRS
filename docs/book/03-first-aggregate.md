# Chapter 3. Your First Aggregate: a Shopping Cart

Time to write code that is about *your* business instead of plumbing. By the end of this chapter
you will have a complete shopping cart aggregate: the state it keeps, the events that change it, the
commands people send it, the rules that decide, and the context that ties it all together. It is
all plain Scala: no actors, no database, nothing to start. You can even try it out with a simple
fold, and we will, at the end. Chapter 4 then brings it to life.

---

## The plan, on a napkin

A cart has a name and a list of items. People can create a cart, add an item to it and remove an
item from it. One house rule: **no more than five items per cart.**

Before writing any code, list what people can ask for and what happens when the cart says yes:

| Somebody asks (a command) | If the cart agrees, this happened (an event) |
|---|---|
| `CreateShoppingCart("Groceries")` | `ShoppingCartCreated("Groceries")` |
| `AddItem(..., "apples")` | `ItemAdded("apples")` |
| `RemoveItem(..., 1)` | `ItemRemoved(1)` |

Notice the grammar. Commands are orders in the imperative ("create", "add"); events are facts in
the past tense ("created", "added"). That little habit keeps your head straight for the rest of the
book: a command can still be refused, an event already happened.

The cart of this chapter is E1, the book's first example, in
`examples/src/main/scala/io/reactivecqrs/examples/e1/Carts.scala`, compiled and tested with the
library. In your project, give it a package of yours, such as `com.example.shop.cart`, in the files
suggested in [chapter 2](02-setting-up.md). The test domain's cart, in
`testdomain/src/main/scala/io/reactivecqrs/testdomain/shoppingcart/`, can do more (undo, copies,
deletion), and you will meet those extras in [chapter 6](06-events.md).

## Step 1: the state

What does a cart need to remember? Its name and its items. That is the **state**, also called the
**aggregate root**: an ordinary case class.

<!-- snippet: e1/Carts.scala#state -->
```scala
case class ShoppingCart(name: String, items: Vector[Item])

case class Item(id: Int, name: String)
```

Two things are deliberately missing: the cart's id and its version. The library keeps those next to
your state, never inside it, and hands you all three together when you ask for a cart (an
`Aggregate(id, version, aggregateRoot)`, in chapter 4).

**Keep the state immutable.** Case classes, `Vector`, `List`, `Map`: yes. A `var` or a mutable
collection: never. The library hands the same state value to several threads at once (your command
handlers run in futures, and the event bus passes the state to projections), and immutable values
are the only ones that survive that without surprises.

> **Warning:** choose the package and the name of the state class with care. Its full name,
> `com.example.shop.cart.ShoppingCart`, *is* the name of the aggregate type in the database. Rename
> or move the class later and the library sees a brand-new aggregate type with no aggregates in
> it, while the old ones sit in the database under the old name.

## Step 2: the events

Each line in the cart's diary is an event: a small case class that extends `Event`, with the state
class as its type parameter. `Event`, and every other class of the library this chapter uses, comes
from two packages of the API, imported at the top of the file:

<!-- snippet: e1/Carts.scala#imports -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
```

The cart's three events:

<!-- snippet: e1/Carts.scala#events -->
```scala
case class ShoppingCartCreated(name: String) extends FirstEvent[ShoppingCart] {
  def spaceId: SpaceId = SpaceId(0)
}

case class ItemAdded(name: String) extends Event[ShoppingCart]

case class ItemRemoved(id: Int) extends Event[ShoppingCart]
```

The type parameter, `Event[ShoppingCart]`, is how the library knows which aggregate type an event
belongs to.

`ShoppingCartCreated` is special: it is the event that *starts* a cart, so it extends
`FirstEvent`. A first event has one duty: it must say which **space** the new aggregate belongs to.
A space is an organization, a tenant of your application; `SpaceId(0)` is the platform itself.
Until your application has organizations of its own, every first event says `SpaceId(0)`, as the
test domain's does. Spaces get a chapter of their own ([chapter 16](16-organizations.md)).

> **Remember:** every first event must give a space. A new aggregate whose first event gives none
> (`SpaceId.unknown`, or any other negative number) is refused: the command is answered with a
> `FailureResponse` saying the new aggregate needs a space, and nothing is stored.

A few rules make events pleasant to live with:

- **Past tense, always.** `ItemAdded`, not `AddItem`.
- **Facts, not instructions, and only the facts.** `ItemAdded("apples")` does not carry the new
  item's id: the state can work that out by itself (step 5 shows how). Keep events small.
- **Plain data.** Strings, numbers, `Option`s, collections and other small case classes. Events
  are stored as JSON by mpjsons, which reads your classes by their fields.
- **Names are forever.** An event is stored under its class name, and old events are read back
  with that name, years later. Never rename, move or reshape an event class that has been stored.
  When an event must change, add a new class next to it (`ItemAddedV2`) and keep the old one;
  [chapter 6](06-events.md) shows how.

> **Under the hood:** this is what the library stores for the second event of a cart: a row in the
> `events` table with the cart's id, version 2, the user, the time, the number of the class
> `com.example.shop.cart.ItemAdded` (from the `types_names` dictionary of chapter 2), and the JSON
> `{"name":"apples"}`. [ARCHITECTURE.md, section 2](../ARCHITECTURE.md) has the details.

## Step 3: the commands

Commands are case classes too. Each extends one of the library's command classes, which decides
how it is handled:

<!-- snippet: e1/Carts.scala#commands -->
```scala
case class CreateShoppingCart(userId: UserId, name: String)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]]

case class AddItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                   name: String) extends Command[ShoppingCart, CustomCommandResponse[_]]

case class RemoveItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                      id: Int) extends Command[ShoppingCart, CustomCommandResponse[_]]
```

- A **`FirstCommand`** creates a new aggregate. It has no `aggregateId`: the cart does not exist
  yet, and the library gives it an id.
- A **`Command`** changes an existing aggregate. It names the aggregate (`aggregateId`) and the
  version you looked at when you decided to send it (`expectedVersion`). This is the optimistic
  locking of [chapter 1](01-cqrs-without-the-headache.md): if the cart has moved on since, the
  command is refused and nothing changes.
- Every command carries a **`userId`**: who is asking. The library stores it with every event the
  command produces.

The first type parameter is the aggregate the command is for. The second is the kind of answer the
caller gets; `CustomCommandResponse[_]` means "any of the library's answers", and it is what every
command in the test domain uses. Use it too.

> **Tip:** the field names are not up to you. `userId`, `aggregateId` and `expectedVersion` are
> members the base classes declare, and your case class fields fill them in. Spell them exactly
> so; any other fields (`name`, `id`) are yours to name.

> **Remember:** in the test domain, `CreateShoppingCart` has one more field in front,
> `idempotencyId: Option[SagaStep]`, and also mixes in `IdempotentCommand`. That makes the command
> safe to send twice, and it is why the library's specs write `CreateShoppingCart(None, userId,
> "Groceries")`. We leave it out here to keep the first cart simple; idempotent commands are in
> [chapter 5](05-command-flavors.md), with the other kinds of command.

## Step 4: the rules (command handlers)

A **command handler** looks at the current state and a command, and decides. In ReactiveCQRS the
command handlers of an aggregate are one function: give it the current state, and it gives you
back a partial function from commands to results. It lives in the context class of step 6; here it
is on its own:

<!-- snippet: e1/Carts.scala#rules -->
```scala
override def commandHandlers = cart => {
  case c: CreateShoppingCart =>
    CommandSuccess(ShoppingCartCreated(c.name))
  case c: AddItem =>
    if (cart.items.size >= 5) CommandFailure("Cannot have more than 5 items in your cart")
    else CommandSuccess(ItemAdded(c.name))
  case c: RemoveItem =>
    if (cart.items.exists(_.id == c.id)) CommandSuccess(ItemRemoved(c.id))
    else CommandFailure("There is no item " + c.id + " in the cart")
}
```

Read it like a list of house rules. `cart` is the cart as it is right now; for a `FirstCommand`,
which has no cart yet, it is the blank starting state of step 6. Each `case` says what to do with
one kind of command. Every case ends in a **result**:

| You return | It means | The caller gets (chapter 4) |
|---|---|---|
| `CommandSuccess(event)`, or `CommandSuccess(Seq(event1, event2))` | Yes: store these events. | `SuccessResponse` |
| `CommandSuccess(event, info)` | Yes, and here is something for the caller. | `CustomSuccessResponse` carrying `info` |
| `CommandFailure("reason")`, or `CommandFailure(List("reason", "another"))` | No, and this is why. Nothing is stored. | `FailureResponse` |
| `AsyncCommandResult(future)` | I need to wait for something first; the answer is one of the above, later. | whatever the future brings |

You never check `expectedVersion` yourself. The library compares it with the cart's version
*before* your handler runs, and a command with an outdated version never reaches it.

Sometimes a rule needs to wait: for a price list, say, or a stock service. Then return an
`AsyncCommandResult` around a `Future` of the result. Here is a variant of `AddItem` with an
imaginary stock service of your own, whose `isAvailable` answers a `Future[Boolean]` (and with an
`ExecutionContext` of yours in scope):

<!-- snippet: e1/TryingItOutSpec.scala#asyncAddItem -->
```scala
case c: AddItem =>
  AsyncCommandResult(stock.isAvailable(c.name).map { available =>
    if (available) CommandSuccess(ItemAdded(c.name))
    else CommandFailure(c.name + " is out of stock")
  })
```

Inside an aggregate context you may even return the `Future` itself: the context turns a `Future`
of a result into an `AsyncCommandResult` for you. The test domain's `createShoppingCart` does
exactly that. [Chapter 5](05-command-flavors.md) has more on slow handlers.

What if no `case` matches the command? The caller gets a `CommandHandlingError`, and the log gets
the details. So: one `case` per command of the aggregate.

> **Warning:** do not send e-mails, charge cards or call other systems from a command handler.
> A handler may run more than once for one command (a concurrent command is run again after a
> clash, [chapter 5](05-command-flavors.md)), and its result may be thrown away (another command
> changed the cart meanwhile, or the handler took longer than the command timeout). Side effects
> belong *after* the events are stored: in projections and sagas, chapters 8 and 12.

> **Tip:** command handlers run on a thread pool of their own, `reactivecqrs.handler-dispatcher`
> (chapter 2), so a slow one never stalls the event bus, projections or sagas. It does hold one of
> that pool's 32 threads all along, though, and when every thread is held, every command waits. Put
> long waits on threads of your own with `AsyncCommandResult`. [docs/SCALING.md](../SCALING.md),
> section 8, shows what happens otherwise.

## Step 5: how events change the state (event handlers)

The command handler decided; the event is the decision written down. Now somebody has to apply
that event to the state. That is the job of the **event handlers**, again one function:

<!-- snippet: e1/Carts.scala#handlers -->
```scala
override def eventHandlers = (userId, timestamp, cart) => {
  case e: ShoppingCartCreated =>
    ShoppingCart(e.name, Vector.empty)
  case e: ItemAdded =>
    val nextId = cart.items.foldLeft(0)((maxId, item) => math.max(maxId, item.id)) + 1
    cart.copy(items = cart.items :+ Item(nextId, e.name))
  case e: ItemRemoved =>
    cart.copy(items = cart.items.filterNot(_.id == e.id))
}
```

It gets three things: `userId`, the user whose command stored the event; `timestamp`, the moment
it was stored (a `java.time.Instant`); and `cart`, the state before the event. It returns the state
after the event. That is all an event handler ever does.

Look at `ItemAdded`: the new item's id is worked out from the state (one more than the largest id so
far). Since the handler gets the same state every time it replays the same diary, it always comes
up with the same ids. That is the kind of thinking event handlers need, because they run again and
again:

1. **when a command succeeds**, to work out the new state before anything is stored;
2. **whenever the cart is read back from the database**: the library replays every one of the
   cart's events through these handlers (the diary of chapter 1, read from the top);
3. **when a projection is rebuilt** from the stored events ([chapter 10](10-rebuilding-projections.md)).

Hence the three golden rules of event handlers:

- **Never refuse.** The event already happened; the time for "no" was the command handler.
  Whatever the state, return a new state.
- **No side effects.** A handler that sends an e-mail sends it on every replay.
- **Same input, same output.** No `Instant.now()`, no random numbers, no reading from the database.
  Need the time? Use `timestamp`, which is the same on every replay.

An event handler that throws is a bad day. While a command runs, it makes the command fail with an
`EventHandlingError`, and nothing is stored. During a replay it is worse: the cart cannot be read
back at all until you fix the handler.

> **Under the hood:** returning `null` from an event handler marks the aggregate as deleted (the
> test domain's `ShoppingCartDeleted` does that), and undo and copy events need special care in
> handlers. Both belong to [chapter 6](06-events.md). How the library runs the handlers is in
> [ARCHITECTURE.md, section 2.6](../ARCHITECTURE.md).

## Step 6: the context that ties it together

The last piece is the **aggregate context**: one class per aggregate type that hands the library
everything from steps 1 to 5. Here is the whole cart:

<!-- snippet: e1/Carts.scala#context -->
```scala
class ShoppingCartContext extends AggregateContext[ShoppingCart] {

  override val version: Int = 1

  override def initialAggregateRoot: ShoppingCart = ShoppingCart("", Vector.empty)

  override def commandHandlers = cart => {
    case c: CreateShoppingCart =>
      CommandSuccess(ShoppingCartCreated(c.name))
    case c: AddItem =>
      if (cart.items.size >= 5) CommandFailure("Cannot have more than 5 items in your cart")
      else CommandSuccess(ItemAdded(c.name))
    case c: RemoveItem =>
      if (cart.items.exists(_.id == c.id)) CommandSuccess(ItemRemoved(c.id))
      else CommandFailure("There is no item " + c.id + " in the cart")
  }

  override def eventHandlers = (userId, timestamp, cart) => {
    case e: ShoppingCartCreated =>
      ShoppingCart(e.name, Vector.empty)
    case e: ItemAdded =>
      val nextId = cart.items.foldLeft(0)((maxId, item) => math.max(maxId, item.id)) + 1
      cart.copy(items = cart.items :+ Item(nextId, e.name))
    case e: ItemRemoved =>
      cart.copy(items = cart.items.filterNot(_.id == e.id))
  }
}
```

Four members, four jobs:

- **`initialAggregateRoot`** is the blank page before the first event: the state a `FirstCommand`'s
  handler sees, and the state the first event handler starts from. An empty cart with no name will
  do.
- **`version`** is the version of this context's *behaviour*, not of any cart. Start at 1. Raise it
  when you change what the event handlers make of the events: stored snapshots of the old
  behaviour are then ignored ([chapter 13](13-snapshots.md)), and managed projections that read
  this type's states are rebuilt ([chapter 10](10-rebuilding-projections.md)).
- **`commandHandlers`** and **`eventHandlers`** are steps 4 and 5.

The test domain keeps its handlers in two objects of plain functions, `CommandsHandlers` and
`EventsHandlers`, and calls them from the context; for a small aggregate, writing them inline as
above is just as good.

Everything else in `AggregateContext` has a sensible default, and later chapters switch it on when
you need it:

| Member | Default | Where you will meet it |
|---|---|---|
| `commandTimeout` | 60 seconds | [chapter 5](05-command-flavors.md) |
| `commandProcessing` | `CommandProcessing.Concurrent` (one at a time is `Serial`) | [chapter 5](05-command-flavors.md) |
| `snapshots` | `None` | [chapter 13](13-snapshots.md) |
| `squash` | `None` | [chapter 14](14-squash.md) |
| `rewriteHistoryCommandHandlers` | fails every history-rewrite command | [chapter 15](15-rewriting-history.md) |
| `placement` | `Placement.Local` | [chapter 17](17-cluster.md) |
| `writtenFrom` | empty | [chapter 17](17-cluster.md) |

> **Remember:** by default one aggregate holds at most 10 000 events (chapter 2). Keep aggregates
> small and focused, like a single cart rather than "all carts of a customer". Chapters 13 and 14
> show what to do when a history grows long anyway.

## Trying it out with a fold

Here is the best part of all this: the handlers are plain functions, so you can try them without
starting anything. Replaying a diary is a fold over the events:

<!-- snippet: e1/TryingItOutSpec.scala#fold -->
```scala
import java.time.Instant
import io.reactivecqrs.api.id.UserId

val context = new ShoppingCartContext
val alice = UserId(1)
val now = Instant.now()

val diary = List(ShoppingCartCreated("Groceries"), ItemAdded("apples"), ItemAdded("oranges"), ItemRemoved(1))

val cart = diary.foldLeft(context.initialAggregateRoot) { (state, event) =>
  context.eventHandlers(alice, now, state)(event)
}
// cart: ShoppingCart(Groceries,Vector(Item(2,oranges)))
```

That fold is, in miniature, what the library does whenever it reads a cart back from the database.
And asking for a decision is one function call:

<!-- snippet: e1/TryingItOutSpec.scala#decision -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId

val decision = context.commandHandlers(cart)(AddItem(alice, AggregateId(1001), AggregateVersion(4), "pears"))
// decision: CommandSuccess(List(ItemAdded(pears)),NoInfo())
```

The `NoInfo()` at the end is the "no extra information" of a plain `CommandSuccess`, the library's
own little case class `io.reactivecqrs.api.NoInfo`. A plain success is answered with
`SuccessResponse`; a command that answers more (chapter 5) puts its information there instead.

Notice that the version in the command did not matter here: checking it is the library's job, not
the handler's, and calling the handler directly skips that check. [Chapter 19](19-testing.md) turns
these one-liners into proper tests.

## Four rules of thumb

Everything in this chapter boils down to four habits:

1. **Events are facts.** Past tense, immutable, small, and never renamed once stored.
2. **Command handlers decide.** They look at the state, say yes with events or no with a reason,
   and keep their hands off the outside world.
3. **Event handlers obey.** They never refuse, never have side effects, and give the same state for
   the same events, every time.
4. **The state is rebuilt by replaying events.** The diary is the truth; the state is a picture of
   it that can be redrawn at any moment.

## What you learned

- An aggregate is four kinds of code: the **state** (an immutable case class), the **events**, the
  **commands** and the **context**.
- A `FirstEvent` starts an aggregate and must name its **space**; `SpaceId(0)` is the platform.
- A `FirstCommand` creates an aggregate; a `Command` changes one and carries `expectedVersion` for
  optimistic locking.
- **Command handlers** return `CommandSuccess` with events, `CommandFailure` with reasons, or an
  `AsyncCommandResult` when they must wait.
- **Event handlers** take `(userId, timestamp, state)` and an event, and return the new state; they
  must be pure, because they run on every replay.
- The **`AggregateContext`** ties it together with `initialAggregateRoot`, `version`,
  `commandHandlers` and `eventHandlers`.
- Class names of the state and of the events are stored in the database, so choose them carefully.

Next: [chapter 4](04-first-command.md) wires the cart into a running system and sends it its first
command.

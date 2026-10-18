# Chapter 6. Events: Your Single Source of Truth

Commands come and go; events stay. Every cart you will ever show, every report and every projection
is made from the events stored in PostgreSQL, so it pays to know them well. By the end of this
chapter you will know what your event handlers receive and the rules they must follow, the special
kinds of event the library understands (first, undo, duplication and permanent delete), how to
delete an aggregate gently or for good, how to record who acted and why, how to change an event
class once it is stored without breaking anything, and the limits that keep aggregates healthy.

The examples continue with the setup of chapter 5: `commandBus` is the carts' command bus, and
`alice` and `bob` are `UserId(1)` and `UserId(2)`. The blocks that send something run in
`EventsSpec`, next to chapter 5's `CommandKindsSpec`, which checks what each one answers.

---

## Event handlers, up close

In chapter 3 you wrote event handlers for three events. The test domain's cart, the bigger sibling of
yours, handles a few more. Here is its complete list, from `ShoppingCartAggregateContext`:

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#eventHandlers -->
```scala
override def eventHandlers = (userId, timestamp, shoppingCart) => {
  case e: ShoppingCartDuplicated => shoppingCart // the state that arrives is already the copy
  case e: ShoppingCartCreated => shoppingCartCreated(e)
  case e: ItemAdded => itemAdded(shoppingCart, e)
  case e: ItemRemoved => itemRemoved(shoppingCart, e)
  case e: ShoppingCartDeleted => null
  case e: CartNameRewritten => cartNameRewritten(shoppingCart, e) // shoppingCart.copy(name = e.name)
  case e: CartSquashed => ShoppingCart(e.name, e.items)
}
```

This chapter explains the deletion, the undo and the copy; the rewrite is chapter 15's, and the
squash event chapter 14's. First, the handler itself. It receives three things and gives back one:

- `userId`: who sent the command that stored the event (the command's `userId`);
- `timestamp`: when the event was stored. All events of one command share one instant, cut to
  microseconds, and it reads back exactly the same every time the event is replayed;
- the state *before* the event;
- and it returns the state *after* the event: a new value, built with `copy`, never a changed one.

The cart does not need the first two, but plenty of aggregates do. Picture a library loan, where the
book is due back two weeks after it was borrowed, and the borrower is whoever borrowed it:

<!-- snippet: cart/Loans.scala#loan -->
```scala
import java.time.Instant
import java.time.temporal.ChronoUnit
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{SpaceId, UserId}

case class Loan(title: String, borrower: UserId, due: Instant, returned: Boolean)

case class LoanStarted(title: String) extends FirstEvent[Loan] {
  def spaceId: SpaceId = SpaceId(0)
}
case class LoanExtended(days: Int) extends Event[Loan]
case class BookReturned() extends Event[Loan]
```

and in the loan's context, the event handlers:

<!-- snippet: cart/Loans.scala#loanHandlers -->
```scala
override def eventHandlers = (userId, timestamp, loan) => {
  case e: LoanStarted  => Loan(e.title, borrower = userId, due = timestamp.plus(14, ChronoUnit.DAYS), returned = false)
  case e: LoanExtended => loan.copy(due = loan.due.plus(e.days.toLong, ChronoUnit.DAYS))
  case _: BookReturned => loan.copy(returned = true)
}
```

Notice what is *not* there: `Instant.now()`. If the handler asked the clock, the due date would come
out different every time the loan is read back from the store. The `timestamp` parameter is the
moment the loan really started, today and in five years.

### Where your handlers run, and why they must never fail

The same handler code runs in more places than you might expect:

1. **Before the events are stored**, once per new event, on a copy of the state. If a handler throws,
   the command gets `EventHandlingError` and nothing is stored.
2. **Every time the aggregate is read from the store**: when its owner starts (on first use, after a
   restart, after it was put to sleep for being idle) and for every look into the past (chapter 7).
3. **When a projection is rebuilt** from the stored events (chapter 10).

So your event handlers must:

- **handle every event the aggregate ever stored**, old classes included, for as long as such events
  exist;
- **never throw.** An event is a fact; it already happened. Checks and refusals belong in the
  command handler, which can still say no;
- **be pure**: the same event on the same state always gives the same result. No clock, no random
  numbers, no database or service calls;
- **return immutable values** (case classes, `Vector`, `Map`). The state is handed to command
  handlers and subscribers on other threads.

Here is the most tempting mistake:

```scala
case e: ItemAdded =>
  // Don't! Checks belong in the command handler. If the limit changes one day, the carts stored
  // before the change still have to replay.
  if (shoppingCart.items.size > 5) throw new IllegalStateException("Cart full")
  itemAdded(shoppingCart, e)
```

> **Warning:** a handler that throws on a stored event, or has no case for it, makes that aggregate
> unreadable. Its owner answers commands with `CommandHandlingError` and queries with
> `Failure(AggregateRestoreFailedException)` until the code is fixed; it tries again with a later
> request. One forgotten `case` can lock every cart that holds that event.

## FirstEvent: every diary has a first page

The first event of a new aggregate must be a `FirstEvent`. Besides being page one of the diary, it
says which *space* the aggregate belongs to: the organization (the tenant, the customer company) it
lives in. The cart lives in the platform itself, space 0:

<!-- snippet: shoppingcart/Events.scala#created -->
```scala
case class ShoppingCartCreated(name: String) extends FirstEvent[ShoppingCart] {
  def spaceId: SpaceId = SpaceId(0)
}
```

In an application with many organizations the space usually comes from the command, and a case
class field can provide it directly (an example, not in the test domain):

<!-- snippet: cart/EventsSpec.scala#cartOpened -->
```scala
case class CartOpened(spaceId: SpaceId, name: String) extends FirstEvent[ShoppingCart]
```

Two things go wrong if you skip this:

- A first event with a negative space (such as `SpaceId.unknown`, which is -1) is refused: the
  command gets a `FailureResponse` saying that a new aggregate needs a space, and nothing is stored.
  Use `SpaceId(0)` when the aggregate belongs to the platform rather than to an organization.
- A plain `Event` as the first event of a new aggregate fails when it is stored, with
  `EventHandlingError`: the store does not know where the aggregate belongs.

Spaces have a whole chapter of their own: [chapter 16](16-organizations.md).

## Deleting, the gentle way

The cart can be deleted. Its command handler stores `ShoppingCartDeleted()`, and the event handler
returns `null`:

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#deletedHandler -->
```scala
case e: ShoppingCartDeleted => null
```

A `null` state means "deleted". From then on:

- `GetAggregate(cartId)` answers with an `Aggregate` whose `aggregateRoot` is `None`;
- further commands for the cart are refused with `CommandHandlingError` (the log says the aggregate
  "is already deleted");
- nothing is erased. The whole diary is still there, projections see `ShoppingCartDeleted` and remove
  the cart from their lists, and the reads of chapter 7 still show the cart as it was before.

This is a *soft* delete, and it is the right choice most of the time: you keep the history, and
nobody can change the cart any more.

## Undo: Ctrl+Z for an aggregate

Here is a little drama from the test suite. Alice creates the Groceries cart and adds apples; Bob
adds oranges, then removes item 1, which unfortunately is the apples:

```
version 1  ShoppingCartCreated(Groceries)   Alice
version 2  ItemAdded(apples)                Alice
version 3  ItemAdded(oranges)               Bob
version 4  ItemRemoved(1)                   Bob, by mistake
```

Alice presses the cart's undo button. The cart has a command for it,

<!-- snippet: shoppingcart/Commands.scala#undo -->
```scala
case class UndoShoppingCartChange(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                                  stepsToUndo: Int) extends Command[ShoppingCart, CustomCommandResponse[_]]
```

an undo event,

<!-- snippet: shoppingcart/Events.scala#undone -->
```scala
case class ShoppingCartChangesUndone(eventsCount: Int) extends UndoEvent[ShoppingCart]
```

and a case in its command handlers that turns the one into the other:

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#undoHandler -->
```scala
case c: UndoShoppingCartChange => CommandSuccess(ShoppingCartChangesUndone(c.stepsToUndo))
```

An `UndoEvent` has an `eventsCount`: how many of the newest events it cancels.

<!-- snippet: cart/EventsSpec.scala#undo -->
```scala
val undone = commandBus ? UndoShoppingCartChange(alice, cartId, AggregateVersion(4), stepsToUndo = 1)
// SuccessResponse(cartId, AggregateVersion(5)); the cart holds apples and oranges again
```

What to know about undo:

- **An undo is an event too.** The version moves forward to 5, never back. The removal stays in the
  diary, marked as cancelled from version 5 on, so a read of version 4 (chapter 7) still shows the
  cart without apples, because that is how it was then.
- **It cancels the newest events that are still in effect.** Undo once more and it steps further
  back: it would cancel version 3, the oranges. Undos never cancel each other, and there is no redo.
- **Your event handlers need no case for the undo event.** The library does the undoing: it never
  hands an undo event to your handlers, and after the commit it works out the real state itself, by
  reading the aggregate back from the store and skipping the cancelled events. (Before 1.0 a missing
  case failed the command with `EventHandlingError`, issue F148; a case left in from then does no
  harm, it is simply never called.)
- **An undo cannot reach below a squash point** (chapter 14); such a command gets
  `EventHandlingError`.
- **Projections see the undo event like any other.** Listeners that build from events must undo
  their own work; listeners that receive the aggregate's state get the corrected state with it
  (chapter 8).

> **Under the hood:** the cancelled events are recorded in the `noop_events` table, effective from
> the undo's version, and the aggregate's snapshot is thrown away. See
> [ARCHITECTURE.md, sections 2.6 and 4.2](../ARCHITECTURE.md).

## Duplication: a new aggregate that starts as a copy

"Save as..." in your word processor: last week's shopping list becomes the start of this week's.
The cart can do that, with a command

<!-- snippet: shoppingcart/Commands.scala#duplicate -->
```scala
case class DuplicateShoppingCart(userId: UserId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]]
```

and a duplication event:

<!-- snippet: shoppingcart/Events.scala#duplicated -->
```scala
case class ShoppingCartDuplicated(spaceId: SpaceId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion)
  extends DuplicationEvent[ShoppingCart]
```

Its command handler stores the event,

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#duplicateHandler -->
```scala
case c: DuplicateShoppingCart =>
  CommandSuccess(ShoppingCartDuplicated(SpaceId(0), c.baseAggregateId, c.baseAggregateVersion))
```

and its event handler gives back the state it receives, unchanged:

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#duplicatedHandler -->
```scala
case e: ShoppingCartDuplicated => shoppingCart // the state that arrives is already the copy
```

Continuing the story, Alice copies the Groceries cart as it was at version 3:

<!-- snippet: cart/EventsSpec.scala#duplicate -->
```scala
val copied = commandBus ? DuplicateShoppingCart(alice, cartId, AggregateVersion(3))
// SuccessResponse(copyId, AggregateVersion(1)): a new cart with apples and oranges
```

The copy is a cart of its own, at version 1, and lives its own life: add tomatoes to it and they
become its item 3, while the Groceries cart never sees them.

What to know about duplication:

- **A `DuplicationEvent` is a `FirstEvent`**, so it names the copy's space, which may differ from the
  base's.
- **The event handler returns the state unchanged.** When the library reads a copy, it first applies
  the base's events up to the copied version, so the state your handler receives is already the copy.
- **The copy shares the base's history up to that version; it does not copy it.** Whatever happens
  to the base afterwards (new events, undos) does not touch the copy. A rewrite of the base's history
  below that version does, as they share it (chapter 15); and a permanent delete of the base first
  gives the copy a first event of its own (next section).
- **A copy cannot start below the base's squash point** (chapter 14), **nor above the base's version**:
  a copy "at version 10" of a cart at version 5 is refused with `EventHandlingError`. Take the version
  from a `GetAggregate` answer.

## Permanent delete: when the data must really go

A soft delete keeps everything, which is usually what you want. Sometimes, though, the data must
really be gone. A `PermanentDeleteEvent` does that, in two steps:

1. The event is stored and delivered like any other, and the command is answered `SuccessResponse`
   at once.
2. Once every subscriber has acknowledged that event, so each projection had its chance to remove its
   own copy of the data, the library deletes the aggregate for good: its events and its row, its
   waiting outbox rows, its snapshot, its archived events (chapter 14) and the projections' progress
   records for it.

The cart in the test domain has no permanent delete. A context of your own adds one in a few lines;
this chapter's spec runs this one:

<!-- snippet: cart/EventsSpec.scala#erase -->
```scala
case class ShoppingCartErased() extends PermanentDeleteEvent[ShoppingCart]

case class EraseShoppingCart(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion)
  extends Command[ShoppingCart, CustomCommandResponse[_]]

class ErasableCarts extends ShoppingCartAggregateContext {
  override def commandHandlers = cart => {
    case _: EraseShoppingCart => CommandSuccess(ShoppingCartErased())
    case other => super.commandHandlers(cart)(other)
  }

  override def eventHandlers = (userId, timestamp, cart) => {
    case _: ShoppingCartErased => cart
    case other => super.eventHandlers(userId, timestamp, cart)(other)
  }
}
```

Afterwards the cart reads like one that never existed: `GetAggregate` answers with version 0 and no
state, and `GetEventsForAggregate` (chapter 7) with an empty list.

What to know about permanent delete:

- **It waits while a projection has a recorded failure for the aggregate** (chapter 9). Once the
  failure is cleared, the next delivery of the event carries out the delete.
- **Copies are looked after.** If other aggregates were duplicated from it, each one first gets a
  first event of its own, made with the type's squash event (chapter 14), so the copies keep their
  state. A type without a squash policy cannot do that: the delete then waits, and the log says why.
  The cart has a squash policy.
- **Your projections' tables are yours.** The library does not touch them: delete their rows when
  your projections see the event. The stored answers of the aggregate's idempotent commands lose
  their custom info with it, but keep their keys until their retention period ends (30 days by
  default): a client that retries a keyed creation gets the old answer, and does not create the
  aggregate again. Answers stored by older versions of the library, which do not name their
  aggregate, are left as they are and expire likewise.

> **Tip:** plan ahead if you need both kinds of delete. A command cannot run on an aggregate whose
> state is `null`, so a cart soft-deleted the cart's way can no longer be erased by a command. If you
> may need to erase soft-deleted aggregates later, keep the soft delete as a flag in the state
> (`deleted = true`) and refuse other commands in your command handlers yourself.

## Who did it, and why: the metadata envelope

A parcel and its shipping label: the parcel is what was sent, the label says who sent it, for whom
and under which order number. Events have a label too.

Every event is stored with the command's `userId`. Often you need more: an assistant (a person, an
AI agent, an integration) acted for Bob; the request came in through the public API; it belongs to
support case 12. A command that mixes in `WithMetadata` gives every event it stores an
`EventMetadata` envelope:

<!-- snippet: shoppingcart/Commands.scala#onBehalf -->
```scala
/** Adds an item for someone else: its event carries a metadata envelope. */
case class AddItemOnBehalf(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, name: String,
                           metadata: EventMetadata)
  extends Command[ShoppingCart, CustomCommandResponse[_]] with WithMetadata
```

| Field of `EventMetadata` | What it holds |
|---|---|
| `actor` | who acted, written as your application likes it: `"user:5"`, `"agent:planner"` |
| `correlationId` | ties together everything done for one request or case |
| `causationId` | what caused this: a message, another event |
| `traceContext` | the W3C trace context, for distributed tracing |
| `spaceId` | the organization |
| `custom` | a `Map[String, String]` of your own |

Only `actor` is required:

<!-- snippet: cart/EventsSpec.scala#onBehalf -->
```scala
val onBehalf = EventMetadata("agent:planner", correlationId = Some("case 12"), custom = Map("channel" -> "api"))

val added = commandBus ? AddItemOnBehalf(bob, cartId, AggregateVersion(1), "apple", onBehalf)
// SuccessResponse(cartId, AggregateVersion(2)); the ItemAdded event carries the envelope
```

Where does the label show up?

- **Not in event handlers.** They keep their `(userId, timestamp, state)`, so the state never depends
  on the label.
- **In projections**, with every event, as `EventInfo.metadata`, whether the event arrives live or is
  delivered again later (chapter 8).
- **In the history reads** of chapter 7.

An event stored without an envelope (by a command without `WithMetadata`) reads as
`EventMetadata.ofUser(userId)`, an envelope whose actor is `"user:"` followed by the user's id.

## Changing an event once it is stored

A bound diary: you cannot reprint page 12, but you can write on a new page that from now on, things
are written differently.

Events are stored as JSON under the full name of their class. Change the class (rename a field, add
or remove one, change a type, rename the class or move it to another package) and the stored JSON no
longer fits it. Depending on the change, the old events fail to read, and with them every aggregate
that holds one; or a new field quietly comes back empty (zero, `null` or `None`, never the default
value written in the class).

So the rule is short: **never change the shape of a stored event class.** Add a new class and keep
the old one. Say cart items get a quantity one day:

```scala
@deprecated("Written before items had quantities; use ItemAddedV2", "2.0")
case class ItemAdded(name: String) extends Event[ShoppingCart]

case class ItemAddedV2(name: String, quantity: Int) extends Event[ShoppingCart]
```

(The test domain's cart has no quantities; this is how it would get them.) Then:

1. Command handlers write only `ItemAddedV2` from now on.
2. Event handlers handle both, for as long as stored `ItemAdded` events exist:

   ```scala
   case e: ItemAdded   => addItem(shoppingCart, e.name, quantity = 1)
   case e: ItemAddedV2 => addItem(shoppingCart, e.name, e.quantity)
   ```

3. Projections handle both too (chapter 8).

> **Warning:** in a cluster upgraded one node at a time, a node still running the old version cannot
> read the new class. Do not store it until every node can: the context's `writtenFrom` is made for
> exactly that ([chapter 17](17-cluster.md)).

### Check before you upgrade

`StoredDataCheck` reads every stored JSON value with the classes of the version you are about to
deploy, and reports, per table and class, how many rows it read and which could not be read. Run it
from your application (which has all your classes), connected to a copy of production:

<!-- snippet: cart/EventsSpec.scala#check -->
```scala
import io.reactivecqrs.core.tools.StoredDataCheck

val report = StoredDataCheck.run(mpjsons, typesNames)
println(report.text)
if (report.unreadable > 0) println("Not yet: some stored data cannot be read by this version")
```

It reads the events, the archived events, the saga orders and the stored command responses, plus
any projection tables you list (`StoredDataCheck.ProjectionTable[YourDocument]("name")`). It only
reads. A table your database does not have, such as the saga table of an application without sagas,
is not read, and the report names it. Its report also counts aggregates without a space, and events by their old type version (see
[ARCHITECTURE.md, section 2.3](../ARCHITECTURE.md)).

### When can the old class go?

<!-- snippet: cart/EventsSpec.scala#usersOf -->
```scala
val users = StoredDataCheck.usersOf(classOf[ItemAdded].getName)
println(s"${users.events} stored and ${users.archived} archived ItemAdded events")
users.aggregates.foreach(u => println(s"aggregate ${u.aggregateId}: ${u.events} stored, ${u.archived} archived"))
```

When both counts are zero, nothing uses the class any more, and it can leave your code. `usersOf`
reads both tables whole, as there is no index by class, so run it on a copy or while the database is
quiet. Old events do not disappear by themselves; a squash (chapter 14) is one way they go.

## Limits that keep aggregates healthy

Two limits guard against runaway aggregates. Both are constructor parameters of
`PostgresEventStoreState`, so they apply to every aggregate type that uses that store:

| Parameter | Default | Beyond it |
|---|---|---|
| `aggregateVersionLimit` | 10 000 events per aggregate | The command gets `EventHandlingError`; the log names `TooManyEventsException`. |
| `eventSizeLimit` | 100 000 characters of JSON per event | The command gets `EventHandlingError`; the log names `EventTooLargeException`. |
| `eventSizeLimitPerClassName` | none | A limit per event class, in place of `eventSizeLimit`, higher or lower. |

As the events of one command are stored together or not at all, one event too many or too large
fails the whole command. Setting them looks like this:

<!-- snippet: cart/EventsSpec.scala#limits -->
```scala
val eventStore = new PostgresEventStoreState(mpjsons, typesNames,
  aggregateVersionLimit = 20000,                                       // carts with very long lives
  eventSizeLimitPerClassName = Map(classOf[ItemAdded].getName -> 1000)  // an item name never needs more
).initSchema()
```

Before you raise them, consider why they are there. Every event of an aggregate is read when its
owner starts, unless it has a snapshot: measured on 17/10/2026, reading 50 000 events took 392 ms and
reading from a snapshot 12 ms (a single run on one development machine,
[`docs/perf/load-M5.md`](../perf/load-M5.md)). Large events cost memory and disk in proportion.

> **Tip:** when an aggregate approaches the limit, ask whether it should be split by a natural
> boundary (one per month, one per document version), turn on snapshots ([chapter 13](13-snapshots.md)),
> or fold old history away with squash ([chapter 14](14-squash.md)). For large documents, store the
> file elsewhere and put only a reference in the event ([`docs/SCALING.md`](../SCALING.md),
> section 7).

## What you learned

- Event handlers receive the user, the time the event was stored and the state before it, and return
  the new state; they are pure, handle every stored event and never throw.
- A new aggregate starts with a `FirstEvent` that names its space; `SpaceId(0)` is the platform.
- Returning `null` deletes an aggregate gently, keeping its history; a `PermanentDeleteEvent` removes
  it for good once every subscriber has seen it.
- An `UndoEvent` cancels the newest events still in effect, and needs no case in your handlers; a
  `DuplicationEvent` starts a new aggregate as a copy of another at a version, and its case returns
  the state it gets, which is already the copy.
- A command that mixes in `WithMetadata` labels its events with an `EventMetadata` envelope that
  projections and history reads see.
- A stored event class never changes shape: add a `V2` class, handle both, and check stored data
  with `StoredDataCheck` before upgrading.
- An aggregate holds at most 10 000 events, of at most 100 000 characters each, unless you set the
  event store's limits otherwise.

Next: [chapter 7](07-reading-the-past.md) opens the diary and reads the past.

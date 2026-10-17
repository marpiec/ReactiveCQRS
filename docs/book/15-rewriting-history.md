# Chapter 15. Rewriting History, Carefully

Chapter 1 made a promise: events are facts, and a removal is a new event, not an eraser. This
chapter is about the one tool that breaks that promise. By the end of it you will be able to change
events that are already stored (fix a name everywhere it was ever written, or wipe a customer's
personal data), make your read models hear about the change, and you will know exactly which places
a rewrite does not reach yet. Handle it like a sharp knife: rarely, deliberately, and with both eyes
open.

---

## Correction fluid, used sparingly

Back to the family diary. On its first page someone wrote the cart's name as "Grocceries". Normally
you add a line at the bottom: "renamed to Groceries". The old line stays, and that is fine: it is
what happened, and the diary is honest about it.

But sometimes the old words must not stay anywhere. A customer asks to be forgotten, and their name
is in the diary. Privacy laws can require exactly that: the EU's General Data Protection Regulation,
for example, gives people a right to have their personal data erased
([Regulation (EU) 2016/679, Article 17](https://eur-lex.europa.eu/eli/reg/2016/679/oj)). Then you
reach for the correction fluid: you change the first page itself, and you add a new line at the
bottom saying "page 1 was corrected", so that everyone who reads the diary from where they stopped
hears about it.

That is a **history rewrite**: a command that changes events already stored, together with new
events that tell everybody else.

> **Remember:** a new event is almost always the right answer. Rewrite history only when the old
> value must not survive. What the law asks of your system is a question for your legal advisers;
> this chapter shows what the library can do about it.

## The rewrite command

Like ordinary commands ([chapter 5](05-command-flavors.md)), rewrite commands come in two flavors:

- **`RewriteHistoryCommand`** names the version it expects (`expectedVersion`). If the aggregate has
  moved on, it is answered `AggregateConcurrentModificationError`, like an ordinary `Command`.
- **`RewriteHistoryConcurrentCommand`** has no expected version. It runs on whatever version the
  aggregate has, and after a clash the library runs it again, like a `ConcurrentCommand`.

Both have one more member, `eventsTypes`: the event classes you want to change. The test domain has
one of each:

```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}

case class RewriteCartName(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                           name: String) extends RewriteHistoryCommand[ShoppingCart, CustomCommandResponse[_]] {
  override def eventsTypes = Set(classOf[ShoppingCartCreated])
}

/** Renames the cart throughout its history, on whatever version it has (like an anonymization). */
case class RewriteCartNameConcurrently(userId: UserId, aggregateId: AggregateId, name: String)
  extends RewriteHistoryConcurrentCommand[ShoppingCart, CustomCommandResponse[_]] {
  override def eventsTypes = Set(classOf[ShoppingCartCreated])
}
```

The cart's name is written only in its creation event, so that is the only class they list.

## The rewrite handler

Rewrite commands do not go to `commandHandlers`. They have a handler of their own in the context,
`rewriteHistoryCommandHandlers`, which receives two things: the events to rewrite and the current
state.

```scala
override def rewriteHistoryCommandHandlers = (events, shoppingCart) => {
  case c: RewriteCartName => rewriteCartName(c.name, events)
  case c: RewriteCartNameConcurrently => rewriteCartName(c.name, events)
}
```

`events` is an `Iterable[EventWithVersion[ShoppingCart]]`: the aggregate's own events of the
classes in `eventsTypes`, oldest first, each with its version. The test domain's handler:

```scala
def rewriteCartName(name: String, events: Iterable[EventWithVersion[ShoppingCart]]) = {
  val rewritten: Iterable[EventWithVersion[ShoppingCart]] = events.map(ev => ev.event match {
    case e: ShoppingCartCreated => EventWithVersion(ev.version, e.copy(name = name))
    case _ => ev
  })
  RewriteCommandSuccess(rewritten.toSeq, CartNameRewritten(name))
}
```

It answers with a `RewriteCommandSuccess`, which carries two lists:

1. the **rewritten events**, each with the version of the stored event it replaces;
2. the **new events** to add at the end, here a single `CartNameRewritten(name)`.

The full form is `RewriteCommandSuccess(eventsRewritten, events, responseInfo)`; shorter forms take
one new event or a sequence of them, without the response info. Like any handler, a rewrite handler
may also say no with `CommandFailure`, or take its time with `AsyncCommandResult`
([chapter 5](05-command-flavors.md)).

> **Tip:** a context that never overrides `rewriteHistoryCommandHandlers` answers a rewrite command
> with `CommandHandlingError`, and the log asks you to implement the handler. If you see that in
> production, someone sent a rewrite to the wrong aggregate type.

## Sending it

```scala
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._

implicit val timeout: Timeout = Timeout(10.seconds)

val answer: Future[CustomCommandResponse[_]] =
  (carts ? RewriteCartName(alice, cartId, AggregateVersion(3), "Groceries")).mapTo[CustomCommandResponse[_]]
// SuccessResponse(cartId, AggregateVersion(4))
```

Our cart was created with the typo and then got apples and oranges. Here is its diary before and
after:

```
version  before                                  after
1        ShoppingCartCreated("Grocceries")       ShoppingCartCreated("Groceries")    rewritten in place
2        ItemAdded("apples")                     ItemAdded("apples")
3        ItemAdded("oranges")                    ItemAdded("oranges")
4                                                CartNameRewritten("Groceries")      new, and published
```

The version moved from 3 to 4 because of the one new event; the rewritten event kept its version,
version 1.

What the library did, in order:

1. The cart's owner read the cart's own events of the listed classes and called your handler with
   them and the current state.
2. In one transaction, it replaced the stored events, appended the new ones, and deleted the
   snapshots ([chapter 13](13-snapshots.md)) of the cart and of the copies that read its history:
   they were made from the old values.
3. After the commit, the owner read the cart's state again from the store, replaying the corrected
   history. The owners of those copies were stopped, so they read their states again at their next
   message.
4. Only the new events were published to projections and sagas.

> **Under the hood:** each rewritten event is written with an `UPDATE` of its row in the same
> transaction as the new events, and only where the stored event has the same class; otherwise the
> whole command rolls back (decision D17, issue F8). See
> [ARCHITECTURE.md, section 4.5](../ARCHITECTURE.md).

## The rules of the game

1. **A rewritten event keeps its class.** Turning a `ShoppingCartCreated` into an `ItemAdded` is
   refused: the command is answered `EventHandlingError`, and nothing of it is stored. The history
   must keep meaning what it meant.
2. **You get the classes you list, and only the aggregate's own events.** A copy's rewrite never
   reaches the events it reads from its original ([chapter 6](06-events.md) explains copies).
   Events that an undo cancelled are included: they are still in the diary, even if they no
   longer count.
3. **Return the events you changed, with their versions.** Unchanged ones may come back as they are;
   they are simply written back unchanged.
4. **Always add at least one new event.** See the warning below.
5. **The metadata envelope stays.** A rewrite changes the event, not the envelope stored with it
   ([chapter 6](06-events.md)). Keep values you may one day have to erase out of metadata.
6. **The new events are ordinary events.** They are stored with the user of the rewrite command, so
   they double as a record of who corrected the history, and when. Mix `WithMetadata` into the
   rewrite command to record why, for example the number of the erasure request.

> **Warning:** today, a rewrite whose result has no new event is answered `SuccessResponse` and
> *nothing is stored*, not even the rewritten events. Always return at least one new event, such as
> `CartNameRewritten`. The fix is planned for 1.0, together with the complete rewrite described at
> the end of this chapter.

## Telling the read models: the compensating event

Projections never see a rewritten event: it is not published again. A cart list that shows
"Grocceries" goes on showing it, unless something tells it. That is the job of the new event,
`CartNameRewritten` in the test domain: a **compensating event**. Every projection that shows the
cart's name handles it, for example:

```scala
case CartNameRewritten(name) =>
  documentStore.updateDocument(0, aggregateId.asLong, _ => Document(name))
```

The aggregate's own event handler applies it too. It sets the name, which the corrected creation
event has already given, so nothing changes, and the history stays consistent from top to bottom:

```scala
case e: CartNameRewritten => cartNameRewritten(shoppingCart, e) // shoppingCart.copy(name = e.name)
```

A projection rebuilt later ([chapter 10](10-rebuilding-projections.md)) reads the corrected history
from the store, so it never sees the old value at all.

> **Warning:** a compensating event fixes what a projection *shows*, not what it has *kept*. If a
> projection copied the old value elsewhere (a search index, a change log, the document of another
> aggregate), its handler for the compensating event must clean that up too, or you rebuild the
> projection from the corrected history ([chapter 10](10-rebuilding-projections.md)).

## What a rewrite does not reach yet

A rewrite changes the stored events of one aggregate, of the classes you name, and the states and
snapshots built from them. Here is where an old value can still hide:

- **Squash events.** After a squash ([chapter 14](14-squash.md)) the old name may sit inside the
  squash event: `CartSquashed` holds the cart's name too, and `RewriteCartName` lists only
  `ShoppingCartCreated`. If your aggregates are squashed, list the squash event's class in
  `eventsTypes` as well, and rewrite it the same way
  (`case e: CartSquashed => EventWithVersion(ev.version, e.copy(name = name))`).
- **The archive.** Events that a squash moved to the archive ([chapter 14](14-squash.md)) keep
  their old values.
- **Copies that stand on their own.** A copy that still reads its original's history sees the
  rewrite (its owner even reloads). A copy that a squash or a permanent delete has detached carries
  its own summary of the old state, which a rewrite of the original does not touch.
- **Read models.** Only through your compensating event, or a rebuild.
- **Rows your own code wrote with the events**, through an outbox writer (`WithOutboxWriter`).
- **Everything else that copied the value**: events of other aggregates, stored responses of
  idempotent commands, saga data, metadata envelopes, logs.

> **Tip:** before your first erasure, make a list per aggregate type: which event classes (and which
> squash event class) hold personal data, which projections show it, and which of your own tables
> copy it. That list becomes your rewrite command's `eventsTypes`, the handlers of your compensating
> event, and your rebuild plan.

### Coming in 1.0: the complete rewrite

A *complete* rewrite is being built for 1.0. It is meant to replace a value wherever the library
stored it, follow an aggregate's copies, rebuild the read models that hold the value, and record who
asked and when, without recording the values themselves. Its API is not settled yet, and this
chapter will describe it once it lands. Until then, use the rewrite above, with the step-by-step
below.

## Erasing a customer's name, step by step

1. **Find where the value lives**: the event classes that hold it (and the squash event class, if
   the type squashes), the projections that show or copy it, and your own tables.
2. **Write a rewrite command** that lists those classes in `eventsTypes`, and a handler that
   replaces the value and adds a compensating event.
3. **Teach your projections** the compensating event, and plan a rebuild for those that keep copies
   of the value elsewhere.
4. **Send the rewrite.** When you do not know the current version, use the concurrent flavor, such
   as `RewriteCartNameConcurrently(userId, cartId, "(erased)")`.
5. **Check the result.** `GetEventsForAggregate(cartId)` ([chapter 7](07-reading-the-past.md))
   returns the stored events, now holding the new value, and the cart itself shows it.
6. **Clean up what the rewrite does not reach yet**: the archive, detached copies, your own tables.

## What you learned

- A history rewrite changes events that are already stored; use it only when the old value must
  not survive.
- `RewriteHistoryCommand` (with `expectedVersion`) and `RewriteHistoryConcurrentCommand` name the
  event classes they change in `eventsTypes`; the context's `rewriteHistoryCommandHandlers` gets
  those events and the state, and answers `RewriteCommandSuccess(rewritten, newEvents)`.
- A rewritten event keeps its class and its version; afterwards the aggregate and the copies that
  read its history read their states again, and their snapshots are deleted.
- Only the new events are published, so every rewrite comes with a compensating event that your
  projections handle; never send a rewrite without a new event.
- Squash events (unless you list them), the archive, detached copies, read models and your own
  tables are not reached yet; a complete rewrite is coming in 1.0.

Next: [chapter 16](16-organizations.md) opens Part VI with organizations: many customers sharing one
installation.

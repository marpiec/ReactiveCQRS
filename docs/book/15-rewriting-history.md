# Chapter 15. Rewriting History, Carefully

Chapter 1 made a promise: events are facts, and a removal is a new event, not an eraser. This
chapter is about the one tool that breaks that promise. By the end of it you will be able to change
events that are already stored (fix a name everywhere it was ever written), make your read models
hear about the change, and wipe a value from everywhere the library keeps it with the *complete
rewrite*. Handle it like a sharp knife: rarely, deliberately, and with both eyes open.

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
4. **Add a new event, as a rule.** A rewrite without one is stored too, but it publishes nothing:
   see the warning below.
5. **The metadata envelope stays.** A rewrite changes the event, not the envelope stored with it
   ([chapter 6](06-events.md)). Keep values you may one day have to erase out of metadata.
6. **The new events are ordinary events.** They are stored with the user of the rewrite command, so
   they double as a record of who corrected the history, and when. Mix `WithMetadata` into the
   rewrite command to record why, for example the number of the erasure request.

> **Warning:** a rewrite whose result has no new event rewrites the events, keeps the version, and
> publishes nothing, so your projections hear nothing and go on showing the old value. Add a
> compensating event, or rebuild the projections afterwards. (Before 1.0 such a rewrite was answered
> `SuccessResponse` and *nothing at all was stored*, issue F146.)

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

## What a plain rewrite does not reach

A rewrite command changes the stored events of one aggregate, of the classes you name, and the
states and snapshots built from them. Here is where an old value can still hide:

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

That is fine for fixing a typo. For erasing a value that must not survive anywhere, there is a
bigger tool.

## The complete rewrite: erasing everywhere

Back to the diary. The family made photocopies of it: one cousin's copy is stapled to the original
and still reads its first pages from it; another cousin copied the first pages by hand into a
notebook of her own. Old pages were also moved into a box in the attic (the archive). Erasing a name
properly means the original, the box in the attic, both copies, and every summary written from
them. That is the **complete rewrite**.

### Step 1: describe the change as data

A change is a small case class that says what to erase. It extends `HistoryChange`:

```scala
import io.reactivecqrs.api.HistoryChange

case class EraseCustomer(customer: CustomerId) extends HistoryChange
```

### Step 2: teach each context what the change does

The context gets `historyChanges`: for a change, a partial function from an event to the event it
becomes. Events it does not match stay as they are.

```scala
override def historyChanges: HistoryChange => PartialFunction[Event[Order], Event[Order]] = {
  case EraseCustomer(c) => {
    case e: OrderPlaced if e.customer == c => e.copy(customerName = "erased", email = None)
    case s: OrderSquashed if s.customer == c => s.copy(customerName = "erased", email = None)
  }
  case _ => PartialFunction.empty
}
```

Notice the second line: it handles the squash event too, because after a squash
([chapter 14](14-squash.md)) the name may live in there. If the metadata envelopes of your events
can hold the value (an `actor` or a `custom` entry, [chapter 6](06-events.md)), override
`metadataChanges` the same way.

> **Remember:** a change must be *idempotent*: run it twice and the second run finds nothing to
> change. Replacing the name with "erased" is idempotent; appending " (erased)" to it is not.

### Step 3: run it

```scala
import io.reactivecqrs.core.rewrite.HistoryRewriter

val report: Future[RewriteReport] =
  new HistoryRewriter(orders, eventStore, projections = Map("orders" -> ordersProjection))
    .rewrite(orderId, EraseCustomer(customerId), operator)
```

`orders` is the type's command bus; `projections` are the managed projections
([chapter 10](10-rebuilding-projections.md)) that show the value, by name. What happens:

1. **The aggregate first.** Its owner, between commands, applies the change to every stored event of
   the aggregate (squash events included) and to its archived events, in one transaction. The
   version does not change, and nothing is published. Snapshots made from the old values are
   dropped, and the owner reads its state again.
2. **Then every copy.** Copies that still read the aggregate, copies that were detached from it (the
   library remembers each detach in a *lineage*), and copies of those, one at a time. Each copy's
   own events may hold the value too.
3. **Then the read models.** Each projection you named rebuilds every aggregate that changed, in
   place ([chapter 10](10-rebuilding-projections.md)). It needs `clearForRebuild`.
4. **And a note in the logbook.** Each aggregate's rewrite leaves a row in `history_rewrites`: who,
   when, which change, which event classes and fields. Never the values.

Rows of your own that an outbox writer wrote with the events (`WithOutboxWriter`) can be cleaned
in the same transaction: override `OutboxWriter.rewrite(aggregateId, change)`.

### Step 4: read the report

`RewriteReport` lists the aggregates rewritten (with how many events and archived events), those
the change left alone, those refused (with why: the change made an event another class, or touched
an undo or a duplication event), the projections rebuilt and those that failed, and `bases`: the
aggregates that the first one reads, which the rewrite does not change, since the value may have
come from there and only you can decide. `report.complete` is true when nothing was refused and
every projection rebuilt. When it is not, fix the cause and run it again: the change finds nothing
to change where it already did its work.

> **Warning:** the complete rewrite covers what the library stores for the aggregate and its copies.
> It does not reach other aggregates that copied the value into their own events, saga data, stored
> responses of idempotent commands (they expire), projections you did not name, logs, or exported
> files. User ids (`events.user_id`) are identifiers, not personal data, and stay; erase the person
> in their own user aggregate.

## Erasing a customer's name, step by step

1. **Find where the value lives**: the event classes that hold it (squash event class included),
   their metadata, the projections that show it, your own tables, and other aggregates that copied
   it.
2. **Write the change** (`EraseCustomer`) and each context's `historyChanges` (and
   `metadataChanges` if needed); override `OutboxWriter.rewrite` for your own rows.
3. **Run `HistoryRewriter.rewrite`** for each aggregate that holds the value, naming the
   projections that show it.
4. **Read the report.** Run it again if it is not `complete`; look at the `bases` it names.
5. **Check the result**: `GetEventsForAggregate` ([chapter 7](07-reading-the-past.md)) and
   `GetArchivedEvents` return the erased values, and so do the projections.

## What you learned

- A history rewrite changes events that are already stored; use it only when the old value must
  not survive.
- `RewriteHistoryCommand` (with `expectedVersion`) and `RewriteHistoryConcurrentCommand` name the
  event classes they change in `eventsTypes`; the context's `rewriteHistoryCommandHandlers` gets
  those events and the state, and answers `RewriteCommandSuccess(rewritten, newEvents)`.
- A rewritten event keeps its class and its version; afterwards the aggregate and the copies that
  read its history read their states again, and their snapshots are deleted.
- Only the new events are published, so a rewrite comes with a compensating event that your
  projections handle; a rewrite without one is stored but tells nobody.
- The complete rewrite (`HistoryChange`, the context's `historyChanges`, `HistoryRewriter`)
  erases a value from an aggregate's events, squash events and archive, from every copy, attached
  or detached, and from the projections you name, and leaves an audit row without the values.

Next: [chapter 16](16-organizations.md) opens Part VI with organizations: many customers sharing one
installation.

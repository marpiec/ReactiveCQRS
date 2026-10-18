# Chapter 14. Squash: Folding Old History Away

Some aggregates live for years. Every command adds a line to their diary, and every time such an
aggregate wakes up, its owner reads all of those lines again. In this chapter you will learn to
*squash* an aggregate: fold the old part of its history into one summary event, on request or on a
schedule, while the events your auditors care about are kept in an archive. You will also learn
when the library says "not now", and why that is a good thing.

---

## The summary page

Picture the diary of Alice's shopping cart after a busy year: three thousand lines. Nobody has
looked at the first two thousand nine hundred in months, but every time the cart's owner starts, it
replays all of them, one by one, to rebuild the cart.

Now imagine tearing out pages 1 to 2 900 and gluing in, at page 2 900, one summary page:

> *At this point the cart was called "Groceries" and held pears and plums.*

The pages after it stay exactly as they were. The numbering does not change: the summary is page
2 900, and the next line is still page 2 901. Anyone who reads the diary from the top gets the same
cart as before, after reading a hundred lines instead of three thousand.

That is a **squash**. In ReactiveCQRS words:

- the events at versions 1 to N are replaced by one **squash event**, stored at version N, the
  **squash point**;
- the aggregate keeps its version, its state and every event after N;
- a restore reads the squash event and the events after it, so it is quick again;
- the rows of the old events are deleted, so the database can reuse their space.

How slow is a long diary? On the machine where the library's performance is measured, a new owner
restored an aggregate of 50 000 events in 392 ms (measured on 17/10/2026, a single run of the
`load-M5` bench with PostgreSQL on the same machine; see [docs/SCALING.md, section 3](../SCALING.md)).
Fine once; not fine for every cart that wakes up.

> **Remember:** a squash changes the history for good. Once the old events are gone, nobody can read
> the cart as it was at version 3, undo across the squash point, or copy the cart from before it.
> That is the price of the summary page, so this chapter spends some time on what the library
> checks before it lets you pay it.

### Squash or snapshot?

[Chapter 13](13-snapshots.md) gave you snapshots, another cure for long histories. They solve
different problems:

| | Snapshot | Squash |
|---|---|---|
| What it is | a copy of the state, kept next to the history | a summary event that replaces the history up to a point |
| The history | unchanged | the events before the point are gone |
| Reading an old version | works as before | refused below the point |
| If it goes missing | the library reads all the events instead | nothing can go missing: it *is* the history now |
| Disk | one more row per aggregate | fewer rows, once PostgreSQL has vacuumed |
| Turned on with | `snapshots = Some(SnapshotPolicy(...))` | `squash = Some(SquashPolicy(...))` |

If all you need is speed, snapshots are the gentle choice. Reach for squash when the old history is
a burden in itself: disk space, old event classes you would like to retire, data you have no reason
to keep. One aggregate type can use both.

> **Remember:** a squash keeps the version. The event store's limit of versions per aggregate
> (`aggregateVersionLimit`, 10 000 by default) counts versions, not stored events, so a squash does
> not make room under it.

## Step 1: give your aggregate a squash event

The summary page is an event like any other, and you write it. It must be able to rebuild the
whole state on its own. In the test domain:

<!-- snippet: shoppingcart/Events.scala#cartSquashed -->
```scala
/** The squash event of a cart: its state at the squash point. */
case class CartSquashed(spaceId: SpaceId, name: String, items: Vector[Item]) extends FirstEvent[ShoppingCart]
```

Why a `FirstEvent`? After a squash it *is* the first event of the aggregate's history, and first
events name the organization the aggregate belongs to, its space ([chapter 16](16-organizations.md)).
Every cart of the test domain lives in space 0.

Then tell the context about it. Its event handlers apply the squash event, like any other,

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#cartSquashedHandler -->
```scala
case e: CartSquashed => ShoppingCart(e.name, e.items)
```

and its squash policy says how to make one from a state:

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#squashPolicy -->
```scala
override val squash: Option[SquashPolicy[ShoppingCart]] =
  Some(SquashPolicy(cart => CartSquashed(SpaceId(0), cart.name, cart.items)))
```

`SquashPolicy` takes three things:

- **`toEvent`**: a function from the state to your squash event. The library calls it with the
  state at the squash point.
- **`automatic`**: an optional rule for squashing without being asked; `None` by default. It comes
  later in this chapter.
- **`keep`**: the classes of audit events to archive instead of deleting; empty by default. Also
  later in this chapter.

> **Remember:** the squash event is stored for as long as the aggregate lives. Treat its class like
> any other event class: when its shape must change, add a new class (`CartSquashedV2`) and keep
> the old one ([chapter 6](06-events.md)). That is also why the library never stores a raw dump of
> the state as history: an event class is something you can evolve; a dump of last year's state
> class is not.

### Projections must understand it too

When you squash, nobody is told: the squash publishes nothing. Projections that follow the cart
already have every event up to the squash point (you will see below that the library makes sure of
that), and the summary gives the same state, so their read models stay right.

Two kinds of projections do meet the squash event: one being rebuilt
([chapter 10](10-rebuilding-projections.md)), and one added after the squash. For them it is the
*first* event they ever see of that cart, at version N. So handle it like a creation that brings
the whole state. The test domain's cart list does it like this:

<!-- snippet: shoppingcart/ShoppingCartsListProjection.scala#squashedDocument -->
```scala
case CartSquashed(_, name, _) => // a squash event holds the whole state: the document is made from it
  documentStore.updateDocument(0, aggregateId.asLong, _ => Document(name))
```

`updateDocument` inserts the document when it is missing and replaces it when it is there, so this
works whether the projection knew the cart before or not. A projection that listens to states
(`AggregateListener`, [chapter 8](08-projections.md)) is simply told that the cart was created
(`created = true`) when its subscription starts at a squash event.

> **Warning:** a projection with no case for the squash event works today and goes wrong on its
> next rebuild: it fails on every squashed aggregate, or leaves it out. Teach every projection of
> the type about the squash event *before* you squash anything.

## Step 2: squash on request

Squashing one aggregate is one message to its command bus. This chapter's blocks run in `SquashingSpec`
(`examples/src/test/scala/io/reactivecqrs/examples/cart/`), which checks what they do:

<!-- snippet: cart/SquashingSpec.scala#request -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.UserId
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._

implicit val timeout: Timeout = Timeout(10.seconds)

val answer: Future[SquashResponse] =
  (carts ? SquashAggregate(cartId, AggregateVersion(5), UserId(7))).mapTo[SquashResponse]
```

The command bus passes the request to the cart's owner, which answers with one of two messages
(not wrapped in a `Try`):

- `AggregateSquashed(cartId, AggregateVersion(5))`: done.
- `SquashRefused(cartId, reason)`: nothing was changed, and `reason` says why in plain words.

`SquashAggregate` takes four values:

- **`id`**: the aggregate.
- **`upTo`**: the squash point, from 1 to the aggregate's current version.
- **`userId`**: who asked. It is stored with the squash event; `UserId(0)` if you leave it out.
- **`metadata`**: an optional metadata envelope for the squash event ([chapter 6](06-events.md)),
  handy for recording why: `Some(EventMetadata("operator:retention", causationId = Some("ticket 4711")))`.

The owner does the work between two commands of the cart. The cart's version and state stay as they
are, so commands keep working while it happens.

### What happened to the diary

Let's follow the test suite's own example. A cart called "Groceries" gets an apple, a pear and a
plum, loses the apple (the item with id 1) and gets a fig. Then it is squashed up to version 5:

```
version  before                               after SquashAggregate(cartId, AggregateVersion(5), ...)
1        ShoppingCartCreated("Groceries")
2        ItemAdded("apple")
3        ItemAdded("pear")
4        ItemAdded("plum")
5        ItemRemoved(1)                       CartSquashed(SpaceId(0), "Groceries",
                                                Vector(Item(2, "pear"), Item(3, "plum")))
6        ItemAdded("fig")                     ItemAdded("fig")
```

After the squash:

- The cart is still at version 6, with the same name and the same items. Its live owner, a brand-new
  owner and a fresh restore all agree.
- The next command still says `AggregateVersion(6)`, and is answered
  `SuccessResponse(cartId, AggregateVersion(7))`.
- `GetEventsForAggregate(cartId)` returns two events: the squash event at version 5, with the user
  and metadata of the squash request and the time of the event it replaced, and the fig at
  version 6.

> **Under the hood:** the owner checks the preconditions and builds the squash event first; then
> the store does the rest in one transaction. It locks the aggregate's duplication chain, checks the
> preconditions again, detaches copies, moves kept events to the archive, deletes the old events
> and their undo marks, replaces the event at version N with the squash event (keeping its row id
> and time, so the commit order of all events holds) and records the point in
> `aggregates.squashed_version`. Decisions D47 to D52; see
> [ARCHITECTURE.md, section 4.6](../ARCHITECTURE.md).

## The honesty check

A summary page that lies is worse than none. So before anything is stored, the library tests your
squash event: it applies it to `initialAggregateRoot` with your event handlers, exactly as a restore
will, and compares the result with the real state at the squash point, using `==`. If they differ,
the squash is refused, and the reason says that the squash event "does not give back the state it
replaces".

The squash is refused too when your `toEvent` function throws, or when your event handlers fail on
the squash event.

> **Warning:** the check applies the squash event the way a restore will: on behalf of the user who
> asked for the squash (`SquashAggregate.userId`), at the time of the event it replaces. If your
> event handlers copy the user or the time into the state ("last changed by Bob"), the squash event
> must carry those values itself and its handler must take them from there. Otherwise the check
> refuses the squash (always for automatic squashes, which ask as user 0), or lets it pass only when
> the right person happens to ask. The refusal reminds you of this (issue F155).

### Leaving data out on purpose

Sometimes a summary *should* forget something: an old, closed cart whose items nobody needs any
more. Mix `SquashMayChangeState` into such a squash event class, and the check only makes sure your
handlers can apply it, without comparing. This chapter's spec has one that keeps only the cart's name:

<!-- snippet: cart/SquashingSpec.scala#cartArchived -->
```scala
case class CartArchived(spaceId: SpaceId, name: String) extends FirstEvent[ShoppingCart] with SquashMayChangeState
```

with `case e: CartArchived => ShoppingCart(e.name, Vector.empty)` among its event handlers. A cart
with an apple, a pear and a plum (versions 2 to 4), squashed up to 3 with it, holds only the plum
afterwards: the items up to the squash point are forgotten.

Because the history now gives a different state than before, the library also throws away every
snapshot of the aggregate and of the copies that read its history, and has their owners read their
states again.

> **Warning:** projections are *not* told. A read model built from the events keeps showing the
> forgotten data until you rebuild it ([chapter 10](10-rebuilding-projections.md)). Plan the rebuild
> together with the squash.

## When the answer is "not now"

A refusal changes nothing, and most refusals go away by themselves. The reason is a sentence meant
for people; here is what the common ones mean:

| The reason mentions | Why | What to do |
|---|---|---|
| "squash is not enabled" | the context has no `squash` policy | give it one (step 1) |
| "is not between 1 and" | `upTo` is 0, or beyond the current version | pick a version that exists |
| "already squashed up to version" | it is squashed that far, or further | nothing; or choose a later point |
| "still wait in the outbox" | some subscriber has not confirmed events at or below the point yet | try again soon; it passes once the projections catch up |
| "a subscription of the aggregate is failed" | a projection gave up on this aggregate ([chapter 9](09-projection-failures.md)) | repair the failure first |
| "an undo after version" | a later undo cancelled events at or below the point | squash at the undo's version, or later |
| "does not give back the state it replaces" | the honesty check failed | fix `toEvent` or the handlers, or use `SquashMayChangeState` |
| "duplicate" | a copy that must be detached is not ready (below) | wait, or repair that copy's projection failure |
| "changed meanwhile" | someone copied the aggregate during the squash | try again |
| "every node runs version" | a rolling upgrade is under way and your squash event's class is listed in `writtenFrom` | wait until every node runs the new version ([chapter 17](17-cluster.md)) |

The outbox and failure rules protect your projections. A projection must have every event up to the
squash point before those events disappear; otherwise it could never get them again.

> **Tip:** the squash scheduler you are about to meet simply tries a refused aggregate again in a
> later run. When you squash by hand, do the same: log the reason and try later.

## Life after a squash

### Reading the past

The historical reads of [chapter 7](07-reading-the-past.md) still work at the squash point and
after it. Below it, they refuse instead of guessing:

<!-- snippet: cart/SquashingSpec.scala#old -->
```scala
import scala.util.Try

val old: Future[Try[Aggregate[ShoppingCart]]] =
  (carts ? GetAggregateForVersion(cartId, AggregateVersion(3))).mapTo[Try[Aggregate[ShoppingCart]]]
// Failure(AggregateSquashedException): "The history of aggregate ... is squashed up to version 5;
// version 3 cannot be read"
```

`AggregateSquashedException` tells you the squash point (`squashedUpTo`) and what you asked for
(`requested`, and `at` for a read at an instant). The same answer comes for
`GetEventsForAggregateForVersion` below the point, and for `GetAggregateAtInstant` at a moment
before the squash event's time. `GetEventsForAggregate` simply starts at the squash event.

### Undo and duplication

An undo that would reach the squash point is refused, and so is a duplication below it. Both are
answered `EventHandlingError`, and the log says why. Later events can be undone as before, and the
cart can be copied at the squash point or later. With the cart squashed up to 4 and now at
version 5:

<!-- snippet: cart/SquashingSpec.scala#belowThePoint -->
```scala
val commands = Seq(
  UndoShoppingCartChange(userId, cartId, AggregateVersion(5), 2), // would reach version 4: EventHandlingError
  UndoShoppingCartChange(userId, cartId, AggregateVersion(5), 1), // SuccessResponse(cartId, AggregateVersion(6))
  DuplicateShoppingCart(userId, cartId, AggregateVersion(3)),     // below the point: EventHandlingError
  DuplicateShoppingCart(userId, cartId, AggregateVersion(4)))     // fine: the copy starts from the summary
```

### Snapshots

A snapshot at or below the squash point is deleted (the squash event does its job now); a snapshot
above the point stays.

### Copies: detaching duplicates

A duplicate ([chapter 6](06-events.md)) is like a new diary that begins with "for the first pages,
read the original up to page 3". Tear those pages out of the original, and the copy loses its
beginning. So before squashing, the library *detaches* every copy that reads the original below the
squash point: the copy's first event, its duplication event, is replaced by your squash event made
from the copy's own state at version 1. From then on the copy stands on its own, with exactly the
state it had.

- Copies made at the squash point or later are left alone: they read the summary page, which gives
  the same state.
- A copy of a copy is detached the same way.
- Squashing a copy itself cuts it loose from its original, too.

A detach waits, and with it the squash, while the copy's first event was undone, still waits in the
outbox, or a subscription of the copy is failed. Detaching also restores each copy at version 1
first, so squashing an aggregate that has been copied many times takes longer.

### Disk space

Deleted rows do not shrink the database files at once. PostgreSQL's vacuum, usually autovacuum,
marks their space free for new rows, but it does not give it back to the operating system (except
for free pages at the very end of a table). To shrink the files you need `VACUUM FULL`, which
rewrites the table and locks it while it runs, or the `pg_repack` extension, which does the same
online. Sources: [ARCHITECTURE.md, section 4.6](../ARCHITECTURE.md), the PostgreSQL manual on
[routine vacuuming](https://www.postgresql.org/docs/current/routine-vacuuming.html), and the
[pg_repack documentation](https://reorg.github.io/pg_repack/).

> **Tip:** squashing a few aggregates every hour suits autovacuum well: the freed space is reused by
> new events. A one-off squash of a whole store frees a lot at once; that is when shrinking the
> files with `pg_repack` may be worth it.

## Squashing automatically

Squashing by hand is fine for one cart. For thousands, give the context a rule and let the library
apply it.

### The rule

Here it is in `AutomaticallySquashedCarts`, a context of carts that extends the test domain's:

<!-- snippet: cart/SquashingSpec.scala#automatic -->
```scala
override val squash: Option[SquashPolicy[ShoppingCart]] = Some(SquashPolicy(
  toEvent = cart => CartSquashed(SpaceId(0), cart.name, cart.items),
  automatic = Some(AutomaticSquash(afterEvents = 1000, keepLast = 100))))
```

`AutomaticSquash(afterEvents, keepLast = 0, keepNewerThan = None)` reads: "once at least
`afterEvents` events, not counting the newest `keepLast`, are not squashed yet, squash up to just
before those `keepLast`". Here is the rule above at work on one cart:

| The cart is at version | Squashed up to | Events that could go | Squash? |
|---|---|---|---|
| 1 250 | never | 1 250 − 100 = 1 150 | yes, up to 1 150 |
| 1 900 | 1 150 | 1 900 − 100 − 1 150 = 650 | not yet |
| 2 300 | 1 150 | 2 300 − 100 − 1 150 = 1 050 | yes, up to 2 200 |

With `keepNewerThan`, recent events stay too, however many there are:

<!-- snippet: cart/SquashingSpec.scala#keepNewerThan -->
```scala
AutomaticSquash(afterEvents = 1000, keepLast = 100, keepNewerThan = Some(30.days))
```

Now the squash point is also below the aggregate's first event not older than 30 days, by the times
of the aggregate's own events (an imported organization keeps its events' original times). If that
leaves fewer than `afterEvents` events to squash, the aggregate waits for a later run.
`afterEvents` must be positive and `keepLast` not negative, or the rule refuses to be created.

### Starting the scheduler

A rule is only a rule; something has to act on it. That is the `SquashScheduler`: one per system,
started by the host next to the command buses.

<!-- snippet: cart/SquashingSpec.scala#scheduler -->
```scala
import io.reactivecqrs.core.aggregate.SquashScheduler
import io.reactivecqrs.core.aggregate.SquashScheduler.SquashTarget
import io.reactivecqrs.core.cluster.Singletons
import org.apache.pekko.actor.{ActorRef, Props}

val squashScheduler: ActorRef = Singletons.start(system, "squashScheduler",
  Props(new SquashScheduler(eventStore, Seq(SquashTarget.of(new AutomaticallySquashedCarts, carts)))))
```

- **`SquashTarget.of(context, commandBus)`** names one aggregate type: its context's rule and its
  command bus. List one target per type you want squashed. A context without an `automatic` rule
  makes it throw an `IllegalArgumentException`.
- **`Singletons.start`** is the same as `system.actorOf(props, name)` on a single node; in a
  cluster it keeps exactly one scheduler running ([chapter 17](17-cluster.md)).

What the scheduler does, with the default `SquashSchedulerOptions`:

1. A minute after it starts (`firstRunAfter = 1.minute`), and then every hour
   (`interval = 1.hour`), it begins a *run*.
2. For each type, it finds up to 100 aggregates that the rule selects (`perRun = 100`), in id order.
   When the previous run found its full 100, it goes on after the last of them; otherwise it starts
   from the beginning again.
3. It asks the type's command bus to squash them, one at a time, waiting up to a minute for each
   answer (`answerTimeout = 1.minute`) before it goes on with the next.
4. A refused aggregate is simply tried again in a later run. A run that comes while the previous
   one is still going is skipped.

All four can be changed, in the scheduler's `Props`:

<!-- snippet: cart/SquashingSpec.scala#options -->
```scala
import io.reactivecqrs.core.aggregate.SquashSchedulerOptions

val withOptions = Props(new SquashScheduler(eventStore, Seq(SquashTarget.of(new AutomaticallySquashedCarts, carts)),
  SquashSchedulerOptions(firstRunAfter = 5.minutes, interval = 30.minutes, perRun = 500, answerTimeout = 2.minutes)))
```

Its squash events are stored with `UserId(0)` and a metadata envelope whose actor is
`"system:squash-scheduler"` (`SquashScheduler.metadata`), so you can tell them from squashes made by
hand.

> **Tip:** one squash at a time means that automatic squashing holds at most one database
> connection, however many aggregates qualify. It is maintenance, and it behaves like it.

> **Under the hood:** with `keepNewerThan`, each run first finds the oldest event that is not older
> than the age, by a binary search of the events table's primary key (about 40 reads, by the
> library's own estimate in [docs/SCALING.md, section 3](../SCALING.md)), then probes each candidate
> aggregate once. Ages are exact up to the longest command's duration and the clocks' skew between
> nodes. Decision D52 and requirement R11; see [ARCHITECTURE.md, section 4.6](../ARCHITECTURE.md).

### Watching it

<!-- snippet: cart/SquashingSpec.scala#status -->
```scala
val status: Future[SquashScheduler.SquashStatus] =
  (squashScheduler ? SquashScheduler.GetSquashStatus).mapTo[SquashScheduler.SquashStatus]
// SquashStatus(squashed = 1, refused = 0, waiting = 0)
```

`squashed` and `refused` count since the scheduler started; `waiting` is what the current run still
has to do. A `refused` count that keeps growing usually means a projection is failing on those
aggregates ([chapter 9](09-projection-failures.md)); each refusal is logged with its reason at debug
level. Want a run right now, in a test say? Send the scheduler `SquashScheduler.Run`.

## Keeping the events your auditors need

Back to the diary. Before tearing out the old pages, you photocopy the ones the auditor wants (say,
every "item added") and file the copies in a binder labelled with the cart's number. The diary gets
its summary page; the binder keeps the originals.

That binder is the **archive**. Name the audit event classes in `keep`:

<!-- snippet: cart/SquashingSpec.scala#keep -->
```scala
override val squash: Option[SquashPolicy[ShoppingCart]] =
  Some(SquashPolicy(cart => CartSquashed(SpaceId(0), cart.name, cart.items), keep = Set(classOf[ItemAdded])))
```

At every squash, the events of those classes at or below the squash point move into the archive (the
`events_archive` table), in the squash's own transaction. Each keeps everything it had: version,
user, time, metadata and, if an undo had cancelled it, the version of that undo. That includes the
event at the squash point itself, archived before the squash event takes its place, and the first
event of a copy that the squash detaches, as long as they are of a kept class.

The test suite's example: a cart gets an apple (version 2) and a pear (3), loses the apple (4) and
gets a plum (5). It is squashed up to 4. Now ask for its archive:

<!-- snippet: cart/SquashingSpec.scala#archived -->
```scala
val archived: Future[Try[ArchivedEvents[ShoppingCart]]] =
  (carts ? GetArchivedEvents(cartId)).mapTo[Try[ArchivedEvents[ShoppingCart]]]
```

The answer is `Success(ArchivedEvents(cartId, events))`, oldest first; here, the two `ItemAdded`
events at versions 2 and 3. Each is an `ArchivedEvent(info, undoneFrom, archived)`:

- **`info`**: the `EventInfo` as it was stored: version, event, user, time and metadata;
- **`undoneFrom`**: the version of the undo that cancelled it, if one did (`None` here);
- **`archived`**: when the squash moved it.

The removal at version 4 is not in the archive: `ItemRemoved` is not a kept class, so it went with
the rest of the old history. The diary itself now holds `CartSquashed` at 4 and the plum at 5.

The archive is read on request only. Restores, re-publications, historical reads and projection
rebuilds never look at it: what projections know comes from the squash event on. A permanent
delete ([chapter 6](06-events.md)) removes the archive together with the aggregate.

Put the pieces together and you get a retention policy: "keep the last 90 days as they are, keep the
audit events for good, let the rest go":

<!-- snippet: cart/SquashingSpec.scala#retention -->
```scala
override val squash: Option[SquashPolicy[ShoppingCart]] = Some(SquashPolicy(
  toEvent = cart => CartSquashed(SpaceId(0), cart.name, cart.items),
  automatic = Some(AutomaticSquash(afterEvents = 500, keepNewerThan = Some(90.days))),
  keep = Set(classOf[ItemAdded])))
```

> **Warning:** name your audit classes in `keep` *before* you turn squash on. Events squashed away
> without being kept are gone for good; adding a class to `keep` later saves only what has not been
> squashed yet.

### Who still uses this event class?

Event classes pile up over the years: a changed event shape becomes a new class
([chapter 6](06-events.md)), and the old class must stay in your code for as long as any stored
event uses it. Squash is how old events finally leave the history. To find out whether a class is
still needed, ask `StoredDataCheck`:

<!-- snippet: cart/SquashingSpec.scala#usersOf -->
```scala
import io.reactivecqrs.core.tools.StoredDataCheck

val users = StoredDataCheck.usersOf(classOf[ItemAdded].getName)
```

It answers `ClassUsers(className, events, archived, aggregates)`: how many stored events and how
many archived events have that class, and the first 100 aggregates that use it (`limit = 100` by
default), each as a `ClassUser(aggregateId, events, archived)`. For the cart above, its entry is
`ClassUser(cartId.asLong, 1, 2)`: one `ItemAdded` still in its history (the plum) and two in its
archive. When `events` and `archived` are both 0, no stored event uses the class any more, and you
can delete it from your code.

> **Warning:** `usersOf` reads the whole `events` table and the whole archive, because no index is
> kept by event class. Run it on a copy of production, or when the database is quiet.

## What you learned

- A squash replaces events 1 to N with one squash event at version N; the version, the state and
  the later events stay, and the library checks that the summary gives back exactly the state it
  replaces (unless its class mixes in `SquashMayChangeState`).
- You opt in per aggregate type with `SquashPolicy(toEvent, automatic, keep)`; your event handlers
  and your projections handle the squash event as a creation that brings the whole state.
- `SquashAggregate(id, upTo, userId, metadata)` is answered `AggregateSquashed` or
  `SquashRefused(reason)`; a refusal changes nothing and usually passes later.
- After a squash, reads, undos and duplications below the point are refused, and copies that read
  below it are detached first.
- `AutomaticSquash(afterEvents, keepLast, keepNewerThan)` and a `SquashScheduler` started by the
  host squash without being asked; `GetSquashStatus` shows how it goes.
- `keep` moves audit events into an archive that `GetArchivedEvents` reads, and
  `StoredDataCheck.usersOf` tells you when an old event class can go.
- Disk space comes back through (auto)vacuum; files shrink only with `VACUUM FULL` or `pg_repack`.

Next: [chapter 15](15-rewriting-history.md) takes out the correction fluid: changing events that are
already stored, carefully.

# Chapter 7. Looking Back in Time

A whiteboard only knows what is written on it now. A diary knows everything that ever happened. Since
ReactiveCQRS keeps every event, you can ask an aggregate questions a normal database cannot answer:
"what was in this cart at 18:00 yesterday?", "who removed the apples?", "what did the cart look like
before that undo?". By the end of this chapter you will know the messages that read an aggregate's
past, what they answer, how to read your own writes reliably, and what happens to the past once
old history has been folded away.

---

## Six questions for the command bus

You ask about the past the same way you send commands: you ask the aggregate type's command bus.

| Message | Asks | You get |
|---|---|---|
| `GetAggregate(id)` | the aggregate now (chapter 4) | `Try[Aggregate[R]]` |
| `GetEventsForAggregate(id)` | every event | `Try[List[EventInfo[R]]]` |
| `GetEventsForAggregateForVersion(id, version)` | the events up to a version | `Try[List[EventInfo[R]]]` |
| `GetAggregateForVersion(id, version)` | the aggregate as it was at a version | `Try[Aggregate[R]]` |
| `GetAggregateAtInstant(id, instant)` | the aggregate as it was at a moment | `Try[Aggregate[R]]` |
| `GetAggregateMinVersion(id, version, maxMillis)` | the aggregate once it has reached a version | `Try[Aggregate[R]]` |

`R` is the aggregate's state class: `ShoppingCart` for carts. All six live in `io.reactivecqrs.api`.

> **Remember:** unlike command responses, these answers are a `scala.util.Try`. A read that failed
> still arrives as a *successful* `Future`, holding a `Failure`. Always look inside the `Try`.

The examples use chapter 5's setup and the Groceries cart of chapter 6, and run in `PastSpec`, which
checks what each one answers. The cart's story, with one more event: after Alice's undo, she removed
the oranges herself.

```
version 1  ShoppingCartCreated(Groceries)   Alice
version 2  ItemAdded(apples)                Alice
version 3  ItemAdded(oranges)               Bob
version 4  ItemRemoved(1)                   Bob, by mistake
version 5  ShoppingCartChangesUndone(1)     Alice: the apples are back
version 6  ItemRemoved(2)                   Alice: no oranges after all
```

Two more imports are needed:

<!-- snippet: cart/PastSpec.scala#imports -->
```scala
import java.time.Instant
import scala.util.{Failure, Success, Try}
```

## The whole diary: GetEventsForAggregate

<!-- snippet: cart/PastSpec.scala#diary -->
```scala
val diary = (commandBus ? GetEventsForAggregate(cartId)).mapTo[Try[List[EventInfo[ShoppingCart]]]]
diary.foreach {
  case Success(events) =>
    events.foreach(e => println(f"${e.version.asInt}%2d  ${e.event.toString}%-30s  ${e.metadata.actor}"))
  case Failure(error) =>
    println("Cannot read the cart: " + error.getMessage)
}
```

prints:

```
 1  ShoppingCartCreated(Groceries)  user:1
 2  ItemAdded(apples)               user:1
 3  ItemAdded(oranges)              user:2
 4  ItemRemoved(1)                  user:2
 5  ShoppingCartChangesUndone(1)    user:1
 6  ItemRemoved(2)                  user:1
```

Each `EventInfo` holds:

- `version`: the event's version;
- `event`: the event itself;
- `userId`: who sent the command that stored it;
- `timestamp`: when it was stored;
- `metadata`: its envelope from chapter 6, or, for an event stored without one, an envelope whose
  actor is `"user:"` and the user's id (that is what the last column shows).

What to know about this list:

- **It is the full diary.** The cancelled removal (version 4) is in it, and so is the undo (version
  5). Nothing in an `EventInfo` marks an event as cancelled; the undo events tell that part of the
  story.
- **For a duplicate, the list starts with its base's events** up to the copied version, numbered as
  they are in the base, followed by the duplicate's own events from version 1.
- **It comes in one message.** That is no problem for a cart, but a long history weighs: measured on
  17/10/2026, all 50 001 events of one aggregate took 264 ms to read, and the heap peaked at 168 MB
  (a single run on one development machine, [`docs/perf/load-M5.md`](../perf/load-M5.md)). For long
  histories, read up to a version instead.

## Part of the diary: GetEventsForAggregateForVersion

The same, stopping at a version:

<!-- snippet: cart/PastSpec.scala#upTo -->
```scala
val firstThree = commandBus ? GetEventsForAggregateForVersion(cartId, AggregateVersion(3))
// Success(List(...)) with the events of versions 1, 2 and 3
```

## The aggregate as it was: GetAggregateForVersion

This one rebuilds the state as it stood right after a given version:

<!-- snippet: cart/PastSpec.scala#atVersion -->
```scala
val atVersion4 = (commandBus ? GetAggregateForVersion(cartId, AggregateVersion(4))).mapTo[Try[Aggregate[ShoppingCart]]]
atVersion4.foreach(println)
```

The answer is `Success(Aggregate(cartId, AggregateVersion(4), Some(ShoppingCart("Groceries",
Vector(Item(2, "oranges"))))))`. At version 4 the apples were gone: the undo came later, at version
5, so a read of version 4 shows the cart exactly as Bob left it. Ask for version 5 and you get both
items back; ask for version 6 and only the apples remain.

What to know:

- **Version 0** answers `Aggregate(cartId, AggregateVersion.ZERO, None)`: before the first event,
  nothing existed.
- **A version past the newest** gives you the newest. Check the version in the answer when it
  matters.
- **A version at which the aggregate was deleted** (its state was `null`, chapter 6) gives an
  `Aggregate` whose `aggregateRoot` is `None`.
- **With snapshots** (chapter 13) the read starts from a snapshot at or before the version, when
  there is one, and replays only the events after it. The answer is the same; it only comes faster.

## The aggregate at a moment: GetAggregateAtInstant

"What was in the cart when the order was placed, at 18:00 UTC on 16/10/2026?"

<!-- snippet: cart/PastSpec.scala#atInstant -->
```scala
val whenOrdered = (commandBus ? GetAggregateAtInstant(cartId, Instant.parse("2026-10-16T18:00:00Z")))
  .mapTo[Try[Aggregate[ShoppingCart]]]
whenOrdered.foreach {
  case Success(cart)  => println(s"At that moment: version ${cart.version.asInt}, ${cart.aggregateRoot}")
  case Failure(error) => println("Cannot read the cart: " + error.getMessage)
}
```

The library finds the newest version stored at or before that instant, and reads the aggregate at
that version, with undos as they were then: an event cancelled *after* the instant still counts. So
if the order was placed between versions 4 and 5, you see the cart without apples, which is what the
customer saw.

What to know:

- **The times are the stored timestamps**: when the events were stored, by the clock of the node
  that stored them; the same `timestamp` your event handlers receive.
- **An instant before the first event** gives version 0 and no state.
- **Snapshots are not used** for reads at an instant, as a snapshot does not record when its version
  was reached. The whole history up to that moment is read.

> **Tip:** in a cluster, `GetAggregateAtInstant(id, Instant.now)` is a handy way to read a large state
> on the node you are on: it is restored there from the store, instead of being sent over from the
> node whose owner holds it ([chapter 17](17-cluster.md)).

## Reading your own writes: GetAggregateMinVersion

You order a coffee and get receipt number 42. At the counter you ask for number 42; if it is not
ready yet, you wait a moment rather than walk away with number 41.

Every successful command tells you the version it produced: `SuccessResponse(cartId,
AggregateVersion(7))`. A read made later should show at least that version. On one node it does: the
owner updates its state before it answers a command, so a `GetAggregate` sent after the answer already
shows the change. But you cannot always be sure: the reader may not be the one who sent the command,
the command may have been sent without waiting for its answer, or the change may still be on its way
(a saga's next step, chapter 12). `GetAggregateMinVersion` turns "at least version 7" into a promise:

<!-- snippet: cart/PastSpec.scala#minVersion -->
```scala
val atLeast7 = (commandBus ? GetAggregateMinVersion(cartId, AggregateVersion(7), 2000)).mapTo[Try[Aggregate[ShoppingCart]]]
atLeast7.foreach {
  case Success(cart) =>
    println(s"Up to date: version ${cart.version.asInt}")  // version 7 or later
  case Failure(e: AggregateInIncorrectVersionException) =>
    println(s"Still at version ${e.currentVersion.asInt}, waiting for ${e.requestedVersion.asInt}")
  case Failure(error) =>
    println("Cannot read the cart: " + error.getMessage)
}
```

- When the aggregate is at that version or later, the answer comes at once.
- Otherwise the aggregate's owner holds the question and answers the moment that version is stored.
- If the version does not come within `maxMillis` milliseconds (2 seconds here), the answer is
  `Failure(AggregateInIncorrectVersionException)`, which says the version the aggregate is at and the
  one you asked for.

> **Remember:** your ask timeout is a separate clock. Make it longer than `maxMillis`, or your
> `Future` may fail before the owner had its chance to answer.

> **Tip:** a good habit for user interfaces: keep the version from the last command's answer, and
> pass it with the next read. Chapter 8 has a similar tool for projections, which always run a little
> behind the aggregates.

## Below the squash line

Long histories can be folded away with *squash* ([chapter 14](14-squash.md)): the events from
version 1 to N are replaced by a single squash event at version N, which holds the state they built.
The aggregate's version and state stay as they were. Its past, though, is shorter:

- `GetEventsForAggregate` starts with the squash event, at version N;
- reads of a version below N (`GetAggregateForVersion`, `GetEventsForAggregateForVersion`), and reads
  at an instant before the squash event's time, answer `Failure(AggregateSquashedException)`. The
  history they need is gone, and the library will not guess;
- reads of version N or later work as before.

<!-- snippet: cart/PastSpec.scala#squashed -->
```scala
case Failure(e: AggregateSquashedException) =>
  println(s"The history before version ${e.squashedUpTo.asInt} was folded away")
```

The exception also says which version you asked for (`requested`) and, for a read at an instant,
which instant (`at`).

### The archive

Some events matter for audits even after a squash: who approved the order, who added what. A squash
policy can name the event classes to keep, and the squash then moves those events into an archive
instead of deleting them. `GetArchivedEvents(id)` reads it:

<!-- snippet: cart/PastSpec.scala#archive -->
```scala
val archived = (commandBus ? GetArchivedEvents(cartId)).mapTo[Try[ArchivedEvents[ShoppingCart]]]
archived.foreach {
  case Success(archive) =>
    archive.events.foreach(a => println(s"${a.info.version.asInt}  ${a.info.event}  archived at ${a.archived}"))
  case Failure(error) =>
    println("Cannot read the archive: " + error.getMessage)
}
```

The events come oldest first. Each `ArchivedEvent` holds the event's `EventInfo` (`info`), the version
of the undo that cancelled it if one did (`undoneFrom`), and when it was archived (`archived`). For an
aggregate with nothing archived, `events` is simply empty. Restores, re-deliveries, rebuilds and the
other history reads never read the archive: it exists to be looked at. Chapter 14 shows how to choose
what goes into it.

## When a read fails

Two failures are worth telling apart:

- `Failure(AggregateSquashedException)`: not an error at all, but a fact about the history (above).
- `Failure(AggregateRestoreFailedException)`: the aggregate could not be read. Its cause is inside:
  usually a stored event that your code can no longer read (a changed or missing event class,
  chapter 6) or a problem with the database. The library also logs it, with the aggregate's id.

## Where the reads run

The historical reads (`GetEventsForAggregate`, `GetEventsForAggregateForVersion`,
`GetAggregateForVersion`, `GetAggregateAtInstant` and `GetArchivedEvents`) do not involve the
aggregate's owner at all. The command bus reads the store in the background, on the node you asked,
on the library's thread pool for database work. So they do not wake up a sleeping aggregate, do not
wait behind its commands, and see exactly what has been committed.

`GetAggregate` and `GetAggregateMinVersion` are different: the aggregate's owner answers them,
starting one if needed, because they are about the aggregate's present.

> **Tip:** these reads are about one aggregate at a time, which suits audit screens, "what changed?"
> panels, support tools and debugging. Questions across many aggregates ("every cart with oranges in
> it") belong to projections: that is the next chapter.

> **Under the hood:** the historical reads of states and events go through the restore module,
> `AggregateRestorer`, which hides duplication chains, undone events and snapshots; an owner uses it
> too, to read its aggregate when it starts. The archive is read straight from its table. See
> [ARCHITECTURE.md, section 4.3](../ARCHITECTURE.md).

## What you learned

- The command bus answers questions about an aggregate's past, and the answers are a `Try`, so
  always look inside.
- `GetEventsForAggregate` gives the whole diary as `EventInfo`s, with version, user, time and
  metadata; `GetEventsForAggregateForVersion` stops at a version.
- `GetAggregateForVersion` and `GetAggregateAtInstant` rebuild the state as it was at a version or a
  moment, with undos as they were then.
- `GetAggregateMinVersion` waits until the aggregate reaches a version, or fails with
  `AggregateInIncorrectVersionException` after `maxMillis`: the tool for reading your own writes.
- Below a squash point, history reads fail with `AggregateSquashedException`; events kept by the
  squash policy stay readable with `GetArchivedEvents`.
- Historical reads come from the store, on the asking node, without waking the aggregate's owner.

Next: [chapter 8](08-projections.md) turns events into views that people can query.

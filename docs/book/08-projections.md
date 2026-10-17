# Chapter 8. Projections: Views People Can Query

By the end of this chapter you will have a list of shopping carts in PostgreSQL that keeps itself
up to date, and you will be able to ask it questions such as "what is in cart 42?" or "which carts
hold more than three items?". You will meet the three kinds of listeners, the document store that
keeps the data, and a trick for showing people their own change the moment they make it.

---

## The noticeboard in the hallway

Remember the family diary from chapter 1? Reading the whole diary every time somebody asks "what is
on the list?" gets old fast. So someone pins a noticeboard in the hallway, and whenever a new line
goes into the diary, they update the board. Nobody writes on the board directly: it only ever
follows the diary. If the board gets ruined, you take it down and redo it from the diary.

The board is a **read model**: data shaped for reading. The person who keeps it up to date is a
**projection**. In ReactiveCQRS a projection is an actor, a subclass of `ProjectionActor`, that

- listens to the events of one or more aggregate types,
- writes what it learns into its read model, usually a **document store** (a PostgreSQL table of
  JSON documents),
- answers questions about it, like any actor: you send it a message, it replies.

```
cart owner ──events──▶ event bus ──▶ CartsList projection ──writes──▶ table projection_carts_list
                                         ▲           │
                       GetCartSummary(42)│           │Some(CartSummary(...))
                                         │           ▼
                                      your code (a web page, a report)
```

The test domain has two small projections of carts, in `ShoppingCartsListProjection.scala`: one
that listens to events and one that listens to states. In this chapter we build a slightly richer
cousin of the second one, step by step.

## The four blanks of a projection

Every projection fills in the same four blanks:

```scala
class MyProjection(val subscriptionsState: SubscriptionsState) extends ProjectionActor {
  override protected val projectionName: String = "MyProjection"
  protected val listeners = List(...)                      // what it follows, and what it does
  override protected def receiveQuery: Receive = { ... }   // the questions it answers
}
```

- **`subscriptionsState`** is the projection's notebook: for every aggregate, the last version it
  applied. Thanks to it, a projection knows where it stopped after a restart, and it recognises an
  event it has seen before (chapter 9 tells that story). You create one `PostgresSubscriptionsState`
  per system and give it to every projection.
- **`projectionName`** is a label for log lines and status reports.
- **`listeners`** say which aggregate types the projection follows, and what it does with their news.
- **`receiveQuery`** answers questions.

## Three ways to listen

Picture a football match you cannot go to. You can follow it on the radio, where the commentator
tells you every goal as it happens; you can glance at the scoreboard, which shows only the score
right now; or you can watch it on TV, with the commentary and the score in the corner. A projection
has the same three choices:

| Listener | Your function receives | Like |
|---|---|---|
| `EventsListener[R]` | `(AggregateId, Seq[EventInfo[R]])` | the radio: every new event |
| `AggregateListener[R]` | `(AggregateId, AggregateVersion, Boolean, Option[R])` | the scoreboard: the state now |
| `AggregateWithEventsListener[R]` | `(AggregateId, AggregateVersion, Option[R], Seq[EventInfo[R]])` | the TV: both |

`R` is the aggregate root class: `ShoppingCart` for carts. Each function returns another function,
`DBSession => Unit`, which is the work to do inside a database transaction (more on that soon).
The parameters, in order:

- **`AggregateId`**: which cart.
- **`Seq[EventInfo[R]]`**: the new events, oldest first. An `EventInfo` holds the `version`, the
  `event` itself, the `userId` of whoever caused it, its `timestamp` and its `metadata` (chapter 6).
  There can be several at once (a command that stored three events, or deliveries that arrived
  together), so always loop over them.
- **`AggregateVersion`**: the cart's version after these events.
- **`Boolean`**, the state listener's `created` flag: true when this is the first time the
  projection hears about the cart, normally at version 1. That is your cue to insert a new
  document rather than update one.
- **`Option[R]`**: the cart's state after these events. `None` means the cart is gone: the cart's
  event handler for `ShoppingCartDeleted` returns `null`, and the projection receives `None`.

A state listener sees the state after the whole batch, not each step on the way. That is exactly
what a scoreboard does.

Which should you pick?

- Choose the **state** (`AggregateListener`) when your read model is a picture of the current
  state, such as a list of carts with their names and sizes. Undo, duplication, history rewrites
  and squash (chapters 6, 14 and 15) all come for free, because the state you get is already right.
- Choose the **events** (`EventsListener`) when you care about what happened rather than where it
  ended: an activity log, how often milk gets added, who removed what.
- Choose **both** (`AggregateWithEventsListener`) when you need the picture and the story.

> **Remember:** an events listener gets every event class of its type: undo events, the squash
> event of chapter 14, and whatever classes you add next year. A `match` with no `case _ =>` that
> meets an event it does not know throws a `MatchError`, and a listener that throws is a failure
> (chapter 9). The test domain's events-based cart list only prints an apology when it sees an
> undo: working out what was undone is your job with events, and nobody's job with states.

Two small rules: one listener per kind per aggregate type, and the type must be a real class. List
two `EventsListener[ShoppingCart]`, or one for `Any` or `Nothing`, and the projection refuses to
start: the bus never gets its subscriptions and keeps waiting, and the log says why ("Projection ...
declares 2 EventsListeners of ...").

## Where read models live: document stores

Picture a filing cabinet with one drawer per kind of document and one folder per cart, each
labelled with the cart's number. That is a **document store**: a `DocumentStore[T]` keeps documents
of type `T` under keys of type `Long`, usually the aggregate's id (`cartId.asLong`).

- **`PostgresDocumentStore[T]`** keeps them in a table called `projection_<name>`, with the columns
  `space_id`, `id`, `version` and `document` (the document as `JSONB`, written by mpjsons). It
  creates the table, and the indexes you ask for, when you create the store.
- **`MemoryDocumentStore[T]`** keeps them in a map in memory. It is meant for tests (chapter 19);
  it ignores spaces, so its `clearSpace` does nothing.

```scala
import io.reactivecqrs.core.documentstore.{MultipleTextIndex, NoopDocumentStoreCache, PostgresDocumentStore}

case class CartSummary(name: String, itemCount: Int, version: Int)

val cartsStore = new PostgresDocumentStore[CartSummary](
  "carts_list",                           // the table: projection_carts_list
  mpjsons,                                // the MPJsons you made in chapter 4
  new NoopDocumentStoreCache,             // a cache that keeps nothing
  Seq(MultipleTextIndex(1, Seq("name")))) // an index on the JSON field "name"
```

- The **name** may hold letters, digits and underscores only, and the longest name the store makes
  (the table, or an index such as `projection_carts_list_idx_1`) must fit PostgreSQL's 63
  characters. Otherwise the store refuses to start with an `IllegalArgumentException`.
- The **cache**: `NoopDocumentStoreCache` keeps nothing, so every read goes to the database. A cache
  of your own implements the small `DocumentStoreCache` trait.
- The **indexes** speed up searches on JSON fields: `MultipleTextIndex`, `UniqueTextIndex`,
  `MultipleIntIndex`, `UniqueIntIndex`, `MultipleLongIndex`, `UniqueLongIndex`,
  `MultipleTextArrayIndex`, and `MultipleCombinedIndex` or `UniqueCombinedIndex` over several
  paths. The number (here `1`) names the index. At start, the store creates the indexes it does not
  have yet and drops those that are no longer on the list.

> **Warning:** the store recognises an index by its number alone. Change the path of index 1 but
> keep the number, and the old index stays as it was. Give a changed index a new number.

The operations you will use most:

| Call | What it does |
|---|---|
| `insertDocument(spaceId, key, document)` | adds a new document; fails if the key is taken |
| `overwriteDocument(key, document)` | replaces an existing document; fails if there is none |
| `updateDocument(spaceId, key, modify)` | reads, changes and writes back; `modify` gets an `Option[Document[T]]` (`None`: no document yet) and returns the new `Document[T]` |
| `updateExistingDocument(key, modify)` | the same, for a document that must exist |
| `removeDocument(key)` | deletes it |
| `getDocument(key)` | `Option[Document[T]]` |
| `getDocuments(keys)` | a `Map[Long, Document[T]]` of the ones found |
| `findAll()` | every document, as a `Map[Long, Document[T]]` |
| `countAll()` | how many there are |
| `findDocumentByPath(path, value)` | the documents whose text at `path` equals `value` |
| `findDocumentByPaths(conditions*)` | the documents that meet every condition, such as `ExpectedSingleLongValue(Seq("shop"), 7)` |
| `findDocument(query)` | a search with conditions, sorting and paging (later in this chapter) |
| `clearSpace(spaceId)`, `clearAllData()` | deletes one organization's documents, or all of them |

`Document[T]` is a tiny wrapper: `Document(summary)` wraps, `doc.document` unwraps. The `spaceId`
is the organization the document belongs to (chapter 16). All our carts live in space 0, because
that is what `ShoppingCartCreated` says (`spaceId = SpaceId(0)`).

> **Remember:** `updateDocument` may call your `modify` function more than once: if another writer
> changed the document in between, it reads the new one and tries again, up to 10 times. Keep
> `modify` pure: compute, don't act.

> **Tip:** the table's own `version` column counts the document's writes; it is not the cart's
> version. If you need the cart's version in the read model (and you will, later in this chapter),
> keep it inside the document, as `CartSummary` does.

## The transaction your listener runs in

Every write takes an implicit `DBSession`. Inside a listener, that session is a transaction the
projection opened for you, and the very same transaction records the projection's progress: "cart
42 is applied up to version 3". So the read model and the bookmark commit together or not at all.
If your listener throws halfway, neither the document nor the bookmark moves, and the projection
tries again (chapter 9).

That is why a listener is a function that takes the session. Mark it `implicit`, and the store
calls inside pick it up on their own:

```scala
AggregateListener[ShoppingCart] { (cartId, version, created, cart) => implicit session =>
  // every store call here runs in `session`
}
```

> **Warning:** never commit or roll back the session you are given, and never open a transaction of
> your own for the read model. When updates arrive quickly, the projection applies up to 100 carts'
> updates in one transaction; if one of them fails, it rolls them all back and applies each cart on
> its own, so that only the faulty cart is retried. A commit of yours would break that.

Outside a listener (in a script, a test or a repair job) you open the session yourself, for example
with ScalikeJDBC's `DB.localTx { implicit session => ... }`. Reads such as `getDocument` and
`findAll` need no session: without one, they open a read-only session of their own. Inside a
listener every read runs in the listener's session too, so it sees what the listener wrote a moment
before, and takes no second connection from the pool.

> **Under the hood:** the transaction comes from `SubscriptionsState.localTx`, and the projection's
> version moves with a compare-and-set, so two writers can never both move it. Grouping and its
> fallback are described in [ARCHITECTURE.md, section 6.3](../ARCHITECTURE.md); the document store
> in section 6.8; the notebook (`SubscriptionsState`) in section 7.

## Building the cart list, step by step

Our read model holds one document per cart: its name, how many items it holds, and the cart's
version. We reuse what chapter 4 built: the actor system `system`, `mpjsons`, `typesNames`,
the event bus `eventBus`, the user `alice`, and the cart command bus `carts`.

1. **Write down the document and the questions.**

   ```scala
   import io.reactivecqrs.api.id.AggregateId

   case class CartSummary(name: String, itemCount: Int, version: Int)

   case class GetCartSummary(cartId: AggregateId)   // answered with Option[CartSummary]
   case object GetAllCarts                          // answered with Map[AggregateId, CartSummary]
   case class FindCartsByName(name: String)         // answered with Vector[CartSummary]
   ```

2. **Write the projection.**

   ```scala
   import io.reactivecqrs.core.documentstore.DocumentStore
   import io.reactivecqrs.core.projection.{ProjectionActor, SubscriptionsState}
   import io.reactivecqrs.testdomain.shoppingcart.ShoppingCart

   class CartsListProjection(val subscriptionsState: SubscriptionsState,
                             store: DocumentStore[CartSummary]) extends ProjectionActor {

     override protected val projectionName: String = "CartsList"

     protected val listeners = List(
       AggregateListener[ShoppingCart] { (cartId, version, created, cart) => implicit session =>
         cart match {
           case Some(c) =>
             val summary = CartSummary(c.name, c.items.size, version.asInt)
             if (created) store.insertDocument(0, cartId.asLong, summary)
             else store.overwriteDocument(cartId.asLong, summary)
           case None =>
             store.removeDocument(cartId.asLong)
         }
       })

     override protected def receiveQuery: Receive = {
       case GetCartSummary(cartId) =>
         sender() ! store.getDocument(cartId.asLong).map(_.document)
       case GetAllCarts =>
         sender() ! store.findAll().map { case (id, doc) => AggregateId(id) -> doc.document }
       case FindCartsByName(name) =>
         sender() ! store.findDocumentByPath(Seq("name"), name).values.map(_.document).toVector
     }
   }
   ```

   The listener reads like the scoreboard it is: a new cart gets a document, a changed cart gets its
   document replaced, a deleted cart loses it. This is the test domain's
   `ShoppingCartsListProjectionAggregatesBased`, plus the item count and the version.

3. **Create the notebook and the store**, once per system and once per store:

   ```scala
   import io.reactivecqrs.core.projection.PostgresSubscriptionsState

   val subscriptionsState = new PostgresSubscriptionsState(typesNames).initSchema()
   val cartsStore = new PostgresDocumentStore[CartSummary]("carts_list", mpjsons,
     new NoopDocumentStoreCache, Seq(MultipleTextIndex(1, Seq("name"))))
   ```

   `initSchema()` creates the `subscriptions` table (the notebook) and `subscription_failures`
   (chapter 9) if they are not there yet.

4. **Start the projection on the blocking dispatcher.**

   ```scala
   import org.apache.pekko.actor.Props
   import io.reactivecqrs.core.util.BlockingDispatcher

   val cartsList = system.actorOf(
     Props(new CartsListProjection(subscriptionsState, cartsStore)).withDispatcher(BlockingDispatcher.id),
     "CartsList")
   ```

   A projection talks to PostgreSQL on its own thread. `BlockingDispatcher.id`, that is
   `reactivecqrs.blocking-dispatcher` (32 threads unless you configure it otherwise), is the pool the
   library keeps for that kind of work, so the rest of your system never waits behind a slow query.

5. **Put it on the event bus's list of subscribers.** In chapter 4 the host told the bus who its
   subscribers are with `RegisterSubscribers`. The projection goes on that list, together with
   every other projection, saga and listener of the system:

   ```scala
   import io.reactivecqrs.core.eventbus.EventsBusActor

   eventBus ! EventsBusActor.RegisterSubscribers(List(cartsList))
   ```

   The bus asks each subscriber what it wants (the projection answers by itself, one subscription
   per listener) and publishes nothing until every subscriber on the list has answered. While it
   waits, it logs `Event bus waits for the subscriptions of: ...` every few seconds, which is handy
   when you forgot to start one of them.

> **Warning:** start a projection on the default dispatcher and it still works, but it logs a
> warning at start ("runs its listener transactions on the default dispatcher; create it with
> Props(...).withDispatcher(BlockingDispatcher.id)"), and every query and transaction it runs holds
> up the threads the rest of Pekko needs. Always add `.withDispatcher(BlockingDispatcher.id)`.

> **Warning:** a projection started this way keeps its notebook under its **class name** (for
> example `com.example.CartsListProjection`), not under `projectionName`. Rename or move the class
> and the projection forgets what it applied: the next event of each cart makes it start that cart
> again from version 1, straight into documents that already exist. The managed projections of
> chapter 10 keep their notebook under a name you choose.

## Trying it out

Create a cart, add milk, and ask:

```scala
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import io.reactivecqrs.api._
import io.reactivecqrs.testdomain.shoppingcart._
import scala.concurrent.Await
import scala.concurrent.duration._

implicit val timeout: Timeout = Timeout(5.seconds)

def send(command: AnyRef): CustomCommandResponse[_] =
  Await.result((carts ? command).mapTo[CustomCommandResponse[_]], 5.seconds)

val SuccessResponse(cartId, _) = send(CreateShoppingCart(None, alice, "Groceries")) // a sketch: real code checks
send(AddItem(alice, cartId, AggregateVersion(1), "milk"))       // SuccessResponse(Id(42),Version(2))

Thread.sleep(500) // give the projection a moment; a better way comes later in this chapter

Await.result((cartsList ? GetCartSummary(cartId)).mapTo[Option[CartSummary]], 5.seconds)
// Some(CartSummary(Groceries,1,2))
```

Look in PostgreSQL and you will find the cart's row in `projection_carts_list`, with
`{"name": "Groceries", "version": 2, "itemCount": 1}` in its `document` column (PostgreSQL keeps
the keys of a `JSONB` value in an order of its own).

> **Remember:** a projection started this way hears only about events published after it joined.
> A cart that existed before shows up the next time it changes: the projection notices that it
> missed versions 1 to 3, asks the cart's owner for them, and then applies 1 to 4 in order (the
> library's `DeliveryEndToEndSpec` shows exactly that). A cart that never changes again never shows
> up. To fill a new projection with the whole history, start it as a managed projection (chapter 10).

## Asking sharper questions

`findDocument` takes a `DocumentStoreQuery`: conditions on JSON paths, a sort order, an offset and a
limit. A path is the list of field names leading to a value: `Seq("name")`, or
`Seq("address", "city")` for a nested one. Here are the biggest carts, biggest first:

```scala
import io.reactivecqrs.core.documentstore.{DocumentStoreQuery, ExpectedGreaterThanIntValue, SortDescInt}

case class FindBigCarts(moreThan: Int) // answered with Seq[(AggregateId, CartSummary)]

// one more case in receiveQuery:
case FindBigCarts(moreThan) =>
  val query = DocumentStoreQuery(
    where = Seq(ExpectedGreaterThanIntValue(Seq("itemCount"), moreThan)),
    sortBy = Seq(SortDescInt("itemCount")),
    offset = 0,
    limit = 20)
  sender() ! store.findDocument(query).map { case (id, doc) => AggregateId(id) -> doc.document }
```

The conditions all have to hold. Besides `ExpectedGreaterThanIntValue` there are
`ExpectedSingleTextValue`, `ExpectedSingleTextValueLike` (an SQL `ILIKE` pattern such as `"%groc%"`),
`ExpectedMultipleTextValues` (one of several), `ExpectedSingleIntValue`, `ExpectedSingleLongValue`,
`ExpectedSingleBooleanValue`, `ExpectedLessThanIntValue`, `ExpectedNoValue` and a few more for
arrays. Sorts come as `SortAscText`, `SortDescText`, `SortAscInt`, `SortDescInt`, `SortAscLong` and
`SortDescLong`. A `limit` of `-1` means no limit, and `DocumentStoreQuery.basic(where)` is the
shortcut for "these conditions, no sorting, no limit".

> **Tip:** a query with an empty `where` finds every document, sorted and paged as you ask; for all
> of them at once, `findAll()` is simpler.

What happens when a query goes wrong? A query that throws is answered with `Status.Failure`, so
your `ask` fails with that exception, and the projection carries on with its work.

> **Remember:** queries and updates take turns in the same mailbox. Before it answers a query, the
> projection applies every delivery it has already received, so answers are consistent; but while
> a query runs, no update is applied. Keep queries quick: look up by key, search through indexes,
> and page with `limit`.

## The same idea with events: an activity log

Some read models are stories, not pictures. Here is a log of everything that happened to each
cart, built from events:

```scala
import io.reactivecqrs.api._
import io.reactivecqrs.core.documentstore.Document

case class CartActivity(lines: Vector[String])
case class GetCartActivity(cartId: AggregateId) // answered with Option[CartActivity]

class CartActivityProjection(val subscriptionsState: SubscriptionsState,
                             store: DocumentStore[CartActivity]) extends ProjectionActor {

  override protected val projectionName: String = "CartActivity"

  protected val listeners = List(
    EventsListener[ShoppingCart] { (cartId, events) => implicit session =>
      val lines = events.map(e => "v" + e.version.asInt + ", user " + e.userId.asLong + ": " + describe(e.event))
      store.updateDocument(0, cartId.asLong, existing =>
        Document(CartActivity(existing.map(_.document.lines).getOrElse(Vector.empty) ++ lines)))
    })

  private def describe(event: Event[ShoppingCart]): String = event match {
    case ShoppingCartCreated(name) => "created \"" + name + "\""
    case ShoppingCartDuplicated(_, baseId, baseVersion) => "copied from cart " + baseId.asLong + " at version " + baseVersion.asInt
    case ItemAdded(name) => "added " + name
    case ItemRemoved(itemId) => "removed item " + itemId
    case ShoppingCartChangesUndone(count) => "undid the last " + count + " change(s)"
    case ShoppingCartDeleted() => "deleted the cart"
    case other => other.getClass.getSimpleName // anything else, the squash event included
  }

  override protected def receiveQuery: Receive = {
    case GetCartActivity(cartId) => sender() ! store.getDocument(cartId.asLong).map(_.document)
  }
}
```

Two things to notice. `updateDocument` inserts the document when there is none yet (`existing` is
`None`), so this listener needs no `created` flag. And an undo is simply one more line in the log:
for a story, events are the natural fit. You start it and register it exactly like the cart list.

## Seeing your own change straight away

Alice adds bread, and her page reloads the cart at once. The command has succeeded, but its event
may still be on its way to the projection, so the page shows the cart without the bread. Annoying!

A cloakroom attendant faces this all day: you hand in your coat and ask for it back the next
second. A good attendant does not say "we have no coat of yours"; they say "one moment", and look
again once the coat reaches the rack. A **delayed query** is that attendant: when the answer is not
there yet, the projection waits for its next updates before it says no.

You know what to wait for: the command's answer, `SuccessResponse(cartId, version)`, tells you the
version your change created. So the question becomes "cart 42, as soon as it is at version 3 or
later":

```scala
case class GetCartSummaryAtLeast(cartId: AggregateId, version: AggregateVersion) // answered with Option[CartSummary]

// one more case in receiveQuery:
case GetCartSummaryAtLeast(cartId, version) =>
  delayIfNotAvailable(sender(), () =>
    store.getDocument(cartId.asLong).map(_.document).filter(_.version >= version.asInt),
    2000)
```

`delayIfNotAvailable(respondTo, search, forMaximumMillis)` runs `search` at once. A `Some` goes to
`respondTo` straight away. A `None` parks the query: the projection runs the search again after it
applies updates, and answers as soon as it finds something. When 2 000 milliseconds pass without
luck, it searches one last time and answers what it finds, `None` if still nothing. On the calling
side:

```scala
send(AddItem(alice, cartId, AggregateVersion(2), "bread")) match {
  case SuccessResponse(_, version) =>
    Await.result((cartsList ? GetCartSummaryAtLeast(cartId, version)).mapTo[Option[CartSummary]], 5.seconds)
    // Some(CartSummary(Groceries,2,3)), as soon as the projection has it
  case other => println("Not added: " + other)
}
```

> **Tip:** make the ask timeout (here 5 seconds) longer than the wait (2 seconds), so that you get
> the projection's `None` rather than an `AskTimeoutException`.

> **Tip:** this works because the document carries the cart's version. Put the aggregate's version
> into every document you want to read your own writes from. When you need the cart itself rather
> than a read model, ask its command bus with `GetAggregateMinVersion` (chapter 7).

The plain version runs its search on the projection's thread, which is fine for a lookup by key.
For a heavier search you can hand the work to a `Future`:

| Method | `search` returns | The answer |
|---|---|---|
| `delayIfNotAvailable(respondTo, search, maxMillis)` | `Option[T]` | `Some(t)`, or the last search's answer at the deadline |
| `delayIfNotAvailableAsync(respondTo, search, maxMillis)` | `Future[Option[T]]` | `Some(t)`, or the last search's answer at the deadline |
| `delayIfNotAvailableAsyncCustom(respondTo, search, found, maxMillis)` | `Future[T]`, plus `found: T => Boolean` | the `T` once `found` says yes, or the last search's `T` at the deadline |

Each one also comes in a form that takes the deadline as an `Instant` instead of milliseconds. A
search that throws, or a `Future` that fails, is answered with `Status.Failure`.

> **Under the hood:** a parked query waits inside the projection (as a private `SyncDelayedQuery`,
> `AsyncDelayedQuery` or `AsyncDelayedQueryCustom`; you never create those yourself). It is searched
> again once per burst of applied updates, not once per update, and at its deadline it gets one last
> search. An async search still running when an update is applied runs again once it answers, so
> it never misses that update. See [ARCHITECTURE.md, section 6.5](../ARCHITECTURE.md).

## Options worth knowing

A projection takes its options in its constructor, as a `ProjectionActorOptions`. Start from
`ProjectionActorOptions.DEFAULT` and change what you need:

```scala
import io.reactivecqrs.core.projection.ProjectionActorOptions

class CartsListProjection(val subscriptionsState: SubscriptionsState, store: DocumentStore[CartSummary])
  extends ProjectionActor(ProjectionActorOptions.DEFAULT.withGroupUpdatesDelayMillis(200).withMinimumDelayVersion(2)) {
  // ... as before
}
```

| Option (default) | What it does | How to set it |
|---|---|---|
| `groupUpdatesDelayMillis` (0) | waits this long before applying a cart's deliveries, so that several updates of one cart are applied together | `withGroupUpdatesDelayMillis(200)` |
| `minimumDelayVersion` (1) | deliveries that start below this version never wait; with 2, a new cart shows up at once | `withMinimumDelayVersion(2)` |
| `eventsToProcessImmediately` (none) | event classes that never wait (for listeners that receive events) | `withEventsToProcessImmediately(classOf[ShoppingCartDeleted])` |
| `maxBatchesPerTransaction` (100) | when deliveries pile up, how many carts' updates one transaction may apply; 1 applies each cart alone | `.copy(maxBatchesPerTransaction = 1)` |
| `parallelUpdateProcessing` (0) | above 1, that many threads apply updates; each cart still keeps its order | `withParallelUpdateProcessing(4)` |
| `eventsLogger` (none) | a logger that gets a debug line for every delivery and every applied batch | `withEventsLogger(LoggerFactory.getLogger("carts-list"))` |
| `retries`, `retryFailuresOnStart`, `gapRequestRepeat`, `gapGiveUpAfter`, `failureRetryInterval` | what happens when things go wrong | chapter 9 |

The first three only matter together: with `groupUpdatesDelayMillis` at 0 (the default), nothing
waits anyway. `withEventsLogger` takes an SLF4J `Logger`.

> **Warning:** with `parallelUpdateProcessing`, your listeners run on several threads at once, on the
> same objects: they must be thread-safe, and the JDBC pool needs at least that many connections.
> It also turns off the grouping of `maxBatchesPerTransaction`. Most projections are fine without it.

> **Tip:** for a projection with no queries at all, such as one that only feeds another system,
> extend `AggregateListenerActor` instead of `ProjectionActor`: it is the same thing with a
> `receiveQuery` that ignores every message. Chapter 9 uses one.

## What you learned

- A **projection** is a `ProjectionActor` with listeners, a read model and queries; its notebook,
  the `SubscriptionsState`, records per aggregate the last version it applied.
- There are three **listeners**: `AggregateListener` for the state (pictures),
  `EventsListener` for the events (stories), and `AggregateWithEventsListener` for both.
- A **document store** keeps JSON documents by key: `PostgresDocumentStore` in a table
  `projection_<name>` with a `space_id` column, `MemoryDocumentStore` for tests.
- A listener runs in a **transaction** that also records the projection's progress; pass it to the
  store and never commit it yourself.
- Start a projection with `Props(...).withDispatcher(BlockingDispatcher.id)` and put it on the
  bus's `RegisterSubscribers` list.
- **Delayed queries** (`delayIfNotAvailable` and friends) let people read their own writes.

Next: [chapter 9](09-projection-failures.md) breaks things on purpose, and shows how projections
recover.

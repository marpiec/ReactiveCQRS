# Chapter 10. Rebuilding Projections Without Downtime

By the end of this chapter you will be able to ship a new version of a projection, fix a bug in
one, or add a brand-new read model over years of old events, while the old read model keeps
answering queries the whole time. You will also repair the document of a single cart without
touching the rest. The trick is called a *managed projection*, and it costs you about ten lines of
wiring.

---

## Why rebuild at all?

Remember the bank from chapter 1: the ledger records every transaction, and the summary in your
banking app is a projection of it. One day the bank decides the summary should show monthly totals
too. Nobody asks the customers to type their transactions in again. The bank reads the ledger from
the first line and writes a new summary.

You will want the same for your read models, for everyday reasons:

- **A new version.** The cart list should show names in capitals, or an item count next to each
  name.
- **A bug fix.** A listener wrote wrong data for months. You fix the code, and the wrong rows must
  be written again.
- **A new read model.** A projection you add today must know about carts created last year. A plain
  projection (chapter 8) hears about an old cart only when that cart changes again.
- **A changed aggregate.** You changed how a cart's events build its state and raised the context's
  `version`. Read models made from the states have to be made again.
- **A repair.** One document got damaged (a hand-written SQL fix gone wrong, say), and you want just
  that one written again from its events.

## The shop next door

Rebuilding a read model in place has an ugly side: while you empty its tables and fill them again,
anyone who asks gets half an answer. So ReactiveCQRS does not rebuild in place.

Picture a shop that needs new shelves. Instead of closing for a week, you fit out the empty shop
next door and stock it from the warehouse records, while the old shop keeps selling. When the new
one is ready, you move the sign over the door. Customers never find the door locked.

That is how a **managed projection** works:

```
                                ┌── build 1 (version 1), tables *_v1 ── answers queries, takes live events
you ── queries ──▶ supervisor ──┤
                                └── build 2 (version 2), tables *_v2 ── filled from the event store
```

- A **build** is one copy of the projection, with tables of its own. Builds are numbered 1, 2, 3...
- The **active build** answers queries. The **build in progress** is being filled from the event
  store.
- The **supervisor** is the actor you talk to. It runs the builds, passes your queries to the active
  one, and switches to the new one once it has caught up: one update in the database, the sign
  moved.
- The **registry** is a few tables where the library remembers, per projection, the active build and
  the build in progress.

After the switch, the old build's tables are dropped. You never write any of that bookkeeping.

## Setting up managed projections

We will manage a small projection of cart names, modelled on `ShoppingCartsListProjectionAggregatesBased`
in the test domain. It keeps one document per cart, holding the cart's name:

```scala
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.documentstore.DocumentStore
import io.reactivecqrs.core.projection.{ProjectionActor, SubscriptionsState}
import io.reactivecqrs.testdomain.shoppingcart.ShoppingCart

case class GetCartName(id: AggregateId)

class CartNames(val subscriptionsState: SubscriptionsState, store: DocumentStore[String]) extends ProjectionActor {

  protected val listeners = List(AggregateListener[ShoppingCart] { (id, _, created, cart) => implicit session =>
    cart match {
      case Some(c) if created => store.insertDocument(0, id.asLong, c.name)
      case Some(c)            => store.overwriteDocument(id.asLong, c.name)
      case None               => store.removeDocument(id.asLong) // the cart was deleted
    }
  })

  override protected def receiveQuery: Receive = {
    case GetCartName(id) => sender() ! store.getDocument(id.asLong).map(_.document)
  }

  override protected val projectionName: String = "CartNames"
}
```

Nothing here is new since chapter 8. What changes is how you start it. Four steps:

1. **Create the registry**, once, when the application starts:

   ```scala
   import io.reactivecqrs.core.rebuild.PostgresProjectionRegistry

   val registry = new PostgresProjectionRegistry().initSchema()
   ```

2. **Create one `Projections`** for the actor system. It runs every managed projection:

   ```scala
   import io.reactivecqrs.core.rebuild.{Projections, ProjectionsSettings}

   val projections = new Projections(system, ProjectionsSettings(eventStore, subscriptionsState, registry,
     contexts = Seq(new ShoppingCartAggregateContext)))
   ```

   `eventStore` is where builds read events from, and `subscriptionsState` is the one your
   projections already use (chapter 8). `contexts` lists the aggregate contexts whose *states* your
   managed projections receive: `CartNames` listens to cart states, so the cart context goes in. Three
   more parameters have defaults: `builder` (how a build reads, `BuilderSettings()`),
   `buildsAtOnce = 2`, and `restartAfter = 10.seconds` (how long the supervisor waits before it tries
   again after something failed).

3. **Start the projection** through it:

   ```scala
   import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}

   val cartNames: ActorRef = projections.start[CartNames]("cart-names", 1, "cartNames") { build =>
     new CartNames(subscriptionsState,
       new PostgresDocumentStore[String](build.documentStore("cart_names"), mpjsons, new NoopDocumentStoreCache))
   }
   ```

   Read it from left to right: the projection's class, its **name** (`"cart-names"`), its
   **version** (`1`), the actor name of its supervisor (`"cartNames"`), and a function that makes one
   instance of the projection for a given build. The function asks the build for the table name:
   `build.documentStore("cart_names")` gives `cart_names_v1` for the first build of version 1, and
   the document store makes the table `projection_cart_names_v1` from it.

4. **Use the supervisor wherever you would have used the projection**: register it with the event
   bus, and send it your queries.

   ```scala
   import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
   import org.apache.pekko.pattern.ask

   eventBus ! RegisterSubscribers(Seq(cartNames)) // together with your other subscribers, in one message

   val name: Future[Option[String]] = (cartNames ? GetCartName(cartId)).mapTo[Option[String]]
   ```

The first time this runs, the registry has never heard of `cart-names`, so the supervisor starts
build 1 and fills it with every cart already in the store. Build 1 is all there is, so it answers
queries from the start, with whatever it has so far. Once it has caught up, it simply carries on
with live events. From now on the registry says: `cart-names`, active build 1, version 1.

> **Remember:** the name is the projection's identity in the database. To change a projection,
> change its version and keep its name.

> **Warning:** make every store inside the function, and name every table through the build:
> `build.documentStore(table)` for a document store, `build.table(name)` and `build.sequence(name)`
> for tables and sequences of your own. Then two builds never share a table, and a dropped build
> takes all its tables with it. A store (or a cache) made once, outside the function, would be
> shared by two builds and would mix their documents.

> **Tip:** keep table names short. They may hold letters, digits and underscores only, the build
> adds a suffix such as `_v3_b4`, the document store adds `projection_` in front, and PostgreSQL
> allows 63 characters: a store whose full name would be longer fails as it is created.

You do not need `.withDispatcher(BlockingDispatcher.id)` for managed projections, by the way: the
supervisor creates every instance on the blocking dispatcher for you.

## Shipping a new version

Say the shop wants cart names in capital letters. Here is the whole release:

1. Change the listener to store `c.name.toUpperCase`, in both `insertDocument` and
   `overwriteDocument`.
2. Raise the version in `start`: `projections.start[CartNames]("cart-names", 2, "cartNames") { ... }`.
3. Deploy and start the application.

That's all you do. What happens next:

- The supervisor reads the registry: the active build is at version 1, the code at version 2. It
  starts build 2, with tables of its own (`projection_cart_names_v2`).
- Build 1 keeps answering queries.
- A builder reads the carts from the event store and hands them to build 2, many at a time, one
  transaction per chunk.
- Once build 2 has every event, the registry switches in one update. Build 2 answers from then on.
- Build 1 leaves the event bus and stops; its tables, subscription versions and failure records are
  dropped.

The log tells the story. With our projection it reads like this (other lines left out):

```
Projection cart-names serves build 1 (version 1), builds 2 (version 2)
Projection projection:cart-names_v2 is live; reconciling it with the store
Projection cart-names switched to build 2 (version 2)
Projection cart-names: build 1 is dropped
```

You can also ask the supervisor. `GetBuilds` is answered with `Builds(name, active, building,
caughtUp)`:

```scala
import io.reactivecqrs.core.rebuild.ProjectionSupervisor._

(cartNames ? GetBuilds).mapTo[Builds]
// while it builds:  active = build 1, building = build 2
// after the switch: active = build 2, building = None
```

`active` and `building` are `BuildRecord`s: the build's `number`, its `version`, its table `suffix`
(`"_v2"`), and the `subscriber` name its subscription versions are stored under
(`"projection:cart-names_v2"`). `caughtUp` is true once the build in progress has every event; it
replaces the active build as soon as it has no failed subscription (more on that below). The
building record's `position` is where the build saved its progress last.

To watch a build at work, ask the same supervisor for `GetBuildStatus`:

```scala
import io.reactivecqrs.core.rebuild.ProjectionBuilder.{BuildStatus, GetBuildStatus}

(cartNames ? GetBuildStatus).mapTo[BuildStatus]
// BuildStatus("Bulk", 182044, 91000, 0, 0, 0): reading the store, 91 000 events sent so far
```

The `phase` goes `Bulk` (reading the store), `GoingLive` and `Reconciling` (joining the live
events), then `CaughtUp`. Without a builder at work the supervisor answers `Waiting` (the build
waits for its turn, or for a restart after a failure) or `NoBuild`. The other numbers count the
events sent, the gaps filled, the aggregates handed over to live delivery and the events that
could not be read.

> **Remember:** a build that is interrupted (you stop the application, or it crashes) carries on
> from where it was at the next start; it does not begin again. But deploy version 3 while version 2
> is still building, and the half-built version 2 is thrown away: version 3 builds instead.

## What the old build does in the meantime

While version 2 builds, version 1's tables answer queries. But version 1's *code* is gone: you
deployed version 2. So what about the events that arrive meanwhile? You choose, with the
`whileBuilding` parameter of `start`:

- **`ServeLive`** (the default): the active build keeps applying live events to its old tables, with
  the new code. Queries see fresh data, and queries parked until a document appears (chapter 8)
  are answered. Choose it when the new code can work on the old documents, as the capital-letters
  version can: both versions store a `String`.
- **`ServeStale`**: the active build answers from its tables as they are, and takes no events until
  the switch. Choose it when the new code cannot work on the old documents. Say version 3 stores a
  `CartSummary(name, itemsCount)` instead of a `String`: version 3's code cannot read version 2's
  rows.

```scala
projections.start[CartNames]("cart-names", 3, "cartNames", whileBuilding = ServeStale) { build => ... }
```

Both live in `io.reactivecqrs.core.rebuild.ProjectionSupervisor`.

> **Tip:** the function is given the build, and the build knows its own version (`build.version`).
> An instance made for the active build of version 1 can keep version 1's behaviour while version 2
> builds. If `CartNames` took a `capitals` flag, you would write
> `new CartNames(subscriptionsState, store, capitals = build.version >= 2)`. `ProjectionsSpec` does
> exactly this.

## What your projection sees while it is built

A build hands your projection the same events, in the same order per aggregate, that live delivery
did, undo events included. So the rebuilt tables come out the same as tables kept live;
`RebuildEquivalenceSpec` checks this on random histories. Three things are different, and a
projection can tell:

- **`rebuilding`** is `true` while the instance is being built and does not serve yet, and while a
  targeted rebuild (below) gives it old events again. Anything with a side effect, such as a message
  to another actor or a push update to a screen (chapter 11), checks it first. `ManagedNotes` in
  `ProjectionsSpec` pushes its updates like this:

  ```scala
  if (!rebuilding) sendUpdate[NoteLines](lines)
  ```

- **`onActivated()`** is called once, when a built instance starts to serve. Override it if your
  projection has something to finish then.
- **Chunks.** A build commits many aggregates per transaction and resumes after a stop from its last
  saved chunk. A listener that keeps state in memory between calls, outside its tables, cannot rely
  on that state while it is built.

## When does a build start?

Four things start one:

1. **A new projection.** The first start of a name the registry does not know builds it.
2. **A new version.** The code's version differs from the active build's.
3. **A changed context.** The `version` of an aggregate context whose states the projection
   receives differs from the one the active build was made with. Raise
   `ShoppingCartAggregateContext.version`, and `CartNames` builds again at its own version. A
   projection that listens to cart events only (an `EventsListener`) keeps its build.
4. **You ask for it**, with `RebuildProjection` (next section).

At most two builds run at once (`buildsAtOnce`); the others wait for their turn.

## Asking for a rebuild

Fixed a bug in a listener without changing anything else? Ask for a fresh build of the same
version:

```scala
cartNames ? RebuildProjection
// answers RebuildStarted(2): build 2 of version 1, in tables with the suffix "_v1_b2"
```

The same dance follows: build 2 fills from the store while build 1 serves, then the switch and the
drop. While a build or a targeted rebuild is already running, the supervisor answers
`RebuildRefused(reason)` instead.

> **Tip:** a rebuild can also be the quickest cure for a projection that fell far behind, say after
> a long outage: a build reads the store a thousand events at a time, while catching up through the
> event bus and the outbox goes aggregate by aggregate (`docs/SCALING.md`, sections 4 and 5).

## A build that will not switch

A build replaces the active one only when it has no failed subscription. If a listener fails on
some cart during the build (chapter 9 explains failures), the build waits: the supervisor logs a
warning and looks again every 30 seconds. The active build serves as usual meanwhile.

To reach the build in progress, wrap your message in `ToBuildInProgress`. The supervisor forwards it
to the build instead of the active one:

```scala
import io.reactivecqrs.core.projection.{GetProjectionStatus, RetryAllFailedSubscriptions, SkipFailedVersion}

cartNames ? ToBuildInProgress(GetProjectionStatus)          // the build's ProjectionStatus
cartNames ? ToBuildInProgress(RetryAllFailedSubscriptions)  // FailedSubscriptionsRetried(count)
cartNames ? ToBuildInProgress(SkipFailedVersion(cartId))    // FailedSubscriptionsHandled(cartId, count)
```

Once the failures are retried or skipped, the build switches at its next look. With no build
running, the answer is `RebuildRefused("no build is running")`.

## Repairing a few aggregates: targeted rebuilds

Sometimes one page of a catalogue has a typo. You reprint that page, not the whole catalogue. A
**targeted rebuild** does the same for a read model: it rebuilds, in place, what the active build
made from one aggregate, one aggregate type or one organization, and leaves everything else alone.

First, the projection has to know how to clear what it made from a target. Override
`clearForRebuild` in `CartNames`:

```scala
import io.reactivecqrs.core.projection.RebuildTarget
import scalikejdbc.DBSession

override protected def clearForRebuild(target: RebuildTarget)(implicit session: DBSession): Unit = target match {
  case RebuildTarget.OneAggregate(id) => store.removeDocument(id.asLong)
  case RebuildTarget.OneSpace(space)  => store.clearSpace(space.asLong)
  case RebuildTarget.OneType(_)       => store.clearAllData() // CartNames receives carts only
}
```

That is the pattern of `ManagedNotes` in `ProjectionsSpec`. Then ask the supervisor:

```scala
import io.reactivecqrs.api.AggregateType
import io.reactivecqrs.api.id.SpaceId

cartNames ? RebuildAggregates(RebuildTarget.OneAggregate(cartId))
// answers AggregatesRebuilt(RebuildTarget.OneAggregate(cartId), 1)

cartNames ? RebuildAggregates(RebuildTarget.OneType(AggregateType.of[ShoppingCart])) // every cart
cartNames ? RebuildAggregates(RebuildTarget.OneSpace(SpaceId(7)))                    // organization 7
```

What happens:

1. The supervisor writes the target down in the registry, so a rebuild cut short by a stop runs
   again at the next start.
2. The active build applies the deliveries it already took, lists the target's aggregates and, in
   one transaction, calls your `clearForRebuild` and forgets how far it got with those aggregates.
3. A builder reads their events from the store, in the order they were committed, and gives them to
   the projection again. The projection keeps serving, and keeps taking live events, meanwhile.
4. The answer is `AggregatesRebuilt(target, count)`, with the number of aggregates rebuilt.

Between steps 2 and 4, queries about the target find its documents missing or half rebuilt.

The supervisor answers `RebuildRefused(reason)` instead when:

- the projection does not implement `clearForRebuild` (the reason says so);
- a build is in progress (it replaces the active build whole anyway), or another targeted rebuild
  is;
- the target is not of an aggregate type the projection receives;
- the target is an organization, and some aggregates of the projection's types do not know their
  organization yet. Aggregates stored by older versions of the library may lack it;
  `backfillSpaces` fixes that, once (chapter 16).

> **Warning:** clear exactly what the target's aggregates produced. Clear less, and stale data stays
> behind. Clear more, and it is gone for good: the rebuild gives back only the target's events.
> `clearAllData()` for `OneType` is right in `CartNames` only because it receives a single type.

> **Tip:** an ask has a timeout, and rebuilding every cart takes far longer than rebuilding one. Give
> the ask a timeout to match, or send the message from an actor that waits for the answer. A
> timed-out ask does not stop the rebuild.

## What stays unmanaged: side effects

Some subscribers are not read models at all: one that sends a welcome e-mail when a cart is created,
one that sends a command to another aggregate. Rebuilding those would be a small disaster: every
e-mail ever sent would go out again.

Keep them as plain projections, created and registered the way chapter 8 shows, not through
`Projections`. The library never rebuilds them: a build delivers events only to the build it
fills. Where to draw the line:

- the side effect **is** the subscriber's job: keep it unmanaged;
- the side effect is a small extra of a read model (a push update to a screen): manage the read
  model, and guard the extra with `if (!rebuilding)`.

## Retiring a projection

Removed a projection from the code? Its builds' tables and bookkeeping stay in the database until
you retire it, including its failure records, which would otherwise hold up permanent deletes of
the aggregates they name. Call this when the application starts:

```scala
projections.retire("cart-names")
```

It drops every build of the projection and forgets the name. In a cluster in the middle of a rolling
upgrade, it waits until every node runs the new version (chapter 17).

## How long it takes

A build reads every event of the types the projection listens to. Measured on one machine (the
bench's `baseline-M4` run of 17/10/2026: Windows 10, 12 CPU threads, OpenJDK 17 with a 2 GB heap,
PostgreSQL 17.4 on the same machine): two new managed projections over 112 211 events were both
built and switched about 16.6 seconds after the system started. The events-based one took 6 769
events per second, and the heap peaked at 150 MB. That is one run on one machine: good for
orientation, not a promise for yours.

From that rate, `docs/SCALING.md` estimates about 4.1 hours to rebuild an events-based projection
over 100 million events (estimated, not measured). A rebuild costs time and database load, not
downtime: the active build serves throughout.

Things that help:

- **State-only projections do less work.** When every listener of a projection is an
  `AggregateListener`, as in `CartNames`, the build skips the history: it hands over each
  aggregate's final state, once.
- **Snapshots help builds too.** A build that replays events in commit order keeps at most 10 000
  aggregate states open; a state it dropped is read again from its snapshot (chapter 13) and the
  events after it.
- **Two builds at a time** (`buildsAtOnce = 2`) is the default and the recommendation: every build
  reads the store and writes tables of its own.
- **Large events?** `ProjectionsSettings(..., builder = BuilderSettings(pageSize = ..., chunkEvents
  = ...))` sets how many events a build reads and applies at once: 1 000 each by default, and at
  most 64 MB per page and 16 MB per chunk. `BuilderSettings` lives in
  `io.reactivecqrs.core.rebuild.ProjectionBuilder`.
- **Upgraded installations:** a build of one type or one organization reads only its own events
  once every stored event has a key. An installation that already had events when keys were
  introduced runs `backfillEventKeys()` once (chapter 18); until then, such builds walk the whole
  store.

> **Under the hood:** a build has three phases. The bulk pass reads the projection's events in
> commit order (a state-only projection gets final states instead) and applies them in chunks of up
> to 1 000 events, one transaction each, two chunks in flight; meanwhile the new build acknowledges
> the bus's deliveries without applying them, as their events are in the store. Then it goes live
> and applies the bus's deliveries. Last, a reconciliation compares every aggregate's version with
> the build's and delivers what is missing: events stored while the pass ran, and rows that
> committed late. The position is saved after every chunk, so a resumed build repeats at most the
> chunks in flight, which the subscription versions make harmless (D57–D63, D70). The full story is
> in [ARCHITECTURE.md, section 8](../ARCHITECTURE.md) and [the M4 design](../design/M4-rebuild.md).

## What you learned

- Start a read model with `projections.start[P](name, version, actorName) { build => ... }`, and
  use the returned supervisor for queries and in `RegisterSubscribers`.
- Every build has tables of its own, named through the build (`build.documentStore(table)`).
- Raising the version builds the new version from the event store while the old one serves, then
  switches in one update and drops the old tables; `whileBuilding` says what the old build does
  meanwhile.
- `RebuildProjection`, `GetBuilds`, `GetBuildStatus` and `ToBuildInProgress` start, watch and
  unblock builds.
- `RebuildAggregates` repairs one aggregate, type or organization in place, through your
  `clearForRebuild`.
- Subscribers whose job is a side effect stay unmanaged, and are never rebuilt.

Next: [chapter 11](11-subscribable-projections.md) pushes live updates from your projections to the
screens that watch them.

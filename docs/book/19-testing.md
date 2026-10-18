# Chapter 19. Testing Your Domain

Event-sourced code is pleasant to test: the rules are plain functions, the history is plain data,
and the library ships in-memory stand-ins for its database parts. By the end of this chapter you
will test your aggregates at three levels: their handlers as plain functions, a whole system in
memory, and the real thing on PostgreSQL. You will also see how to guard the format of stored
events, how to check the messages that cross nodes, and how the library runs a cluster inside one
test.

---

## A test kitchen with three counters

A good chef tests a new dish three ways. First she tastes the sauce on its own: quick, and she can do
it twenty times an hour. Then she cooks the whole dish on a camping stove, to see that the parts work
together. Finally she serves it once in the real kitchen, with the real ovens, before it goes on the
menu.

Tests of a ReactiveCQRS domain have the same three counters:

| Level | What runs | Needs | Catches |
|---|---|---|---|
| 1. Plain functions | your command and event handlers, called directly | nothing | wrong rules, wrong state changes |
| 2. In memory | the real actors (command bus, owners, event bus, projections) over in-memory stores | an actor system | wiring, versions, concurrency, idempotency, what a projection makes |
| 3. PostgreSQL | everything, as in production | a local PostgreSQL | stored formats, SQL, transactions, delivery through the outbox |

Write many tests at level 1, a good number at level 2, and enough at level 3 to cover every path that
touches the database. A fourth level, several nodes in one JVM, comes at the end of the chapter.

## What the library gives you for testing

Two kinds of help:

- **In the `core` module:** the in-memory stand-ins (this chapter calls them *doubles*) and
  `RecordingMetrics`. Your tests can use them directly.
- **In the test kit**, `reactivecqrs-testkit`, a module of its own for your tests:

  ```scala
  libraryDependencies += "io.reactivecqrs" %% "reactivecqrs-testkit" % "1.0.0-SNAPSHOT" % Test
  ```

  It holds `TestDatabase` (a schema per test run), `TestIds`, `TestClock`, `SchemaReference` (the
  golden test of stored formats), `TestSystems` (actor systems that run as a cluster of one when you
  ask), `TestCluster` (several nodes in one JVM) and `ActorAskSupport` (`askFor`, an ask that waits
  for the answer), all in `io.reactivecqrs.testkit`. It depends on no test framework: its helpers
  return values or throw.

The examples below are the specs of E11, in `examples/src/test/scala/io/reactivecqrs/examples/e11/`,
and they test the carts of chapter 2's E1. They are written with ScalaTest 3.2's feature specs and
`must` matchers, and Pekko's TestKit (`pekko-testkit`, which your build adds itself). Those that need
actors extend a small base class, which you write once:

<!-- snippet: e11/ShopSpec.scala#base -->
```scala
import io.reactivecqrs.testkit.{ActorAskSupport, TestSystems}
import org.apache.pekko.testkit.TestKit
import org.scalatest.featurespec.AnyFeatureSpecLike
import org.scalatest.matchers.must.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

/** Pekko's TestKit on an actor system of the test kit: a cluster of one when the tests run with
  * -Dreactivecqrs.test.clusterOfOne=true. */
abstract class ShopSpec extends TestKit(TestSystems.newSystem()) with AnyFeatureSpecLike
  with GivenWhenThen with ActorAskSupport with Matchers with BeforeAndAfterAll {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)
}
```

> **Remember:** every example in this chapter is a spec that runs with the library's suites. For
> more, the library's own specs are in `testdomain/src/test/scala/io/reactivecqrs/testdomain/spec/`;
> the text names the one to open.

## Level 1: command handlers as plain functions

Your `AggregateContext` holds your rules as functions: `commandHandlers` takes the current state and
gives a partial function from a command to a result. You can call it with no actor in sight:

<!-- snippet: e11/CartRulesSpec.scala#rules -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.examples.e1.{AddItem, ItemAdded, ShoppingCart, ShoppingCartContext}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

class CartRulesSpec extends AnyFeatureSpec with Matchers {

  private val context = new ShoppingCartContext
  private val user = UserId(1)
  private val cartId = AggregateId(1001)

  /** What the handler decides for `command` on `cart`; a decision made in a Future is awaited. */
  private def decide(cart: ShoppingCart, command: AnyRef): GenericCommandResult[Any] =
    context.commandHandlers(cart)(command) match {
      case AsyncCommandResult(future) => Await.result(future, 5.seconds)
      case answered => answered
    }

  private def cartWith(items: String*): ShoppingCart = ShoppingCart("Groceries", items.toVector)

  Feature("Adding items") {

    Scenario("a cart of four items takes a fifth") {
      decide(cartWith("a", "b", "c", "d"), AddItem(user, cartId, AggregateVersion(5), "e")) mustBe
        CommandSuccess(ItemAdded("e"))
    }

    Scenario("a cart of five items refuses a sixth") {
      decide(cartWith("a", "b", "c", "d", "e"), AddItem(user, cartId, AggregateVersion(6), "f")) mustBe
        CommandFailure("Cannot have more than 5 items in your cart")
    }
  }
}
```

A few things to notice:

- **The version in the command does not matter here.** Checking `expectedVersion` is the owner's job,
  not the handler's; you test that at level 2.
- **Some handlers answer later.** A handler may return an `AsyncCommandResult` (a `Future`) instead
  of a `CommandSuccess` straight away (chapter 4). `decide` treats both alike, so the spec does not
  change when a handler does.
- **Results compare by value.** `CommandSuccess(ItemAdded("e"))` equals what the handler built, so
  `mustBe` reads like the rule itself.

> **Tip:** test the edges of every rule: the last value that passes and the first that fails. Here:
> four items take a fifth, five refuse a sixth. A rule written as `> 5` instead of `>= 5` would let a
> sixth item in, and the second scenario would say so long before a customer does.

## Level 1: event handlers, and replaying a history

Event handlers are functions too: `eventHandlers(userId, timestamp, state)` gives a partial function
from an event to the next state. Replaying a history is a fold, the same thing an owner does when it
reads an aggregate.

The same fold checks your **squash event** ([chapter 14](14-squash.md)). Every squash checks that
the event handlers, applied to the squash event alone, give back exactly the state it replaces; if
they do not, the squash is refused. Find out in a test instead. E1's carts have no squash event, so
the second scenario takes E5's shopping lists, which have one:

<!-- snippet: e11/ReplaySpec.scala#replay -->
```scala
import java.time.Instant

import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.api.{AggregateContext, Event}
import io.reactivecqrs.examples.e1.{ItemAdded, ShoppingCart, ShoppingCartContext, ShoppingCartCreated}
import io.reactivecqrs.examples.e5.{ItemWritten, ListCreated, ShoppingListContext}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

class ReplaySpec extends AnyFeatureSpec with Matchers {

  /** The state that `events` make, as an owner makes it when it reads the aggregate. */
  private def replay[R](context: AggregateContext[R], events: Event[R]*): R =
    events.foldLeft(context.initialAggregateRoot) { (state, event) =>
      context.eventHandlers(UserId(1), Instant.EPOCH, state)(event)
    }

  Feature("Replaying a history") {

    Scenario("a cart's history replays to the cart") {
      replay(new ShoppingCartContext, ShoppingCartCreated("Groceries"), ItemAdded("apples"), ItemAdded("pears")) mustBe
        ShoppingCart("Groceries", Vector("apples", "pears"))
    }

    Scenario("a shopping list's squash event gives the list back (E5)") {
      val lists = new ShoppingListContext
      val list = replay(lists, ListCreated("Camping"), ItemWritten("tent"), ItemWritten("stove"))
      replay(lists, lists.squash.get.toEvent(list)) mustBe list
    }
  }
}
```

> **Warning:** a fold of event handlers knows nothing of **undo** or **duplication**. In the store, an
> undo marks earlier events as cancelled and a restore skips them, and a copy reads its base's
> history first. Your event handlers have no case for an undo event at all (chapter 6), so a fold
> that meets one fails. Test undo and duplication through the command bus, at level 2 or 3.

## Level 2: the in-memory doubles

Each database part of the library has an in-memory double in `core`:

| Double | Stands in for | Keep in mind |
|---|---|---|
| `MemoryEventStoreState` (`core.eventstore`) | `PostgresEventStoreState` | keeps objects, not JSON; no transaction to roll back; reads at an instant fail |
| `MemoryCommandResponseState` (`core.commandhandler`) | `PostgresCommandResponseState` | not transactional: a response stays stored when the persist fails later |
| `MemorySubscriptionsState` (`core.projection`) | `PostgresSubscriptionsState` | the same compare-and-set rules for projection versions; no transactions |
| `MemoryDocumentStore[T]`, `MemoryDocumentStoreAutoId[T]` (`core.documentstore`) | `PostgresDocumentStore` | the query conditions mirror PostgreSQL's; each document keeps the space it was first written with, and `clearSpace` removes a space's documents |
| `MemoryUidGenerator` (`core.uid`) | `PostgresUidGenerator` | pools of 100 ids, from 1001 |
| `MemoryTypesNamesState` (`core.types`) | `PostgresTypesNamesState` | ids of class names, numbered as they are asked for |
| `MemoryProjectionRegistry` (`core.rebuild`) | `PostgresProjectionRegistry` | dropping a build only forgets it |

There is no in-memory saga state: test sagas on PostgreSQL (`SagaRecoverySpec` does).

> **Warning:** the doubles are for speed, not for proof. They keep objects and never write JSON, so an
> event or document class that mpjsons cannot read back passes every in-memory test and fails on
> PostgreSQL. Nothing is rolled back, and no SQL procedure guards the versions. Run the paths that
> matter at level 3 too.

## Level 2: a whole cart system in memory

Here is a complete system with no database: E1's wiring of chapter 2 (a uid generator, an event bus,
the cart command bus and the carts list), all over doubles. It follows `AggregateOwnerSpec` and
`SerialCommandsSpec`:

<!-- snippet: e11/CartsInMemorySpec.scala#memory -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, MemoryCommandResponseState}
import io.reactivecqrs.core.documentstore.MemoryDocumentStore
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.MemoryEventStoreState
import io.reactivecqrs.core.projection.MemorySubscriptionsState
import io.reactivecqrs.core.uid.{MemoryUidGenerator, UidGeneratorActor}
import io.reactivecqrs.examples.e1._
import org.apache.pekko.actor.{ActorRef, Props}

import scala.util.Try

class CartsInMemorySpec extends ShopSpec {

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new MemoryUidGenerator, new MemoryUidGenerator, new MemoryUidGenerator)))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  private val carts: ActorRef = system.actorOf(AggregateCommandBusActor(new ShoppingCartContext,
    uidGenerator, new MemoryEventStoreState, new MemoryCommandResponseState, eventBus))
  private val cartsList: ActorRef = system.actorOf(Props(new CartsListProjection(
    new MemorySubscriptionsState, new MemoryDocumentStore[CartSummary])))
  eventBus ! RegisterSubscribers(List(cartsList)) // RegisterSubscribers(Nil) when there are none

  private val user = UserId(1)

  private def send(command: AnyRef): CustomCommandResponse[_] = carts.askFor[CustomCommandResponse[_]](command)

  private def newCart(): AggregateId = send(CreateShoppingCart(user, "Groceries")) match {
    case SuccessResponse(cartId, _) => cartId
    case refused => fail("Not created: " + refused)
  }

  Feature("A cart in memory") {

    Scenario("an item added reaches the read model") {
      val cartId = newCart()
      send(AddItem(user, cartId, AggregateVersion(1), "apples")) mustBe SuccessResponse(cartId, AggregateVersion(2))
      val summary = cartsList.askFor[Option[CartSummary]](GetCartSummaryAtLeast(cartId, AggregateVersion(2)))
      summary mustBe Some(CartSummary("Groceries", 1, 2))
    }

    Scenario("the command that expects an old version is refused") {
      Given("a cart with apples in it")
      val cartId = newCart()
      send(AddItem(user, cartId, AggregateVersion(1), "apples")) mustBe SuccessResponse(cartId, AggregateVersion(2))

      When("someone still looking at version 1 adds pears")
      val late = send(AddItem(user, cartId, AggregateVersion(1), "pears"))

      Then("the command is refused, and the cart keeps only the apples")
      late mustBe AggregateConcurrentModificationError(cartId, AggregateType.of[ShoppingCart], AggregateVersion(1), AggregateVersion(2))
      val cart = carts.askFor[Try[Aggregate[ShoppingCart]]](GetAggregate(cartId))
      cart.get.aggregateRoot.get.items mustBe Vector("apples")
    }
  }
}
```

Two helpers make this short:

- **`ShopSpec`**, the base class above: a Pekko `TestKit` with ScalaTest's feature spec,
  `Given`/`When`/`Then`, `must` matchers and `ActorAskSupport` mixed in. `system` is the TestKit's
  actor system.
- **`askFor[T](message)`** (from `ActorAskSupport`) asks an actor and waits for the answer, up to 10
  seconds, as a `T`; `askFor[T](message, 3.seconds)` waits as long as you say. An answer of another
  class fails with a message that names the actor, the message and both classes, and an answer
  `Status.Failure(e)` fails with `e`. Name the type in the brackets every time: it is what the
  answer is checked against.

Note `eventBus ! RegisterSubscribers(...)`: a bus publishes nothing until it was told its
subscribers, even when there are none (`RegisterSubscribers(Nil)`).

At this level you can test everything the owner does: version checks, idempotent commands, serial
processing (`SerialCommandsSpec` sends 1 000 commands at once to one cart and gets 1 000 versions in a
row), timeouts (`AggregateOwnerSpec`), undo and duplication.

## Level 2: a projection on its own

You do not need the write side to test a projection. Play the event bus yourself with a `TestProbe`:
introduce yourself as the bus, hand the projection a delivery, and see what it answers and what it
made. This follows `ProjectionDeliverySpec`, with E1's carts list:

<!-- snippet: e11/CartsListAloneSpec.scala#alone -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.documentstore.MemoryDocumentStore
import io.reactivecqrs.core.eventbus.EventsBusActor.{AggregatesDelivery, GetSubscriptionRequests, MessageAck, SubscriptionRequests}
import io.reactivecqrs.core.projection.MemorySubscriptionsState
import io.reactivecqrs.examples.e1.{CartSummary, CartsListProjection, GetCartSummary, ShoppingCart}
import org.apache.pekko.actor.Props
import org.apache.pekko.testkit.TestProbe

import scala.concurrent.duration._

class CartsListAloneSpec extends ShopSpec {

  Feature("The carts list, fed by hand") {

    Scenario("a new cart's summary appears in the list") {
      val projection = system.actorOf(Props(new CartsListProjection(
        new MemorySubscriptionsState, new MemoryDocumentStore[CartSummary])))
      val bus = TestProbe()
      bus.send(projection, GetSubscriptionRequests(projection))
      bus.expectMsgType[SubscriptionRequests](3.seconds)

      val cartId = AggregateId(1001)
      bus.send(projection, AggregateWithType[ShoppingCart](AggregateType.of[ShoppingCart], cartId,
        AggregateVersion(1), Seq(AggregateVersion(1)), Some(ShoppingCart("Groceries", Vector.empty))))
      bus.expectMsg(3.seconds, MessageAck(projection, cartId, Seq(AggregateVersion(1)), AggregatesDelivery))

      val summary = projection.askFor[Option[CartSummary]](GetCartSummary(cartId))
      summary mustBe Some(CartSummary("Groceries", 0, 1))
    }
  }
}
```

What happened: the projection told the probe what it subscribes to, applied version 1 of cart 1001
(a new cart, so its listener inserted a document), acknowledged it with `MessageAck`, and answered
the query from its in-memory document store.

The same trick tests the harder cases: send version 3 before version 2 and the projection asks for the
gap (`RequestRepublish`); send version 1 twice and the second is only acknowledged. For a projection
that takes events instead of states, send `IdentifiableEvents` instead of `AggregateWithType`.

## Level 3: against PostgreSQL

The real database catches what the doubles cannot: JSON that does not read back, SQL, transactions,
the outbox. The library's specs use a local PostgreSQL with a database `reactivecqrs`, whose user and
password are both `reactivecqrs`, and the test kit's `TestDatabase`:

- **`TestDatabase.init()`** sets up ScalikeJDBC's connection pool, once per JVM, on a **schema of its
  own for this test run** (`test_run_<seconds>_<random>`). A run never sees what an earlier run left
  behind, and every run creates the schema from scratch; schemas of runs older than six hours are
  dropped. To keep a fixed schema (to look at it afterwards), run with
  `-Dreactivecqrs.test.schema=<name>`. Another database is `-Dreactivecqrs.test.url=<jdbc url>`,
  with `-Dreactivecqrs.test.user` and `-Dreactivecqrs.test.password`: a database for tests only, as
  the schemas of old runs are dropped there.
- **Suites run one at a time**, as they share the database (`Test / parallelExecution := false`, and
  one test task at a time in `build.sbt`).
- **Specs of one run share the schema.** Ids from the pools never repeat, so carts made by commands
  (as in E1) keep apart by themselves. For a command that chooses its id (the test domain's
  `CreateShoppingCartWithId`), `TestIds.aggregate()` gives a new one, far above the ids the pools
  hand out.

Here is a level-3 spec. It starts the shop with E1's own `BasicSetup.start`, the function its host
runs, so the test covers the wiring too, and it records the library's metrics with
`RecordingMetrics`:

<!-- snippet: e11/CartsOnPostgresSpec.scala#postgres -->
```scala
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.metrics.{ReactiveMetrics, RecordingMetrics}
import io.reactivecqrs.examples.e1.{BasicSetup, CartSummary}
import io.reactivecqrs.testkit.TestDatabase
import org.apache.pekko.util.Timeout

import scala.concurrent.duration._

class CartsOnPostgresSpec extends ShopSpec {

  TestDatabase.init() // the connection pool, on a schema of its own for this test run

  private val metrics = new RecordingMetrics
  ReactiveMetrics(system).use(metrics)
  private val shop = BasicSetup.start(system) // what the host starts

  Feature("Carts in PostgreSQL") {

    Scenario("a cart is stored, reaches the read model through the outbox, and is measured") {
      implicit val timeout: Timeout = Timeout(30.seconds)
      BasicSetup.fillCart(shop, UserId(1)) mustBe Some(CartSummary("Groceries", 1, 2))
      metrics.counted("reactivecqrs.events.stored", Map("type" -> "ShoppingCart")) mustBe 2L
    }
  }
}
```

- `fillCart` (chapter 2) asks the carts list with a delayed query (`GetCartSummaryAtLeast`, chapter
  8), so its answer comes once the events went from the store through the outbox and the bus to the
  projection.
- **Wait for things, never sleep.** Delivery, projections and the outbox are eventually consistent.
  A delayed query waits in the projection; `awaitAssert` (from Pekko's TestKit) and ScalaTest's
  `eventually` repeat an assertion until it passes or time runs out. Do not `Thread.sleep`.
- **`RecordingMetrics`** keeps every measure in memory: `counted(name, tags)` sums counts,
  `timed(name, tags)` lists times, `gauged(name, tags)` gives the last values of gauges. You name the
  tags you care about; measures with more tags match too. `MetricsSpec` checks the main measures of
  [chapter 18](18-production.md) this way.

### Tests that take minutes, in milliseconds

The outbox poller waits minutes before it re-publishes ([chapter 18](18-production.md)).
`TestClock` is a `java.time.Clock` that moves only when the test moves it; `OutboxPollerSpec` gives
one to the poller, with polls far apart so that it polls only when told:

```scala
import io.reactivecqrs.core.delivery.{OutboxPollerActor, OutboxPollerOptions}
import io.reactivecqrs.testkit.TestClock

val clock = new TestClock()
val poller = system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus,
  OutboxPollerOptions(firstPollAfter = 1.hour, pollInterval = 1.hour), clock)))

clock.advance(3.minutes + 1.second)
poller ! OutboxPollerActor.Poll   // polls now, as if 3 minutes had passed
```

## Guarding stored formats

Stored events live for years, and every one must stay readable by the code you write next year (the
reason [chapter 6](06-events.md) says to add a `V2` class instead of changing a stored one).
`SchemaReference` turns that into a test. You give it one sample of each stored class; it compares
each with a reference JSON file named after the class, and reports what no longer reads. This is
E11's `StoredFormatsSpec`, which guards what every example stores:

<!-- snippet: e11/StoredFormatsSpec.scala#formats -->
```scala
import java.nio.file.Paths

import io.mpjsons.MPJsons
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.examples.e1.{ItemAdded, ShoppingCartCreated}
import io.reactivecqrs.examples.e4.CheckoutSaga.{Charge, Checkout}
import io.reactivecqrs.examples.e4.{ItemsReleased, ItemsReserved, PaymentCharged, PaymentOpened, StockOpened}
import io.reactivecqrs.examples.e5.{ChangesUndone, ItemWritten, ListCopied, ListCreated, ListSquashed, ShoppingList}
import io.reactivecqrs.examples.e6.{ConcertScheduled, SeatBooked}
import io.reactivecqrs.examples.e7.{NoteAdded, TicketOpened}
import io.reactivecqrs.examples.e9.{MemoEdited, MemoSquashed, MemoWritten}
import io.reactivecqrs.testkit.SchemaReference
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

class StoredFormatsSpec extends AnyFeatureSpec with Matchers {

  // One JSON file per class, committed next to this spec; the tests run from the build's root.
  private val references = Paths.get("examples", "src", "test", "resources", "schema-reference")

  // A sample of every class the examples store: events, saga orders, and the states of snapshots.
  private val samples: Seq[AnyRef] = Seq(
    ShoppingCartCreated("Groceries"), ItemAdded("apples"),                                            // E1
    StockOpened(10), ItemsReserved(2), ItemsReleased(2), PaymentOpened(10000), PaymentCharged(2500),  // E4
    Checkout(UserId(1), AggregateId(1), AggregateId(2), 2, 2500),
    Charge(UserId(1), AggregateId(1), AggregateId(2), 2, 2500),
    ListCreated("Camping"), ItemWritten("tent"), ChangesUndone(1),                                    // E5
    ListCopied(SpaceId(0), AggregateId(1), AggregateVersion(2)), ListSquashed(SpaceId(0), "Camping", Vector("tent")),
    ShoppingList("Camping", Vector("tent")),
    ConcertScheduled("Jazz night", 60), SeatBooked("customer 1"),                                     // E6
    TicketOpened("Printer jam"), NoteAdded("Paper reloaded"),                                         // E7
    MemoWritten(SpaceId(7), "Order paper"), MemoEdited("Order toner"), MemoSquashed(SpaceId(7), "Order toner")) // E9

  Feature("Stored formats") {

    Scenario("every stored class reads its committed reference, and what it writes now reads back") {
      SchemaReference.verify(new MPJsons, references, samples) mustBe empty
    }
  }
}
```

A class passes when its committed reference still reads back as the sample (old data stays
readable) and the sample written now reads back as itself (new data is readable). A missing reference
is a failure, unless you run with `-Dreactivecqrs.schema.record=true`, which writes it (for
`ItemAdded("apples")` the file holds `{"name":"apples"}`). Commit the files: from then on, a change to
a stored class that breaks old data turns this test red.

What a sample list needs:

- **Every event class**, `V2` classes included.
- **Every saga order**, the internal ones too: the runner stores each step's order (chapter 12).
  An internal order the saga keeps `private` cannot be made in a test; E4 keeps `Charge`
  package-private for this.
- **The root of every context with snapshots** (chapter 13): a snapshot is the root's JSON.
- **The info of every idempotent command's response** you made yourself; `SuccessResponse` and
  `FailureResponse` are the library's, which it guards itself.

> **Tip:** add a sample the day you write the class. The reference files are your contract with the
> data already in production.

## Messages that cross nodes

If you run a cluster ([chapter 17](17-cluster.md)), your commands, events, states and query answers
travel through the cluster serializer. A round trip through it is a quick test that they can. It
needs an actor system with the cluster provider, which need not join a cluster;
`TestSystems.clusterConfig` is that configuration, made to share the JVM with other nodes. This
follows the library's `ClusterSerializationSpec`:

<!-- snippet: e11/CrossNodeSpec.scala#nodes -->
```scala
import com.typesafe.config.ConfigFactory
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.examples.e1._
import io.reactivecqrs.testkit.TestSystems
import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem}
import org.apache.pekko.serialization.{Serialization, SerializationExtension, Serializers}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

class CrossNodeSpec extends AnyFeatureSpec with Matchers with BeforeAndAfterAll {

  // An actor system with the cluster provider serializes as between nodes; it need not join a cluster.
  private val node = ActorSystem("serialization", TestSystems.clusterConfig.withFallback(ConfigFactory.load()))
  private val serialization = SerializationExtension(node)

  override def afterAll(): Unit = Await.ready(node.terminate(), 30.seconds)

  private def roundTrip(value: AnyRef): AnyRef =
    Serialization.withTransportInformation(node.asInstanceOf[ExtendedActorSystem]) { () =>
      val serializer = serialization.findSerializerFor(value)
      serialization.deserialize(serializer.toBinary(value), serializer.identifier, Serializers.manifestFor(serializer, value)).get
    }

  Feature("Messages between nodes") {

    Scenario("the carts' commands, events, states and questions cross nodes") {
      Seq(CreateShoppingCart(UserId(1), "Groceries"), AddItem(UserId(1), AggregateId(7), AggregateVersion(3), "pears"),
        ItemAdded("pears"), ShoppingCart("Groceries", Vector("apples")), GetCartSummary(AggregateId(7)),
        CartSummary("Groceries", 1, 2)).foreach(message => roundTrip(message) mustBe message)
    }
  }
}
```

A class with a field typed `Any`, a trait or an `ActorRef` fails here, on your machine, instead of
between two production nodes.

## Several nodes in one JVM

The library tests its cluster behaviour by starting several actor systems inside one test, each a
node on a local port, all over the run's PostgreSQL schema. The test kit's `TestCluster` does it for
your tests too:

```scala
import io.reactivecqrs.testkit.{TestCluster, TestDatabase}

TestDatabase.init()
val systems = TestCluster.start(3, "pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout = 3s")
val nodes = systems.map(system => new MyNode(system)) // what your host starts on a node
// ... the scenario ...
TestCluster.stop(systems)
```

- `TestCluster.start(count, extra)` starts the nodes, joins them and waits until all are up; the first
  is the oldest. `extra` is configuration on top.
- `TestCluster.join(to, 2, appVersion = _ => "2.0.0")` adds nodes of another version later, for
  rolling upgrades; `TestCluster.stop(systems)` shuts them down.
- Each node's configuration is the cluster provider on `127.0.0.1` with port 0, the split brain
  resolver with a short `stable-after`, and the settings that let several nodes share one JVM
  (`pekko.cluster.jmx.multi-mbeans-in-same-jvm = on`, `pekko.coordinated-shutdown.exit-jvm = off`).

The library's own `CartCluster` (in the testdomain's test sources) builds a `TestNode` on each:
stores, an event bus and a cart command bus. With it, `ShardedOwnersSpec`, `ClusterDeliverySpec`,
`LocalOwnersSpec`, `ClusterMessageSizeSpec` and `RollingUpgradeSpec` check the behaviour
[chapter 17](17-cluster.md) describes.

There is also a cheap trick: run all your other specs on a **cluster of one**. `TestSystems.newSystem()`
makes one when the tests run with `-Dreactivecqrs.test.clusterOfOne=true`, and a cluster of one must
behave exactly like a single node; the library runs its whole suite both ways.

## Patterns worth stealing from the testdomain specs

- **Hold a command mid-flight with a promise.** `AggregateOwnerSpec`'s `CountWhenReleased` command
  has a handler that returns an `AsyncCommandResult` of a future the test completes when it likes.
  Meanwhile the test can passivate the owner, send other commands, or let the timeout pass.
- **A probe as a subscriber.** In `OutboxPollerSpec` a `TestProbe` is the bus's only subscriber: it
  answers the bus's question itself, then catches deliveries, skipping those of other specs' carts:

  ```scala
  val subscriber = TestProbe()
  val bus = system.actorOf(Props(new EventsBusActor()))
  bus ! RegisterSubscribers(Seq(subscriber.ref))
  subscriber.expectMsg(GetSubscriptionRequests(subscriber.ref))
  subscriber.reply(SubscriptionRequests(Seq(SubscribeForEvents(AggregateType.of[ShoppingCart], subscriber.ref)), subscriber.ref))

  // ... after a command on cartId:
  val versions = subscriber.fishForSpecificMessage(10.seconds) {
    case e: IdentifiableEvents[_] if e.aggregateId == cartId => e.events.map(_.version.asInt)
  }
  bus ! MessageAck(subscriber.ref, cartId, versions.map(AggregateVersion(_)), EventsDelivery)
  ```

- **Wait for things, never sleep.** `awaitAssert` and `eventually` finish as soon as the condition
  holds, and fail with a clear message when it never does.
- **Ids of the test's own.** `TestIds.aggregate()` with a command that chooses its id keeps scenarios
  apart in a shared schema.
- **Given, When, Then.** The specs say what they set up, what they do and what they expect, in words a
  colleague can read in a test report.
- **Run one spec at a time while you work on it:**
  `sbt "testdomain/testOnly *ShardedOwnersSpec"`.

## What you learned

- Test at three levels: handlers as plain functions, the real actors over in-memory doubles, and
  everything on PostgreSQL, with many more tests at the first level than at the last.
- Command and event handlers are functions of the context: call them directly, fold events to replay
  a history, and check that your squash event gives the state back.
- The in-memory doubles in `core` make fast system tests, but they write no JSON and roll nothing
  back, so the important paths run on PostgreSQL too, on a schema of their own per run.
- `askFor[T](message)` asks an actor and checks the answer's class; a `TestProbe` playing the event bus
  tests a projection on its own; `RecordingMetrics` and `TestClock` test measures and timings
  measured in minutes.
- `SchemaReference` guards stored formats, a round trip through the serializer guards messages
  between nodes, and the test kit's `TestCluster` runs several nodes in one test.

Next: [chapter 20](20-ephemeral-aggregates.md), the last one, is about aggregates that live only in
memory: drafts you keep when they matter.

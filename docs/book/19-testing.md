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

- **In the published `core` module:** the in-memory stand-ins (this chapter calls them *doubles*) and
  `RecordingMetrics`. Your tests can use them directly.
- **In the `testutils` module:** the library's own test kit: `CommonSpec`, `ActorAskSupport` (the
  `??` helper), `TestDatabase`, `TestIds`, `TestClock` and `SchemaReference`. Its classes live under
  `src/test`, and the build does not publish test classes (`Test / publishArtifact := false` in
  `project/Common.scala`), so your project cannot depend on them. They are short (none is much longer
  than 60 lines): copy the ones you like. A test kit for applications is coming in 1.0.

The examples below are written as the library's own specs are: ScalaTest 3.2's feature specs with
`must` matchers, and Pekko's TestKit.

> **Remember:** every example in this chapter comes from, or is modelled on, a spec in
> `testdomain/src/test/scala/io/reactivecqrs/testdomain/spec/`. When you want to see more, open the
> spec named next to it.

## Level 1: command handlers as plain functions

Your `AggregateContext` holds your rules as functions: `commandHandlers` takes the current state and
gives a partial function from a command to a result. You can call it with no actor in sight:

```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.testdomain.shoppingcart._
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

class CartRulesSpec extends AnyFeatureSpec with Matchers {

  private val context = new ShoppingCartAggregateContext
  private val alice = UserId(1)
  private val cartId = AggregateId(1001)

  /** What the handler decides for `command` on `cart`; a decision made in a Future is awaited. */
  private def decide(cart: ShoppingCart, command: Any): GenericCommandResult[Any] =
    context.commandHandlers(cart)(command) match {
      case AsyncCommandResult(future) => Await.result(future, 5.seconds)
      case decided => decided
    }

  private def cartWith(items: String*): ShoppingCart =
    ShoppingCart("Groceries", items.zipWithIndex.map { case (name, i) => Item(i + 1, name) }.toVector)

  Feature("Adding items") {

    Scenario("an item is added with ItemAdded") {
      decide(cartWith("milk"), AddItem(alice, cartId, AggregateVersion(2), "bread")) mustBe
        CommandSuccess(ItemAdded("bread"))
    }

    Scenario("a cart with six items refuses a seventh") {
      decide(cartWith("a", "b", "c", "d", "e", "f"), AddItem(alice, cartId, AggregateVersion(7), "g")) mustBe
        CommandFailure("Cannot have more than 5 items in your cart")
    }
  }
}
```

A few things to notice:

- **The version in the command does not matter here.** Checking `expectedVersion` is the owner's job,
  not the handler's; you test that at level 2.
- **Some handlers answer later.** `AddItem`'s handler returns an `AsyncCommandResult` (a `Future`),
  `AddItemConcurrently`'s a `CommandSuccess` straight away. `decide` treats both alike.
- **Results compare by value.** `CommandSuccess(ItemAdded("bread"))` equals what the handler built, so
  `mustBe` reads like the rule itself.

Did you spot the surprise? The refusal says "more than 5 items", yet the test needs a cart with *six*
items to see it. The handler refuses when the cart already holds more than five, so a sixth item still
gets in. Whether that is the rule you meant is a question for whoever owns the shop; a two-line test is
where such questions come up, long before a customer finds them.

> **Tip:** test the edges of every rule: the last value that passes and the first that fails. Here:
> five items take a sixth, six items refuse a seventh.

## Level 1: event handlers, and replaying a history

Event handlers are functions too: `eventHandlers(userId, timestamp, state)` gives a partial function
from an event to the next state. Replaying a history is a fold, the same thing an owner does when it
reads an aggregate:

```scala
import java.time.Instant

private def replay(events: Event[ShoppingCart]*): ShoppingCart =
  events.foldLeft(context.initialAggregateRoot) { (cart, event) =>
    context.eventHandlers(alice, Instant.now, cart)(event)
  }

Scenario("a history replays to the cart") {
  replay(ShoppingCartCreated("Groceries"), ItemAdded("milk"), ItemAdded("bread"), ItemRemoved(1)) mustBe
    ShoppingCart("Groceries", Vector(Item(2, "bread")))
}
```

The same fold checks your **squash event** ([chapter 14](14-squash.md)). Every squash checks that
the event handlers, applied to the squash event alone, give back exactly the state it replaces; if
they do not, the squash is refused. Find out in a test instead:

```scala
Scenario("the squash event gives the cart back") {
  val cart = replay(ShoppingCartCreated("Groceries"), ItemAdded("milk"), ItemAdded("bread"))
  replay(context.squash.get.toEvent(cart)) mustBe cart
}
```

> **Warning:** a fold of event handlers knows nothing of **undo** or **duplication**. In the store, an
> undo marks earlier events as cancelled and a restore skips them, and a duplicate reads its base's
> history first. The cart's handler for `ShoppingCartChangesUndone` just returns the cart unchanged,
> so a plain fold would still apply the undone events. Test undo and duplication through the command
> bus, at level 2 or 3.

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

Here is a complete system with no database: a uid generator, an event bus with no subscribers and the
cart command bus, all over doubles. It follows `AggregateOwnerSpec` and `SerialCommandsSpec`:

```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, MemoryCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.MemoryEventStoreState
import io.reactivecqrs.core.uid.{MemoryUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.CommonSpec
import org.apache.pekko.actor.{ActorRef, Props}

import scala.util.Try

class CartInMemorySpec extends CommonSpec {

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new MemoryUidGenerator, new MemoryUidGenerator, new MemoryUidGenerator)))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  eventBus ! RegisterSubscribers(Nil)
  private val carts: ActorRef = system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext,
    uidGenerator, new MemoryEventStoreState, new MemoryCommandResponseState, eventBus))

  private val alice = UserId(1)

  Feature("Two people change one cart") {

    Scenario("the command that expects an old version is refused") {
      Given("a cart with milk in it")
      val created: CustomCommandResponse[_] = carts ?? CreateShoppingCart(None, alice, "Groceries")
      val cartId = created.asInstanceOf[SuccessResponse].aggregateId
      val first: CustomCommandResponse[_] = carts ?? AddItem(alice, cartId, AggregateVersion(1), "milk")
      first mustBe SuccessResponse(cartId, AggregateVersion(2))

      When("someone still looking at version 1 adds bread")
      val late: CustomCommandResponse[_] = carts ?? AddItem(alice, cartId, AggregateVersion(1), "bread")

      Then("the command is refused, and the cart keeps only the milk")
      late mustBe AggregateConcurrentModificationError(cartId, AggregateType.of[ShoppingCart], AggregateVersion(1), AggregateVersion(2))
      val cart: Try[Aggregate[ShoppingCart]] = carts ?? GetAggregate(cartId)
      cart.get.aggregateRoot.get.items.map(_.name) mustBe Vector("milk")
    }
  }
}
```

Two helpers make this short:

- **`CommonSpec`** is a Pekko `TestKit` with ScalaTest's feature spec, `Given`/`When`/`Then`, `must`
  matchers and `ActorAskSupport` mixed in. `system` is the TestKit's actor system.
- **`??`** (from `ActorAskSupport`) asks an actor and waits for the answer, up to 10 seconds; the type
  you expect (`CustomCommandResponse[_]`, `Try[Aggregate[ShoppingCart]]`) is the type you get.
  `askActor[R](message)(timeout)` does the same with a timeout of your choice.

Note `eventBus ! RegisterSubscribers(Nil)`: a bus publishes nothing until it was told its
subscribers, even when there are none.

At this level you can test everything the owner does: version checks, idempotent commands, serial
processing (`SerialCommandsSpec` sends 1 000 commands at once to one cart and gets 1 000 versions in a
row), timeouts (`AggregateOwnerSpec`), undo and duplication.

## Level 2: a projection on its own

You do not need the write side to test a projection. Play the event bus yourself with a `TestProbe`:
introduce yourself as the bus, hand the projection a delivery, and see what it answers and what it
made. This follows `ProjectionDeliverySpec`, with the cart list projection of the test domain:

```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.documentstore.MemoryDocumentStore
import io.reactivecqrs.core.eventbus.EventsBusActor.{AggregatesDelivery, GetSubscriptionRequests, MessageAck, SubscriptionRequests}
import io.reactivecqrs.core.projection.MemorySubscriptionsState
import io.reactivecqrs.core.util.BlockingDispatcher
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testdomain.shoppingcart.ShoppingCartsListProjection.GetAllCartsNames
import org.apache.pekko.actor.Props
import org.apache.pekko.testkit.TestProbe

import scala.concurrent.duration._

Scenario("a new cart's name appears in the list") {   // in a CommonSpec, like the one above
  val projection = system.actorOf(Props(new ShoppingCartsListProjectionAggregatesBased(
    new MemorySubscriptionsState, new MemoryDocumentStore[String])).withDispatcher(BlockingDispatcher.id))
  val bus = TestProbe()
  bus.send(projection, GetSubscriptionRequests(projection))
  bus.expectMsgType[SubscriptionRequests](3.seconds)

  val cartId = AggregateId(1001)
  bus.send(projection, AggregateWithType[ShoppingCart](AggregateType.of[ShoppingCart], cartId,
    AggregateVersion(1), Seq(AggregateVersion(1)), Some(ShoppingCart("Groceries", Vector.empty))))
  bus.expectMsg(3.seconds, MessageAck(projection, cartId, Seq(AggregateVersion(1)), AggregatesDelivery))

  val names: Vector[String] = projection ?? GetAllCartsNames()
  names mustBe Vector("Groceries")
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
password are both `reactivecqrs`, and `TestDatabase` from `testutils`:

- **`TestDatabase.init()`** sets up ScalikeJDBC's connection pool, once per JVM, on a **schema of its
  own for this test run** (`test_run_<seconds>_<random>`). A run never sees what an earlier run left
  behind, and every run creates the schema from scratch; schemas of runs older than six hours are
  dropped. To keep a fixed schema (to look at it afterwards), run with
  `-Dreactivecqrs.test.schema=<name>`.
- **Suites run one at a time**, as they share the database (`Test / parallelExecution := false`, and
  one test task at a time in `build.sbt`).
- **Specs of one run share the schema**, so each scenario takes ids of its own: `TestIds.aggregate()`
  gives a new id, far above the ids the pools hand out, for commands that choose their id
  (`CreateShoppingCartWithId`).

Here is a level-3 spec, modelled on `MetricsSpec` and `DeliveryEndToEndSpec`. It also records the
library's metrics, with `RecordingMetrics`:

```scala
import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.metrics.{ReactiveMetrics, RecordingMetrics}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase, TestIds}
import org.apache.pekko.actor.Props

import scala.concurrent.duration._

class CartsOnPostgresSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNames = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
  private val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  eventBus ! RegisterSubscribers(Nil)
  private val carts = system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext,
    uidGenerator, eventStore, responses, eventBus))

  private val metrics = new RecordingMetrics
  ReactiveMetrics(system).use(metrics)

  Feature("Carts in PostgreSQL") {

    Scenario("a new cart is stored, published and measured") {
      val cartId = AggregateId(TestIds.aggregate())
      val created: CustomCommandResponse[_] = carts ?? CreateShoppingCartWithId(None, UserId(1), cartId, "Groceries")
      created mustBe SuccessResponse(cartId, AggregateVersion(1))

      eventStore.readEventsRange[ShoppingCart](cartId, AggregateVersion(1), AggregateVersion(1)).map(_.event) mustBe
        Seq(ShoppingCartCreated("Groceries"))
      awaitAssert(eventStore.oldestVersionToPublish(cartId) mustBe None, 5.seconds, 50.millis)
      metrics.counted("reactivecqrs.events.stored", Map("type" -> "ShoppingCart")) must be >= 1L
    }
  }
}
```

- `eventStore.oldestVersionToPublish(cartId)` is `None` once nothing of the cart waits in the outbox:
  its events were delivered to everybody (here: nobody) and acknowledged.
- **`awaitAssert`** (from Pekko's TestKit) repeats an assertion until it passes or time runs out.
  Delivery, projections and the outbox are eventually consistent: wait for them, do not
  `Thread.sleep`. ScalaTest's `eventually` does the same.
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
import io.reactivecqrs.testutils.TestClock

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
`StoredFormatsSpec`, shortened:

```scala
import java.nio.file.Paths
import io.mpjsons.MPJsons
import io.reactivecqrs.testutils.SchemaReference

class StoredFormatsSpec extends AnyFeatureSpec with Matchers {

  private val referenceDir = Paths.get("testdomain", "src", "test", "resources", "schema-reference")

  private val samples: Seq[AnyRef] = Seq(
    ShoppingCartCreated("Groceries"), ItemAdded("apple"), ItemRemoved(1), CartNameRewritten("Food"),
    SuccessResponse(AggregateId(1001), AggregateVersion(2)))

  Feature("Stored formats stay readable") {
    Scenario("events and command responses match their committed references") {
      SchemaReference.verify(new MPJsons, referenceDir, samples) mustBe empty
    }
  }
}
```

A class passes when its committed reference still reads back as the sample (old data stays
readable) and the sample written now reads back as itself (new data is readable). A missing reference
is a failure, unless you run with `-Dreactivecqrs.schema.record=true`, which writes it (for
`ItemAdded("apple")` the file holds `{"name":"apple"}`). Commit the files: from then on, a change to a
stored class that breaks old data turns this test red.

> **Tip:** add a sample of every new event class, saga order and stored response class the day you
> write it. The reference files are your contract with the data already in production.

## Messages that cross nodes

If you run a cluster ([chapter 17](17-cluster.md)), your commands, events, states and query answers
travel through the cluster serializer. A round trip through it is a quick test that they can.
`ClusterSerializationSpec` does it like this, on an actor system with the cluster provider (it need
not join a cluster):

```scala
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem}
import org.apache.pekko.serialization.{Serialization, SerializationExtension, Serializers}

private val node = ActorSystem("serialization", ConfigFactory.parseString(
  """pekko.actor.provider = cluster
    |pekko.remote.artery.canonical { hostname = "127.0.0.1", port = 0 }""".stripMargin).withFallback(ConfigFactory.load()))
private val serialization = SerializationExtension(node)

private def roundTrip(value: AnyRef): AnyRef =
  Serialization.withTransportInformation(node.asInstanceOf[ExtendedActorSystem]) { () =>
    val serializer = serialization.findSerializerFor(value)
    serialization.deserialize(serializer.toBinary(value), serializer.identifier, Serializers.manifestFor(serializer, value)).get
  }

Scenario("the carts' messages cross nodes") {
  Seq(AddItem(UserId(1), AggregateId(7), AggregateVersion(3), "pear"), ItemAdded("pear"),
    ShoppingCart("cart", Vector(Item(1, "apple")))).foreach(message => roundTrip(message) mustBe message)
}
```

A class with a field typed `Any`, a trait or an `ActorRef` fails here, on your machine, instead of
between two production nodes.

## Several nodes in one JVM

The library tests its cluster behaviour by starting several actor systems inside one test, each a
node on a local port, all over the run's PostgreSQL schema. `TestCluster`, in the testdomain's test
sources, does it:

- `TestCluster.start(3, extra)` starts three nodes, joins them and waits until all are up; each
  `TestNode` holds its node's actor system, stores, event bus and cart command bus. `extra` is
  configuration on top, such as a short idle timeout for sharded owners.
- `TestCluster.join(to, 2, appVersion = _ => "2.0.0", ...)` adds nodes of another version later, for
  rolling upgrades; `TestCluster.stop(nodes)` shuts them down.
- Each node's configuration is the cluster provider on `127.0.0.1` with port 0, the split brain
  resolver with a short `stable-after`, and the settings that let several nodes share one JVM
  (`pekko.cluster.jmx.multi-mbeans-in-same-jvm = on`, `pekko.coordinated-shutdown.exit-jvm = off`).

With it, `ShardedOwnersSpec`, `ClusterDeliverySpec`, `LocalOwnersSpec`, `ClusterMessageSizeSpec` and
`RollingUpgradeSpec` check the behaviour [chapter 17](17-cluster.md) describes. `TestCluster` is test
code of the library, like `testutils`; a multi-node harness for applications is coming in 1.0. Until
then, copy the idea: it is about 120 lines.

There is also a cheap trick: run all your other specs on a **cluster of one**. The library's
`CommonSpec` does that when the tests run with `-Dreactivecqrs.test.clusterOfOne=true`, and a cluster
of one must behave exactly like a single node.

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
- A `TestProbe` playing the event bus tests a projection on its own; `RecordingMetrics` and
  `TestClock` test measures and timings measured in minutes.
- `SchemaReference` guards stored formats, a round trip through the serializer guards messages
  between nodes, and `TestCluster` shows how to run several nodes in one test.

Next: [chapter 20](20-ephemeral-aggregates.md), the last one, is about aggregates that live only in
memory: drafts you keep when they matter.

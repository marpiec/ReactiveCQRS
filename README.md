# ReactiveCQRS

**Event sourcing and CQRS for Scala, on Apache Pekko actors and PostgreSQL.**

ReactiveCQRS is a library for building applications whose state is a history of events. You write
your domain as plain, immutable Scala: an aggregate's state, the commands it accepts and the events
it records. The library does the rest: it gives every aggregate one owner actor, checks versions so
that concurrent changes never overwrite each other, stores the events in PostgreSQL, delivers them
reliably to read models (projections) and long-running processes (sagas), and rebuilds the read
models whenever you change them, without downtime.

It was built for, and runs, the Neula platform.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![Scala](https://img.shields.io/badge/scala-2.13-red.svg)
![Java](https://img.shields.io/badge/java-21%2B-orange.svg)

---

## Documentation

| | |
|---|---|
| **[The book: *ReactiveCQRS, Step by Step*](docs/book/README.md)** | Start here. A friendly guide from zero to production in 20 chapters, with examples you can run. |
| [Książka po polsku: *ReactiveCQRS, krok po kroku*](docs/book_pl/README.md) | The book in Polish, chapter by chapter, with the same code. |
| [Changelog](CHANGELOG.md) | What changed in each release, and what a host changes to upgrade. |
| [Upgrading from 0.12](docs/UPGRADE-0.12-TO-1.0.md) | The runbook of the move from 0.12.47 to 1.0: the rehearsal, the stop, the checks. |
| [Architecture](docs/ARCHITECTURE.md) | How the library works inside: actors, tables, delivery, rebuilds, the cluster. |
| [Scaling and limits](docs/SCALING.md) | Behaviour under high volume and load, limits, and recommendations. |
| [Performance reports](docs/perf/README.md) | Measured numbers from the benchmark module. |
| [Known issues and decisions](docs/KNOWN-ISSUES.md) | The open problems, the design decisions behind the library, and the architecture reviews; the closed problems are in [the archive](docs/KNOWN-ISSUES-ARCHIVE.md). |
| [Implementation plan](docs/IMPLEMENTATION-PLAN.md) | The road to 1.0, milestone by milestone, with a progress log. |
| [Design notes](docs/design/) | The design of each milestone, with the failure circumstances each change handles. |

## Features

- **Aggregates as plain Scala.** A state case class, command handlers and event handlers, tied
  together by an `AggregateContext`. No base classes for your state, no codecs to write: events are
  stored as JSON by mpjsons.
- **Commands in flavors.** Create (`FirstCommand`), change at an expected version with an optimistic
  lock (`Command`), change on the latest version with automatic retries (`ConcurrentCommand`),
  idempotent commands that are applied once however often they are sent, serial or concurrent
  processing per aggregate, command timeouts, and asynchronous handlers on threads of their own.
- **Reliable delivery.** A transactional outbox: every stored event reaches every subscriber, which
  acknowledges it; what is not acknowledged is delivered again. Flow control keeps a slow read model
  from flooding memory.
- **Read models.** Projections into a JSONB document store with indexes on JSON paths. A failing
  listener is retried, then recorded and reported, never silently skipped.
- **Rebuilds without downtime.** A managed projection builds its new version from the stored events
  while the old one keeps serving, then switches in one step. One aggregate, one type or one
  organization can be rebuilt in place.
- **Sagas.** Process managers for work that spans aggregates, with compensation when a step fails.
- **Long histories.** Performance snapshots; undo; copies of aggregates; *squash*, which replaces
  old history with one event and keeps chosen audit events in an archive; history rewrites, including
  a complete rewrite that erases a value everywhere the library keeps it, with an audit trail.
- **Organizations.** Every aggregate belongs to a space (a tenant). An organization can be purged
  completely, or exported from one installation and imported into another.
- **Ephemeral aggregates.** Drafts that live in one node's memory, writing and publishing nothing, until
  you keep the ones that matter (`Materialize`).
- **Clusters.** Runs on one machine or a Pekko Cluster: aggregates owned per node or sharded across
  the cluster, read models running once in the cluster, rolling upgrades.
- **Measured.** A benchmark module and documented limits for high volumes and loads.

## Requirements

- JDK 21 or newer (the library is compiled for Java 21)
- Scala 2.13 (the library is built with 2.13.18)
- PostgreSQL (measured on 17.4)
- sbt

Pekko 1.7.1, ScalikeJDBC 4.3.5 and the PostgreSQL driver come with the library. If you add other
Pekko modules to your build, use the same Pekko version.

## Installation

The artifacts are published to Neula's Maven repository:

```scala
resolvers += "Neula releases" at "https://nexus.neula.in/repository/neula-releases"

libraryDependencies ++= Seq(
  "io.reactivecqrs" %% "reactivecqrs-core"    % "1.0.0",
  "io.reactivecqrs" %% "reactivecqrs-testkit" % "1.0.0" % Test, // for your tests
  "ch.qos.logback"  %  "logback-classic"      % "1.5.32" // or any other slf4j backend
)
```

`reactivecqrs-api` holds what your domain code uses (it is a dependency of `reactivecqrs-core`).
To build from source instead, clone this repository and run `sbt publishLocal`.

## Quick start

E1, the book's first example: a shopping cart, and a read model that lists the carts. Its code is in
[examples/src/main/scala/io/reactivecqrs/examples/e1](examples/src/main/scala/io/reactivecqrs/examples/e1);
with PostgreSQL set up as in [chapter 2](docs/book/02-setting-up.md), run it with
`sbt "examples/runMain io.reactivecqrs.examples.e1.BasicSetup"`.

**The domain**: a state, its events and commands, and a context that ties them together.

<!-- snippet: e1/Carts.scala#domain -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}

case class ShoppingCart(name: String, items: Vector[Item])

case class Item(id: Int, name: String)

case class ShoppingCartCreated(name: String) extends FirstEvent[ShoppingCart] {
  def spaceId: SpaceId = SpaceId(0)
}

case class ItemAdded(name: String) extends Event[ShoppingCart]

case class ItemRemoved(id: Int) extends Event[ShoppingCart]

case class CreateShoppingCart(userId: UserId, name: String)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]]

case class AddItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                   name: String) extends Command[ShoppingCart, CustomCommandResponse[_]]

case class RemoveItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                      id: Int) extends Command[ShoppingCart, CustomCommandResponse[_]]

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

**The read model**: a projection that keeps one document per cart and answers questions about it.

<!-- snippet: e1/CartsList.scala#projection -->
```scala
case class CartSummary(name: String, itemCount: Int, version: Int)

case class GetCartSummary(cartId: AggregateId)                                   // answered with Option[CartSummary]
case class GetCartSummaryAtLeast(cartId: AggregateId, version: AggregateVersion) // the same, once it has the version

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
    case GetCartSummaryAtLeast(cartId, version) => // waits up to 2 seconds for the projection to catch up
      delayIfNotAvailable(sender(), () =>
        store.getDocument(cartId.asLong).map(_.document).filter(_.version >= version.asInt), 2000)
  }
}
```

**The wiring**: a connection pool first, then the tables, then the actors.

<!-- snippet: e1/BasicSetup.scala#pool -->
```scala
def connect(url: String, user: String, password: String): Unit = {
  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton(url, user, password,
    ConnectionPoolSettings(initialSize = 5, maxSize = 32, connectionTimeoutMillis = 3000L))
}
```

<!-- snippet: e1/BasicSetup.scala#wiring -->
```scala
val mpjsons = new MPJsons
val typesNames = new PostgresTypesNamesState().initSchema()
val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()

val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
val carts = system.actorOf(
  AggregateCommandBusActor(new ShoppingCartContext, uidGenerator, eventStore, responses, eventBus), "carts")
val cartsStore = new PostgresDocumentStore[CartSummary]("e1_carts_list", mpjsons, new NoopDocumentStoreCache)
val cartsList = system.actorOf(
  Props(new CartsListProjection(subscriptions, cartsStore)).withDispatcher(BlockingDispatcher.id), "cartsList")
eventBus ! EventsBusActor.RegisterSubscribers(List(cartsList)) // every projection, saga and listener
system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus)), "outboxPoller")
```

**Commands and a query**: each answer carries the aggregate's new version, which the next command
expects; the read model's question waits until the projection has caught up with that version.

<!-- snippet: e1/BasicSetup.scala#commands -->
```scala
/** What the host keeps of the shop: where its commands and its questions go. */
final case class Shop(carts: ActorRef, cartsList: ActorRef)

def fillCart(shop: Shop, user: UserId)(implicit timeout: Timeout): Option[CartSummary] = {
  def send(command: AnyRef): CustomCommandResponse[_] =
    Await.result((shop.carts ? command).mapTo[CustomCommandResponse[_]], timeout.duration)

  send(CreateShoppingCart(user, "Groceries")) match {
    case SuccessResponse(cartId, version) =>   // the new cart's id, at version 1
      send(AddItem(user, cartId, version, "apples")) match {
        case SuccessResponse(_, added) =>        // version 2
          Await.result((shop.cartsList ? GetCartSummaryAtLeast(cartId, added)).mapTo[Option[CartSummary]], timeout.duration)
        case refused => throw new IllegalStateException("Not added: " + refused)
      }
    case refused => throw new IllegalStateException("Not created: " + refused)
  }
}
```

From here, [the book](docs/book/README.md) explains each piece and adds sagas, rebuilds, organizations
and the cluster, one chapter at a time.

## Modules

| Module | What it is |
|---|---|
| `api` | The public API a domain author uses: `AggregateContext`, commands, events, results, ids. |
| `core` | The machinery: owners and command buses, the event store, delivery, projections, rebuilds, sagas, the document store, organizations, the cluster. |
| `testdomain` | A worked example (shopping carts and more) and the library's end-to-end tests. |
| `examples` | The book's examples, each runnable and tested (`sbt "examples/runMain io.reactivecqrs.examples.e1.BasicSetup"`); a cluster of three with Docker Compose or Kubernetes in `examples/deploy/e2`. Not published. |
| `testkit` | The test kit, published for your tests: a database schema per test run, the golden test of stored formats, actor systems that run as a cluster of one, several nodes in one JVM, a test clock. |
| `bench` | The benchmark and load driver (`sbt "bench/run <report-name>"`, reports in [docs/perf](docs/perf/README.md)). Not published. |

## Building and testing

```bash
sbt compile
sbt test
```

The tests need a PostgreSQL database `reactivecqrs` on `localhost:5432`, with user and password
`reactivecqrs`; every test run works in a schema of its own. The suites also run as a one-node
cluster:

```bash
sbt -Dreactivecqrs.test.clusterOfOne=true test
```

## Project status

Version 1.0.0 was released on 18/10/2026 (the tag `v1.0.0`); what changed since 0.12.47 is in the
[changelog](CHANGELOG.md). What is still open, such as the upgrade's rehearsal on a copy of production
(F52, F53) and the publication of mpjsons 0.7.0 (F179), is in [known issues](docs/KNOWN-ISSUES.md); the
[implementation plan](docs/IMPLEMENTATION-PLAN.md) records how each milestone was built.

## License

MIT License, see [LICENSE](LICENSE).

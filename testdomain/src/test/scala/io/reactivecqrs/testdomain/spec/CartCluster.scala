package io.reactivecqrs.testdomain.spec

import io.mpjsons.MPJsons
import io.reactivecqrs.api.{AggregateType, AsyncCommandResult, CommandSuccess, Event, Placement}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.testdomain.shoppingcart.{AddItemConcurrently, EventsHandlers, ItemAdded, ShoppingCart, ShoppingCartAggregateContext}
import io.reactivecqrs.testkit.{TestCluster, TestDatabase}
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.cluster.sharding.ClusterSharding
import org.apache.pekko.cluster.Cluster

import scala.concurrent.{ExecutionContext, Future, blocking}

/** Shopping carts with one owner each in the cluster (D43). An item whose name starts with "slow " takes twelve seconds
  * to add, for hand-overs that must wait for the command at work (D72). */
class ShardedCartContext extends ShoppingCartAggregateContext {
  override val placement: Placement = Placement.Sharded(numberOfShards = 10)

  override def commandHandlers = cart => {
    case c: AddItemConcurrently if c.name.startsWith("slow ") =>
      AsyncCommandResult(Future {
        blocking(Thread.sleep(12000))
        CommandSuccess(ItemAdded(c.name))
      }(ExecutionContext.global))
    case other => super.commandHandlers(cart)(other)
  }
}

/** An item added by version 2.0.0 of the carts, which records where it came from (D67: a new event class). */
case class ItemAddedV2(name: String, source: String) extends Event[ShoppingCart]

/** Version 2.0.0 of the sharded carts: an item is added with `ItemAddedV2` once every node runs that version, with
  * `ItemAdded` before; an item named "v2 ..." is added with `ItemAddedV2` at once, which the owner refuses until then. */
class ShardedCartContextV2 extends ShardedCartContext {
  override val writtenFrom: Map[Class[_], String] = Map(classOf[ItemAddedV2] -> "2.0.0")

  override def commandHandlers = cart => {
    case c: AddItemConcurrently if c.name.startsWith("v2 ") || everyNodeRuns("2.0.0") => CommandSuccess(ItemAddedV2(c.name, "version 2"))
    case other => super.commandHandlers(cart)(other)
  }

  override def eventHandlers = (userId, timestamp, cart) => {
    case ItemAddedV2(name, _) => EventsHandlers.itemAdded(cart, ItemAdded(name))
    case other => super.eventHandlers(userId, timestamp, cart)(other)
  }
}

/** One node of a cluster in this JVM (M5): an actor system with what the host starts on every node, over the test
  * database, with stores of its own as a process would have. `subscribers` starts what the node registers on its event
  * bus (projections' proxies, singletons); it runs once the node's buses exist. */
class TestNode(val system: ActorSystem, subscribers: TestNode => Seq[ActorRef] = _ => Nil, cartContext: ShoppingCartAggregateContext = new ShardedCartContext) {
  val mpjsons = new MPJsons
  val typesNames: PostgresTypesNamesState = new PostgresTypesNamesState().initSchema()
  val eventStore: PostgresEventStoreState = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()

  val uidGenerator: ActorRef = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
  val eventBus: ActorRef = system.actorOf(Props(new EventsBusActor()), "eventBus")
  val carts: ActorRef = system.actorOf(AggregateCommandBusActor(cartContext, uidGenerator, eventStore, responses, eventBus), "carts")
  eventBus ! EventsBusActor.RegisterSubscribers(subscribers(this))

  def cluster: Cluster = Cluster(system)

  /** The shard region of the carts' owners on this node. */
  def cartOwners: ActorRef = ClusterSharding(system).shardRegion("owner-" + AggregateType.of[ShoppingCart].typeName)
}

/** The carts of the testdomain on nodes of the test kit's cluster (M5): `TestCluster` starts and joins the actor systems,
  * and each gets a `TestNode` over the test database. */
object CartCluster {

  /** `count` nodes joined into one cluster, every one up; the first is the oldest. `appVersion` and `carts` give a node its
    * `pekko.cluster.app-version` (none: Pekko's 0.0.0) and its carts' context. */
  def start(count: Int, extra: String = "", subscribers: TestNode => Seq[ActorRef] = _ => Nil, appVersion: Int => String = _ => "",
            carts: Int => ShoppingCartAggregateContext = _ => new ShardedCartContext): Vector[TestNode] = {
    TestDatabase.init()
    TestCluster.start(count, extra, appVersion).zipWithIndex.map { case (system, i) => new TestNode(system, subscribers, carts(i)) }
  }

  /** `count` more nodes joined to the cluster of `to`, once every one is up. */
  def join(to: TestNode, count: Int, extra: String = "", subscribers: TestNode => Seq[ActorRef] = _ => Nil, appVersion: Int => String = _ => "",
           carts: Int => ShoppingCartAggregateContext = _ => new ShardedCartContext): Vector[TestNode] =
    TestCluster.join(to.system, count, extra, appVersion).zipWithIndex.map { case (system, i) => new TestNode(system, subscribers, carts(i)) }

  def stop(nodes: Seq[TestNode]): Unit = TestCluster.stop(nodes.map(_.system))
}

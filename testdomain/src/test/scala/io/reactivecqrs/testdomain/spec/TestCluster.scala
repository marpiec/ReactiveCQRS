package io.reactivecqrs.testdomain.spec

import com.typesafe.config.{Config, ConfigFactory}
import io.mpjsons.MPJsons
import io.reactivecqrs.api.{AggregateType, AsyncCommandResult, CommandSuccess, Placement}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.testdomain.shoppingcart.{AddItemConcurrently, ItemAdded, ShoppingCart, ShoppingCartAggregateContext}
import io.reactivecqrs.testutils.TestDatabase
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.cluster.sharding.ClusterSharding
import org.apache.pekko.cluster.{Cluster, MemberStatus}
import org.apache.pekko.testkit.TestKit

import scala.concurrent.duration._
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

/** One node of a cluster in this JVM (M5): an actor system with what the host starts on every node, over the test
  * database, with stores of its own as a process would have. `subscribers` starts what the node registers on its event
  * bus (projections' proxies, singletons); it runs once the node's buses exist. */
class TestNode(val system: ActorSystem, subscribers: TestNode => Seq[ActorRef] = _ => Nil) {
  val mpjsons = new MPJsons
  val typesNames: PostgresTypesNamesState = new PostgresTypesNamesState().initSchema()
  val eventStore: PostgresEventStoreState = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()

  val uidGenerator: ActorRef = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
  val eventBus: ActorRef = system.actorOf(Props(new EventsBusActor()), "eventBus")
  val carts: ActorRef = system.actorOf(AggregateCommandBusActor(new ShardedCartContext, uidGenerator, eventStore, responses, eventBus), "carts")
  eventBus ! EventsBusActor.RegisterSubscribers(subscribers(this))

  def cluster: Cluster = Cluster(system)

  /** The shard region of the carts' owners on this node. */
  def cartOwners: ActorRef = ClusterSharding(system).shardRegion("owner-" + AggregateType.of[ShoppingCart].typeName)
}

object TestCluster {

  /** A node's configuration: a cluster on local ports with the split brain resolver, and `extra` on top. */
  def config(extra: String = ""): Config = ConfigFactory.parseString(
    s"""pekko.actor.provider = cluster
       |pekko.remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
       |pekko.cluster.jmx.multi-mbeans-in-same-jvm = on
       |pekko.cluster.downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
       |pekko.cluster.split-brain-resolver.stable-after = 2s
       |pekko.coordinated-shutdown.exit-jvm = off
       |pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off
       |pekko.loglevel = WARNING
       |$extra""".stripMargin).withFallback(ConfigFactory.load())

  /** `count` nodes joined into one cluster, every one up. */
  def start(count: Int, extra: String = "", subscribers: TestNode => Seq[ActorRef] = _ => Nil): Vector[TestNode] = {
    TestDatabase.init()
    val systems = Vector.fill(count)(ActorSystem("cluster", config(extra)))
    val seed = Cluster(systems.head).selfAddress
    systems.foreach(system => Cluster(system).join(seed))
    awaitUp(systems, count)
    systems.map(new TestNode(_, subscribers))
  }

  def awaitUp(systems: Seq[ActorSystem], count: Int): Unit = {
    val deadline = System.nanoTime() + 30.seconds.toNanos
    while (!systems.forall(system => Cluster(system).state.members.count(_.status == MemberStatus.Up) == count)) {
      if (System.nanoTime() > deadline) throw new IllegalStateException(count + " nodes did not come up")
      Thread.sleep(100)
    }
  }

  def stop(nodes: Seq[TestNode]): Unit =
    nodes.foreach(node => TestKit.shutdownActorSystem(node.system, 30.seconds, verifySystemShutdown = false))
}

package io.reactivecqrs.examples.e2

import com.typesafe.config.{Config, ConfigFactory}
import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api.{AggregateVersion, CustomCommandResponse, SuccessResponse}
import io.reactivecqrs.core.cluster.{Readiness, Singletons}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor
import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.rebuild.{PostgresProjectionRegistry, Projections, ProjectionsSettings}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.examples.e1.{AddItem, BasicSetup, CartSummary, CartsListProjection, CreateShoppingCart, GetCartSummaryAtLeast}
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.management.cluster.bootstrap.ClusterBootstrap
import org.apache.pekko.management.scaladsl.PekkoManagement
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * E2, a cluster (chapter 17): E1's carts on every node, sharded, and their carts list, a managed projection that runs
  * once in the cluster, with the outbox poller. `startNode` is what every node runs; `main` makes a node from the
  * environment (the deployments in `examples/deploy/e2`: three nodes with Docker Compose, or on Kubernetes with the
  * split brain resolver's lease), joining through Cluster Bootstrap; `ClusterSpec` runs `startNode` on three nodes in
  * one JVM. Run one node with `sbt "examples/runMain io.reactivecqrs.examples.e2.ClusterSetup"`: a cluster of one,
  * against the database of chapter 2.
  */
object ClusterSetup {

  /** Where a node sends its commands and its questions. */
  final case class Node(carts: ActorRef, cartsList: ActorRef)

  /** E2's node, with E1's carts run by `cartContext` (the fault tests' adds a command, `FaultTests`). */
  def startNode(system: ActorSystem, cartContext: ShardedCartContext = new ShardedCartContext): Node = {
    // #node
    // The same code on every node (system: this node's ActorSystem; cartContext: E1's carts, sharded, see below)
    val mpjsons = new MPJsons
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
    val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()

    // One of each on every node
    val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
    val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
    val carts = system.actorOf(
      AggregateCommandBusActor(cartContext, uidGenerator, eventStore, responses, eventBus), "carts")

    // Once in the cluster: every node starts them, one node runs them
    val projections = new Projections(system, ProjectionsSettings(eventStore, subscriptions,
      new PostgresProjectionRegistry().initSchema(), contexts = Seq(cartContext)))
    val cartsList: ActorRef = projections.start[CartsListProjection]("e2-carts-list", 1, "cartsList") { build =>
      new CartsListProjection(subscriptions,
        new PostgresDocumentStore[CartSummary](build.documentStore("e2_carts_list"), mpjsons, new NoopDocumentStoreCache))
    }
    Singletons.start(system, "outboxPoller", Props(new OutboxPollerActor(eventStore, eventBus)))

    // Every node registers the projection's proxy on its own bus
    eventBus ! RegisterSubscribers(Seq(cartsList))
    // #node
    Node(carts, cartsList)
  }

  /** A cart made on `node` with one item, and its summary once the carts list has it. */
  def fillCart(node: Node, user: UserId, name: String)(implicit timeout: Timeout): (AggregateId, Option[CartSummary]) = {
    def send(command: AnyRef): SuccessResponse = Await.result((node.carts ? command).mapTo[CustomCommandResponse[_]], timeout.duration) match {
      case success: SuccessResponse => success
      case refused => throw new IllegalStateException("Refused: " + refused)
    }
    val created = send(CreateShoppingCart(user, name))
    val added = send(AddItem(user, created.aggregateId, created.aggregateVersion, "apples"))
    (created.aggregateId, summary(node, created.aggregateId, added.aggregateVersion))
  }

  def summary(node: Node, cartId: AggregateId, version: AggregateVersion)(implicit timeout: Timeout): Option[CartSummary] =
    Await.result((node.cartsList ? GetCartSummaryAtLeast(cartId, version)).mapTo[Option[CartSummary]], timeout.duration)

  // #config
  /** A node's configuration: `e2.conf` (or `e2-kubernetes.conf`, as SHOP_CONFIG says), with this node's address, its
    * contact points for Cluster Bootstrap, how many of them must answer before a new cluster forms, and the release's
    * version, from the environment. Alone, a node is its own contact point: a cluster of one. */
  def nodeConfig(setting: String => Option[String]): Config = {
    val host = setting("NODE_HOST").getOrElse("127.0.0.1")
    val contactPoints = setting("CONTACT_POINTS").fold(Seq(host))(_.split(",").map(_.trim).toSeq)
    val required = setting("REQUIRED_CONTACT_POINTS").getOrElse(contactPoints.size.toString)
    ConfigFactory.parseString(
      s"""pekko.remote.artery.canonical.hostname = "$host"
         |pekko.management.http.hostname = "$host"
         |pekko.discovery.config.services.shop.endpoints = [${contactPoints.map(p => s"""{host = "$p", port = 8558}""").mkString(", ")}]
         |pekko.management.cluster.bootstrap.contact-point-discovery.required-contact-point-nr = $required
         |pekko.cluster.app-version = "${setting("APP_VERSION").getOrElse("1.0.0")}"
         |""".stripMargin)
      .withFallback(ConfigFactory.load(setting("SHOP_CONFIG").getOrElse("e2")))
  }
  // #config

  /** The connection pool, to the database that DB_URL, DB_USER and DB_PASSWORD name (by default chapter 2's). */
  def connectFromEnvironment(): Unit =
    BasicSetup.connect(sys.env.getOrElse("DB_URL", "jdbc:postgresql://localhost:5432/reactivecqrs"),
      sys.env.getOrElse("DB_USER", "reactivecqrs"), sys.env.getOrElse("DB_PASSWORD", "reactivecqrs"))

  /** A node as the deployments start it: connected to its database, joining the cluster through Cluster Bootstrap,
    * and running `startNode`. */
  def startProcess(cartContext: ShardedCartContext = new ShardedCartContext): (ActorSystem, Node) = {
    connectFromEnvironment()
    val system = ActorSystem("shop", nodeConfig(sys.env.get))
    PekkoManagement(system).start() // the health checks, /ready and /alive, and Bootstrap's endpoint, on port 8558
    ClusterBootstrap(system).start() // joins the cluster the contact points report, or forms one with them (F180)
    (system, startNode(system, cartContext))
  }

  def main(args: Array[String]): Unit = {
    val (system, node) = startProcess()

    // Once the node is ready (its bus knows the carts list), it makes a cart and reads it from the carts list.
    val deadline = 2.minutes.fromNow
    while (!Readiness(system).ready && deadline.hasTimeLeft()) Thread.sleep(500)
    implicit val timeout: Timeout = Timeout(70.seconds)
    val host = system.settings.config.getString("pekko.remote.artery.canonical.hostname")
    val (cartId, cart) = fillCart(node, UserId(1), "Cart of " + host)
    println(s"$host made cart ${cartId.asLong}; the carts list, wherever it runs, has: $cart") // CartSummary(Cart of ..., 1, 2)
    Await.ready(system.whenTerminated, Duration.Inf) // a node runs until it is stopped, or leaves the cluster
  }
}

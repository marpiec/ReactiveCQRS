package io.reactivecqrs.testdomain.spec

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.cluster.Singletons
import io.reactivecqrs.core.delivery.{OutboxPollerActor, OutboxPollerOptions, RedeliveryPolicy}
import io.reactivecqrs.core.documentstore.{Document, DocumentStore, NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.projection.{PostgresSubscriptionsState, ProjectionActor, SubscriptionsState}
import io.reactivecqrs.core.rebuild.ProjectionSupervisor.{Builds, GetBuilds, RebuildProjection, RebuildStarted}
import io.reactivecqrs.core.rebuild.{PostgresProjectionRegistry, Projections, ProjectionsSettings}
import io.reactivecqrs.core.saga.{PostgresSagaState, SagaRunner}
import io.reactivecqrs.testdomain.shoppingcart.MultipleCartCreatorSaga.{CartsCreated, CreateMultipleCarts}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testkit.ErrorRecorder
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.event.Logging
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

case class ClusterCartItems(items: Vector[String])
case class GetClusterCartItems(id: AggregateId)

/** The items of every cart: a projection that runs once in the cluster (D53). `running` counts its instances in this JVM,
  * where every node of the test runs. */
class ClusterCartsProjection(val subscriptionsState: SubscriptionsState, store: DocumentStore[ClusterCartItems]) extends ProjectionActor {
  override def preStart(): Unit = {
    super.preStart()
    ClusterCartsProjection.running.incrementAndGet()
  }
  override def postStop(): Unit = {
    ClusterCartsProjection.running.decrementAndGet()
    super.postStop()
  }
  protected val listeners = List(EventsListener[ShoppingCart] { (id, events) => implicit session =>
    val added = events.map(_.event).collect { case ItemAdded(name) => name; case ItemAddedV2(name, _) => name }
    store.updateDocument(0, id.asLong, existing => Document(ClusterCartItems(existing.map(_.document.items).getOrElse(Vector()) ++ added)))
  })
  override protected def receiveQuery: Receive = {
    case GetClusterCartItems(id) => sender() ! store.getDocument(id.asLong).map(_.document.items)
  }
  override protected val projectionName: String = "ClusterCarts"
}

object ClusterCartsProjection {
  val running = new AtomicInteger()
}

// D44, D46, D53, D55: events reach a projection that runs once in the cluster from the owners on every node; queries and
// sagas work from any node; what runs once moves when its node leaves.
class ClusterDeliverySpec extends AnyFeatureSpec with Matchers with Eventually with GivenWhenThen with BeforeAndAfterAll {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(Span(60, Seconds), Span(200, Millis))
  implicit private val timeout: Timeout = Timeout(30.seconds)

  // The poller re-publishes after 2 and 4 seconds, so what waits in the outbox while the projection moves comes soon.
  private val poller = OutboxPollerOptions(firstPollAfter = 1.second, pollInterval = 1.second,
    redelivery = RedeliveryPolicy.Options(attemptsAfter = Seq(2.seconds, 4.seconds), giveUpAfter = 2.minutes))

  // Each node's proxies to the actors that run once.
  private val projectionOn = new ConcurrentHashMap[ActorSystem, ActorRef]()
  private val sagaOn = new ConcurrentHashMap[ActorSystem, ActorRef]()

  private val nodes = CartCluster.start(3, subscribers = node => {
    val subscriptions = new PostgresSubscriptionsState(node.typesNames).initSchema()
    val registry = new PostgresProjectionRegistry().initSchema()
    val projections = new Projections(node.system, ProjectionsSettings(node.eventStore, subscriptions, registry, Seq(new ShardedCartContext)))
    val carts = projections.start[ClusterCartsProjection]("cluster-carts", 1, "clusterCarts") { build =>
      new ClusterCartsProjection(subscriptions, new PostgresDocumentStore[ClusterCartItems](build.documentStore("cluster_carts"), node.mpjsons, new NoopDocumentStoreCache))
    }
    projectionOn.put(node.system, carts)
    Singletons.start(node.system, "outboxPoller", Props(new OutboxPollerActor(node.eventStore, node.eventBus, poller)))
    val sagaState = new PostgresSagaState(node.mpjsons, node.typesNames)
    sagaState.initSchema()
    sagaOn.put(node.system, SagaRunner.start(node.system, new MultipleCartCreatorSaga(node.carts)(node.system.dispatcher, Timeout(60.seconds)),
      node.uidGenerator, sagaState, Some("cartsSaga")))
    Seq(carts)
  })
  private val Vector(nodeA, nodeB, nodeC) = nodes

  override def afterAll(): Unit = CartCluster.stop(nodes)

  private def askFor[T](ref: ActorRef, message: Any): T = Await.result(ref ? message, 30.seconds).asInstanceOf[T]

  private def create(node: TestNode, name: String): AggregateId =
    askFor[CustomCommandResponse[_]](node.carts, CreateShoppingCart(None, UserId(1), name)) match {
      case SuccessResponse(id, _) => id
      case other => fail("Not created: " + other)
    }

  private def addItem(node: TestNode, cart: AggregateId, name: String): Unit =
    askFor[CustomCommandResponse[_]](node.carts, AddItem(UserId(1), cart, AggregateVersion(1), name)) mustBe a[SuccessResponse]

  private def itemsOn(node: TestNode, cart: AggregateId): Option[Vector[String]] =
    askFor[Option[Vector[String]]](projectionOn.get(node.system), GetClusterCartItems(cart))

  Feature("A projection that runs once in the cluster (D53)") {

    Scenario("gets the events of the owners on every node, and answers queries on any node (D55)") {
      val carts = (1 to 12).map(i => create(nodeA, "cart " + i))
      carts.foreach(cart => addItem(nodeB, cart, "apple"))

      eventually {
        for (node <- nodes; cart <- carts) itemsOn(node, cart) mustBe Some(Vector("apple"))
      }
      ClusterCartsProjection.running.get mustBe 1
    }
  }

  Feature("A new build of the projection in the cluster (D57, D70)") {

    Scenario("is subscribed on every node's bus, replaces the active one once every bus let it go, and gets every node's events") {
      val cart = create(nodeA, "rebuilt")
      addItem(nodeC, cart, "fig")
      eventually(itemsOn(nodeA, cart) mustBe Some(Vector("fig")))

      askFor[Any](projectionOn.get(nodeB.system), RebuildProjection) mustBe a[RebuildStarted]
      eventually(askFor[Builds](projectionOn.get(nodeA.system), GetBuilds).building mustBe None)

      val later = (1 to 6).map(i => create(nodeC, "after the switch " + i))
      later.foreach(cart => addItem(nodeA, cart, "kiwi"))
      eventually {
        later.foreach(cart => itemsOn(nodeB, cart) mustBe Some(Vector("kiwi")))
        itemsOn(nodeC, cart) mustBe Some(Vector("fig"))
        ClusterCartsProjection.running.get mustBe 1
      }
    }
  }

  Feature("A saga runner that runs once in the cluster (D46)") {

    Scenario("answers a caller on another node") {
      askFor[Any](sagaOn.get(nodeC.system), CreateMultipleCarts(UserId(1), "from another node", 3)) match {
        case CartsCreated(carts) => carts.size mustBe 3
        case other => fail("Unexpected: " + other)
      }
    }
  }

  Feature("Moving with the node that leaves (D46, D66)") {

    Scenario("the projection moves when its node leaves, and gets what was published meanwhile") {
      Given("carts projected before")
      val before = (1 to 6).map(i => create(nodeB, "before " + i))
      before.foreach(cart => addItem(nodeC, cart, "apple"))
      eventually(before.foreach(cart => itemsOn(nodeC, cart) mustBe Some(Vector("apple"))))

      And("every node keeps the errors it logs")
      val errors = new ConcurrentLinkedQueue[String]()
      nodes.foreach(node => node.system.eventStream.subscribe(node.system.actorOf(Props(new ErrorRecorder(errors))), classOf[Logging.Error]))

      When("the oldest node, where the projection, the poller and the saga run, leaves while items are added")
      nodeA.cluster.leave(nodeA.cluster.selfAddress)
      val during = (1 to 6).map(i => create(nodeB, "during " + i))
      during.foreach(cart => addItem(nodeC, cart, "pear"))
      eventually(nodeB.cluster.state.members.map(_.address) must not contain nodeA.cluster.selfAddress)
      val after = (1 to 6).map(i => create(nodeC, "after " + i))
      after.foreach(cart => addItem(nodeB, cart, "plum"))

      Then("the projection, on another node now, has every item, once")
      eventually {
        before.foreach(cart => itemsOn(nodeB, cart) mustBe Some(Vector("apple")))
        during.foreach(cart => itemsOn(nodeB, cart) mustBe Some(Vector("pear")))
        after.foreach(cart => itemsOn(nodeC, cart) mustBe Some(Vector("plum")))
      }
      ClusterCartsProjection.running.get mustBe 1

      And("no bus logged the instance or the proxies that stopped as errors: the singleton's move is no failure (F176)")
      errors.asScala.filter(_.startsWith("Subscriber ")) mustBe empty
    }
  }
}

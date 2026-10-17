package io.reactivecqrs.testdomain.spec

import java.time.Instant
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.rebuild.{PostgresProjectionRegistry, Projections, ProjectionsSettings}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.TestIds
import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, ExtendedActorSystem, Props}
import org.apache.pekko.cluster.sharding.ShardRegion.{CurrentShardRegionState, GetShardRegionState}
import org.apache.pekko.pattern.ask
import org.apache.pekko.serialization.Serialization
import org.apache.pekko.util.Timeout
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try

/** Records the versions of each delivery it receives, and acknowledges it. */
class DeliveryRecorder(received: ConcurrentLinkedQueue[Seq[Int]]) extends Actor {
  override def receive: Receive = {
    case IdentifiableEvents(_, aggregateId, events, _) =>
      received.add(events.map(_.version.asInt))
      sender() ! MessageAck(self, aggregateId, events.map(_.version), EventsDelivery)
  }
}

// F121: what crosses nodes fits Artery's frame (256 KiB by default), or fails at once.
class ClusterMessageSizeSpec extends AnyFeatureSpec with Matchers with Eventually with GivenWhenThen with BeforeAndAfterAll {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(Span(30, Seconds), Span(200, Millis))
  implicit private val timeout: Timeout = Timeout(30.seconds)

  private val projectionOn = new ConcurrentHashMap[ActorSystem, ActorRef]()

  // The projection runs once, on the oldest node; the carts of these scenarios have their owners on the other one.
  private val nodes = TestCluster.start(2, subscribers = node => {
    val subscriptions = new PostgresSubscriptionsState(node.typesNames).initSchema()
    val projections = new Projections(node.system, ProjectionsSettings(node.eventStore, subscriptions, new PostgresProjectionRegistry().initSchema(),
      Seq(new ShardedCartContext)))
    val carts = projections.start[ClusterCartsProjection]("sized-carts", 1, "sizedCarts") { build =>
      new ClusterCartsProjection(subscriptions, new PostgresDocumentStore[ClusterCartItems](build.documentStore("sized_carts"), node.mpjsons, new NoopDocumentStoreCache))
    }
    projectionOn.put(node.system, carts)
    Seq(carts)
  })
  private val Vector(oldest, other) = nodes

  override def afterAll(): Unit = TestCluster.stop(nodes)

  private def askFor[T](ref: ActorRef, message: Any): T = Await.result(ref ? message, 30.seconds).asInstanceOf[T]

  /** `count` item names of `length` characters: 1 000 of 150 make an event serialized at about 450 bytes. */
  private def names(count: Int, length: Int, prefix: String): Vector[String] =
    (1 to count).toVector.map(i => (prefix + " " + i + " ").padTo(length, 'x'))

  // A new cart whose owner runs on `node`: carts are made through it until one's shard is there.
  private def cartOwnedBy(node: TestNode): AggregateId =
    Iterator.continually(askFor[CustomCommandResponse[_]](node.carts, CreateShoppingCart(None, UserId(1), "sized"))).take(40)
      .collect { case SuccessResponse(id, _) => id }
      .find(id => askFor[CurrentShardRegionState](node.cartOwners, GetShardRegionState).shards.exists(_.entityIds.contains(id.asLong.toString)))
      .getOrElse(throw new IllegalStateException("No cart's owner landed on " + node.cluster.selfAddress))

  private def addItems(node: TestNode, cart: AggregateId, version: Int, items: Vector[String]): CustomCommandResponse[_] =
    askFor[CustomCommandResponse[_]](node.carts, AddItems(UserId(1), cart, AggregateVersion(version), items))

  Feature("Messages between nodes fit the frame (F121)") {

    Scenario("a subscriber on another node takes many large events in messages that fit the frame") {
      Given("a subscriber on the oldest node, of the carts published on the other node's bus")
      val received = new ConcurrentLinkedQueue[Seq[Int]]()
      val recorder = oldest.system.actorOf(Props(new DeliveryRecorder(received)))
      val remote = other.system.asInstanceOf[ExtendedActorSystem].provider.resolveActorRef(Serialization.serializedActorPath(recorder))
      other.eventBus ! AddSubscriptions(Seq(SubscribeForEvents(AggregateType.of[ShoppingCart], remote)))

      When("1 000 events of about 450 bytes each are published there at once, more than one message between nodes carries")
      val id = AggregateId(TestIds.aggregate())
      val events = names(1000, 150, "item").zipWithIndex.map { case (name, i) =>
        EventInfo[ShoppingCart](AggregateVersion(i + 1), ItemAdded(name), UserId(1), Instant.now)
      }
      other.eventBus ! PublishEvents(AggregateType.of[ShoppingCart], events, id, None)

      Then("every event arrives, in several messages")
      eventually(received.asScala.flatten.toSet mustBe (1 to 1000).toSet)
      received.size must be > 1
      other.eventBus ! RemoveSubscriber(remote)
    }

    Scenario("a command with many events reaches a projection on another node") {
      Given("a cart whose owner runs on another node than the projection")
      val cart = cartOwnedBy(other)

      When("one command adds 1 000 items: its events are more than one message between nodes carries")
      val added = names(1000, 150, "added")
      addItems(other, cart, 1, added) mustBe a[SuccessResponse]

      Then("the projection has every item")
      eventually(askFor[Option[Vector[String]]](projectionOn.get(oldest.system), GetClusterCartItems(cart)) mustBe Some(added))
    }

    Scenario("every event of an aggregate whose owner runs on another node comes in one answer") {
      Given("a cart of 1 001 events, more than one message between nodes carries")
      val cart = cartOwnedBy(other)
      addItems(other, cart, 1, names(1000, 150, "event")) mustBe a[SuccessResponse]

      Then("both nodes answer with every event")
      Seq(oldest, other).foreach { node =>
        askFor[Try[Seq[EventInfo[ShoppingCart]]]](node.carts, GetEventsForAggregate(cart)).get.size mustBe 1001
      }
    }

    Scenario("an answer too large for the frame fails at once for an asker on another node") {
      Given("a cart whose state is larger than one message between nodes carries")
      val cart = cartOwnedBy(other)
      addItems(other, cart, 1, names(1000, 150, "first")) mustBe a[SuccessResponse]
      addItems(other, cart, 1001, names(1000, 150, "second")) mustBe a[SuccessResponse]

      Then("asked from the other node, it fails at once, naming the size")
      val started = System.nanoTime()
      val asked = oldest.carts ? GetAggregate(cart)
      Try(Await.result(asked, 30.seconds)).failed.get mustBe a[MessageTooLargeException]
      (System.nanoTime() - started).nanos must be < 10.seconds

      And("asked on the owner's node, it is answered")
      askFor[Try[Aggregate[ShoppingCart]]](other.carts, GetAggregate(cart)).get.aggregateRoot.get.items.size mustBe 2000
    }

    Scenario("a command too large to cross nodes is refused at once, wherever its owner runs") {
      Given("a cart whose owner runs on one of the nodes")
      val cart = cartOwnedBy(other)

      Then("a command larger than one message between nodes carries is refused through either node")
      Seq(oldest, other).foreach { node =>
        addItems(node, cart, 1, names(2000, 150, "big")) match {
          case FailureResponse(reasons) => reasons.head must include("too large")
          case response => fail("Not refused through " + node.cluster.selfAddress + ": " + response)
        }
      }
      askFor[Try[Aggregate[ShoppingCart]]](other.carts, GetAggregate(cart)).get.version mustBe AggregateVersion(1)
    }
  }
}

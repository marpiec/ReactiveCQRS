package io.reactivecqrs.testdomain.spec

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.delivery.RepublishRange
import io.reactivecqrs.core.eventbus.EventsBusActor.{GetSubscriptionRequests, SubscribeForEvents, SubscriptionRequests}
import io.reactivecqrs.core.metrics.{ReactiveMetrics, RecordingMetrics}
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.ask
import org.apache.pekko.testkit.{TestActor, TestProbe}
import org.apache.pekko.util.Timeout
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/** Adds an item and answers with the cart name its handler saw. */
case class AddItemSeeingName(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, name: String)
  extends Command[ShoppingCart, CustomCommandResponse[_]]

/** The carts of every node, with a command whose answer shows the state it ran on. */
class LocalCartContext extends ShoppingCartAggregateContext {
  override def commandHandlers = cart => {
    case c: AddItemSeeingName => CommandSuccess(ItemAdded(c.name), cart.name)
    case other => super.commandHandlers(cart)(other)
  }
}

// F131: a `Local` type has an owner of an aggregate on every node that serves it, and each writes without the others.
// Before an owner answers what must be current, it catches up with the store.
class LocalOwnersSpec extends AnyFeatureSpec with Matchers with GivenWhenThen with BeforeAndAfterAll {

  implicit private val timeout: Timeout = Timeout(30.seconds)
  private val cartType = AggregateType.of[ShoppingCart]

  // A subscriber of the carts' events on each node's bus, in the nodes' order.
  private val subscribers = mutable.Buffer[TestProbe]()
  private val nodes = CartCluster.start(2, subscribers = node => {
    val subscriber = TestProbe()(node.system)
    subscriber.setAutoPilot((sender: ActorRef, message: Any) => {
      message match {
        case GetSubscriptionRequests(registered) => sender ! SubscriptionRequests(Seq(SubscribeForEvents(cartType, subscriber.ref)), registered)
        case _ => ()
      }
      TestActor.KeepRunning
    })
    subscribers += subscriber
    Seq(subscriber.ref)
  }, carts = _ => new LocalCartContext)
  private val Vector(nodeA, nodeB) = nodes: @unchecked

  override def afterAll(): Unit = CartCluster.stop(nodes)

  private def askFor[T](ref: ActorRef, message: Any): T = Await.result(ref ? message, 30.seconds).asInstanceOf[T]

  private def create(node: TestNode, name: String): AggregateId =
    askFor[CustomCommandResponse[_]](node.carts, CreateShoppingCart(None, UserId(1), name)) match {
      case SuccessResponse(id, _) => id
      case other => fail("Not created: " + other)
    }

  private def addItem(node: TestNode, cart: AggregateId, version: Int, name: String): CustomCommandResponse[_] =
    askFor[CustomCommandResponse[_]](node.carts, AddItem(UserId(1), cart, AggregateVersion(version), name))

  private def cartOn(node: TestNode, cart: AggregateId): Aggregate[ShoppingCart] =
    askFor[Try[Aggregate[ShoppingCart]]](node.carts, GetAggregate(cart)).get

  /** A cart created through the first node, whose owner on the second node runs at version 1. */
  private def sharedCart(name: String): AggregateId = {
    val cart = create(nodeA, name)
    cartOn(nodeB, cart).version mustBe AggregateVersion(1)
    cart
  }

  Feature("Owners of a Local type on several nodes (F131)") {

    Scenario("a node's owner reads what another node's owner wrote") {
      val cart = sharedCart("read")
      addItem(nodeA, cart, 1, "apple") mustBe SuccessResponse(cart, AggregateVersion(2))

      val read = cartOn(nodeB, cart)
      read.version mustBe AggregateVersion(2)
      read.aggregateRoot.get.items.map(_.name) mustBe Vector("apple")
    }

    Scenario("a re-publication through the other node holds what the first wrote") {
      val cart = sharedCart("republished")
      addItem(nodeA, cart, 1, "apple") mustBe SuccessResponse(cart, AggregateVersion(2))
      val subscriber = subscribers(1)

      nodeB.carts ! RepublishRange(cart, AggregateVersion(1), subscriber.ref)

      val republished = subscriber.fishForMessage(10.seconds) {
        case events: IdentifiableEvents[_] => events.aggregateId == cart
        case _ => false
      }.asInstanceOf[IdentifiableEvents[_]]
      republished.events.map(_.version.asInt) mustBe Seq(1, 2)
    }

    Scenario("a version asked for that the other node writes while the query waits is found at its deadline") {
      val cart = sharedCart("awaited")
      val query = nodeB.carts ? GetAggregateMinVersion(cart, AggregateVersion(2), 2000)
      addItem(nodeA, cart, 1, "apple") mustBe SuccessResponse(cart, AggregateVersion(2))

      Await.result(query, 10.seconds) match {
        case Success(aggregate: Aggregate[_]) => aggregate.version mustBe AggregateVersion(2)
        case Failure(e) => fail("The query failed: " + e)
        case other => fail("Not an aggregate: " + other)
      }
    }

    Scenario("a version asked for that the other node wrote before is found at once") {
      val cart = sharedCart("written before")
      addItem(nodeA, cart, 1, "apple") mustBe SuccessResponse(cart, AggregateVersion(2))

      val started = System.nanoTime()
      askFor[Try[Aggregate[ShoppingCart]]](nodeB.carts, GetAggregateMinVersion(cart, AggregateVersion(2), 5000)).get.version mustBe AggregateVersion(2)
      (System.nanoTime() - started).nanos must be < 2.seconds
    }

    Scenario("after an undo on the other node, an owner of a context without snapshots catches up once, not before every read (F144)") {
      val cart = sharedCart("undone")
      val restores = new RecordingMetrics
      ReactiveMetrics(nodeB.system).use(restores)
      addItem(nodeA, cart, 1, "apple") mustBe SuccessResponse(cart, AggregateVersion(2))
      askFor[CustomCommandResponse[_]](nodeA.carts, UndoShoppingCartChange(UserId(1), cart, AggregateVersion(2), 1)) mustBe SuccessResponse(cart, AggregateVersion(3))

      (1 to 3).foreach(_ => cartOn(nodeB, cart).version mustBe AggregateVersion(3))

      restores.timed("reactivecqrs.restore").size mustBe 1
    }

    Scenario("F214 an owner reading what the other node keeps writing catches up by the new events, without a restore") {
      val cart = sharedCart("alternating")
      val restores = new RecordingMetrics
      ReactiveMetrics(nodeB.system).use(restores)
      (1 to 5).foreach { i => // a cart holds 5 items at most
        addItem(nodeA, cart, i, "item " + i) mustBe SuccessResponse(cart, AggregateVersion(i + 1))
        cartOn(nodeB, cart).aggregateRoot.get.items.map(_.name).last mustBe "item " + i
      }
      restores.timed("reactivecqrs.restore") mustBe empty
    }

    Scenario("F213 a command expecting a version above the stored one is a conflict at once, without a restore") {
      val cart = create(nodeA, "far ahead")
      val restores = new RecordingMetrics
      ReactiveMetrics(nodeA.system).use(restores)
      (1 to 5).foreach(_ => addItem(nodeA, cart, 1000, "x") mustBe an[AggregateConcurrentModificationError])
      restores.timed("reactivecqrs.restore") mustBe empty
    }

    Scenario("a command runs on what the other node changed without a new version: a duplicate whose base was rewritten (F145)") {
      val base = create(nodeA, "before")
      val copy = askFor[CustomCommandResponse[_]](nodeA.carts, DuplicateShoppingCart(UserId(1), base, AggregateVersion(1))) match {
        case SuccessResponse(id, _) => id
        case other => fail("Not duplicated: " + other)
      }
      cartOn(nodeB, copy).aggregateRoot.get.name mustBe "before"
      askFor[CustomCommandResponse[_]](nodeA.carts, RewriteCartName(UserId(1), base, AggregateVersion(1), "after")) mustBe SuccessResponse(base, AggregateVersion(2))

      askFor[CustomCommandResponse[_]](nodeB.carts, AddItemSeeingName(UserId(1), copy, AggregateVersion(1), "apple")) mustBe
        CustomSuccessResponse(copy, AggregateVersion(2), "after")
    }
  }
}

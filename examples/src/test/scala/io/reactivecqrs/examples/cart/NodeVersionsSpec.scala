package io.reactivecqrs.examples.cart

// Chapter 17's code about a node's readiness and its cluster's versions, on clusters of one node running the test
// domain's carts at version 2.0.0: each block of the chapter is a region here, followed by the lines that check it.

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.cluster.{Members, Readiness}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testkit.TestCluster
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.{Timeout, Version}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

class NodeVersionsSpec extends CartSpec {

  implicit val timeout: Timeout = Timeout(70.seconds)
  private val alice = UserId(1)

  /** `work` on a cluster of one node of `appVersion`, which runs carts of `context`, once the node knows its version. */
  private def onNode[T](appVersion: String, context: ShoppingCartAggregateContext = new ShoppingCartContextV2)
                       (work: (ActorSystem, ActorRef) => T): T = {
    val node = TestCluster.start(1, appVersion = _ => appVersion).head
    try {
      val eventBus = node.actorOf(Props(new EventsBusActor()), "eventBus")
      val carts = node.actorOf(AggregateCommandBusActor(context, uidGenerator, eventStore, responseState, eventBus), "carts")
      eventBus ! EventsBusActor.RegisterSubscribers(Nil)
      awaitAssert(Members(node).oldestAppVersion mustBe Some(Version(appVersion)), 20.seconds, 100.millis)
      work(node, carts)
    } finally TestCluster.stop(Seq(node))
  }

  private def send(carts: ActorRef, command: AnyRef): CustomCommandResponse[_] =
    Await.result((carts ? command).mapTo[CustomCommandResponse[_]], timeout.duration)

  private def newCart(carts: ActorRef): AggregateId = send(carts, CreateShoppingCart(None, alice, "Groceries")) match {
    case SuccessResponse(id, _) => id
    case refused => fail("No cart: " + refused)
  }

  /** The class of the event that an item added on a node of `appVersion` is stored as. */
  private def storedAs(appVersion: String): String = onNode(appVersion) { (_, carts) =>
    val cartId = newCart(carts)
    send(carts, AddItemConcurrently(alice, cartId, "milk")) mustBe SuccessResponse(cartId, AggregateVersion(2))
    carts.askFor[Try[List[EventInfo[ShoppingCart]]]](GetEventsForAggregate(cartId)).get.last.event.getClass.getSimpleName
  }

  Feature("A node's readiness and its cluster's versions") {

    Scenario("a node is ready once its event bus knows its subscribers, and knows its members' versions") {
      onNode("1.0.0") { (node, _) =>
        val system = node
        awaitAssert(Readiness(system).ready mustBe true, 20.seconds, 100.millis)
        // #readiness
        import io.reactivecqrs.core.cluster.Readiness

        Readiness(system).ready        // false while an event bus still waits for its subscribers
        Readiness(system).waitingFor   // the event buses that still wait
        // #readiness
        Readiness(system).waitingFor mustBe empty

        // #members
        import io.reactivecqrs.core.cluster.Members

        val members = Members(system)
        members.oldestAppVersion   // the lowest app-version among the members, e.g. Some(Version("1.0.0"))
        members.olderVersionsRun   // true while a member runs an older version than this node
        members.alone              // true outside a cluster, and in a cluster of one
        // #members
        (members.oldestAppVersion, members.olderVersionsRun, members.alone) mustBe ((Some(Version("1.0.0")), false, true))
      }
    }

    Scenario("the new event class is written once every node runs 2.0.0; before, the old one, and the new one is refused") {
      storedAs("1.0.0") mustBe "ItemAdded"
      storedAs("2.0.0") mustBe "ItemAddedV2"
      onNode("1.0.0", new EagerCartsV2) { (_, carts) =>
        val cartId = newCart(carts)
        send(carts, AddItemConcurrently(alice, cartId, "milk")) mustBe a[CommandRejected]
      }
    }
  }
}

/** Carts that would write the new class at once, whatever the cluster's versions: the owner refuses it on a 1.0.0 node. */
class EagerCartsV2 extends ShoppingCartContextV2 {
  override def commandHandlers = cart => {
    case c: AddItemConcurrently => CommandSuccess(ItemAddedV2(c.name, "too early"))
    case other => super.commandHandlers(cart)(other)
  }
}

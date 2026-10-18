package io.reactivecqrs.examples.e11

// E11, testing (chapter 19): a projection tested on its own, with a probe playing the event bus.

// #alone
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
// #alone

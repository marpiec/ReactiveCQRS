package io.reactivecqrs.examples.e11

// E11, testing (chapter 19): a probe as the event bus's only subscriber, and an outbox poller on a clock the test moves.

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.examples.e1.{CreateShoppingCart, ShoppingCart, ShoppingCartContext}
import io.reactivecqrs.testkit.TestDatabase
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe

import scala.concurrent.duration._

class DeliveriesSpec extends ShopSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNames = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
  private val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")

  /** E1's carts, publishing through `bus`; a new cart's id. */
  private def newCartOn(bus: ActorRef): AggregateId =
    system.actorOf(AggregateCommandBusActor(new ShoppingCartContext, uidGenerator, eventStore, responses, bus))
      .askFor[CustomCommandResponse[_]](CreateShoppingCart(UserId(1), "Groceries")) match {
      case SuccessResponse(cartId, _) => cartId
      case refused => fail("Not created: " + refused)
    }

  private def outboxEmptyFor(cartId: AggregateId): Unit =
    awaitAssert(eventStore.oldestVersionToPublish(cartId) mustBe None, 5.seconds, 50.millis)

  Feature("Testing deliveries") {

    Scenario("a probe as the bus's only subscriber catches a cart's events and acknowledges them") {
      // #probe
      val subscriber = TestProbe()
      val bus = system.actorOf(Props(new EventsBusActor()))
      bus ! RegisterSubscribers(Seq(subscriber.ref))
      subscriber.expectMsg(GetSubscriptionRequests(subscriber.ref))
      subscriber.reply(SubscriptionRequests(Seq(SubscribeForEvents(AggregateType.of[ShoppingCart], subscriber.ref)), subscriber.ref))
      // #probe
      val cartId = newCartOn(bus)
      // #catch
      val versions = subscriber.fishForSpecificMessage(10.seconds) {
        case e: IdentifiableEvents[_] if e.aggregateId == cartId => e.events.map(_.version.asInt)
      }
      bus ! MessageAck(subscriber.ref, cartId, versions.map(AggregateVersion(_)), EventsDelivery)
      // #catch
      versions mustBe Seq(1)
      outboxEmptyFor(cartId)
    }

    Scenario("a delivery never acknowledged comes again once the test clock passes 3 minutes") {
      val subscriber = TestProbe()
      val eventBus = system.actorOf(Props(new EventsBusActor()))
      eventBus ! RegisterSubscribers(Seq(subscriber.ref))
      subscriber.expectMsg(GetSubscriptionRequests(subscriber.ref))
      subscriber.reply(SubscriptionRequests(Seq(SubscribeForEvents(AggregateType.of[ShoppingCart], subscriber.ref)), subscriber.ref))
      val cartId = newCartOn(eventBus)
      def delivered(): Seq[Int] = subscriber.fishForSpecificMessage(10.seconds) {
        case e: IdentifiableEvents[_] if e.aggregateId == cartId => e.events.map(_.version.asInt)
      }
      delivered() mustBe Seq(1) // and lost: never acknowledged

      // #clock
      import io.reactivecqrs.core.delivery.{OutboxPollerActor, OutboxPollerOptions}
      import io.reactivecqrs.testkit.TestClock

      val clock = new TestClock()
      val poller = system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus,
        OutboxPollerOptions(firstPollAfter = 1.hour, pollInterval = 1.hour), clock)))

      clock.advance(3.minutes + 1.second)
      poller ! OutboxPollerActor.Poll   // polls now, as if 3 minutes had passed
      // #clock
      delivered() mustBe Seq(1)
      eventBus ! MessageAck(subscriber.ref, cartId, Seq(AggregateVersion(1)), EventsDelivery)
      outboxEmptyFor(cartId)
    }
  }
}

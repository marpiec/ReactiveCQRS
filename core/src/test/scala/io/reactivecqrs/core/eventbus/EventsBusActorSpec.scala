package io.reactivecqrs.core.eventbus

import java.time.Instant

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.delivery.{RepublishOutbox, RepublishRange}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.testutils.CommonSpec
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe

import scala.concurrent.duration._

case class BusCart(name: String)
case class BusItemAdded(n: Int) extends Event[BusCart]
case class BusCartErased() extends PermanentDeleteEvent[BusCart]

// D7, D15, D20a, D20b: the bus routes, tracks each delivery until ack, failure, deadline or subscriber stop, and acks
// the owner only for events every subscriber took.
class EventsBusActorSpec extends CommonSpec {

  private val cartType = AggregateType.of[BusCart]
  private val options = DeliveryOptions(deadline = 1.second, stallAfter = 300.millis, tickInterval = 50.millis)

  private def newBus(): ActorRef = system.actorOf(Props(new EventsBusActor(options)))

  private def events(versions: Int*): Seq[EventInfo[Any]] =
    versions.map(v => EventInfo[BusCart](AggregateVersion(v), BusItemAdded(v), UserId(1), Instant.now).asInstanceOf[EventInfo[Any]])

  private def publish(bus: ActorRef, owner: TestProbe, id: AggregateId, versions: Int*): Unit =
    bus.tell(PublishEvents[Any](cartType, events(versions: _*), id, Some(BusCart("cart"))), owner.ref)

  private def publishTo(bus: ActorRef, owner: TestProbe, id: AggregateId, onlyTo: ActorRef, versions: Int*): Unit =
    bus.tell(PublishEvents[Any](cartType, events(versions: _*), id, Some(BusCart("cart")), Some(onlyTo)), owner.ref)

  /** Registers subscribers that each take the events of carts. */
  private def busWith(subscribers: TestProbe*): ActorRef = {
    val bus = newBus()
    bus ! RegisterSubscribers(subscribers.map(_.ref))
    subscribers.foreach { subscriber =>
      subscriber.expectMsg(GetSubscriptionRequests)
      subscriber.reply(SubscriptionRequests(Seq(SubscribeForEvents(cartType, subscriber.ref))))
    }
    bus
  }

  private def delivered(subscriber: TestProbe): Seq[Int] =
    subscriber.expectMsgType[IdentifiableEvents[Any]](3.seconds).events.map(_.version.asInt)

  private def ack(bus: ActorRef, subscriber: TestProbe, id: AggregateId, versions: Int*): Unit =
    bus ! MessageAck(subscriber.ref, id, versions.map(AggregateVersion(_)), EventsDelivery)

  private def acked(owner: TestProbe): Seq[Int] = owner.expectMsgType[PublishEventsAck](3.seconds).versions.map(_.asInt)

  private def statusOf(bus: ActorRef): DeliveryStatus = {
    val probe = TestProbe()
    bus.tell(GetDeliveryStatus, probe.ref)
    probe.expectMsgType[DeliveryStatus](3.seconds)
  }

  private def newId(): AggregateId = AggregateId(System.nanoTime())

  Feature("Start-up (D15)") {

    Scenario("publishes nothing until every registered subscriber has answered") {
      val (a, b, owner) = (TestProbe(), TestProbe(), TestProbe())
      val bus = newBus()
      bus ! RegisterSubscribers(Seq(a.ref, b.ref))
      a.expectMsg(GetSubscriptionRequests)
      a.reply(SubscriptionRequests(Seq(SubscribeForEvents(cartType, a.ref))))
      b.expectMsg(GetSubscriptionRequests)
      val id = newId()

      publish(bus, owner, id, 1)
      a.expectNoMessage(300.millis)

      b.reply(SubscriptionRequests(Seq(SubscribeForEvents(cartType, b.ref))))
      delivered(a) mustBe Seq(1)
      delivered(b) mustBe Seq(1)
    }
  }

  Feature("Acknowledging the owner") {

    Scenario("happens once every subscriber has acknowledged") {
      val (a, b, owner) = (TestProbe(), TestProbe(), TestProbe())
      val bus = busWith(a, b)
      val id = newId()
      publish(bus, owner, id, 1, 2)
      delivered(a); delivered(b)

      ack(bus, a, id, 1, 2)
      owner.expectNoMessage(300.millis)
      ack(bus, b, id, 1, 2)

      acked(owner) mustBe Seq(1, 2)
      statusOf(bus).waiting mustBe 0
    }

    Scenario("counts a subscriber's failure like its acknowledgement (D20a)") {
      val (a, b, owner) = (TestProbe(), TestProbe(), TestProbe())
      val bus = busWith(a, b)
      val id = newId()
      publish(bus, owner, id, 1)
      delivered(a); delivered(b)

      ack(bus, a, id, 1)
      bus ! MessageFailed(b.ref, id, Seq(AggregateVersion(1)), "listener failed", EventsDelivery)

      acked(owner) mustBe Seq(1)
    }

    Scenario("names the version of a permanent delete, so the publisher can delete the aggregate for good") {
      val (a, owner) = (TestProbe(), TestProbe())
      val bus = busWith(a)
      val id = newId()
      val erasing = Seq(EventInfo[BusCart](AggregateVersion(1), BusItemAdded(1), UserId(1), Instant.now),
        EventInfo[BusCart](AggregateVersion(2), BusCartErased(), UserId(1), Instant.now)).asInstanceOf[Seq[EventInfo[Any]]]
      bus.tell(PublishEvents[Any](cartType, erasing, id, Some(BusCart("cart"))), owner.ref)
      delivered(a) mustBe Seq(1, 2)

      ack(bus, a, id, 1, 2)

      owner.expectMsgType[PublishEventsAck](3.seconds) mustBe PublishEventsAck(id, Seq(AggregateVersion(1), AggregateVersion(2)), Some(AggregateVersion(2)))
    }

    Scenario("is immediate for events nobody subscribed to") {
      val owner = TestProbe()
      val bus = busWith()

      publish(bus, owner, newId(), 1, 2, 3)

      acked(owner) mustBe Seq(1, 2, 3)
    }

    Scenario("waits for each kind a subscriber takes: one kind's ack is not the other's") {
      val (a, owner) = (TestProbe(), TestProbe())
      val bus = newBus()
      bus ! RegisterSubscribers(Seq(a.ref))
      a.expectMsg(GetSubscriptionRequests)
      a.reply(SubscriptionRequests(Seq(SubscribeForEvents(cartType, a.ref), SubscribeForAggregates(cartType, a.ref))))
      val id = newId()

      publish(bus, owner, id, 1)
      a.expectMsgType[IdentifiableEvents[Any]](3.seconds)
      a.expectMsgType[AggregateWithType[Any]](3.seconds)

      bus ! MessageAck(a.ref, id, Seq(AggregateVersion(1)), AggregatesDelivery)
      owner.expectNoMessage(300.millis)
      bus ! MessageAck(a.ref, id, Seq(AggregateVersion(1)), EventsDelivery)
      acked(owner) mustBe Seq(1)
    }

    Scenario("never happens for a re-publication to one subscriber") {
      val (a, owner) = (TestProbe(), TestProbe())
      val bus = busWith(a)
      val id = newId()

      publishTo(bus, owner, id, a.ref, 1, 2)
      delivered(a) mustBe Seq(1, 2)
      ack(bus, a, id, 1, 2)

      owner.expectNoMessage(300.millis)
      statusOf(bus).waiting mustBe 0
    }
  }

  Feature("Re-publication while a delivery waits") {

    Scenario("sends the event again only to the subscribers that have not acknowledged it") {
      val (a, b, owner) = (TestProbe(), TestProbe(), TestProbe())
      val bus = busWith(a, b)
      val id = newId()
      publish(bus, owner, id, 1)
      delivered(a); delivered(b)
      ack(bus, a, id, 1)

      publish(bus, owner, id, 1)

      delivered(b) mustBe Seq(1)
      a.expectNoMessage(300.millis)
    }
  }

  Feature("Deliveries that end without every subscriber (D20b)") {

    Scenario("one neither acknowledged nor failed by the deadline ends undelivered: no ack, listed in the status") {
      val (a, owner) = (TestProbe(), TestProbe())
      val bus = busWith(a)
      val id = newId()
      publish(bus, owner, id, 7)
      delivered(a)

      owner.expectNoMessage(options.deadline + 500.millis)

      val status = statusOf(bus)
      status.waiting mustBe 0
      status.undelivered.map(u => (u.aggregateId, u.version.asInt, u.subscriber, u.kind)) must contain((id, 7, a.ref.path.name, EventsDelivery))
    }

    Scenario("a subscriber that stops ends its deliveries without an ack; the others keep flowing, and later events stay in the outbox for it") {
      val (a, b, owner) = (TestProbe(), TestProbe(), TestProbe())
      val bus = busWith(a, b)
      val id = newId()
      publish(bus, owner, id, 1)
      delivered(a); delivered(b)
      ack(bus, b, id, 1)

      system.stop(a.ref)
      awaitAssert(statusOf(bus).waiting mustBe 0, 3.seconds, 50.millis)
      owner.expectNoMessage(300.millis) // version 1 stays in the outbox for the stopped subscriber
      statusOf(bus).stopped mustBe Seq(a.ref.path.name)

      publish(bus, owner, id, 2)
      delivered(b) mustBe Seq(2)
      ack(bus, b, id, 2)
      owner.expectNoMessage(300.millis) // version 2 too: after a restart the subscriber gets both
      statusOf(bus).waiting mustBe 0
    }
  }

  Feature("Bookkeeping") {

    Scenario("returns to empty for an aggregate the bus never saw before (F1)") {
      val (a, owner) = (TestProbe(), TestProbe())
      val bus = busWith(a)

      (1 to 100).foreach { _ =>
        val id = newId()
        publish(bus, owner, id, 11)
        delivered(a)
        ack(bus, a, id, 11)
        acked(owner) mustBe Seq(11)
      }

      statusOf(bus).waiting mustBe 0
    }
  }

  Feature("Failures inside the bus (AR7)") {

    Scenario("a publication the bus fails on is dropped without an ack, and the bus keeps routing") {
      val (a, owner) = (TestProbe(), TestProbe())
      val bus = busWith(a)

      owner.send(bus, PublishEvents[Any](cartType, null, newId(), None)) // malformed: no events at all
      owner.expectNoMessage(300.millis)

      val id = newId()
      publish(bus, owner, id, 1)
      delivered(a) mustBe Seq(1)
      ack(bus, a, id, 1)
      acked(owner) mustBe Seq(1)
    }
  }

  Feature("Re-publish requests (D8, D20c)") {


    Scenario("go to the command bus registered for the aggregate type") {
      val (a, commandBus) = (TestProbe(), TestProbe())
      val bus = busWith(a)
      bus ! RegisterPublisher(cartType, commandBus.ref)
      val id = newId()

      bus ! RequestRepublish(a.ref, cartType, id, AggregateVersion(3))
      commandBus.expectMsg(3.seconds, RepublishRange(id, AggregateVersion(3), a.ref))

      bus ! Republish(cartType, id)
      commandBus.expectMsg(3.seconds, RepublishOutbox(id))
    }
  }
}

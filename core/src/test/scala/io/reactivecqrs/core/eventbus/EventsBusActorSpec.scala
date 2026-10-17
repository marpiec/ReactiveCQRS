package io.reactivecqrs.core.eventbus

import java.time.Instant

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.cluster.Readiness
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
      subscriber.expectMsg(GetSubscriptionRequests(subscriber.ref))
      subscriber.reply(SubscriptionRequests(Seq(SubscribeForEvents(cartType, subscriber.ref)), subscriber.ref))
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
      a.expectMsg(GetSubscriptionRequests(a.ref))
      a.reply(SubscriptionRequests(Seq(SubscribeForEvents(cartType, a.ref)), a.ref))
      b.expectMsg(GetSubscriptionRequests(b.ref))
      b.ignoreMsg { case _: GetSubscriptionRequests => true } // asked again every tick until it answers
      val id = newId()

      publish(bus, owner, id, 1)
      a.expectNoMessage(300.millis)

      b.reply(SubscriptionRequests(Seq(SubscribeForEvents(cartType, b.ref)), b.ref))
      delivered(a) mustBe Seq(1)
      delivered(b) mustBe Seq(1)
    }

    Scenario("keeps the node from being ready until every registered subscriber answered (D71)") {
      val a = TestProbe()
      val bus = newBus()
      bus ! RegisterSubscribers(Seq(a.ref))
      a.expectMsg(GetSubscriptionRequests(a.ref))
      Readiness(system).waitingFor must contain(bus.path.toString)
      a.reply(SubscriptionRequests(Seq(SubscribeForEvents(cartType, a.ref)), a.ref))
      awaitAssert(Readiness(system).waitingFor must not contain bus.path.toString, 3.seconds)
    }

    Scenario("asks a subscriber again every tick until it answers: an ask can be lost on the way (M5)") {
      val a = TestProbe()
      newBus() ! RegisterSubscribers(Seq(a.ref))
      a.expectMsg(GetSubscriptionRequests(a.ref))
      a.expectMsg(1.second, GetSubscriptionRequests(a.ref))
    }
  }

  Feature("Subscribers behind a cluster singleton's proxy (M5)") {

    // A proxy forwards the bus's ask to the projection's supervisor, which answers with its instances.
    def busBehindProxy(proxy: TestProbe, supervisor: TestProbe, instance: TestProbe): ActorRef = {
      val bus = newBus()
      bus ! RegisterSubscribers(Seq(proxy.ref))
      proxy.expectMsg(GetSubscriptionRequests(proxy.ref))
      proxy.ignoreMsg { case _: GetSubscriptionRequests => true }
      bus.tell(SubscriptionRequests(Seq(SubscribeForEvents(cartType, instance.ref)), proxy.ref), supervisor.ref)
      bus
    }

    Scenario("an answer from another actor that names the proxy completes the proxy's registration") {
      val (proxy, supervisor, instance, owner) = (TestProbe(), TestProbe(), TestProbe(), TestProbe())
      val bus = busBehindProxy(proxy, supervisor, instance)
      val id = newId()
      publish(bus, owner, id, 1)
      delivered(instance) mustBe Seq(1)
    }

    Scenario("when an instance stops, the proxy is asked again, and the new instance replaces the stopped one") {
      val (proxy, supervisor, owner) = (TestProbe(), TestProbe(), TestProbe())
      val (before, after) = (TestProbe(), TestProbe())
      val bus = busBehindProxy(proxy, supervisor, before)
      val id = newId()
      publish(bus, owner, id, 1)
      delivered(before) mustBe Seq(1)

      Given("the singleton moved: its instance stopped")
      proxy.ignoreNoMsg()
      system.stop(before.ref)
      proxy.expectMsg(3.seconds, GetSubscriptionRequests(proxy.ref))
      publish(bus, owner, id, 2) // nobody takes it: it stays in the outbox

      When("the new supervisor answers with its instance")
      bus.tell(SubscriptionRequests(Seq(SubscribeForEvents(cartType, after.ref)), proxy.ref), supervisor.ref)
      publish(bus, owner, id, 3)

      Then("the new instance takes what comes; the stopped one is gone; nothing was acknowledged to the owner")
      delivered(after) mustBe Seq(3)
      statusOf(bus).stopped mustBe empty
      owner.expectNoMessage(300.millis)
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
      a.expectMsg(GetSubscriptionRequests(a.ref))
      a.reply(SubscriptionRequests(Seq(SubscribeForEvents(cartType, a.ref), SubscribeForAggregates(cartType, a.ref)), a.ref))
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

  Feature("Subscribers added and removed after the start (D57)") {

    Scenario("an added subscription takes the events published after it") {
      val (a, b, owner) = (TestProbe(), TestProbe(), TestProbe())
      val bus = busWith(a)
      val id = newId()

      bus ! AddSubscriptions(Seq(SubscribeForEvents(cartType, b.ref)))
      publish(bus, owner, id, 1)

      delivered(a) mustBe Seq(1)
      delivered(b) mustBe Seq(1)
      ack(bus, a, id, 1)
      owner.expectNoMessage(200.millis) // it waits for the added subscription too
      ack(bus, b, id, 1)
      acked(owner) mustBe Seq(1)
    }

    Scenario("a removed subscriber no longer keeps deliveries waiting: the owner is acknowledged, and it gets nothing more") {
      val (a, retired, owner) = (TestProbe(), TestProbe(), TestProbe())
      val bus = busWith(a, retired)
      val id = newId()
      publish(bus, owner, id, 1)
      delivered(a); delivered(retired)
      ack(bus, a, id, 1)

      val supervisor = TestProbe()
      supervisor.send(bus, RemoveSubscriber(retired.ref))

      supervisor.expectMsg(3.seconds, SubscriberRemoved(retired.ref))
      acked(owner) mustBe Seq(1)
      publish(bus, owner, id, 2)
      delivered(a) mustBe Seq(2)
      retired.expectNoMessage(300.millis)
      statusOf(bus).stopped mustBe empty
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

    Scenario("that starts at the squash event says so to the subscriptions that get that event (F98)") {
      val (a, b, owner) = (TestProbe(), TestProbe(), TestProbe())
      val bus = busWith(a, b)
      val id = newId()
      publish(bus, owner, id, 4)
      delivered(a); delivered(b)
      ack(bus, a, id, 4)

      bus.tell(PublishEvents[Any](cartType, events(4, 5), id, Some(BusCart("cart")), startsAtSquash = true), owner.ref)

      val toA = a.expectMsgType[IdentifiableEvents[Any]](3.seconds)
      (toA.events.map(_.version.asInt), toA.startsAtSquash) mustBe ((Seq(5), false)) // a has 4 already
      val toB = b.expectMsgType[IdentifiableEvents[Any]](3.seconds)
      (toB.events.map(_.version.asInt), toB.startsAtSquash) mustBe ((Seq(4, 5), true))
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

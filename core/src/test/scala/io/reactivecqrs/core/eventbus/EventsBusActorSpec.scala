package io.reactivecqrs.core.eventbus

import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.cluster.Readiness
import io.reactivecqrs.core.delivery.{RepublishOutbox, RepublishRange}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.testkit.{CommonSpec, ErrorRecorder, TestIds, TestSystems}
import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorRef, CoordinatedShutdown, Props, Status}
import org.apache.pekko.event.Logging
import org.apache.pekko.testkit.TestProbe

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._

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
    answered(bus, subscribers: _*)
    bus
  }

  /** Once the bus took the answers, drops the asks that a tick sent before an answer arrived: none come after. */
  private def answered(bus: ActorRef, subscribers: TestProbe*): Unit = {
    statusOf(bus)
    subscribers.foreach(_.receiveWhile(1.second, idle = 20.millis) { case _: GetSubscriptionRequests => () })
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

  private def newId(): AggregateId = AggregateId(TestIds.aggregate())

  Feature("Start-up (D15)") {

    Scenario("publishes nothing until every registered subscriber has answered") {
      val (a, b, owner) = (TestProbe(), TestProbe(), TestProbe())
      val bus = newBus()
      bus ! RegisterSubscribers(Seq(a.ref, b.ref))
      a.expectMsg(GetSubscriptionRequests(a.ref))
      a.reply(SubscriptionRequests(Seq(SubscribeForEvents(cartType, a.ref)), a.ref))
      b.expectMsg(GetSubscriptionRequests(b.ref))
      b.ignoreMsg { case _: GetSubscriptionRequests => true } // asked again every tick until it answers
      answered(bus, a)
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
      val bus = newBus()
      bus ! RegisterSubscribers(Seq(a.ref))
      a.expectMsg(GetSubscriptionRequests(a.ref))
      a.expectMsg(1.second, GetSubscriptionRequests(a.ref))
      system.stop(bus) // it would ask, and warn, every tick for the rest of the run
    }

    Scenario("waits for the subscribers of a second RegisterSubscribers too (F162)") {
      val (a, b, owner) = (TestProbe(), TestProbe(), TestProbe())
      val bus = newBus()
      bus ! RegisterSubscribers(Seq(a.ref))
      bus ! RegisterSubscribers(Seq(b.ref))
      a.expectMsg(GetSubscriptionRequests(a.ref))
      a.ignoreMsg { case _: GetSubscriptionRequests => true }
      b.expectMsg(GetSubscriptionRequests(b.ref))
      b.reply(SubscriptionRequests(Seq(SubscribeForEvents(cartType, b.ref)), b.ref))
      answered(bus, b)
      val id = newId()

      publish(bus, owner, id, 1)
      b.expectNoMessage(300.millis)

      a.reply(SubscriptionRequests(Seq(SubscribeForEvents(cartType, a.ref)), a.ref))
      delivered(a) mustBe Seq(1)
      delivered(b) mustBe Seq(1)
    }

    Scenario("asks a subscriber registered after the start, warns that it missed what was published before, and delivers to it once it answered (F162)") {
      val (a, late, owner, warnings) = (TestProbe(), TestProbe(), TestProbe(), TestProbe())
      val bus = busWith(a)
      system.eventStream.subscribe(warnings.ref, classOf[Logging.Warning])
      try {
        bus ! RegisterSubscribers(Seq(late.ref))
        late.expectMsg(GetSubscriptionRequests(late.ref))
        warnings.fishForMessage(3.seconds) { case w: Logging.Warning => w.message.toString.contains(late.ref.path.name + " after its start") }
      } finally system.eventStream.unsubscribe(warnings.ref)
      late.expectMsg(1.second, GetSubscriptionRequests(late.ref)) // asked every tick until it answers
      late.reply(SubscriptionRequests(Seq(SubscribeForEvents(cartType, late.ref)), late.ref))
      answered(bus, late)
      val id = newId()

      publish(bus, owner, id, 1)
      delivered(a) mustBe Seq(1)
      delivered(late) mustBe Seq(1)
    }

    Scenario("names a subscriber that answers with a failure in a warning, and asks it again (F162)") {
      val (a, owner, warnings) = (TestProbe(), TestProbe(), TestProbe())
      val bus = newBus()
      system.eventStream.subscribe(warnings.ref, classOf[Logging.Warning])
      try {
        bus ! RegisterSubscribers(Seq(a.ref))
        a.expectMsg(GetSubscriptionRequests(a.ref))
        a.reply(Status.Failure(new IllegalStateException("registry unavailable")))
        warnings.fishForMessage(3.seconds) { case w: Logging.Warning =>
          w.message.toString.contains(a.ref.path.name) && w.message.toString.contains("registry unavailable")
        }
      } finally system.eventStream.unsubscribe(warnings.ref)
      a.expectMsg(1.second, GetSubscriptionRequests(a.ref))
      a.reply(SubscriptionRequests(Seq(SubscribeForEvents(cartType, a.ref)), a.ref))
      answered(bus, a)
      val id = newId()

      publish(bus, owner, id, 1)
      delivered(a) mustBe Seq(1)
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

    Scenario("the proxy is asked again every tick until it answers: an ask can be lost while the singleton moves (F129)") {
      val (proxy, supervisor, before, after) = (TestProbe(), TestProbe(), TestProbe(), TestProbe())
      val bus = busBehindProxy(proxy, supervisor, before)
      proxy.ignoreNoMsg()
      system.stop(before.ref)
      proxy.expectMsg(3.seconds, GetSubscriptionRequests(proxy.ref)) // lost on the way
      proxy.expectMsg(1.second, GetSubscriptionRequests(proxy.ref))

      bus.tell(SubscriptionRequests(Seq(SubscribeForEvents(cartType, after.ref)), proxy.ref), supervisor.ref)
      statusOf(bus)
      proxy.receiveWhile(100.millis) { case _: GetSubscriptionRequests => () } // asked before the answer came
      proxy.expectNoMessage(300.millis)
    }

    Scenario("an instance that stops is no error: the proxy is asked for its replacement (F176)") {
      val (proxy, supervisor, instance, errors) = (TestProbe(), TestProbe(), TestProbe(), TestProbe())
      val bus = busBehindProxy(proxy, supervisor, instance)
      statusOf(bus)
      system.eventStream.subscribe(errors.ref, classOf[Logging.Error])
      try {
        proxy.ignoreNoMsg()
        system.stop(instance.ref)
        proxy.expectMsg(3.seconds, GetSubscriptionRequests(proxy.ref)) // asked after the bus logged the stop
        errors.receiveWhile(300.millis) { case e: Logging.Error => e.message.toString }.filter(_.contains(instance.ref.path.name)) mustBe empty
      } finally system.eventStream.unsubscribe(errors.ref)
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
      answered(bus, a)
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

    Scenario("sends each subscription consecutive versions only: what it is not sent is a gap it asks for, never one it steps over (F128)") {
      val (a, b, owner) = (TestProbe(), TestProbe(), TestProbe())
      val bus = busWith(a)
      val id = newId()
      publish(bus, owner, id, 1)
      delivered(a) mustBe Seq(1)
      ack(bus, a, id, 1)
      acked(owner) mustBe Seq(1) // its outbox row goes with the next batch
      publish(bus, owner, id, 2)
      delivered(a) mustBe Seq(2)

      Given("a subscription added while version 2 waits for the first subscriber only")
      bus ! AddSubscriptions(Seq(SubscribeForEvents(cartType, b.ref)))

      When("the outbox is re-published with a new event")
      publish(bus, owner, id, 1, 2, 3)

      Then("the added subscription gets versions 1 and 3 apart")
      delivered(b) mustBe Seq(1)
      delivered(b) mustBe Seq(3)
      delivered(a) mustBe Seq(1, 2, 3)
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
      val (a, b, owner, errors) = (TestProbe(), TestProbe(), TestProbe(), TestProbe())
      val bus = busWith(a, b)
      val id = newId()
      publish(bus, owner, id, 1)
      delivered(a); delivered(b)
      ack(bus, b, id, 1)

      system.eventStream.subscribe(errors.ref, classOf[Logging.Error])
      try {
        system.stop(a.ref)
        // F176: nothing replaces it before the next start, so it is an error
        errors.fishForMessage(3.seconds) { case e: Logging.Error => e.message.toString.contains(a.ref.path.name) }
      } finally system.eventStream.unsubscribe(errors.ref)
      awaitAssert(statusOf(bus).waiting mustBe 0, 3.seconds, 50.millis)
      owner.expectNoMessage(300.millis) // version 1 stays in the outbox for the stopped subscriber
      statusOf(bus).stopped mustBe Seq(a.ref.path.name)

      publish(bus, owner, id, 2)
      delivered(b) mustBe Seq(2)
      ack(bus, b, id, 2)
      owner.expectNoMessage(300.millis) // version 2 too: after a restart the subscriber gets both
      statusOf(bus).waiting mustBe 0
    }

    Scenario("a subscriber that stops while its node shuts down is no error (F176)") {
      Given("a node whose bus has a subscriber, and keeps the errors it logs")
      val node = TestSystems.newSystem("f176")
      val errors = new ConcurrentLinkedQueue[String]()
      val recorder = node.actorOf(Props(new ErrorRecorder(errors)))
      node.eventStream.subscribe(recorder, classOf[Logging.Error])
      val (subscriber, asker) = (TestProbe()(node), TestProbe()(node))
      val bus = node.actorOf(Props(new EventsBusActor(options)))
      bus ! RegisterSubscribers(Seq(subscriber.ref))
      subscriber.expectMsg(GetSubscriptionRequests(subscriber.ref))
      subscriber.reply(SubscriptionRequests(Seq(SubscribeForEvents(cartType, subscriber.ref)), subscriber.ref))

      When("the subscriber stops during the node's shutdown, before the bus, as a cluster singleton does as its node leaves")
      CoordinatedShutdown(node).addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "stop-subscriber") { () =>
        Future {
          node.stop(subscriber.ref)
          awaitAssert({
            bus.tell(GetDeliveryStatus, asker.ref)
            asker.expectMsgType[DeliveryStatus](3.seconds).stopped mustBe Seq(subscriber.ref.path.name)
          }, 3.seconds, 50.millis)
          recorder.tell("ping", asker.ref)
          asker.expectMsg(3.seconds, "pong") // what the bus logged is kept
          Done
        }(ExecutionContext.global)
      }
      Await.ready(node.terminate(), 30.seconds)

      Then("the bus logged no error about it")
      errors.asScala.filter(_.contains(subscriber.ref.path.name)) mustBe empty
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

  Feature("Flow control (F93)") {

    // A window of 2 events in flight per subscriber.
    def windowedBus(window: Int, windowBytes: Long, subscribers: TestProbe*): ActorRef = {
      val bus = system.actorOf(Props(new EventsBusActor(options.copy(deadline = 10.seconds, window = window, windowBytes = windowBytes))))
      bus ! RegisterSubscribers(subscribers.map(_.ref))
      subscribers.foreach { subscriber =>
        subscriber.expectMsg(GetSubscriptionRequests(subscriber.ref))
        subscriber.reply(SubscriptionRequests(Seq(SubscribeForEvents(cartType, subscriber.ref)), subscriber.ref))
      }
      answered(bus, subscribers: _*)
      bus
    }

    def deliveredTo(subscriber: TestProbe): (AggregateId, Seq[Int]) = {
      val delivery = subscriber.expectMsgType[IdentifiableEvents[Any]](3.seconds)
      (delivery.aggregateId, delivery.events.map(_.version.asInt))
    }

    Scenario("a slow subscriber gets at most its window; the rest is held, pulled once half the window is free, and acknowledged to the owner when it has it") {
      val (slow, owner, commandBus, republisher) = (TestProbe(), TestProbe(), TestProbe(), TestProbe())
      val bus = windowedBus(2, Long.MaxValue, slow)
      bus ! RegisterPublisher(cartType, commandBus.ref)
      val Seq(x, y, z) = Seq(newId(), newId(), newId()): @unchecked

      publish(bus, owner, x, 1)
      publish(bus, owner, y, 1)
      publish(bus, owner, z, 1)
      deliveredTo(slow) mustBe (x -> Seq(1))
      deliveredTo(slow) mustBe (y -> Seq(1))
      slow.expectNoMessage(300.millis) // z is held
      val held = statusOf(bus)
      (held.held, held.behind.values.sum) mustBe ((1, 1))

      ack(bus, slow, x, 1)
      acked(owner) mustBe Seq(1)
      commandBus.expectMsg(3.seconds, RepublishRange(z, AggregateVersion(1), slow.ref))

      // The owner re-publishes z to the slow subscriber; once it has it, the owner of the live publication is acknowledged.
      publishTo(bus, republisher, z, slow.ref, 1)
      deliveredTo(slow) mustBe (z -> Seq(1))
      ack(bus, slow, y, 1)
      ack(bus, slow, z, 1)
      Set(acked(owner), acked(owner)) mustBe Set(Seq(1))
      owner.expectNoMessage(300.millis)
      republisher.expectNoMessage(100.millis)
      statusOf(bus) mustBe DeliveryStatus(0, Nil, Nil, 0, Map.empty)
    }

    Scenario("a fast subscriber is not held back by a slow one") {
      val (slow, fast, owner) = (TestProbe(), TestProbe(), TestProbe())
      val bus = windowedBus(2, Long.MaxValue, slow, fast)
      val ids = (1 to 4).map(_ => newId())
      ids.foreach { id =>
        publish(bus, owner, id, 1)
        deliveredTo(fast) mustBe (id -> Seq(1))
        ack(bus, fast, id, 1)
      }
      deliveredTo(slow)._1 mustBe ids(0)
      deliveredTo(slow)._1 mustBe ids(1)
      slow.expectNoMessage(300.millis)
      owner.expectNoMessage(100.millis) // the slow one has not acknowledged anything
    }

    Scenario("an aggregate held for a subscriber keeps its later events held, so the subscriber sees no gap") {
      val (slow, owner, commandBus) = (TestProbe(), TestProbe(), TestProbe())
      val bus = windowedBus(1, Long.MaxValue, slow)
      bus ! RegisterPublisher(cartType, commandBus.ref)
      val (x, y) = (newId(), newId())

      publish(bus, owner, x, 1)
      deliveredTo(slow) mustBe (x -> Seq(1))
      publish(bus, owner, y, 1) // held: the window is full
      ack(bus, slow, x, 1)
      commandBus.expectMsg(3.seconds, RepublishRange(y, AggregateVersion(1), slow.ref))
      publish(bus, owner, y, 2) // the window has room, but y is being pulled
      slow.expectNoMessage(300.millis)

      publishTo(bus, owner, y, slow.ref, 1, 2) // the pulled part holds both
      deliveredTo(slow) mustBe (y -> Seq(1, 2))
    }

    Scenario("the window counts the events' stored sizes too (F126)") {
      val (slow, owner) = (TestProbe(), TestProbe())
      val bus = windowedBus(100, 1000, slow)
      val (x, y) = (newId(), newId())
      bus.tell(PublishEvents[Any](cartType, events(1), x, Some(BusCart("cart")), sizes = Seq(600)), owner.ref)
      deliveredTo(slow) mustBe (x -> Seq(1))
      bus.tell(PublishEvents[Any](cartType, events(1), y, Some(BusCart("cart")), sizes = Seq(600)), owner.ref)
      slow.expectNoMessage(300.millis) // 1 200 bytes would be in flight
      statusOf(bus).held mustBe 1
    }

    Scenario("a held delivery stalls from when its pull sends it, not from when it was held") {
      val (slow, owner, commandBus, warnings) = (TestProbe(), TestProbe(), TestProbe(), TestProbe())
      val bus = windowedBus(1, Long.MaxValue, slow)
      bus ! RegisterPublisher(cartType, commandBus.ref)
      val (x, y) = (newId(), newId())
      publish(bus, owner, x, 1)
      deliveredTo(slow) mustBe (x -> Seq(1))
      publish(bus, owner, y, 1)
      slow.expectNoMessage(options.stallAfter + 100.millis) // y is held longer than a stall

      ack(bus, slow, x, 1)
      commandBus.expectMsg(3.seconds, RepublishRange(y, AggregateVersion(1), slow.ref))
      system.eventStream.subscribe(warnings.ref, classOf[Logging.Warning])
      try {
        publishTo(bus, owner, y, slow.ref, 1)
        deliveredTo(slow) mustBe (y -> Seq(1))
        def aboutY(message: Any) = message.toString.contains(" " + y.asLong + " version 1 to")
        warnings.receiveWhile(options.stallAfter - 100.millis) { case w: Logging.Warning => w.message }.filter(aboutY) mustBe empty
        warnings.fishForMessage(3.seconds) { case w: Logging.Warning => aboutY(w.message) }
      } finally system.eventStream.unsubscribe(warnings.ref)
    }
  }
}

package io.reactivecqrs.core.projection

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.eventbus.EventsBusActor.MessageAck
import io.reactivecqrs.core.projection.SubscribableProjectionActor.{ProjectionSubscriptionsCancelled, SubscribedForProjectionUpdates, SubscriptionUpdated}
import io.reactivecqrs.core.util.Backoff
import io.reactivecqrs.testutils.{CommonSpec, TestIds}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe

import scala.concurrent.duration._

case class PushedNote(text: String)
case class PushedNoteWritten(text: String) extends Event[PushedNote]
case class SubscribeToTexts(subscriptionId: String, listener: ActorRef, filter: String => Option[String])

/** Pushes each written text to its subscribers; its listener fails after pushing, as many times as `failures` says. */
class PushingProjection(val subscriptionsState: SubscriptionsState, failures: AtomicInteger, parallel: Int)
  extends SubscribableProjectionActor(options = ProjectionActorOptions.DEFAULT.withRetries(Backoff(20.millis, 50.millis, maxAttempts = 5))
    .copy(parallelUpdateProcessing = parallel)) {

  protected val listeners = List(EventsListener[PushedNote] { (_, events) => _ =>
    events.foreach(_.event match {
      case PushedNoteWritten(text) => sendUpdate(text)
      case _ => ()
    })
    if (failures.getAndDecrement() > 0) throw new IllegalStateException("listener failed after pushing")
  })

  override protected def receiveSubscriptionRequest: Receive = {
    case SubscribeToTexts(subscriptionId, listener, filter) => handleSubscribe[String, String](subscriptionId, listener, filter)
  }

  override protected def receiveQuery: Receive = PartialFunction.empty

  override protected val projectionName: String = "PushingProjection"
}

// F161: a subscribable projection pushes what its transaction committed, to each subscription on its own.
class SubscribablePushSpec extends CommonSpec {

  private val noteType = AggregateType.of[PushedNote]

  private class Fixture(failures: Int = 0, parallel: Int = 0) {
    val bus = TestProbe()
    val projection: ActorRef = system.actorOf(Props(new PushingProjection(new MemorySubscriptionsState, new AtomicInteger(failures), parallel)))

    def subscribe(subscriptionId: String, listener: TestProbe, filter: String => Option[String] = Some(_)): Unit = {
      projection ! SubscribeToTexts(subscriptionId, listener.ref, filter)
      listener.expectMsg(3.seconds, SubscribedForProjectionUpdates(subscriptionId))
    }

    def written(text: String): AggregateId = {
      val id = AggregateId(TestIds.aggregate())
      bus.send(projection, IdentifiableEvents[PushedNote](noteType, id, Seq(EventInfo[PushedNote](AggregateVersion(1), PushedNoteWritten(text), UserId(1), Instant.now))))
      bus.fishForSpecificMessage(5.seconds) { case MessageAck(_, `id`, _, _) => () }
      id
    }
  }

  Feature("Pushes to the subscriptions of a subscribable projection (F161)") {

    Scenario("an update is pushed once its transaction has committed: the attempts that rolled back push nothing") {
      val f = new Fixture(failures = 2)
      val listener = TestProbe()
      f.subscribe("s-1", listener)

      f.written("hello")

      listener.expectMsg(3.seconds, SubscriptionUpdated("s-1", "hello"))
      listener.expectNoMessage(300.millis)
    }

    Scenario("the same with listeners in parallel, on threads of their own") {
      val f = new Fixture(failures = 2, parallel = 2)
      val listener = TestProbe()
      f.subscribe("s-1", listener)

      f.written("hello")

      listener.expectMsg(3.seconds, SubscriptionUpdated("s-1", "hello"))
      listener.expectNoMessage(300.millis)
    }

    Scenario("a filter that throws keeps the update from no other subscription") {
      val f = new Fixture
      val (quiet, failing) = (TestProbe(), TestProbe())
      f.subscribe("quiet", quiet)
      f.subscribe("failing", failing, _ => throw new IllegalStateException("filter failed"))

      f.written("hello")

      quiet.expectMsg(3.seconds, SubscriptionUpdated("quiet", "hello"))
      failing.expectNoMessage(300.millis)
    }

    Scenario("a subscription id used again replaces its subscription: the newest listener gets each update once, the one before is told it is cancelled") {
      val f = new Fixture
      val (first, second) = (TestProbe(), TestProbe())
      f.subscribe("s", first)
      f.subscribe("s", second)
      first.expectMsg(3.seconds, ProjectionSubscriptionsCancelled(List("s")))

      f.written("hello")

      second.expectMsg(3.seconds, SubscriptionUpdated("s", "hello"))
      second.expectNoMessage(300.millis)
      first.expectNoMessage(100.millis)
    }
  }
}

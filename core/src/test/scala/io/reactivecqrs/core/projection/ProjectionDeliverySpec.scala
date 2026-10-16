package io.reactivecqrs.core.projection

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.util.Backoff
import io.reactivecqrs.testutils.CommonSpec
import org.apache.pekko.actor.{ActorRef, Props, Status}
import org.apache.pekko.testkit.TestProbe

import scala.concurrent.duration._

case class GateNote(text: String)
case class GateNoteChanged(n: Int) extends Event[GateNote]

case object GetApplied
case object FailingQuery

/** Records what its listeners applied; fails for an aggregate as many times as `failures` says. */
class RecordingProjection(val subscriptionsState: SubscriptionsState, options: ProjectionActorOptions,
                          failures: ConcurrentHashMap[AggregateId, AtomicInteger], slow: Boolean = false) extends ProjectionActor(options) {

  @volatile private var applied = Vector[(String, AggregateId, Seq[Int])]()

  private def failIfAsked(id: AggregateId): Unit =
    Option(failures.get(id)).filter(_.getAndDecrement() > 0).foreach(_ => throw new IllegalStateException("listener failed on " + id.asLong))

  protected val listeners = List(
    EventsListener.noDBSession[GateNote] { (id, events) =>
      if (slow) Thread.sleep(50)
      failIfAsked(id)
      synchronized(applied :+= (("events", id, events.map(_.version.asInt))))
    },
    AggregateListener.noDBSession[GateNote] { (id, version, created, note) =>
      synchronized(applied :+= (("state", id, Seq(version.asInt, if (created) 1 else 0, note.map(_.text.length).getOrElse(-1)))))
    })

  override protected def receiveQuery: Receive = {
    case GetApplied => sender() ! synchronized(applied)
    case FailingQuery => throw new IllegalStateException("query failed")
  }

  override protected def onClearProjectionData(): Unit = ()
  override protected val projectionName: String = "RecordingProjection"
  override protected val version: Int = 1
}

// D4, D20a, D20c, F5, F28, F45, F80: a projection applies each version once and in order, asks for missing versions,
// and turns a listener that keeps failing into a recorded failure without stopping other aggregates.
class ProjectionDeliverySpec extends CommonSpec {

  private val noteType = AggregateType(classOf[GateNote].getName)
  private val quickRetries = Backoff(20.millis, 50.millis, maxAttempts = 3)

  private class Fixture(options: ProjectionActorOptions = ProjectionActorOptions.DEFAULT.withRetries(quickRetries),
                        val state: SubscriptionsState = new MemorySubscriptionsState, slow: Boolean = false, register: Boolean = true) {
    val failures = new ConcurrentHashMap[AggregateId, AtomicInteger]()
    val bus = TestProbe()
    val projection: ActorRef = system.actorOf(Props(new RecordingProjection(state, options, failures, slow)))
    if (register) {
      bus.send(projection, GetSubscriptionRequests)
      bus.expectMsgType[SubscriptionRequests](3.seconds)
    }

    def failTimes(id: AggregateId, times: Int): Unit = failures.put(id, new AtomicInteger(times))

    def events(id: AggregateId, versions: Int*): Unit =
      bus.send(projection, IdentifiableEvents[GateNote](noteType, id, versions.map(v =>
        EventInfo[GateNote](AggregateVersion(v), GateNoteChanged(v), UserId(1), Instant.now)), replayed = false))

    def stateAt(id: AggregateId, text: String, versions: Int*): Unit =
      bus.send(projection, AggregateWithType[GateNote](noteType, id, AggregateVersion(versions.last), versions.map(AggregateVersion(_)), Some(GateNote(text)), replayed = false))

    def acked(id: AggregateId): Seq[Int] =
      bus.fishForSpecificMessage(3.seconds) { case MessageAck(_, `id`, versions) => versions.map(_.asInt) }

    def failedFor(id: AggregateId): String =
      bus.fishForSpecificMessage(3.seconds) { case MessageFailed(_, `id`, _, error) => error }

    def requestedFrom(id: AggregateId): Int =
      bus.fishForSpecificMessage(3.seconds) { case RequestRepublish(_, `noteType`, `id`, from) => from.asInt }

    def applied(kind: String): Seq[(AggregateId, Seq[Int])] =
      projection.askActor[Vector[(String, AggregateId, Seq[Int])]](GetApplied)(3.seconds).collect { case (`kind`, id, versions) => (id, versions) }

    def status(): ProjectionStatus = projection.askActor[ProjectionStatus](GetProjectionStatus)(3.seconds)
  }

  private def newId(): AggregateId = AggregateId(System.nanoTime())

  Feature("Applying deliveries (D4)") {

    Scenario("new versions are applied once, and a partly applied delivery only with its new part (F5)") {
      val f = new Fixture
      val id = newId()

      f.events(id, 1, 2)
      f.acked(id) mustBe Seq(1, 2)
      f.events(id, 2, 3)
      f.acked(id) mustBe Seq(2, 3)
      f.events(id, 1, 2, 3)
      f.acked(id) mustBe Seq(1, 2, 3)

      f.applied("events") mustBe Seq(id -> Seq(1, 2), id -> Seq(3))
    }

    Scenario("a state listener gets the state after the last new version, and knows when it starts at version 1") {
      val f = new Fixture
      val id = newId()

      f.stateAt(id, "a", 1)
      f.acked(id)
      f.stateAt(id, "abc", 2, 3)
      f.acked(id)

      f.applied("state") mustBe Seq(id -> Seq(1, 1, 1), id -> Seq(3, 0, 3))
    }
  }

  Feature("Missing versions (D20c, F45)") {

    Scenario("a gap is asked from the owner, and the waiting delivery is applied with the re-published versions") {
      val f = new Fixture
      val id = newId()
      f.events(id, 1)
      f.acked(id)

      f.events(id, 4)
      f.requestedFrom(id) mustBe 2
      f.status().waitingForGap mustBe Seq(id)

      f.events(id, 2, 3, 4) // the re-publication
      Set(f.acked(id), f.acked(id)) mustBe Set(Seq(4), Seq(2, 3, 4))
      f.applied("events") mustBe Seq(id -> Seq(1), id -> Seq(2, 3, 4))
      f.status().waitingForGap mustBe empty
    }

    Scenario("a request that gets no answer is sent again") {
      val f = new Fixture(ProjectionActorOptions.DEFAULT.copy(gapRequestRepeat = 200.millis))
      val id = newId()

      f.events(id, 3)

      f.requestedFrom(id) mustBe 1
      f.requestedFrom(id) mustBe 1
    }
  }

  Feature("A listener that fails (D20a)") {

    Scenario("is retried, and a failure that heals meanwhile leaves no trace") {
      val f = new Fixture
      val id = newId()
      f.failTimes(id, 2)

      f.events(id, 1)

      f.acked(id) mustBe Seq(1)
      f.applied("events") mustBe Seq(id -> Seq(1))
      f.status().failures mustBe empty
    }

    Scenario("that keeps failing is recorded; that aggregate's deliveries fail, other aggregates keep flowing (R1)") {
      val f = new Fixture
      val (broken, healthy) = (newId(), newId())
      f.failTimes(broken, 100)

      f.events(broken, 1, 2)
      f.failedFor(broken) must include("listener failed")

      f.status().failures.map(fl => (fl.aggregateId, fl.version.asInt, fl.subscriptionType)) mustBe Seq((broken, 1, SubscriptionType.EVENTS))
      f.state.failures(classOf[RecordingProjection].getName).map(_.aggregateId) mustBe Seq(broken)

      f.events(broken, 3)
      f.failedFor(broken) must include("listener failed")
      f.events(healthy, 1)
      f.acked(healthy) mustBe Seq(1)
    }

    Scenario("RetryFailedSubscription clears the failure and asks for the failed version again") {
      val f = new Fixture
      val id = newId()
      f.events(id, 1)
      f.acked(id) mustBe Seq(1)
      f.failTimes(id, 3) // every attempt of the next delivery, so it is recorded; afterwards the listener works
      f.events(id, 2, 3)
      f.failedFor(id)

      f.projection.askActor[FailedSubscriptionsHandled](RetryFailedSubscription(id))(3.seconds) mustBe FailedSubscriptionsHandled(id, 1)

      f.requestedFrom(id) mustBe 2
      f.status().failures mustBe empty
      f.events(id, 2, 3)
      f.acked(id) mustBe Seq(2, 3)
      f.applied("events") mustBe Seq(id -> Seq(1), id -> Seq(2, 3))
    }

    Scenario("SkipFailedVersion marks the failed version as applied and asks for the versions after it") {
      val f = new Fixture
      val id = newId()
      f.failTimes(id, 3)
      f.events(id, 1)
      f.failedFor(id)

      f.projection.askActor[FailedSubscriptionsHandled](SkipFailedVersion(id))(3.seconds) mustBe FailedSubscriptionsHandled(id, 1)

      f.requestedFrom(id) mustBe 2
      f.state.lastVersionForEventsSubscription(classOf[RecordingProjection].getName, id) mustBe AggregateVersion(1)
      f.applied("events") mustBe empty
    }

    Scenario("failures recorded before the start are retried when the projection starts") {
      val state = new MemorySubscriptionsState
      val id = newId()
      state.recordFailure(classOf[RecordingProjection].getName,
        SubscriptionFailure(SubscriptionType.EVENTS, noteType, id, AggregateVersion(4), "failed before", Instant.now))

      val f = new Fixture(state = state)

      f.requestedFrom(id) mustBe 4
      state.failures(classOf[RecordingProjection].getName) mustBe empty
    }
  }

  Feature("Grouping and parallel processing") {

    Scenario("with a grouping delay, live deliveries are applied together") {
      val f = new Fixture(ProjectionActorOptions.DEFAULT.withGroupUpdatesDelayMillis(300))
      val id = newId()

      f.events(id, 1)
      f.events(id, 2)
      f.events(id, 3)

      Seq(f.acked(id), f.acked(id), f.acked(id)).flatten.sorted mustBe Seq(1, 2, 3)
      f.applied("events") mustBe Seq(id -> Seq(1, 2, 3))
    }

    Scenario("in parallel mode each aggregate keeps its order, and a retry waits for its back-off") {
      val f = new Fixture(ProjectionActorOptions.DEFAULT.withParallelUpdateProcessing(4).withRetries(quickRetries), slow = true)
      val (a, b) = (newId(), newId())
      f.failTimes(b, 1)

      f.events(a, 1)
      f.events(b, 1)
      f.events(a, 2)
      f.events(b, 2)

      awaitAssert({
        val applied = f.applied("events")
        applied.filter(_._1 == a).flatMap(_._2) mustBe Seq(1, 2)
        applied.filter(_._1 == b).flatMap(_._2) mustBe Seq(1, 2)
      }, 5.seconds, 50.millis)
    }
  }

  Feature("Queries (AR7, F80)") {

    Scenario("a query that fails is answered with the failure, and the projection keeps working") {
      val f = new Fixture
      val id = newId()
      val asker = TestProbe()

      asker.send(f.projection, FailingQuery)
      asker.expectMsgType[Status.Failure](3.seconds)

      f.events(id, 1)
      f.acked(id) mustBe Seq(1)
    }
  }
}

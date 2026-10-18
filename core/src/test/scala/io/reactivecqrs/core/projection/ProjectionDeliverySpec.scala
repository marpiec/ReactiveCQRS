package io.reactivecqrs.core.projection

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.util.Backoff
import io.reactivecqrs.testkit.{CommonSpec, TestIds}
import org.apache.pekko.actor.{ActorRef, Props, Status}
import org.apache.pekko.testkit.TestProbe

import scala.concurrent.duration._

case class GateNote(text: String)
case class GateNoteChanged(n: Int) extends Event[GateNote]

case object GetApplied
case object GetRebuildingSeen
case object FailingQuery

/** Records what its listeners applied; fails for an aggregate as many times as `failures` says. */
class RecordingProjection(val subscriptionsState: SubscriptionsState, options: ProjectionActorOptions,
                          failures: ConcurrentHashMap[AggregateId, AtomicInteger], slowMillis: Int = 0) extends ProjectionActor(options) {

  @volatile private var applied = Vector[(String, AggregateId, Seq[Int])]()
  @volatile private var rebuildingSeen = Vector[(AggregateId, Boolean)]()

  private def failIfAsked(id: AggregateId): Unit =
    Option(failures.get(id)).filter(_.getAndDecrement() > 0).foreach(_ => throw new IllegalStateException("listener failed on " + id.asLong))

  protected val listeners = List(
    EventsListener.noDBSession[GateNote] { (id, events) =>
      if (slowMillis > 0) Thread.sleep(slowMillis)
      failIfAsked(id)
      synchronized {
        applied :+= (("events", id, events.map(_.version.asInt)))
        rebuildingSeen :+= id -> rebuilding
      }
    },
    AggregateListener.noDBSession[GateNote] { (id, version, created, note) =>
      synchronized(applied :+= (("state", id, Seq(version.asInt, if (created) 1 else 0, note.map(_.text.length).getOrElse(-1)))))
    })

  override protected def receiveQuery: Receive = {
    case GetApplied => sender() ! synchronized(applied)
    case GetRebuildingSeen => sender() ! synchronized(rebuildingSeen)
    case FailingQuery => throw new IllegalStateException("query failed")
  }

  // It keeps nothing of its own to clear.
  override protected def clearForRebuild(target: RebuildTarget)(implicit session: scalikejdbc.DBSession): Unit = ()

  override protected val projectionName: String = "RecordingProjection"
}

/** Declares two listeners of one kind for one type, of which only one could be called (F164). */
class TwiceListeningProjection(val subscriptionsState: SubscriptionsState) extends ProjectionActor(ProjectionActorOptions.DEFAULT) {
  protected val listeners = List(
    EventsListener.noDBSession[GateNote] { (_, _) => () },
    AggregateListener.noDBSession[GateNote] { (_, _, _, _) => () },
    EventsListener.noDBSession[GateNote] { (_, _) => () })

  override protected def receiveQuery: Receive = PartialFunction.empty

  override protected val projectionName: String = "TwiceListeningProjection"
}

case class FindNoteVersion(id: AggregateId, waitMillis: Int)
case class FindNoteVersionOrZero(id: AggregateId, waitMillis: Int)

/** Each note's last version, in memory; its searches read at once and answer `searchMillis` later, so an update can be
  * applied while a search is under way (F163). */
class SearchedNotes(val subscriptionsState: SubscriptionsState, searchMillis: Long) extends ProjectionActor {
  private val versions = new ConcurrentHashMap[AggregateId, Integer]()
  private implicit val searches: scala.concurrent.ExecutionContext = context.dispatcher

  protected val listeners = List(EventsListener.noDBSession[GateNote] { (id, events) => versions.put(id, events.last.version.asInt) })

  private def search(id: AggregateId): () => scala.concurrent.Future[Option[Int]] = () => {
    val found = Option(versions.get(id)).map(_.intValue)
    scala.concurrent.Future { Thread.sleep(searchMillis); found }
  }

  override protected def receiveQuery: Receive = {
    case FindNoteVersion(id, wait) => delayIfNotAvailableAsync[Int](sender(), search(id), wait)
    case FindNoteVersionOrZero(id, wait) =>
      delayIfNotAvailableAsyncCustom[Int](sender(), () => search(id)().map(_.getOrElse(0)), (version: Int) => version > 0, wait)
  }

  override protected val projectionName: String = "SearchedNotes"
}

/** A store whose failure records fail until `storable`, as during a database outage. */
class UnstorableFailures extends MemorySubscriptionsState {
  @volatile var storable = false
  override def recordFailure(subscriberName: String, failure: SubscriptionFailure): Unit =
    if (storable) super.recordFailure(subscriberName, failure) else throw new IllegalStateException("database unavailable")
}

// D4, D20a, D20c, F5, F28, F45, F80: a projection applies each version once and in order, asks for missing versions,
// and turns a listener that keeps failing into a recorded failure without stopping other aggregates.
class ProjectionDeliverySpec extends CommonSpec {

  private val noteType = AggregateType.of[GateNote]
  private val quickRetries = Backoff(20.millis, 50.millis, maxAttempts = 3)

  private class Fixture(options: ProjectionActorOptions = ProjectionActorOptions.DEFAULT.withRetries(quickRetries),
                        val state: SubscriptionsState = new MemorySubscriptionsState, slowMillis: Int = 0, register: Boolean = true) {
    val failures = new ConcurrentHashMap[AggregateId, AtomicInteger]()
    val bus = TestProbe()
    val projection: ActorRef = system.actorOf(Props(new RecordingProjection(state, options, failures, slowMillis)))
    if (register) {
      bus.send(projection, GetSubscriptionRequests(projection))
      bus.expectMsgType[SubscriptionRequests](3.seconds)
    }

    def failTimes(id: AggregateId, times: Int): Unit = failures.put(id, new AtomicInteger(times))

    def events(id: AggregateId, versions: Int*): Unit =
      bus.send(projection, IdentifiableEvents[GateNote](noteType, id, versions.map(v =>
        EventInfo[GateNote](AggregateVersion(v), GateNoteChanged(v), UserId(1), Instant.now))))

    def stateAt(id: AggregateId, text: String, versions: Int*): Unit =
      bus.send(projection, AggregateWithType[GateNote](noteType, id, AggregateVersion(versions.last), versions.map(AggregateVersion(_)), Some(GateNote(text))))

    // A builder (D58) feeding the projection from the store.
    val builder = TestProbe()

    def chunk(id: Long, items: ChunkItem*): ChunkApplied = {
      builder.send(projection, ApplyChunk(id, items))
      builder.expectMsgType[ChunkApplied](3.seconds)
    }

    def acked(id: AggregateId): Seq[Int] =
      bus.fishForSpecificMessage(3.seconds) { case MessageAck(_, `id`, versions, _) => versions.map(_.asInt) }

    def failedFor(id: AggregateId): String =
      bus.fishForSpecificMessage(3.seconds) { case MessageFailed(_, `id`, _, error, _) => error }

    def requestedFrom(id: AggregateId): Int =
      bus.fishForSpecificMessage(3.seconds) { case RequestRepublish(_, `noteType`, `id`, from) => from.asInt }

    def outboxRepublished(id: AggregateId): Unit =
      bus.fishForSpecificMessage(3.seconds) { case Republish(`noteType`, `id`) => () }

    def applied(kind: String): Seq[(AggregateId, Seq[Int])] =
      projection.askFor[Vector[(String, AggregateId, Seq[Int])]](GetApplied, 3.seconds).collect { case (`kind`, id, versions) => (id, versions) }

    def status(): ProjectionStatus = projection.askFor[ProjectionStatus](GetProjectionStatus, 3.seconds)

    /** Every answer about the aggregate that arrives within `within`: acknowledgements and failures, in order. */
    def answers(id: AggregateId, within: FiniteDuration): Seq[String] = bus.receiveWhile(within) {
      case MessageAck(_, `id`, versions, _) => "ack " + versions.map(_.asInt).mkString(",")
      case MessageFailed(_, `id`, versions, _, _) => "failed " + versions.map(_.asInt).mkString(",")
      case _ => "other"
    }.filterNot(_ == "other")
  }

  private def newId(): AggregateId = AggregateId(TestIds.aggregate())

  private def eventsItem(id: AggregateId, versions: Int*): ChunkItem =
    ChunkItem(SubscriptionType.EVENTS, noteType, id, AggregateVersion(versions.head), AggregateVersion(versions.last),
      versions.map(v => EventInfo[Any](AggregateVersion(v), GateNoteChanged(v).asInstanceOf[Event[Any]], UserId(1), Instant.now)), None)

  private def stateItem(id: AggregateId, text: String, first: Int, last: Int, startsSequence: Boolean): ChunkItem =
    ChunkItem(SubscriptionType.AGGREGATES, noteType, id, AggregateVersion(first), AggregateVersion(last), Seq.empty, Some(GateNote(text)), startsSequence)

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

    Scenario("a re-publication that starts at the squash event starts a subscription that never saw the aggregate (F98)") {
      val f = new Fixture
      val id = newId()

      f.bus.send(f.projection, IdentifiableEvents[GateNote](noteType, id, Seq(7, 8).map(v =>
        EventInfo[GateNote](AggregateVersion(v), GateNoteChanged(v), UserId(1), Instant.now)), startsAtSquash = true))
      f.bus.send(f.projection, AggregateWithType[GateNote](noteType, id, AggregateVersion(8), Seq(AggregateVersion(7), AggregateVersion(8)),
        Some(GateNote("ab")), startsAtSquash = true))

      f.acked(id) mustBe Seq(7, 8)
      f.acked(id) mustBe Seq(7, 8)
      f.applied("events") mustBe Seq(id -> Seq(7, 8))
      f.applied("state") mustBe Seq(id -> Seq(8, 1, 2)) // told it is created: the subscription starts here
    }
  }

  Feature("Chunks from a builder (D58, D61)") {

    Scenario("a chunk applies many aggregates in commit order, and nothing that was applied before") {
      val f = new Fixture
      val (a, b) = (newId(), newId())

      f.chunk(1, eventsItem(a, 1, 2), eventsItem(b, 1), eventsItem(a, 3)) mustBe ChunkApplied(1, Nil, 0)
      f.applied("events") mustBe Seq(a -> Seq(1, 2), b -> Seq(1), a -> Seq(3))

      f.chunk(2, eventsItem(a, 2, 3), eventsItem(b, 1, 2)) mustBe ChunkApplied(2, Nil, 0)
      f.applied("events") mustBe Seq(a -> Seq(1, 2), b -> Seq(1), a -> Seq(3), b -> Seq(2))
      f.builder.expectNoMessage(200.millis) // nothing is acknowledged item by item
    }

    Scenario("an item after missing versions is reported as a gap, and the rest of its aggregate in the chunk waits") {
      val f = new Fixture
      val (a, b) = (newId(), newId())

      f.chunk(1, eventsItem(a, 3), eventsItem(b, 1), eventsItem(a, 4)) mustBe
        ChunkApplied(1, Seq(ChunkGap(SubscriptionType.EVENTS, noteType, a, AggregateVersion(1))), 0)
      f.applied("events") mustBe Seq(b -> Seq(1))
    }

    Scenario("a whole state starts the subscription, and the state listener is told it is created (D62)") {
      val f = new Fixture
      val id = newId()

      f.chunk(1, stateItem(id, "abc", 1, 5, startsSequence = true)) mustBe ChunkApplied(1, Nil, 0)
      f.applied("state") mustBe Seq(id -> Seq(5, 1, 3))
    }

    Scenario("a chunk that fails is applied one aggregate at a time, and each of its items is answered to the builder") {
      val f = new Fixture
      val (a, b) = (newId(), newId())
      f.failTimes(b, 1)

      f.chunk(1, eventsItem(a, 1), eventsItem(b, 1)) mustBe ChunkApplied(1, Nil, 2)

      f.builder.receiveN(2, 3.seconds).collect { case MessageAck(_, id, versions, _) => id -> versions.map(_.asInt) }.toSet mustBe
        Set(a -> Seq(1), b -> Seq(1))
      f.applied("events").filter(_._1 == b) mustBe Seq(b -> Seq(1))
    }

    Scenario("a subscription with deliveries at work takes its items in its queue") {
      val f = new Fixture
      val id = newId()
      f.events(id, 3)
      f.requestedFrom(id) mustBe 1

      f.chunk(1, eventsItem(id, 1, 2)) mustBe ChunkApplied(1, Nil, 1)

      f.builder.expectMsgType[MessageAck](3.seconds).versions.map(_.asInt) mustBe Seq(1, 2)
      f.acked(id) mustBe Seq(3)
      f.applied("events") mustBe Seq(id -> Seq(1, 2, 3))
    }

    Scenario("while built, the projection takes no deliveries from the bus and asks the builder for missing versions, until GoLive") {
      val f = new Fixture
      val (a, b) = (newId(), newId())
      f.builder.send(f.projection, BuildFromStore(f.builder.ref))

      f.events(a, 1)
      f.acked(a) mustBe Seq(1) // the builder reads it from the store
      f.applied("events") mustBe empty

      f.failTimes(b, 100)
      f.chunk(1, eventsItem(b, 1)) mustBe ChunkApplied(1, Nil, 1)
      f.builder.fishForSpecificMessage(3.seconds) { case MessageFailed(_, `b`, _, _, _) => () }
      f.failTimes(b, 0)
      f.projection.askFor[FailedSubscriptionsHandled](RetryFailedSubscription(b), 3.seconds) mustBe FailedSubscriptionsHandled(b, 1)
      f.builder.fishForSpecificMessage(3.seconds) { case RequestRepublish(_, _, `b`, from) => from.asInt } mustBe 1

      f.builder.send(f.projection, GoLive)
      f.builder.fishForSpecificMessage(3.seconds) { case ProjectionLive => () }
      f.events(a, 1, 2)
      f.acked(a) mustBe Seq(1, 2)
      f.applied("events") mustBe Seq(a -> Seq(1, 2))
    }

    Scenario("a chunk is history to the listeners of a live projection: rebuilding is true while they apply it (F114)") {
      val f = new Fixture
      val (a, b) = (newId(), newId())

      f.chunk(1, eventsItem(a, 1)) mustBe ChunkApplied(1, Nil, 0)
      f.events(b, 1)
      f.acked(b)

      f.projection.askFor[Vector[(AggregateId, Boolean)]](GetRebuildingSeen, 3.seconds) mustBe Vector(a -> true, b -> false)
    }

    Scenario("a reset for a targeted rebuild answers the deliveries waiting for its aggregates, which the builder gives again") {
      val f = new Fixture
      val id = newId()
      f.events(id, 3)
      f.requestedFrom(id) mustBe 1

      f.builder.send(f.projection, ResetForRebuild(RebuildTarget.OneAggregate(id), () => Seq(id)))

      f.builder.expectMsg(3.seconds, ResetForRebuildDone(1))
      f.acked(id) mustBe Seq(3)
    }

    Scenario("GoLive retries the subscriptions that failed while the projection was built") {
      val f = new Fixture
      val id = newId()
      f.builder.send(f.projection, BuildFromStore(f.builder.ref))
      f.failTimes(id, 100)
      f.chunk(1, eventsItem(id, 1)) mustBe ChunkApplied(1, Nil, 1)
      f.builder.fishForSpecificMessage(3.seconds) { case MessageFailed(_, `id`, _, _, _) => () }

      f.builder.send(f.projection, GoLive)

      f.requestedFrom(id) mustBe 1 // from the owner: the projection is live
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

    Scenario("a request that gets no answer is sent again, less and less often") {
      val f = new Fixture(ProjectionActorOptions.DEFAULT.copy(gapRequestRepeat = 200.millis))
      val id = newId()

      f.events(id, 3)
      f.requestedFrom(id) mustBe 1

      // Again after 200 ms, then 400 ms: a busy projection must not ask for the same versions every 200 ms.
      val again = f.bus.receiveWhile(1100.millis) { case RequestRepublish(_, _, `id`, from) => from.asInt }
      again.size must (be >= 2 and be <= 3)
      again.toSet mustBe Set(1)
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

    Scenario("RetryFailedSubscription asks for the failed version again, until it comes, and clears the failure once it is applied") {
      val f = new Fixture(ProjectionActorOptions.DEFAULT.withRetries(quickRetries).copy(gapRequestRepeat = 200.millis))
      val id = newId()
      f.events(id, 1)
      f.acked(id) mustBe Seq(1)
      f.failTimes(id, 3) // every attempt of the next delivery, so it is recorded; afterwards the listener works
      f.events(id, 2, 3)
      f.failedFor(id)

      f.projection.askFor[FailedSubscriptionsHandled](RetryFailedSubscription(id), 3.seconds) mustBe FailedSubscriptionsHandled(id, 1)

      f.requestedFrom(id) mustBe 2
      f.requestedFrom(id) mustBe 2 // asked again: the first request may not have reached the owner
      f.state.failures(classOf[RecordingProjection].getName).map(_.aggregateId) mustBe Seq(id)

      f.events(id, 2, 3)
      f.acked(id) mustBe Seq(2, 3)
      f.applied("events") mustBe Seq(id -> Seq(1), id -> Seq(2, 3))
      f.state.failures(classOf[RecordingProjection].getName) mustBe empty
      f.status().failures mustBe empty
    }

    Scenario("RetryAllFailedSubscriptions retries every failed aggregate") {
      val f = new Fixture
      val (a, b) = (newId(), newId())
      f.failTimes(a, 3)
      f.failTimes(b, 3)
      f.events(a, 1)
      f.failedFor(a)
      f.events(b, 1)
      f.failedFor(b)

      f.projection.askFor[FailedSubscriptionsRetried](RetryAllFailedSubscriptions, 3.seconds) mustBe FailedSubscriptionsRetried(2)

      f.requestedFrom(a) mustBe 1
      f.requestedFrom(b) mustBe 1
    }

    Scenario("a delivery covered by a batch that then fails is not acknowledged before the batch is applied") {
      val f = new Fixture
      val id = newId()
      f.failTimes(id, 100)

      f.events(id, 1, 2)
      f.events(id, 2) // a re-delivery, covered by the first one

      f.answers(id, 1.second) mustBe Seq("failed 1,2", "failed 2")
    }

    Scenario("a failure that cannot be stored is not answered; the batch is tried again until the failure is stored") {
      val state = new UnstorableFailures
      val f = new Fixture(state = state)
      val id = newId()
      f.failTimes(id, 1000)

      f.events(id, 1)
      f.answers(id, 500.millis) mustBe empty

      state.storable = true
      f.failedFor(id) must include("listener failed")
      state.failures(classOf[RecordingProjection].getName).map(_.aggregateId) mustBe Seq(id)
    }

    Scenario("a gap that is never filled ends as a recorded failure") {
      val f = new Fixture(ProjectionActorOptions.DEFAULT.withRetries(quickRetries).copy(gapRequestRepeat = 100.millis, gapGiveUpAfter = 500.millis))
      val id = newId()

      f.events(id, 3)

      f.failedFor(id) must include("missing")
      f.status().failures.map(fl => (fl.aggregateId, fl.version.asInt)) mustBe Seq((id, 1))
      f.status().waitingForGap mustBe empty
    }

    Scenario("SkipFailedVersion marks the failed version as applied and asks for the versions after it") {
      val f = new Fixture
      val id = newId()
      f.failTimes(id, 3)
      f.events(id, 1)
      f.failedFor(id)

      f.projection.askFor[FailedSubscriptionsHandled](SkipFailedVersion(id), 3.seconds) mustBe FailedSubscriptionsHandled(id, 1)

      f.requestedFrom(id) mustBe 2
      f.state.lastVersion(classOf[RecordingProjection].getName, SubscriptionType.EVENTS, id) mustBe AggregateVersion(1)
      f.applied("events") mustBe empty
    }

    Scenario("a failure cleared by a retry or a skip has the owner re-publish what waits in its outbox, such as a permanent delete the failure held (F166)") {
      val f = new Fixture(ProjectionActorOptions.DEFAULT.withRetries(quickRetries).copy(gapRequestRepeat = 200.millis))
      val (retried, skipped) = (newId(), newId())
      f.failTimes(retried, 3)
      f.failTimes(skipped, 3)
      f.events(retried, 1)
      f.failedFor(retried)
      f.events(skipped, 1)
      f.failedFor(skipped)

      f.projection.askFor[FailedSubscriptionsHandled](RetryFailedSubscription(retried), 3.seconds) mustBe FailedSubscriptionsHandled(retried, 1)
      f.requestedFrom(retried) mustBe 1
      f.events(retried, 1)
      f.acked(retried) mustBe Seq(1)
      f.outboxRepublished(retried)

      f.projection.askFor[FailedSubscriptionsHandled](SkipFailedVersion(skipped), 3.seconds) mustBe FailedSubscriptionsHandled(skipped, 1)
      f.outboxRepublished(skipped)
    }

    Scenario("SkipFailedVersion also skips a failure at the squash event a subscription starts at (F113)") {
      val f = new Fixture
      val id = newId()
      f.failTimes(id, 100)
      f.bus.send(f.projection, IdentifiableEvents[GateNote](noteType, id, Seq(7, 8).map(v =>
        EventInfo[GateNote](AggregateVersion(v), GateNoteChanged(v), UserId(1), Instant.now)), startsAtSquash = true))
      f.failedFor(id)
      f.status().failures.map(_.version.asInt) mustBe Seq(7)

      f.projection.askFor[FailedSubscriptionsHandled](SkipFailedVersion(id), 3.seconds) mustBe FailedSubscriptionsHandled(id, 1)

      f.state.lastVersion(classOf[RecordingProjection].getName, SubscriptionType.EVENTS, id) mustBe AggregateVersion(7)
      f.status().failures mustBe empty
    }

    Scenario("failures recorded before the start are retried when the projection starts") {
      val state = new MemorySubscriptionsState
      val id = newId()
      state.recordFailure(classOf[RecordingProjection].getName,
        SubscriptionFailure(SubscriptionType.EVENTS, noteType, id, AggregateVersion(4), "failed before", Instant.now))

      val f = new Fixture(state = state)

      // From the version after the stored one: this test's state starts at zero; a real one stands at 3, before the failure.
      f.requestedFrom(id) mustBe 1
      f.events(id, 1, 2, 3, 4) // the re-publication
      f.acked(id) mustBe Seq(1, 2, 3, 4)
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

    Scenario("in parallel mode a gap check while a batch runs does not apply that batch's versions a second time") {
      val options = ProjectionActorOptions.DEFAULT.withParallelUpdateProcessing(2).withGroupUpdatesDelayMillis(100).copy(gapRequestRepeat = 50.millis)
      val f = new Fixture(options, slowMillis = 400)
      val id = newId()
      f.events(id, 1)
      f.acked(id)

      f.events(id, 2)
      f.events(id, 4)
      Thread.sleep(200) // one drain started the slow batch of 2 and asked for 3
      f.events(id, 2) // a re-delivery while that batch runs

      Thread.sleep(1000)
      f.applied("events").filter(_._1 == id).flatMap(_._2) mustBe Seq(1, 2)
    }

    Scenario("in parallel mode each aggregate keeps its order, and a retry waits for its back-off") {
      val f = new Fixture(ProjectionActorOptions.DEFAULT.withParallelUpdateProcessing(4).withRetries(quickRetries), slowMillis = 50)
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

    Scenario("a delayed query whose search is under way when the update is applied gets the update, not the deadline's answer (F163)") {
      val bus = TestProbe()
      val projection = system.actorOf(Props(new SearchedNotes(new MemorySubscriptionsState, searchMillis = 300)))
      val (id, asker) = (newId(), TestProbe())

      asker.send(projection, FindNoteVersion(id, 3000))
      Thread.sleep(50) // the search has read, and answers later
      bus.send(projection, IdentifiableEvents[GateNote](noteType, id, Seq(EventInfo[GateNote](AggregateVersion(1), GateNoteChanged(1), UserId(1), Instant.now))))

      asker.expectMsg(2.seconds, Some(1))
    }

    Scenario("at its deadline a delayed query is answered with a last search; the custom one with what that search found, of its type (F163)") {
      val projection = system.actorOf(Props(new SearchedNotes(new MemorySubscriptionsState, searchMillis = 10)))
      val asker = TestProbe()

      asker.send(projection, FindNoteVersionOrZero(newId(), 300))
      asker.expectMsg(3.seconds, 0)
      asker.send(projection, FindNoteVersion(newId(), 300))
      asker.expectMsg(3.seconds, None)
    }
  }

  Feature("Declared listeners") {

    Scenario("two listeners of one kind for one aggregate type are refused: the bus gets the failure, not the subscriptions (F164)") {
      val bus = TestProbe()
      val projection = system.actorOf(Props(new TwiceListeningProjection(new MemorySubscriptionsState)))
      bus.send(projection, GetSubscriptionRequests(projection))
      val refusal = bus.expectMsgType[Status.Failure](3.seconds).cause.getMessage
      refusal must include ("2 EventsListeners of " + noteType.typeName)
      refusal must not include "AggregateListener"
    }
  }
}

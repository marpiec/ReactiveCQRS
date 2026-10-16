package io.reactivecqrs.testdomain.spec

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventsreplayer.EventsReplayerActor.{EventsReplayed, ReplayAllEvents}
import io.reactivecqrs.core.eventsreplayer.{EventsReplayerActor, ReplayerConfig, ReplayerRepositoryActorFactory}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.{MemorySubscriptionsState, ProjectionActor, SubscriptionsState}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.core.util.BlockingDispatcher
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}

import scala.concurrent.duration._

// A note aggregate that only this spec writes: a replay goes over every aggregate of a type, and the carts the other
// specs leave behind hold undone events, which a replay leaves out (F58, M4).
case class ReplayNote(text: String)
case class ReplayNoteCreated(text: String) extends FirstEvent[ReplayNote] {
  def spaceId: SpaceId = SpaceId(0)
}
case class ReplayNoteChanged(text: String) extends Event[ReplayNote]

case class CreateReplayNote(userId: UserId, text: String) extends FirstCommand[ReplayNote, CustomCommandResponse[_]]
case class ChangeReplayNote(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, text: String)
  extends Command[ReplayNote, CustomCommandResponse[_]]

class ReplayNoteContext extends AggregateContext[ReplayNote] {
  override val version: Int = 1
  override def initialAggregateRoot: ReplayNote = ReplayNote("")
  override def commandHandlers = _ => {
    case c: CreateReplayNote => CommandSuccess(ReplayNoteCreated(c.text))
    case c: ChangeReplayNote => CommandSuccess(ReplayNoteChanged(c.text))
  }
  override def eventHandlers = (_, _, _) => {
    case e: ReplayNoteCreated => ReplayNote(e.text)
    case e: ReplayNoteChanged => ReplayNote(e.text)
  }
}

case object GetNoteTexts

/** The latest text of every note, in memory. */
class ReplayNotesProjection(val subscriptionsState: SubscriptionsState) extends ProjectionActor {
  private var texts = Map[AggregateId, String]()

  protected val listeners = List(AggregateListener.noDBSession[ReplayNote] { (id, _, _, note) =>
    texts += id -> note.map(_.text).getOrElse("")
  })

  override protected def receiveQuery: Receive = {
    case GetNoteTexts => sender() ! texts
  }

  override protected def onClearProjectionData(): Unit = texts = Map.empty
  override protected val projectionName: String = "ReplayNotesProjection"
  override protected val version: Int = 1
}

class EventsReplaySpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))

  private val userId = UserId(1)
  private val noteType = AggregateType.of[ReplayNote]

  /** A bus with one new projection, as a rebuild starts from cleared read models. */
  private def projectionOnNewBus(): (ActorRef, ActorRef) = {
    val bus = system.actorOf(Props(new EventsBusActor()))
    val projection = system.actorOf(Props(new ReplayNotesProjection(new MemorySubscriptionsState)).withDispatcher(BlockingDispatcher.id))
    bus ! RegisterSubscribers(Seq(projection))
    (bus, projection)
  }

  private def textsIn(projection: ActorRef): Map[AggregateId, String] = projection ?? GetNoteTexts

  Feature("Rebuilding a projection from stored events") {

    Scenario("a replay of events that were delivered already ends, and the projection gets every note (F4)") {
      Given("notes whose events a live projection applied, so their outbox rows are gone")
      val (liveBus, liveProjection) = projectionOnNewBus()
      val commandBus = system.actorOf(AggregateCommandBusActor(new ReplayNoteContext, uidGenerator, eventStore,
        new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema(), liveBus, eventsReplayMode = false))
      val notes: Map[AggregateId, String] = (1 to 3).map { n =>
        val created: SuccessResponse = commandBus ?? CreateReplayNote(userId, "draft " + n)
        val changed: CustomCommandResponse[_] = commandBus ?? ChangeReplayNote(userId, created.aggregateId, AggregateVersion(1), "note " + n)
        changed mustBe SuccessResponse(created.aggregateId, AggregateVersion(2))
        created.aggregateId -> ("note " + n)
      }.toMap
      awaitAssert({
        textsIn(liveProjection).filter(note => notes.contains(note._1)) mustBe notes
        notes.keys.foreach(id => eventStore.oldestVersionToPublish(id) mustBe None)
      }, 10.seconds, 100.millis)

      When("a new projection replays every stored note event")
      val (replayBus, rebuilt) = projectionOnNewBus()
      val replayer = system.actorOf(Props(new EventsReplayerActor(eventStore, replayBus, new MemorySubscriptionsState, ReplayerConfig(),
        List(ReplayerRepositoryActorFactory(new ReplayNoteContext)))))
      val replayed: EventsReplayed = replayer.askActor[EventsReplayed](ReplayAllEvents(batchPerAggregate = false, Seq(noteType), 0))(30.seconds)

      Then("the replay ends, and the new projection has the latest text of every note")
      replayed.eventsCount must be >= 6L
      textsIn(rebuilt).filter(note => notes.contains(note._1)) mustBe notes
    }
  }

}

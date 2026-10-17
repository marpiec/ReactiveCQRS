package io.reactivecqrs.testdomain.spec

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.documentstore.{Document, DocumentStore, NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.{AggregateScope, CommittedEvent, CommittedPage, EventStoreState, PostgresEventStoreState, StoredAggregate}
import io.reactivecqrs.core.projection._
import io.reactivecqrs.core.rebuild.ProjectionBuilder._
import io.reactivecqrs.core.rebuild.{ProjectionBuilder, ReplaySource}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.core.util.BlockingDispatcher
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{Actor, ActorRef, Props}
import org.apache.pekko.testkit.TestProbe
import scalikejdbc._

import scala.concurrent.duration._

// An aggregate of this spec only, so a build reads nothing of the other specs: a note with lines, with undo,
// duplication, deletion and squash.

case class BuildNote(title: String, lines: Vector[String], space: Long = 0)

case class BuildNoteCreated(title: String, space: Long = 0) extends FirstEvent[BuildNote] {
  def spaceId: SpaceId = SpaceId(space)
}
case class BuildNoteLineAdded(line: String) extends Event[BuildNote]
case class BuildNoteUndone(eventsCount: Int) extends UndoEvent[BuildNote]
case class BuildNoteDuplicated(spaceId: SpaceId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion) extends DuplicationEvent[BuildNote]
case class BuildNoteDeleted() extends Event[BuildNote]
case class BuildNoteSquashed(spaceId: SpaceId, title: String, lines: Vector[String]) extends FirstEvent[BuildNote]

case class CreateBuildNote(userId: UserId, title: String, space: Long = 0) extends FirstCommand[BuildNote, CustomCommandResponse[_]]
case class AddBuildNoteLine(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, line: String)
  extends Command[BuildNote, CustomCommandResponse[_]]
case class UndoBuildNote(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, count: Int)
  extends Command[BuildNote, CustomCommandResponse[_]]
case class DuplicateBuildNote(userId: UserId, baseId: AggregateId, baseVersion: AggregateVersion) extends FirstCommand[BuildNote, CustomCommandResponse[_]]
case class DeleteBuildNote(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion) extends Command[BuildNote, CustomCommandResponse[_]]

class BuildNoteContext extends AggregateContext[BuildNote] {
  override val version: Int = 1
  override def initialAggregateRoot: BuildNote = BuildNote("", Vector())
  override def commandHandlers = _ => {
    case c: CreateBuildNote => CommandSuccess(BuildNoteCreated(c.title, c.space))
    case c: AddBuildNoteLine => CommandSuccess(BuildNoteLineAdded(c.line))
    case c: UndoBuildNote => CommandSuccess(BuildNoteUndone(c.count))
    case c: DuplicateBuildNote => CommandSuccess(BuildNoteDuplicated(SpaceId(0), c.baseId, c.baseVersion))
    case _: DeleteBuildNote => CommandSuccess(BuildNoteDeleted())
  }
  override def eventHandlers = (_, _, note) => {
    case e: BuildNoteCreated => BuildNote(e.title, Vector(), e.space)
    case e: BuildNoteLineAdded => note.copy(lines = note.lines :+ e.line)
    case _: BuildNoteUndone => note
    case _: BuildNoteDuplicated => note
    case _: BuildNoteDeleted => null
    case e: BuildNoteSquashed => BuildNote(e.title, e.lines, e.spaceId.asLong)
  }
  override val squash: Option[SquashPolicy[BuildNote]] = Some(SquashPolicy(note => BuildNoteSquashed(SpaceId(note.space), note.title, note.lines)))
}

// Projections that record what they are given, under the identity they are created with, to compare one built live
// with one built from the store.

case class NoteLog(entries: Vector[String])
case class NoteLines(title: String, lines: Vector[String])
case class NoteView(title: String, lines: Vector[String], version: Int)

abstract class RecordingNotes(identity: String, val subscriptionsState: SubscriptionsState) extends ProjectionActor {
  override protected def subscriberName: String = identity
  override protected def receiveQuery: Receive = PartialFunction.empty
  override protected val projectionName: String = identity
}

/** Every event of each note, as "version:EventClass" (D60: the sequence live delivery sent). */
class NoteEventsLog(identity: String, subscriptionsState: SubscriptionsState, store: DocumentStore[NoteLog]) extends RecordingNotes(identity, subscriptionsState) {
  protected val listeners = List(EventsListener[BuildNote] { (id, events) => implicit session =>
    val added = events.map(e => e.version.asInt + ":" + e.event.getClass.getSimpleName)
    store.updateDocument(0, id.asLong, existing => Document(NoteLog(existing.map(_.document.entries).getOrElse(Vector()) ++ added)))
  })
}

/** Each note's lines, from its state only: built from the final states (D62). */
class NoteStates(identity: String, subscriptionsState: SubscriptionsState, store: DocumentStore[NoteLines]) extends RecordingNotes(identity, subscriptionsState) {
  protected val listeners = List(AggregateListener[BuildNote] { (id, _, created, note) => implicit session =>
    note match {
      case Some(n) if created => store.insertDocument(0, id.asLong, NoteLines(n.title, n.lines))
      case Some(n) => store.overwriteDocument(id.asLong, NoteLines(n.title, n.lines))
      case None => store.removeDocument(id.asLong)
    }
  })
}

/** Each note's lines and version, from its state with its events: built in commit order with replay states (D59). */
class NoteViews(identity: String, subscriptionsState: SubscriptionsState, store: DocumentStore[NoteView]) extends RecordingNotes(identity, subscriptionsState) {
  protected val listeners = List(AggregateWithEventsListener[BuildNote] { (id, version, note, _) => implicit session =>
    note match {
      case Some(n) => store.updateDocument(0, id.asLong, _ => Document(NoteView(n.title, n.lines, version.asInt)))
      case None => store.removeDocument(id.asLong)
    }
  })
}

/** A build's position in memory; `onSave` sees each one saved. */
class MemoryBuildPositions(onSave: BuildPosition => Unit = _ => ()) extends BuildPositions {
  private val current = new AtomicReference(BuildPosition.Start)
  override def read(): BuildPosition = current.get()
  override def save(position: BuildPosition): Unit = { current.set(position); onSave(position) }
}

/** The store, but its first read of the row `hidden` leaves that row out, as if its transaction committed late. */
class LateRowEventStore(mpjsons: MPJsons, typesNamesState: PostgresTypesNamesState, hidden: Long) extends PostgresEventStoreState(mpjsons, typesNamesState) {
  private val hiddenOnce = new AtomicInteger(1)
  override def readCommittedPage(types: Seq[AggregateType], after: Long, limit: Int, scope: AggregateScope, maxBytes: Long): CommittedPage = {
    val page = super.readCommittedPage(types, after, limit, scope, maxBytes)
    if (page.events.exists(_.id == hidden) && hiddenOnce.getAndDecrement() > 0) page.copy(events = page.events.filterNot(_.id == hidden)) else page
  }
}

/** The store, but its first read of events by version fails as a lost connection does (F117). */
class FlakyRangeEventStore(mpjsons: MPJsons, typesNamesState: PostgresTypesNamesState) extends PostgresEventStoreState(mpjsons, typesNamesState) {
  private val failures = new AtomicInteger(1)
  override def readPart[AGGREGATE_ROOT](aggregateId: AggregateId, from: AggregateVersion, to: AggregateVersion, maxBytes: Long): Seq[(EventInfo[AGGREGATE_ROOT], Int)] =
    if (failures.getAndDecrement() > 0) throw new java.sql.SQLException("connection lost", "08006") else super.readPart(aggregateId, from, to, maxBytes)
}

/** The store, but the aggregates it lists show no squash point, as if each was squashed after it was listed. */
class StaleSquashEventStore(mpjsons: MPJsons, typesNamesState: PostgresTypesNamesState) extends PostgresEventStoreState(mpjsons, typesNamesState) {
  override def aggregatesOf(types: Seq[AggregateType], after: AggregateId, limit: Int, scope: AggregateScope): Seq[StoredAggregate] =
    super.aggregatesOf(types, after, limit, scope).map(_.copy(squashedUpTo = None))
}

/** Counts the states it restores, per aggregate. */
class CountingReplaySource(context: AggregateContext[_], eventStore: EventStoreState) extends ReplaySource(context.asInstanceOf[AggregateContext[Any]], eventStore) {
  private val restored = new ConcurrentLinkedQueue[AggregateId]()
  def restoresOf(id: AggregateId): Int = restored.toArray.count(_ == id)
  override def stateAt(id: AggregateId, version: Option[AggregateVersion]): Any = {
    restored.add(id)
    super.stateAt(id, version)
  }
}

/** Passes everything on to the projection, recording how many events each chunk holds. */
class ChunkRecorder(projection: ActorRef, chunkEvents: ConcurrentLinkedQueue[Int]) extends Actor {
  override def receive: Receive = {
    case chunk: ApplyChunk =>
      chunkEvents.add(chunk.items.map(_.events.size).sum)
      projection.forward(chunk)
    case other => projection.forward(other)
  }
}

// D58–D62: a projection built from the store gets what live delivery gave it.
class ProjectionBuilderSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val subscriptions = new PostgresSubscriptionsState(typesNamesState).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
  private val noteContext = new BuildNoteContext
  private val sources = ReplaySource.of(Seq(noteContext), eventStore)
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val userId = UserId(1)

  private def logs(table: String) = new PostgresDocumentStore[NoteLog](table, mpjsons, new NoopDocumentStoreCache)
  private def lines(table: String) = new PostgresDocumentStore[NoteLines](table, mpjsons, new NoopDocumentStoreCache)
  private def views(table: String) = new PostgresDocumentStore[NoteView](table, mpjsons, new NoopDocumentStoreCache)

  private def onBlocking(props: Props): ActorRef = system.actorOf(props.withDispatcher(BlockingDispatcher.id))

  /** A live system: an event bus with the given projections and a command bus of notes. */
  private class Live(projections: Seq[ActorRef]) {
    val bus: ActorRef = system.actorOf(Props(new EventsBusActor()))
    bus ! RegisterSubscribers(projections)
    val notes: ActorRef = system.actorOf(AggregateCommandBusActor(noteContext, uidGenerator, eventStore, responses, bus))

    def send(command: AnyRef): SuccessResponse = notes.askActor[CustomCommandResponse[_]](command)(10.seconds) match {
      case success: SuccessResponse => success
      case other => fail("command failed: " + other)
    }
    def create(title: String): AggregateId = send(CreateBuildNote(userId, title)).aggregateId
    def add(id: AggregateId, at: Int, line: String): Unit = send(AddBuildNoteLine(userId, id, AggregateVersion(at), line))

    /** Waits until every subscriber took every event of these notes: their outbox rows are gone. */
    def delivered(ids: Seq[AggregateId]): Unit =
      awaitAssert(ids.foreach(id => eventStore.oldestVersionToPublish(id) mustBe None), 20.seconds, 50.millis)
  }

  private def builderOf(target: ActorRef, identity: String, reporter: ActorRef, store: EventStoreState = eventStore,
                        positions: BuildPositions = new MemoryBuildPositions, settings: BuilderSettings = BuilderSettings(pageSize = 50, chunkEvents = 20),
                        replaySources: Map[AggregateType, ReplaySource] = sources): ActorRef = {
    val plan = BuildPlan(target.askActor[Listeners](GetListeners)(5.seconds).subscriptions)
    onBlocking(Props(new ProjectionBuilder(target, identity, plan, store, subscriptions, replaySources, positions, reporter, settings)))
  }

  /** Builds `target` from the store and waits until it caught up. */
  private def build(target: ActorRef, identity: String, store: EventStoreState = eventStore, positions: BuildPositions = new MemoryBuildPositions,
                    settings: BuilderSettings = BuilderSettings(pageSize = 50, chunkEvents = 20),
                    replaySources: Map[AggregateType, ReplaySource] = sources): Unit = {
    val reporter = TestProbe()
    builderOf(target, identity, reporter.ref, store, positions, settings, replaySources)
    reporter.expectMsg(60.seconds, BuildCaughtUp)
  }

  private def documents[T <: AnyRef](store: PostgresDocumentStore[T], ids: Seq[AggregateId]): Map[AggregateId, T] =
    ids.flatMap(id => store.getDocument(id.asLong).map(id -> _.document)).toMap

  Feature("Building a projection from the store (D58–D62)") {

    Scenario("gives each projection what live delivery gave it: every event, undone ones and undos too, and each state") {
      Given("three live projections and notes with undo, duplication, deletion and a squash")
      val (liveLogs, liveLines, liveViews) = (logs("m4b_live_log"), lines("m4b_live_lines"), views("m4b_live_view"))
      val live = new Live(Seq(
        onBlocking(Props(new NoteEventsLog("m4b-live-log", subscriptions, liveLogs))),
        onBlocking(Props(new NoteStates("m4b-live-lines", subscriptions, liveLines))),
        onBlocking(Props(new NoteViews("m4b-live-view", subscriptions, liveViews)))))

      val plain = live.create("plain")
      live.add(plain, 1, "apple")
      live.add(plain, 2, "pear")
      val undone = live.create("undone")
      live.add(undone, 1, "a")
      live.add(undone, 2, "b")
      live.send(UndoBuildNote(userId, undone, AggregateVersion(3), 1))
      live.add(undone, 4, "c")
      val copy = live.send(DuplicateBuildNote(userId, plain, AggregateVersion(2))).aggregateId
      live.add(copy, 1, "copied")
      live.add(plain, 3, "plum") // the base goes on after the copy
      val deleted = live.create("deleted")
      live.add(deleted, 1, "e")
      live.send(DeleteBuildNote(userId, deleted, AggregateVersion(2)))
      val squashed = live.create("squashed")
      live.add(squashed, 1, "x")
      live.add(squashed, 2, "y")
      val all = Seq(plain, undone, copy, deleted, squashed)
      live.delivered(all)
      live.notes.askActor[SquashResponse](SquashAggregate(squashed, AggregateVersion(3)))(10.seconds) mustBe AggregateSquashed(squashed, AggregateVersion(3))
      live.add(squashed, 3, "z")
      live.delivered(all)

      When("the same projections are built from the store")
      val (rebuiltLogs, rebuiltLines, rebuiltViews) = (logs("m4b_rebuilt_log"), lines("m4b_rebuilt_lines"), views("m4b_rebuilt_view"))
      build(onBlocking(Props(new NoteEventsLog("m4b-rebuilt-log", subscriptions, rebuiltLogs))), "m4b-rebuilt-log")
      build(onBlocking(Props(new NoteStates("m4b-rebuilt-lines", subscriptions, rebuiltLines))), "m4b-rebuilt-lines")
      build(onBlocking(Props(new NoteViews("m4b-rebuilt-view", subscriptions, rebuiltViews))), "m4b-rebuilt-view")

      Then("the states and views are the same, of the squashed note too (D85)")
      documents(rebuiltLines, all) mustBe documents(liveLines, all)
      documents(rebuiltViews, all) mustBe documents(liveViews, all)
      documents(rebuiltLines, all).keySet mustBe all.toSet - deleted

      And("the events are the same, undone ones and undos included; the squashed note starts at its squash event (F98)")
      val unsquashed = all.filterNot(_ == squashed)
      documents(rebuiltLogs, unsquashed) mustBe documents(liveLogs, unsquashed)
      documents(liveLogs, Seq(undone))(undone).entries mustBe Vector("1:BuildNoteCreated", "2:BuildNoteLineAdded", "3:BuildNoteLineAdded",
        "4:BuildNoteUndone", "5:BuildNoteLineAdded")
      documents(rebuiltLogs, Seq(squashed))(squashed).entries mustBe Vector("3:BuildNoteSquashed", "4:BuildNoteLineAdded")
    }

    Scenario("continues after a stop from the position it saved, applying nothing twice (D61)") {
      Given("a live log of many notes")
      val liveLogs = logs("m4b_resume_live")
      val live = new Live(Seq(onBlocking(Props(new NoteEventsLog("m4b-resume-live", subscriptions, liveLogs)))))
      val ids = (1 to 12).map { n =>
        val id = live.create("note " + n)
        (1 to 3).foreach(v => live.add(id, v, "line " + v))
        id
      }
      live.delivered(ids)

      When("a build stops after its second chunk is applied, and starts again from the position it saved")
      val rebuiltLogs = logs("m4b_resume_rebuilt")
      val settings = BuilderSettings(pageSize = 10, chunkEvents = 4)
      val firstTarget = onBlocking(Props(new NoteEventsLog("m4b-resume-rebuilt", subscriptions, rebuiltLogs)))
      val saves = new AtomicInteger()
      val stopped = TestProbe()
      val positions = new MemoryBuildPositions(position => if (!position.bulkDone && saves.incrementAndGet() == 2) {
        system.stop(firstTarget) // its builder stops with it
        stopped.ref ! position
      })
      builderOf(firstTarget, "m4b-resume-rebuilt", TestProbe().ref, positions = positions, settings = settings)
      stopped.expectMsgType[BuildPosition](30.seconds).position must be > 0L
      positions.read().bulkDone mustBe false

      build(onBlocking(Props(new NoteEventsLog("m4b-resume-rebuilt", subscriptions, rebuiltLogs))), "m4b-resume-rebuilt", positions = positions, settings = settings)

      Then("every note's log is the live one: no event is missing or applied twice")
      documents(rebuiltLogs, ids) mustBe documents(liveLogs, ids)
    }

    // F126: pages and chunks are bounded by bytes too; a page its bytes cut short is not the last.
    Scenario("pages and chunks cut by their bytes give the same projection, and a page cut short does not end the bulk pass") {
      Given("a live log of notes with long lines")
      val liveLogs = logs("m4b_bytes_live")
      val live = new Live(Seq(onBlocking(Props(new NoteEventsLog("m4b-bytes-live", subscriptions, liveLogs)))))
      val ids = (1 to 5).map { n =>
        val id = live.create("note " + n)
        (1 to 3).foreach(v => live.add(id, v, "line " + v + " " + "x" * 300))
        id
      }
      live.delivered(ids)
      val last = DB.readOnly { implicit session =>
        SQL("SELECT max(id) FROM events WHERE aggregate_id IN (" + ids.map(_.asLong).mkString(",") + ")").map(_.long(1)).single().get
      }

      When("it is built with pages and chunks of about one event's bytes")
      val rebuiltLogs = logs("m4b_bytes_rebuilt")
      val bulkReached = new java.util.concurrent.atomic.AtomicLong()
      val positions = new MemoryBuildPositions(position => if (!position.bulkDone) bulkReached.accumulateAndGet(position.position, math.max))
      build(onBlocking(Props(new NoteEventsLog("m4b-bytes-rebuilt", subscriptions, rebuiltLogs))), "m4b-bytes-rebuilt", positions = positions,
        settings = BuilderSettings(pageSize = 50, chunkEvents = 20, pageBytes = 300, chunkBytes = 300))

      Then("every note's log is the live one, and the bulk pass read to the last of them")
      documents(rebuiltLogs, ids) mustBe documents(liveLogs, ids)
      bulkReached.get() must be >= last
    }

    Scenario("a build that stopped after its bulk pass gets everything it lacks from the reconciliation") {
      Given("live projections of notes with undo, duplication and a squash")
      val (liveLogs, liveLines, liveViews) = (logs("m4b_reconcile_live_log"), lines("m4b_reconcile_live_lines"), views("m4b_reconcile_live_view"))
      val live = new Live(Seq(
        onBlocking(Props(new NoteEventsLog("m4b-reconcile-live-log", subscriptions, liveLogs))),
        onBlocking(Props(new NoteStates("m4b-reconcile-live-lines", subscriptions, liveLines))),
        onBlocking(Props(new NoteViews("m4b-reconcile-live-view", subscriptions, liveViews)))))
      val plain = live.create("plain")
      live.add(plain, 1, "a")
      val undone = live.create("undone")
      live.add(undone, 1, "b")
      live.send(UndoBuildNote(userId, undone, AggregateVersion(2), 1))
      val copy = live.send(DuplicateBuildNote(userId, plain, AggregateVersion(2))).aggregateId
      val squashed = live.create("squashed")
      live.add(squashed, 1, "c")
      val all = Seq(plain, undone, copy, squashed)
      live.delivered(all)
      live.notes.askActor[SquashResponse](SquashAggregate(squashed, AggregateVersion(2)))(10.seconds) mustBe AggregateSquashed(squashed, AggregateVersion(2))
      live.add(squashed, 2, "d")
      live.delivered(all)

      When("new projections are built with their bulk pass taken as done, so only the reconciliation delivers")
      val (rebuiltLogs, rebuiltLines, rebuiltViews) = (logs("m4b_reconcile_log"), lines("m4b_reconcile_lines"), views("m4b_reconcile_view"))
      def afterBulk = new MemoryBuildPositions { save(BuildPosition(bulkDone = true, 0L)) }
      build(onBlocking(Props(new NoteEventsLog("m4b-reconcile-log", subscriptions, rebuiltLogs))), "m4b-reconcile-log", positions = afterBulk)
      build(onBlocking(Props(new NoteStates("m4b-reconcile-lines", subscriptions, rebuiltLines))), "m4b-reconcile-lines", positions = afterBulk)
      build(onBlocking(Props(new NoteViews("m4b-reconcile-view", subscriptions, rebuiltViews))), "m4b-reconcile-view", positions = afterBulk)

      Then("they hold what the live ones hold; the squashed note's events start at its squash event")
      documents(rebuiltLines, all) mustBe documents(liveLines, all)
      documents(rebuiltViews, all) mustBe documents(liveViews, all)
      val unsquashed = all.filterNot(_ == squashed)
      documents(rebuiltLogs, unsquashed) mustBe documents(liveLogs, unsquashed)
      documents(rebuiltLogs, Seq(squashed))(squashed).entries mustBe Vector("2:BuildNoteSquashed", "3:BuildNoteLineAdded")
    }

    Scenario("a row committed after later ones is read again by version when the projection finds the gap") {
      val liveLogs = logs("m4b_late_live")
      val live = new Live(Seq(onBlocking(Props(new NoteEventsLog("m4b-late-live", subscriptions, liveLogs)))))
      val id = live.create("late")
      (1 to 4).foreach(v => live.add(id, v, "line " + v))
      live.delivered(Seq(id))
      val secondEvent = DB.readOnly(implicit s => sql"SELECT id FROM events WHERE aggregate_id = ${id.asLong} AND version = 2".map(_.long(1)).single().get)

      val rebuiltLogs = logs("m4b_late_rebuilt")
      build(onBlocking(Props(new NoteEventsLog("m4b-late-rebuilt", subscriptions, rebuiltLogs))), "m4b-late-rebuilt",
        store = new LateRowEventStore(mpjsons, typesNamesState, secondEvent))

      documents(rebuiltLogs, Seq(id)) mustBe documents(liveLogs, Seq(id))
    }

    Scenario("events committed while the projection is built reach it, through the reconciliation or live (D57)") {
      Given("a projection being built, subscribed to the bus from the start")
      val (liveLogs, rebuiltLogs) = (logs("m4b_during_live"), logs("m4b_during_rebuilt"))
      val target = onBlocking(Props(new NoteEventsLog("m4b-during-rebuilt", subscriptions, rebuiltLogs)))
      target ! BuildFromStore(TestProbe().ref) // before the bus registers it, as the supervisor does
      val live = new Live(Seq(onBlocking(Props(new NoteEventsLog("m4b-during-live", subscriptions, liveLogs))), target))
      val before = (1 to 5).map(n => live.create("before " + n))

      When("notes change while it is built, slowly, in chunks of one event")
      val reporter = TestProbe()
      builderOf(target, "m4b-during-rebuilt", reporter.ref, settings = BuilderSettings(pageSize = 2, chunkEvents = 1))
      before.foreach(id => live.add(id, 1, "during"))
      val during = (1 to 5).map(n => live.create("during " + n))
      reporter.expectMsg(60.seconds, BuildCaughtUp)
      val after = (1 to 3).map(n => live.create("after " + n))
      before.foreach(id => live.add(id, 2, "after"))

      Then("it ends with every event of every note, as the live projection has them")
      val all = before ++ during ++ after
      live.delivered(all)
      awaitAssert(documents(rebuiltLogs, all) mustBe documents(liveLogs, all), 10.seconds, 100.millis)
    }
  }

  Feature("A row that cannot be read (F82)") {

    Scenario("fails its aggregate's subscription, visibly, and the build goes on") {
      val liveLogs = logs("m4b_unreadable_live")
      val live = new Live(Seq(onBlocking(Props(new NoteEventsLog("m4b-unreadable-live", subscriptions, liveLogs)))))
      val (broken, healthy) = (live.create("broken"), live.create("healthy"))
      live.add(broken, 1, "x")
      live.add(healthy, 1, "y")
      live.delivered(Seq(broken, healthy))
      DB.autoCommit(implicit s => sql"UPDATE events SET event = '{not json' WHERE aggregate_id = ${broken.asLong} AND version = 2".update())

      val rebuiltLogs = logs("m4b_unreadable_rebuilt")
      val target = onBlocking(Props(new NoteEventsLog("m4b-unreadable-rebuilt", subscriptions, rebuiltLogs)))
      build(target, "m4b-unreadable-rebuilt")

      documents(rebuiltLogs, Seq(healthy)) mustBe documents(liveLogs, Seq(healthy))
      documents(rebuiltLogs, Seq(broken))(broken).entries mustBe Vector("1:BuildNoteCreated")
      target.askActor[ProjectionStatus](GetProjectionStatus)(5.seconds).failures.map(f => (f.aggregateId, f.version.asInt)) must contain((broken, 2))

      // The damaged note would fail the builds of the scenarios that read every note after this one.
      DB.autoCommit(implicit s => sql"DELETE FROM events WHERE aggregate_id = ${broken.asLong}".update())
      DB.autoCommit(implicit s => sql"DELETE FROM aggregates WHERE id = ${broken.asLong}".update())
    }

    Scenario("a store that cannot be read for a moment is read again, and fails nothing (F117)") {
      val liveLogs = logs("m4b_flaky_live")
      val live = new Live(Seq(onBlocking(Props(new NoteEventsLog("m4b-flaky-live", subscriptions, liveLogs)))))
      val note = live.create("note")
      live.add(note, 1, "x")
      live.delivered(Seq(note))

      val rebuiltLogs = logs("m4b_flaky_rebuilt")
      val target = onBlocking(Props(new NoteEventsLog("m4b-flaky-rebuilt", subscriptions, rebuiltLogs)))
      val afterBulk = new MemoryBuildPositions { save(BuildPosition(bulkDone = true, 0L)) } // the reconciliation reads by version
      build(target, "m4b-flaky-rebuilt", store = new FlakyRangeEventStore(mpjsons, typesNamesState), positions = afterBulk,
        settings = BuilderSettings(pageSize = 50, chunkEvents = 20, retryAfter = 200.millis))

      documents(rebuiltLogs, Seq(note)) mustBe documents(liveLogs, Seq(note))
      target.askActor[ProjectionStatus](GetProjectionStatus)(5.seconds).failures mustBe empty
    }
  }

  Feature("Ranges read by version, and the size of chunks (F139, F140)") {

    Scenario("a range from far below the squash point starts at the squash event, also when the squash came after the aggregate was listed (F139)") {
      Given("a note squashed at version 6 and written to after")
      val liveLogs = logs("m4b_squash_hole_live")
      val live = new Live(Seq(onBlocking(Props(new NoteEventsLog("m4b-squash-hole-live", subscriptions, liveLogs)))))
      val note = live.create("squashed far")
      (1 to 6).foreach(v => live.add(note, v, "line " + v))
      live.delivered(Seq(note))
      live.notes.askActor[SquashResponse](SquashAggregate(note, AggregateVersion(6)))(10.seconds) mustBe AggregateSquashed(note, AggregateVersion(6))
      live.add(note, 7, "after")
      live.delivered(Seq(note))

      When("projections are built by their reconciliation, reading two aggregates and two versions at a time")
      def afterBulk = new MemoryBuildPositions { save(BuildPosition(bulkDone = true, 0L)) }
      val small = BuilderSettings(pageSize = 2, chunkEvents = 2)
      val (known, stale) = (logs("m4b_squash_hole_known"), logs("m4b_squash_hole_stale"))
      build(onBlocking(Props(new NoteEventsLog("m4b-squash-hole-known", subscriptions, known))), "m4b-squash-hole-known", positions = afterBulk, settings = small)
      build(onBlocking(Props(new NoteEventsLog("m4b-squash-hole-stale", subscriptions, stale))), "m4b-squash-hole-stale",
        store = new StaleSquashEventStore(mpjsons, typesNamesState), positions = afterBulk, settings = small)

      Then("each has the squash event and what followed it")
      val expected = Vector("6:BuildNoteSquashed", "7:BuildNoteLineAdded", "8:BuildNoteLineAdded")
      documents(known, Seq(note))(note).entries mustBe expected
      documents(stale, Seq(note))(note).entries mustBe expected
    }

    Scenario("a long run of one aggregate is cut to fit chunks, and a range read by version restores its state once (F140)") {
      Given("a note with ten lines")
      val (liveLogs, liveViews) = (logs("m4b_long_run_live"), views("m4b_long_run_live_view"))
      val live = new Live(Seq(onBlocking(Props(new NoteEventsLog("m4b-long-run-live", subscriptions, liveLogs))),
        onBlocking(Props(new NoteViews("m4b-long-run-live-view", subscriptions, liveViews)))))
      val note = live.create("long")
      (1 to 10).foreach(v => live.add(note, v, "line " + v))
      live.delivered(Seq(note))
      val settings = BuilderSettings(pageSize = 50, chunkEvents = 4)

      When("a log is built in chunks of four events")
      val chunkSizes = new ConcurrentLinkedQueue[Int]()
      val rebuiltLogs = logs("m4b_long_run_rebuilt")
      val logProjection = onBlocking(Props(new NoteEventsLog("m4b-long-run-rebuilt", subscriptions, rebuiltLogs)))
      build(system.actorOf(Props(new ChunkRecorder(logProjection, chunkSizes))), "m4b-long-run-rebuilt", settings = settings)

      Then("no chunk holds more, and the log is the live one")
      chunkSizes.toArray.map(_.asInstanceOf[Int]).max must be <= 4
      documents(rebuiltLogs, Seq(note)) mustBe documents(liveLogs, Seq(note))

      When("a view is built by its reconciliation, reading the note in parts of four events")
      val counting = new CountingReplaySource(noteContext, eventStore)
      val rebuiltViews = views("m4b_long_run_rebuilt_view")
      def afterBulk = new MemoryBuildPositions { save(BuildPosition(bulkDone = true, 0L)) }
      build(onBlocking(Props(new NoteViews("m4b-long-run-rebuilt-view", subscriptions, rebuiltViews))), "m4b-long-run-rebuilt-view",
        positions = afterBulk, settings = settings, replaySources = Map(AggregateType.of[BuildNote] -> counting))

      Then("its state was restored once, and the view is the live one")
      counting.restoresOf(note) mustBe 1
      documents(rebuiltViews, Seq(note)) mustBe documents(liveViews, Seq(note))
    }
  }
}

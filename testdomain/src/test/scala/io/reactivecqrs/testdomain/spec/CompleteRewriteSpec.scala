package io.reactivecqrs.testdomain.spec

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.aggregate.{OutboxWriter, WithOutboxWriter}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.documentstore.{Document, DocumentStore, NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection._
import io.reactivecqrs.core.rebuild.ProjectionSupervisor.{Builds, GetBuilds}
import io.reactivecqrs.core.rebuild.{PostgresProjectionRegistry, Projections, ProjectionsSettings}
import io.reactivecqrs.core.rewrite.{HistoryRewriter, RewriteReport}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import scalikejdbc._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

// D76: a note erased everywhere a folder's history, and its copies, keep it.
case class EraseNote(text: String) extends HistoryChange
/** A change that would make a note an event of another class. */
case class Recolour(text: String) extends HistoryChange

// It handles every Folder event of the specs (MovableFolderContext), as its projection builds over every folder of the
// run's schema, those of the other specs too.
// Its outbox writer keeps each note in a table of the host's own, and erases it there with the events (D76).
class ErasableFolders extends MovableFolderContext with WithOutboxWriter[Folder] {
  override val outboxWriter: OutboxWriter[Folder] = new OutboxWriter[Folder] {
    override def write(aggregateId: AggregateId, version: AggregateVersion, events: Seq[EventInfo[Folder]])(implicit session: DBSession): Unit =
      events.foreach(_.event match {
        case NoteAdded(text) => sql"INSERT INTO rewrite_spec_notes (aggregate_id, note) VALUES (${aggregateId.asLong}, $text)".update()
        case _ => ()
      })
    override def rewrite(aggregateId: AggregateId, change: HistoryChange)(implicit session: DBSession): Unit = change match {
      case EraseNote(text) => sql"UPDATE rewrite_spec_notes SET note = 'erased' WHERE aggregate_id = ${aggregateId.asLong} AND note = $text".update()
      case _ => ()
    }
  }

  override def historyChanges: HistoryChange => PartialFunction[Event[Folder], Event[Folder]] = {
    case EraseNote(text) => {
      case NoteAdded(`text`) => NoteAdded("erased")
      case squashed: FolderSquashed if squashed.notes.contains(text) => squashed.copy(notes = squashed.notes.map(n => if (n == text) "erased" else n))
    }
    case _ => PartialFunction.empty
  }
}

/** The folders' notes, which a targeted rebuild can clear and build again for one folder. */
class ErasableFolderNotes(val subscriptionsState: SubscriptionsState, store: DocumentStore[FolderDoc]) extends ProjectionActor {
  protected val listeners = List(AggregateListener[Folder] { (id, _, _, folder) => implicit session =>
    folder match {
      case Some(f) => store.updateDocument(f.space.asLong, id.asLong, _ => Document(FolderDoc(f.name, f.notes)))
      case None => store.removeDocument(id.asLong)
    }
  })
  override protected def clearForRebuild(target: RebuildTarget)(implicit session: DBSession): Unit = target match {
    case RebuildTarget.OneAggregate(id) => store.removeDocument(id.asLong)
    case RebuildTarget.OneSpace(space) => store.clearSpace(space.asLong)
    case RebuildTarget.OneType(_) => store.clearAllData()
  }
  override protected def receiveQuery: Receive = {
    case GetFolderDoc(id) => sender() ! store.getDocument(id.asLong).map(_.document)
  }
  override protected val projectionName: String = "ErasableFolderNotes"
}

class CompleteRewriteSpec extends CommonSpec {

  TestDatabase.init()
  DB.autoCommit(implicit session => sql"CREATE TABLE IF NOT EXISTS rewrite_spec_notes (aggregate_id BIGINT NOT NULL, note TEXT NOT NULL)".execute())

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
  private val subscriptions = new PostgresSubscriptionsState(typesNamesState).initSchema()
  private val registry = new PostgresProjectionRegistry().initSchema()
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val context = new ErasableFolders
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  private val folders = system.actorOf(AggregateCommandBusActor(context, uidGenerator, eventStore, responses, eventBus))
  private val notes: ActorRef = new Projections(system, ProjectionsSettings(eventStore, subscriptions, registry, Seq(context)))
    .start[ErasableFolderNotes]("rewrite-notes", 1, "rewrite-notes") { build =>
      new ErasableFolderNotes(subscriptions, new PostgresDocumentStore[FolderDoc](build.documentStore("rewrite_notes"), mpjsons, new NoopDocumentStoreCache))
    }
  eventBus ! RegisterSubscribers(Seq(notes))
  private val user = UserId(5)

  private def send(command: AnyRef): SuccessResponse = folders.askActor[CustomCommandResponse[_]](command)(10.seconds) match {
    case success: SuccessResponse => success
    case other => fail("command failed: " + other)
  }
  private def folder(id: AggregateId): Folder = folders.askActor[Try[Aggregate[Folder]]](GetAggregate(id))(10.seconds).get.aggregateRoot.get
  private def doc(id: AggregateId): Option[FolderDoc] = notes.askActor[Option[FolderDoc]](GetFolderDoc(id))(5.seconds)
  private def delivered(ids: AggregateId*): Unit = awaitAssert(ids.foreach(id => eventStore.oldestVersionToPublish(id) mustBe None), 20.seconds, 50.millis)
  private def rewrite(id: AggregateId, change: HistoryChange): RewriteReport =
    Await.result(new HistoryRewriter(folders, eventStore, Map("notes" -> notes)).rewrite(id, change, user), 60.seconds)

  Feature("A complete rewrite (D76)") {

    Scenario("erases a value from an aggregate, its archive, its copies attached and detached, and the projections given") {
      awaitAssert({ // its first build done: a targeted rebuild is refused while a build runs
        val builds = notes.askActor[Builds](GetBuilds)(5.seconds)
        (builds.active.map(_.number), builds.building) mustBe ((Some(1), None))
      }, 60.seconds, 100.millis)

      Given("a folder with a secret note, squashed with its notes kept in the archive, a copy reading it and a copy detached from it")
      val space = SpaceId(8000000000L + System.nanoTime() % 1000000L)
      val base = send(CreateFolder(user, space, "Alpha")).aggregateId
      send(AddNote(user, base, AggregateVersion(1), "secret"))
      send(AddNote(user, base, AggregateVersion(2), "public"))
      val detached = send(CopyFolder(user, space, base, AggregateVersion(2))).aggregateId
      val attached = send(CopyFolder(user, space, base, AggregateVersion(3))).aggregateId
      send(AddNote(user, attached, AggregateVersion(1), "secret"))
      delivered(base, attached, detached)
      folders.askActor[SquashResponse](SquashAggregate(base, AggregateVersion(3), user))(10.seconds) mustBe AggregateSquashed(base, AggregateVersion(3))
      awaitAssert(doc(attached) mustBe Some(FolderDoc("Alpha", Vector("secret", "public", "secret"))), 20.seconds, 50.millis)

      val ids = Seq(base, attached, detached).map(_.asLong)
      def rowsHolding(value: String): Long = DB.readOnly(implicit session => sql"""SELECT
        |   (SELECT count(*) FROM events WHERE aggregate_id IN ($ids) AND position($value IN event) > 0)
        | + (SELECT count(*) FROM events_archive WHERE aggregate_id IN ($ids) AND position($value IN event) > 0)
        | + (SELECT count(*) FROM snapshots WHERE aggregate_id IN ($ids) AND position($value IN state) > 0)
        | + (SELECT count(*) FROM rewrite_spec_notes WHERE aggregate_id IN ($ids) AND note = $value)""".stripMargin
        .map(_.long(1)).single().get)
      rowsHolding("secret") must be > 0L

      When("the note is erased from the folder and from everything copied from it")
      val report = rewrite(base, EraseNote("secret"))

      Then("the folder, its archive, its copies and the projection hold it no more")
      report.complete mustBe true
      report.rewritten.map(r => (r.id, r.events, r.archived)).toSet mustBe Set((base, 1, 1), (attached, 1, 0), (detached, 1, 0))
      report.rebuilt mustBe Seq("notes")
      folder(base).notes mustBe Vector("erased", "public")
      folder(attached).notes mustBe Vector("erased", "public", "erased")
      folder(detached).notes mustBe Vector("erased")
      eventStore.readArchive[Folder](base).map(_.info.event) mustBe Seq(NoteAdded("erased"), NoteAdded("public"))
      Seq(base, attached, detached).map(doc) mustBe Seq(Some(FolderDoc("Alpha", Vector("erased", "public"))),
        Some(FolderDoc("Alpha", Vector("erased", "public", "erased"))), Some(FolderDoc("Alpha", Vector("erased"))))

      And("no stored row of theirs holds it: events, archive, snapshots, the host's own rows")
      rowsHolding("secret") mustBe 0L

      And("each aggregate's rewrite left an audit row naming the fields it changed, never the value")
      DB.readOnly(implicit session => sql"""SELECT aggregate_id, fields, events, archived, change_class FROM history_rewrites
             | WHERE aggregate_id IN (${Seq(base, attached, detached).map(_.asLong)})""".stripMargin
        .map(rs => rs.long(1) -> (rs.string(2), rs.int(3), rs.int(4), rs.string(5))).list().toMap) mustBe Map(
        base.asLong -> ("notes,text", 1, 1, classOf[EraseNote].getName),
        attached.asLong -> ("text", 1, 0, classOf[EraseNote].getName),
        detached.asLong -> ("notes", 1, 0, classOf[EraseNote].getName))

      And("a second run finds nothing to change, and names the base the copy reads, which it leaves alone")
      val again = rewrite(attached, EraseNote("secret"))
      (again.complete, again.rewritten, again.unchanged, again.bases) mustBe ((true, Nil, Seq(attached), Seq(base)))
    }

    Scenario("that would change an undo or an event's class is refused for that aggregate, which keeps its history") {
      val refusing = new ErasableFolders {
        override def historyChanges: HistoryChange => PartialFunction[Event[Folder], Event[Folder]] = {
          case Recolour(text) => { case NoteAdded(`text`) => FolderSquashed(SpaceId(0), "not a note", Vector.empty) }
          case _ => PartialFunction.empty
        }
      }
      val bus = system.actorOf(AggregateCommandBusActor(refusing, uidGenerator, eventStore, responses, eventBus))
      val id = bus.askActor[CustomCommandResponse[_]](CreateFolder(user, SpaceId(0), "Beta"))(10.seconds).asInstanceOf[SuccessResponse].aggregateId
      bus.askActor[CustomCommandResponse[_]](AddNote(user, id, AggregateVersion(1), "red"))(10.seconds) mustBe a[SuccessResponse]

      val refused = bus.askActor[RewriteResponse](RewriteEverywhere(id, Recolour("red"), user))(10.seconds)
      refused.asInstanceOf[RewriteRefused].reason must include("D17")
      eventStore.readEventsRange[Folder](id, AggregateVersion(2), AggregateVersion(2)).map(_.event) mustBe Seq(NoteAdded("red"))
      system.stop(bus)
    }
  }
}

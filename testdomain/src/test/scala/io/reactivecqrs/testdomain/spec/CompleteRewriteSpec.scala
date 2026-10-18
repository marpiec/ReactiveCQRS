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
import io.reactivecqrs.core.rebuild.ProjectionSupervisor.{AggregatesRebuilt, Builds, GetBuilds, RebuildAggregates, RebuildProjection, RebuildRefused, RebuildStarted}
import io.reactivecqrs.core.rebuild.{PostgresProjectionRegistry, Projections, ProjectionsSettings}
import io.reactivecqrs.core.rewrite.{HistoryRewriter, RewriteReport}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testkit.{CommonSpec, LogCapture, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import scalikejdbc._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

// D76: a note erased everywhere a folder's history, and its copies, keep it; in an event that cannot be read, as text (F245).
case class EraseNote(text: String) extends HistoryChange {
  override def inUnreadableEvent(json: String): Option[String] = Some(json.replace("\"" + text + "\"", "\"erased\""))
}
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

/** The notes, but while it is built from the store, the folder holding `marker` makes it wait for `go`, saying so on `seen`. */
class PausingFolderNotes(val subscriptionsState: SubscriptionsState, store: DocumentStore[FolderDoc], marker: String,
                         seen: java.util.concurrent.CountDownLatch, go: java.util.concurrent.CountDownLatch) extends ProjectionActor {
  protected val listeners = List(AggregateListener[Folder] { (id, _, _, folder) => implicit session =>
    folder match {
      case Some(f) =>
        if (rebuilding && f.notes.contains(marker) && seen.getCount > 0) {
          seen.countDown()
          go.await(30, java.util.concurrent.TimeUnit.SECONDS)
        }
        store.updateDocument(f.space.asLong, id.asLong, _ => Document(FolderDoc(f.name, f.notes)))
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
  override protected val projectionName: String = "PausingFolderNotes"
}

/** Adds a note that copies the folder's last one, in a handler that waits for `release` the first time it runs (F195). */
case class CopyLastNote(userId: UserId, aggregateId: AggregateId) extends ConcurrentCommand[Folder, CustomCommandResponse[_]]

class CopyingFolders(started: java.util.concurrent.CountDownLatch, release: java.util.concurrent.CountDownLatch) extends ErasableFolders {
  private val waited = new java.util.concurrent.atomic.AtomicBoolean(false)
  override def commandHandlers = folder => {
    case _: CopyLastNote => AsyncCommandResult(scala.concurrent.Future {
      if (waited.compareAndSet(false, true)) {
        started.countDown()
        release.await(30, java.util.concurrent.TimeUnit.SECONDS)
      }
      CommandSuccess(NoteAdded(folder.notes.last))
    }(scala.concurrent.ExecutionContext.global))
    case other => super.commandHandlers(folder)(other)
  }
}

/** A projection that refuses its first targeted rebuild and does every later one, counting them. */
class RefusingOnce extends org.apache.pekko.actor.Actor {
  private var asked = 0
  override def receive: Receive = {
    case RebuildAggregates(target) =>
      asked += 1
      sender() ! (if (asked == 1) RebuildRefused("a build is in progress") else AggregatesRebuilt(target, 1))
    case "asked" => sender() ! asked
  }
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

  private def send(command: AnyRef): SuccessResponse = folders.askFor[CustomCommandResponse[_]](command) match {
    case success: SuccessResponse => success
    case other => fail("command failed: " + other)
  }
  private def folder(id: AggregateId): Folder = folders.askFor[Try[Aggregate[Folder]]](GetAggregate(id)).get.aggregateRoot.get
  private def doc(id: AggregateId): Option[FolderDoc] = notes.askFor[Option[FolderDoc]](GetFolderDoc(id), 5.seconds)
  private def delivered(ids: AggregateId*): Unit = awaitAssert(ids.foreach(id => eventStore.oldestVersionToPublish(id) mustBe None), 20.seconds, 50.millis)
  // Every projection a scenario started: each erasure must rebuild them all.
  private var projectionsGiven = Map[String, ActorRef]("notes" -> notes)
  private def rewrite(id: AggregateId, change: HistoryChange): RewriteReport =
    Await.result(new HistoryRewriter(folders, eventStore, projectionsGiven).rewrite(id, change, user), 60.seconds)

  Feature("A complete rewrite (D76)") {

    Scenario("erases a value from an aggregate, its archive, its copies attached and detached, and the projections given") {
      awaitAssert({ // its first build done: a targeted rebuild is refused while a build runs
        val builds = notes.askFor[Builds](GetBuilds, 5.seconds)
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
      folders.askFor[SquashResponse](SquashAggregate(base, AggregateVersion(3), user)) mustBe AggregateSquashed(base, AggregateVersion(3))
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
      val id = bus.askFor[CustomCommandResponse[_]](CreateFolder(user, SpaceId(0), "Beta")).asInstanceOf[SuccessResponse].aggregateId
      bus.askFor[CustomCommandResponse[_]](AddNote(user, id, AggregateVersion(1), "red")) mustBe a[SuccessResponse]

      val refused = bus.askFor[RewriteResponse](RewriteEverywhere(id, Recolour("red"), user))
      refused.asInstanceOf[RewriteRefused].reason must include("D17")
      eventStore.readEventsRange[Folder](id, AggregateVersion(2), AggregateVersion(2)).map(_.event) mustBe Seq(NoteAdded("red"))
      system.stop(bus)
    }
  }

  // §12 of the risk assessment: each erasure path, then a search for the erased value in every table and the logs.
  private def marker(): String = "marker" + System.nanoTime()

  /** The tables holding `value` in a row: the event store's, the failures, the host's notes and every read model. */
  private def tablesHolding(value: String): Seq[String] = DB.readOnly { implicit session =>
    val projections = sql"""SELECT table_name FROM information_schema.columns WHERE table_schema = current_schema() AND column_name = 'document'"""
      .map(_.string(1)).list().map(_ -> "document::text")
    (Seq("events" -> "event", "events_archive" -> "event", "snapshots" -> "state", "subscription_failures" -> "error",
      "commands_responses" -> "info", "rewrite_spec_notes" -> "note") ++ projections).filter { case (table, column) =>
      SQL(s"SELECT EXISTS (SELECT 1 FROM $table WHERE position(? IN $column) > 0)").bind(value).map(_.boolean(1)).single().get
    }.map(_._1)
  }

  private def nowhere(value: String, logs: LogCapture): Unit = {
    tablesHolding(value) mustBe empty
    logs.containing(value) mustBe empty
  }

  private def switchedTo(projection: ActorRef, build: Int): Unit = awaitAssert({
    val builds = projection.askFor[Builds](GetBuilds, 5.seconds)
    (builds.active.map(_.number), builds.building) mustBe ((Some(build), None))
  }, 60.seconds, 100.millis)

  Feature("Erasure reaches every copy of a value (risk assessment §12)") {

    Scenario("F194 a copy attached to the erased folder, with no change of its own, is rebuilt in the projections") {
      val logs = new LogCapture(system)
      try {
        switchedTo(notes, 1)
        val secret = marker()
        val base = send(CreateFolder(user, SpaceId(0), "Zeta")).aggregateId
        send(AddNote(user, base, AggregateVersion(1), secret))
        val copy = send(CopyFolder(user, SpaceId(0), base, AggregateVersion(2))).aggregateId
        delivered(base, copy)
        awaitAssert(doc(copy) mustBe Some(FolderDoc("Zeta", Vector(secret))), 20.seconds, 50.millis)

        val report = rewrite(base, EraseNote(secret))

        (report.complete, report.unchanged) mustBe ((true, Seq(copy)))
        doc(copy) mustBe Some(FolderDoc("Zeta", Vector("erased")))
        folder(copy).notes mustBe Vector("erased")
        nowhere(secret, logs)
      } finally logs.close()
    }

    Scenario("F192 a projection that could not rebuild is asked again by the next run, which reports it rebuilt") {
      val refusing = system.actorOf(Props(new RefusingOnce))
      val id = send(CreateFolder(user, SpaceId(0), "Eta")).aggregateId
      send(AddNote(user, id, AggregateVersion(1), "private"))
      def run() = Await.result(new HistoryRewriter(folders, eventStore, Map("refusing" -> refusing)).rewrite(id, EraseNote("private"), user), 60.seconds)

      val first = run()
      (first.complete, first.failed.keySet) mustBe ((false, Set("refusing")))
      val second = run()
      (second.complete, second.rebuilt, second.rewritten, second.unchanged) mustBe ((true, Seq("refusing"), Nil, Seq(id)))
      refusing.askFor[Int]("asked") mustBe 2
    }

    Scenario("F192 two erasures at once are both rebuilt: the projection takes the second after the first") {
      val logs = new LogCapture(system)
      try {
        switchedTo(notes, 1)
        val (one, two) = (marker(), marker())
        val ids = Seq(one, two).map { secret =>
          val id = send(CreateFolder(user, SpaceId(0), "Theta")).aggregateId
          send(AddNote(user, id, AggregateVersion(1), secret))
          id
        }
        delivered(ids: _*)
        awaitAssert(ids.map(doc(_).map(_.notes)) mustBe Seq(Some(Vector(one)), Some(Vector(two))), 20.seconds, 50.millis)

        val rewriter = new HistoryRewriter(folders, eventStore, projectionsGiven)
        val (first, second) = (rewriter.rewrite(ids.head, EraseNote(one), user), rewriter.rewrite(ids(1), EraseNote(two), user))
        val reports = Seq(first, second).map(Await.result(_, 60.seconds))

        reports.map(_.complete) mustBe Seq(true, true)
        ids.map(doc(_).map(_.notes)) mustBe Seq(Some(Vector("erased")), Some(Vector("erased")))
        Seq(one, two).foreach(nowhere(_, logs))
      } finally logs.close()
    }

    Scenario("F192 an erasure while the projection is built again reaches the build before it serves") {
      val logs = new LogCapture(system)
      try {
        val secret = marker()
        val (seen, go) = (new java.util.concurrent.CountDownLatch(1), new java.util.concurrent.CountDownLatch(1))
        val name = "rewrite-build-" + System.nanoTime() % 100000
        val pausing = new Projections(system, ProjectionsSettings(eventStore, subscriptions, registry, Seq(context)))
          .start[PausingFolderNotes](name, 1, name) { build =>
            new PausingFolderNotes(subscriptions, new PostgresDocumentStore[FolderDoc](build.documentStore(name.replace('-', '_')), mpjsons,
              new NoopDocumentStoreCache), secret, seen, go)
          }
        eventBus ! RegisterSubscribers(Seq(pausing))
        projectionsGiven += "pausing" -> pausing
        switchedTo(pausing, 1)
        def pausingDoc(id: AggregateId) = pausing.askFor[Option[FolderDoc]](GetFolderDoc(id), 5.seconds)
        val id = send(CreateFolder(user, SpaceId(0), "Iota")).aggregateId
        send(AddNote(user, id, AggregateVersion(1), secret))
        delivered(id)
        awaitAssert(pausingDoc(id) mustBe Some(FolderDoc("Iota", Vector(secret))), 20.seconds, 50.millis)

        Given("a build of the projection that has read the folder with the value, and waits")
        pausing.askFor[RebuildStarted](RebuildProjection, 5.seconds).build mustBe 2
        seen.await(60, java.util.concurrent.TimeUnit.SECONDS) mustBe true

        When("the value is erased")
        val report = Await.result(new HistoryRewriter(folders, eventStore, projectionsGiven).rewrite(id, EraseNote(secret), user), 60.seconds)
        report.complete mustBe true
        pausingDoc(id) mustBe Some(FolderDoc("Iota", Vector("erased")))

        Then("the build, once caught up, rebuilds the folder before it switches in")
        go.countDown()
        switchedTo(pausing, 2)
        pausingDoc(id) mustBe Some(FolderDoc("Iota", Vector("erased")))
        nowhere(secret, logs)
      } finally logs.close()
    }

    Scenario("F195 a command whose handler ran across an erasure runs again on the erased state") {
      val logs = new LogCapture(system)
      try {
        val secret = marker()
        val (started, release) = (new java.util.concurrent.CountDownLatch(1), new java.util.concurrent.CountDownLatch(1))
        val copying = system.actorOf(AggregateCommandBusActor(new CopyingFolders(started, release), uidGenerator, eventStore, responses, eventBus))
        val id = copying.askFor[CustomCommandResponse[_]](CreateFolder(user, SpaceId(0), "Kappa")).asInstanceOf[SuccessResponse].aggregateId
        copying.askFor[CustomCommandResponse[_]](AddNote(user, id, AggregateVersion(1), secret)) mustBe a[SuccessResponse]

        val copied = scala.concurrent.Future(copying.askFor[CustomCommandResponse[_]](CopyLastNote(user, id), 30.seconds))(system.dispatcher)
        started.await(10, java.util.concurrent.TimeUnit.SECONDS) mustBe true
        Await.result(new HistoryRewriter(copying, eventStore, projectionsGiven).rewrite(id, EraseNote(secret), user), 30.seconds).complete mustBe true
        release.countDown()

        Await.result(copied, 30.seconds) mustBe SuccessResponse(id, AggregateVersion(3))
        copying.askFor[Try[Aggregate[Folder]]](GetAggregate(id)).get.aggregateRoot.get.notes mustBe Vector("erased", "erased")
        delivered(id)
        nowhere(secret, logs)
        system.stop(copying)
      } finally logs.close()
    }

    Scenario("F245 a folder with an event that cannot be read is erased through the change's text form") {
      val logs = new LogCapture(system)
      val secret = marker()
      val id = send(CreateFolder(user, SpaceId(0), "Lambda")).aggregateId
      send(AddNote(user, id, AggregateVersion(1), secret))
      send(AddNote(user, id, AggregateVersion(2), "kept"))
      delivered(id)
      def setNote(json: String): Unit = DB.autoCommit { implicit session =>
        sql"UPDATE events SET event = $json WHERE aggregate_id = ${id.asLong} AND version = 2".update()
      }
      setNote("{\"text\":\"" + secret + "\"") // cut short: it cannot be read
      folders ! io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate(id)
      // The owner stops with a snapshot at version 3; without it, the folder cannot be read at all.
      def snapshotVersion = DB.readOnly(implicit session => sql"SELECT version FROM snapshots WHERE aggregate_id = ${id.asLong}".map(_.int(1)).single())
      awaitAssert(snapshotVersion mustBe Some(3), 10.seconds, 50.millis)
      DB.autoCommit(implicit session => sql"DELETE FROM snapshots WHERE aggregate_id = ${id.asLong}".update())
      folders.askFor[Try[Aggregate[Folder]]](GetAggregate(id)).isFailure mustBe true
      try {
        val report = rewrite(id, EraseNote(secret))

        report.rewritten.map(r => (r.id, r.events, r.unreadable)) mustBe Seq((id, 1, 1))
        DB.readOnly(implicit session => sql"SELECT event FROM events WHERE aggregate_id = ${id.asLong} AND version = 2".map(_.string(1)).single()) mustBe
          Some("{\"text\":\"erased\"")
        nowhere(secret, logs)
      } finally {
        setNote("{\"text\":\"erased\"}") // readable again, for the builds of the other specs in this schema
        logs.close()
      }
    }
  }
}

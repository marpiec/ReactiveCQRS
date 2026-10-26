package io.reactivecqrs.testdomain.spec

import java.io.StringWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.aggregate.AggregateRestorer
import io.reactivecqrs.core.cluster.Singletons
import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.rebuild.ProjectionBuilder.{BuildStatus, GetBuildStatus}
import io.reactivecqrs.core.rebuild.{PostgresProjectionRegistry, Projections, ProjectionsSettings}
import io.reactivecqrs.core.eventstore.{NewEvents, PostgresEventStoreState}
import io.reactivecqrs.core.space.SpaceFormat.{ExportHeader, ExportedAggregate, ExportedEvent, Line}
import io.reactivecqrs.core.space.{ExportReport, ImportReport, SpaceExport, SpaceFormat, SpaceImport}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase, TestIds}
import org.apache.pekko.actor.{ActorRef, PoisonPill, Props}
import org.apache.pekko.testkit.TestProbe
import scalikejdbc._

import scala.concurrent.Await
import scala.concurrent.duration._

// D40: moving an organization between installations.
case class NotesUndone(eventsCount: Int) extends UndoEvent[Folder]
case class FolderLinked(other: AggregateId, by: UserId) extends Event[Folder]

class MovableFolderContext extends FolderContext {
  override def eventHandlers = (userId, time, folder) => super.eventHandlers(userId, time, folder).orElse[Any, Folder] {
    case _: NotesUndone => folder
    case _: FolderLinked => folder
  }
}

class ExportImportSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val source = new PostgresEventStoreState(mpjsons, new PostgresTypesNamesState().initSchema()).initSchema()
  private val context = new MovableFolderContext
  private val user = UserId(5)

  private def stored(store: PostgresEventStoreState, id: AggregateId, expected: Int, events: Event[Folder]*): Unit =
    store.localTx(session => store.persistEvents(id, NewEvents(user, AggregateVersion(expected), Instant.now, events))(session)).get

  private def delivered(ids: AggregateId*): Unit = DB.autoCommit { implicit session =>
    sql"DELETE FROM events_to_publish WHERE aggregate_id IN (${ids.map(_.asLong)})".update()
  }

  private def stateOf(store: PostgresEventStoreState, id: AggregateId): Option[Folder] =
    Option(new AggregateRestorer[Folder](store, context.eventHandlers, () => context.initialAggregateRoot).restore(id).root)

  // The sequences are read here, in the current schema: inside Props they would be read later, maybe after a switch.
  private def withUidGenerator[A](block: ActorRef => A): A = {
    val (aggregates, commands, sagas) = (new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"),
      new PostgresUidGenerator("sagas_uids_seq"))
    val generator = system.actorOf(Props(new UidGeneratorActor(aggregates, commands, sagas)))
    try block(generator) finally generator ! PoisonPill
  }

  /** The folders' notes as a managed projection builds them from the store of the schema the pool points to, read with
    * `read` once `ready` holds and no build runs; stopped, with all its actors, before the pool may point elsewhere. */
  private def folderNotes[A](name: String, ready: (AggregateId => Option[FolderDoc]) => Unit)(read: (AggregateId => Option[FolderDoc]) => A): A = {
    val typesNames = new PostgresTypesNamesState().initSchema()
    val store = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
    val bus = system.actorOf(Props(new EventsBusActor()), name + "-bus")
    val notes = new Projections(system, ProjectionsSettings(store, subscriptions, new PostgresProjectionRegistry().initSchema(), Seq(context),
      restartAfter = 1.second)).start[ErasableFolderNotes]("folder-notes", 1, name + "-notes") { build =>
      new ErasableFolderNotes(subscriptions, new PostgresDocumentStore[FolderDoc](build.documentStore("folder_notes"), mpjsons, new NoopDocumentStoreCache))
    }
    bus ! RegisterSubscribers(Seq(notes))
    def doc(id: AggregateId): Option[FolderDoc] = notes.askFor[Option[FolderDoc]](GetFolderDoc(id), 5.seconds)
    try {
      awaitAssert(ready(doc), 60.seconds, 100.millis)
      awaitAssert(Seq("CaughtUp", "NoBuild") must contain (notes.askFor[BuildStatus](GetBuildStatus, 5.seconds).phase), 60.seconds, 100.millis)
      read(doc)
    } finally {
      // The supervisor, or in a cluster its singleton's manager, ends after its instances and builder.
      val supervisor = if (Singletons.isCluster(system)) Await.result(system.actorSelection("/user/" + name + "-notes").resolveOne(5.seconds), 5.seconds) else notes
      val watcher = TestProbe()
      watcher.watch(supervisor)
      Singletons.stop(system, notes)
      watcher.expectTerminated(supervisor, 30.seconds)
      system.stop(bus)
    }
  }

  Feature("Moving an organization between installations (D40)") {

    Scenario("an organization exported from one installation is imported into another, ids it has already moved, references following") {
      Given("an organization with a squashed, archived and undone history, copies within it, one detached, and a copy of another organization's folder")
      val space = 7000000000L + System.nanoTime() % 1000000L
      val (here, elsewhere, into) = (SpaceId(space), SpaceId(space + 1), SpaceId(space + 2))
      val Seq(outside, folder, copy, theirs, early) = Vector.fill(5)(AggregateId(TestIds.aggregate())): @unchecked
      stored(source, outside, 0, FolderCreated(elsewhere, "Outside"), NoteAdded("x"))
      stored(source, folder, 0, FolderCreated(here, "Alpha"), NoteAdded("one"), NoteAdded("two"), FolderLinked(outside, user))
      stored(source, copy, 0, FolderCopied(here, folder, AggregateVersion(2)))
      stored(source, copy, 1, NoteAdded("own"))
      stored(source, theirs, 0, FolderCopied(here, outside, AggregateVersion(2)))
      stored(source, early, 0, FolderCopied(here, folder, AggregateVersion(1))) // the squash detaches it
      delivered(outside, folder, copy, theirs, early)
      source.squash(folder, AggregateVersion(2), FolderSquashed(here, "Alpha", Vector("one")), UserId(9), None,
        Seq(early -> FolderSquashed(here, "Alpha", Vector.empty)), Set(classOf[NoteAdded].getName)) mustBe Right(())
      stored(source, folder, 4, NoteAdded("three"))
      stored(source, folder, 5, NotesUndone(1))

      When("it is exported")
      val file = Files.createTempFile("space-", ".jsonl")
      val writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)
      val exported = try new SpaceExport(source, Seq(context)).write(here, writer) finally writer.close()
      (exported.complete, exported.aggregates, exported.events, exported.archived, exported.detached) mustBe ((true, 4, 9L, 1L, Seq(theirs)))

      TestDatabase.inSchema("target") {
        val target = new PostgresEventStoreState(new MPJsons, new PostgresTypesNamesState().initSchema()).initSchema()
        stored(target, folder, 0, FolderCreated(SpaceId(999), "Taken")) // the folder's id is taken there

        And("imported into an installation that has one of its ids already")
        val imported = fromAnotherInstallation(target, file, into)

        Then("everything is written, the taken id moved, and the reference out of the organization reported")
        imported.complete mustBe true
        (imported.aggregates, imported.events, imported.archived, imported.remapped.keySet) mustBe ((4, 9L, 1L, Set(folder)))
        val moved = imported.remapped(folder)
        (imported.unknownReferences, imported.someUnknown) mustBe ((1L, Seq(outside)))

        And("each folder reads as it did, in its new organization, and the copy reads its base under the base's new id")
        stateOf(target, moved) mustBe Some(Folder(into, "Alpha", Vector("one", "two")))
        stateOf(target, copy) mustBe Some(Folder(into, "Alpha", Vector("one", "own")))
        stateOf(target, theirs) mustBe Some(Folder(into, "Outside", Vector("x")))
        stateOf(target, early) mustBe Some(Folder(into, "Alpha", Vector.empty))
        stateOf(target, folder) mustBe Some(Folder(SpaceId(999), "Taken", Vector.empty))
        target.readEventsRange[Folder](copy, AggregateVersion(1), AggregateVersion(1)).map(_.event) mustBe Seq(FolderCopied(into, moved, AggregateVersion(2)))
        target.readArchive[Folder](moved).map(e => (e.info.version.asInt, e.info.event)) mustBe Seq(2 -> NoteAdded("one"))
        DB.readOnly(implicit session => sql"SELECT count(*) FROM events WHERE space_id = ${into.asLong}".map(_.long(1)).single().get) mustBe 9L
        And("the copy within the organization reads its base's new id; the detached one reads nothing outside")
        DB.readOnly(implicit session => sql"SELECT id, base_id FROM aggregates WHERE id IN (${Seq(copy.asLong, theirs.asLong)}) ORDER BY id, base_order"
          .map(rs => (rs.long(1), rs.long(2))).list()) mustBe Seq((copy.asLong, moved.asLong), (copy.asLong, copy.asLong), (theirs.asLong, theirs.asLong))
        And("the copy detached before the move still names the base it holds values of, under the base's new id (D76)")
        DB.readOnly(implicit session => sql"SELECT copied_from, at_version FROM aggregate_lineage WHERE aggregate_id = ${early.asLong}"
          .map(rs => (rs.long(1), rs.int(2))).list()) mustBe Seq((moved.asLong, 1))

        And("a second import into the same space is refused, writing nothing")
        val again = fromAnotherInstallation(target, file, into)
        again.refused mustBe Seq("space " + into.asLong + " has aggregates already; an import writes into an empty space")
        again.events mustBe 0L
      }
      Files.delete(file)
    }

    Scenario("a projection of the organization, built in the other installation, holds what the source's holds, under the moved ids (D40)") {
      Given("an organization's folders, a copy and an undone note, and their projection in the source installation")
      val space = 7100000000L + System.nanoTime() % 1000000L
      val (here, into) = (SpaceId(space), SpaceId(space + 1))
      val Seq(folder, copy, other) = Vector.fill(3)(AggregateId(TestIds.aggregate())): @unchecked
      stored(source, folder, 0, FolderCreated(here, "Alpha"), NoteAdded("one"), NoteAdded("two"))
      stored(source, copy, 0, FolderCopied(here, folder, AggregateVersion(2)))
      stored(source, copy, 1, NoteAdded("own"))
      stored(source, other, 0, FolderCreated(here, "Beta"), NoteAdded("x"), NotesUndone(1))
      delivered(folder, copy, other)
      val ids = Seq(folder, copy, other)
      val expected = Seq(FolderDoc("Alpha", Vector("one", "two")), FolderDoc("Alpha", Vector("one", "own")), FolderDoc("Beta", Vector.empty))
      val inSource = folderNotes("source", doc => ids.map(doc).flatten mustBe expected)(doc => ids.map(doc))
      inSource mustBe expected.map(Some(_))

      When("it is exported and imported into an installation that holds one of its ids")
      val file = Files.createTempFile("space-", ".jsonl")
      val writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)
      try new SpaceExport(source, Seq(context)).write(here, writer).complete mustBe true finally writer.close()
      TestDatabase.inSchema("projected") {
        val target = new PostgresEventStoreState(new MPJsons, new PostgresTypesNamesState().initSchema()).initSchema()
        stored(target, folder, 0, FolderCreated(SpaceId(999), "Taken"))
        val imported = fromAnotherInstallation(target, file, into)
        imported.complete mustBe true
        val moved = ids.map(id => imported.remapped.getOrElse(id, id))
        moved.head must not be folder

        Then("the projection built there holds the same documents under the moved ids, and the other installation's own")
        val inTarget = folderNotes("target", doc => moved.map(doc).flatten mustBe expected)(doc => (moved.map(doc), doc(folder)))
        inTarget mustBe ((inSource, Some(FolderDoc("Taken", Vector.empty))))
      }
      Files.delete(file)
    }

    Scenario("an export that cannot detach a copy of another organization's folder, and an import of classes this installation lacks, are refused") {
      val space = 7100000000L + System.nanoTime() % 1000000L
      val (here, elsewhere) = (SpaceId(space), SpaceId(space + 1))
      val Seq(outside, theirs) = Vector.fill(2)(AggregateId(TestIds.aggregate())): @unchecked
      stored(source, outside, 0, FolderCreated(elsewhere, "Outside"))
      stored(source, theirs, 0, FolderCopied(here, outside, AggregateVersion(1)))

      val writer = new StringWriter()
      val refused = new SpaceExport(source, contexts = Nil).write(here, writer)
      refused.refused.head must include("aggregate " + theirs.asLong + " reads outside the space")
      writer.toString mustBe empty

      val file = Files.createTempFile("space-", ".jsonl")
      val lines = Seq(Line(header = Some(ExportHeader(SpaceFormat.Version, 0, space, Instant.now.toString))),
        Line(aggregate = Some(ExportedAggregate(42, classOf[Folder].getName, 1, None, Instant.now.toString, Nil, detached = false, lineage = Nil))),
        Line(event = Some(ExportedEvent(42, 1, "com.example.Gone", "{}", 5, Instant.now.toString, None, None))))
      Files.write(file, lines.map(mpjsons.serialize(_, classOf[Line].getName)).mkString("\n").getBytes(StandardCharsets.UTF_8))
      val imported = withUidGenerator(generator => new SpaceImport(source, generator, Seq(context)).read(file, SpaceId(space + 2)))
      imported.refused must contain("classes missing here: com.example.Gone")
      Files.delete(file)
    }
  }

  /** The import into a target that is another installation (D39): its generator hands out installation 1's ids, so the
    * exported ids, of installation 0's range, keep theirs unless the target has them already (F248). */
  private def fromAnotherInstallation(target: PostgresEventStoreState, file: java.nio.file.Path, into: SpaceId): ImportReport =
    withUidGenerator { generator =>
      DB.autoCommit(implicit session =>
        sql"SELECT setval('aggregates_uids_seq', greatest(1000000000000, (SELECT last_value FROM aggregates_uids_seq)))".map(_.long(1)).single())
      new SpaceImport(target, generator, Seq(context)).read(file, into)
    }

  private def exportOf(space: SpaceId, writer: java.io.Writer = null): (java.nio.file.Path, ExportReport) = {
    val file = Files.createTempFile("space-", ".jsonl")
    val out = Option(writer).getOrElse(Files.newBufferedWriter(file, StandardCharsets.UTF_8))
    val report = try new SpaceExport(source, Seq(context)).write(space, out) finally out.close()
    (file, report)
  }

  private def aggregatesIn(space: SpaceId): Long =
    DB.readOnly(implicit session => sql"SELECT count(*) FROM aggregates WHERE space_id = ${space.asLong}".map(_.long(1)).single().get)

  Feature("A file the import can trust (risk assessment, step 2)") {

    Scenario("F198 a command that commits while the organization is exported is in the file whole, or not at all") {
      val space = 7200000000L + System.nanoTime() % 1000000L
      val (here, into) = (SpaceId(space), SpaceId(space + 1))
      val folder = AggregateId(TestIds.aggregate())
      stored(source, folder, 0, FolderCreated(here, "Alpha"), NoteAdded("one"))
      delivered(folder)

      Given("an export during which a command on the folder commits, once its line is written")
      val file = Files.createTempFile("space-", ".jsonl")
      val out = Files.newBufferedWriter(file, StandardCharsets.UTF_8)
      var committed = false
      val interfering = new java.io.Writer {
        override def write(chars: Array[Char], off: Int, len: Int): Unit = {
          if (!committed && new String(chars, off, len).contains("\"aggregate\":{\"value\"")) {
            committed = true
            stored(source, folder, 2, NoteAdded("during"))
          }
          out.write(chars, off, len)
        }
        override def flush(): Unit = out.flush()
        override def close(): Unit = out.close()
      }
      val (_, exported) = exportOf(here, interfering)
      (exported.complete, committed) mustBe ((true, true))

      Then("the file holds the folder as it was when the export began: its version and its events agree")
      TestDatabase.inSchema("during") {
        val target = new PostgresEventStoreState(new MPJsons, new PostgresTypesNamesState().initSchema()).initSchema()
        fromAnotherInstallation(target, file, into).complete mustBe true
        stateOf(target, folder) mustBe Some(Folder(into, "Alpha", Vector("one")))
        DB.readOnly(implicit session => sql"""SELECT a.base_version, (SELECT max(version) FROM events e WHERE e.aggregate_id = a.id)
             | FROM aggregates a WHERE a.id = ${folder.asLong} AND a.base_id = a.id""".stripMargin.map(rs => (rs.int(1), rs.int(2))).single()) mustBe Some((2, 2))
      }
      Files.delete(file)
    }

    Scenario("F199 a file cut short, changed or naming what it does not hold is refused, and nothing is written") {
      val space = 7300000000L + System.nanoTime() % 1000000L
      val (here, into) = (SpaceId(space), SpaceId(space + 1))
      val Seq(folder, copy) = Vector.fill(2)(AggregateId(TestIds.aggregate())): @unchecked
      stored(source, folder, 0, FolderCreated(here, "Alpha"), NoteAdded("one"), NoteAdded("two"))
      stored(source, copy, 0, FolderCopied(here, folder, AggregateVersion(2)))
      delivered(folder, copy)
      val (file, exported) = exportOf(here)
      exported.complete mustBe true
      val whole = new String(Files.readAllBytes(file), StandardCharsets.UTF_8).split("\n").toVector
      def importOf(lines: Seq[String]): ImportReport = {
        Files.write(file, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))
        withUidGenerator(generator => new SpaceImport(source, generator, Seq(context)).read(file, into))
      }

      importOf(whole.dropRight(1)).refused must contain("the file has no trailer: it was cut short, or its export stopped")
      val withoutAnEvent = importOf(whole.patch(whole.indexWhere(_.contains("\"event\":{\"value\"")), Nil, 1))
      withoutAnEvent.refused.mkString must include("the file's lines do not match its trailer's digest")
      withoutAnEvent.refused.mkString must include("but its events end at")
      val withoutTheBase = importOf(whole.filterNot(_.contains("\"id\":" + folder.asLong + ",")))
      withoutTheBase.refused.mkString must include("aggregate " + copy.asLong + " reads aggregate " + folder.asLong + ", which the file does not have")
      val notAnEvent = importOf(whole.map(_.replace(classOf[NoteAdded].getName, "java.lang.String")))
      notAnEvent.refused.mkString must include("java.lang.String is not an Event")
      withUidGenerator(generator => new SpaceImport(source, generator, Nil).read(file, into)).refused.mkString must include("give the import the aggregate contexts")
      aggregatesIn(into) mustBe 0L

      importOf(whole).complete mustBe true
      Files.delete(file)
    }

    Scenario("F248 an id in the range the target's own generator hands out gets a new one; the generator is not moved") {
      val space = 7400000000L + System.nanoTime() % 1000000L
      val (here, into) = (SpaceId(space), SpaceId(space + 1))
      val (low, high) = (AggregateId(TestIds.aggregate()), AggregateId(TestIds.aggregate() + 1000000000L))
      stored(source, low, 0, FolderCreated(here, "Low"))
      stored(source, high, 0, FolderCreated(here, "High"))
      delivered(low, high)
      val (file, _) = exportOf(here)
      TestDatabase.inSchema("ranges") {
        val target = new PostgresEventStoreState(new MPJsons, new PostgresTypesNamesState().initSchema()).initSchema()
        withUidGenerator { generator =>
          // Below one id, above the other: a pool may hold the one, the sequence would reach the other.
          DB.autoCommit(implicit session => sql"SELECT setval('aggregates_uids_seq', ${low.asLong + 1})".map(_.long(1)).single())
          val imported = new SpaceImport(target, generator, Seq(context), bytesPerTransaction = 1).read(file, into)
          (imported.complete, imported.remapped.keySet) mustBe ((true, Set(low, high)))
          stateOf(target, imported.remapped(high)) mustBe Some(Folder(into, "High", Vector.empty))
          DB.autoCommit(implicit session => sql"SELECT nextval('aggregates_uids_seq')".map(_.long(1)).single().get) must be < high.asLong
        }
      }
      Files.delete(file)
    }
  }
}

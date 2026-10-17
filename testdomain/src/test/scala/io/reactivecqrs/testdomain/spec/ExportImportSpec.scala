package io.reactivecqrs.testdomain.spec

import java.io.StringWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.aggregate.AggregateRestorer
import io.reactivecqrs.core.eventstore.{NewEvents, PostgresEventStoreState}
import io.reactivecqrs.core.space.SpaceFormat.{ExportHeader, ExportedAggregate, ExportedEvent, Line}
import io.reactivecqrs.core.space.{SpaceExport, SpaceFormat, SpaceImport}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase, TestIds}
import org.apache.pekko.actor.{ActorRef, PoisonPill, Props}
import scalikejdbc._

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

  Feature("Moving an organization between installations (D40)") {

    Scenario("an organization exported from one installation is imported into another, ids it has already moved, references following") {
      Given("an organization with a squashed, archived and undone history, copies within it, one detached, and a copy of another organization's folder")
      val space = 7000000000L + System.nanoTime() % 1000000L
      val (here, elsewhere, into) = (SpaceId(space), SpaceId(space + 1), SpaceId(space + 2))
      val List(outside, folder, copy, theirs, early) = List.fill(5)(AggregateId(TestIds.aggregate()))
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
        val imported = withUidGenerator(generator => new SpaceImport(target, generator).read(file, into))

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
        DB.readOnly(implicit session => sql"SELECT count(*) FROM event_keys WHERE space_id = ${into.asLong}".map(_.long(1)).single().get) mustBe 9L
        And("the copy within the organization reads its base's new id; the detached one reads nothing outside")
        DB.readOnly(implicit session => sql"SELECT id, base_id FROM aggregates WHERE id IN (${Seq(copy.asLong, theirs.asLong)}) ORDER BY id, base_order"
          .map(rs => (rs.long(1), rs.long(2))).list()) mustBe Seq((copy.asLong, moved.asLong), (copy.asLong, copy.asLong), (theirs.asLong, theirs.asLong))
        And("the copy detached before the move still names the base it holds values of, under the base's new id (D76)")
        DB.readOnly(implicit session => sql"SELECT copied_from, at_version FROM aggregate_lineage WHERE aggregate_id = ${early.asLong}"
          .map(rs => (rs.long(1), rs.int(2))).list()) mustBe Seq((moved.asLong, 1))

        And("a second import into the same space is refused, writing nothing")
        val again = withUidGenerator(generator => new SpaceImport(target, generator).read(file, into))
        again.refused mustBe Seq("space " + into.asLong + " has aggregates already; an import writes into an empty space")
        again.events mustBe 0L
      }
      Files.delete(file)
    }

    Scenario("an export that cannot detach a copy of another organization's folder, and an import of classes this installation lacks, are refused") {
      val space = 7100000000L + System.nanoTime() % 1000000L
      val (here, elsewhere) = (SpaceId(space), SpaceId(space + 1))
      val List(outside, theirs) = List.fill(2)(AggregateId(TestIds.aggregate()))
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
      val imported = withUidGenerator(generator => new SpaceImport(source, generator).read(file, SpaceId(space + 2)))
      imported.refused mustBe Seq("classes missing here: com.example.Gone")
      Files.delete(file)
    }
  }
}

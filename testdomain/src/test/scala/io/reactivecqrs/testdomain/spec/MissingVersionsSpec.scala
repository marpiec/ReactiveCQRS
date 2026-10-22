package io.reactivecqrs.testdomain.spec

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.cluster.Singletons
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.documentstore.{Document, DocumentStore, NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore._
import io.reactivecqrs.core.projection._
import io.reactivecqrs.core.rebuild.ProjectionBuilder.{BuildStatus, BuilderSettings, GetBuildStatus}
import io.reactivecqrs.core.rebuild.ProjectionSupervisor._
import io.reactivecqrs.core.rebuild._
import io.reactivecqrs.core.tools.RestoreCheck
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import scalikejdbc._

import scala.concurrent.duration._

case class GetGapNote(id: AggregateId)

/** Each note's lines and version, from its state with its events. */
class GapNoteViews(val subscriptionsState: SubscriptionsState, store: DocumentStore[NoteView]) extends ProjectionActor {
  protected val listeners = List(AggregateWithEventsListener[BuildNote] { (id, version, note, _) => implicit session =>
    note.foreach(n => store.updateDocument(0, id.asLong, _ => Document(NoteView(n.title, n.lines, version.asInt))))
  })
  override protected def receiveQuery: Receive = { case GetGapNote(id) => sender() ! store.getDocument(id.asLong).map(_.document) }
  override protected val projectionName: String = "GapNoteViews"
}

/** Each note's event versions, from its events. */
class GapNoteEvents(val subscriptionsState: SubscriptionsState, store: DocumentStore[NoteLog]) extends ProjectionActor {
  protected val listeners = List(EventsListener[BuildNote] { (id, events) => implicit session =>
    store.updateDocument(0, id.asLong, existing => Document(NoteLog(existing.map(_.document.entries).getOrElse(Vector()) ++ events.map(_.version.asInt.toString))))
  })
  override protected def receiveQuery: Receive = { case GetGapNote(id) => sender() ! store.getDocument(id.asLong).map(_.document) }
  override protected val projectionName: String = "GapNoteEvents"
}

/** The store, counting the reads by version of one aggregate; a page of the bulk pass waits while `held`. */
class WatchedEventStore(mpjsons: MPJsons, typesNames: PostgresTypesNamesState) extends PostgresEventStoreState(mpjsons, typesNames) {
  @volatile var watched: Long = -1
  val partReads = new AtomicInteger()
  val held = new AtomicBoolean(false)
  val holding = new CountDownLatch(1)
  val release = new CountDownLatch(1)
  override def readPart[AGGREGATE_ROOT](aggregateId: AggregateId, from: AggregateVersion, to: AggregateVersion, maxBytes: Long): Seq[(EventInfo[AGGREGATE_ROOT], Int)] = {
    if (aggregateId.asLong == watched) partReads.incrementAndGet()
    super.readPart[AGGREGATE_ROOT](aggregateId, from, to, maxBytes)
  }
  override def readCommittedPage(types: Seq[AggregateType], after: Long, limit: Int, scope: AggregateScope, maxBytes: Long): CommittedPage = {
    if (held.get()) {
      holding.countDown()
      release.await(30, TimeUnit.SECONDS)
    }
    super.readCommittedPage(types, after, limit, scope, maxBytes)
  }
}

// CR-6: a version missing from the store stops no build for ever: the build fails the aggregate, visibly, and an operator
// moves past it. CR-7: a build's status is answered at once, also while its builder reads.
class MissingVersionsSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val userId = UserId(1)
  private var nodes = 0

  private class Installation {
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new WatchedEventStore(mpjsons, typesNames).initSchema().asInstanceOf[WatchedEventStore]
    val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
    val registry = new PostgresProjectionRegistry().initSchema()
    val uidGenerator: ActorRef = system.actorOf(Props(new UidGeneratorActor(
      new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))

    class Node(version: Int, events: Boolean) {
      nodes += 1
      val bus: ActorRef = system.actorOf(Props(new EventsBusActor()), "gap-bus-" + nodes)
      val notes: ActorRef = system.actorOf(AggregateCommandBusActor(new BuildNoteContext, uidGenerator, eventStore, responses, bus))
      val projections = new Projections(system, ProjectionsSettings(eventStore, subscriptions, registry, Seq(new BuildNoteContext),
        BuilderSettings(pageSize = 5, chunkEvents = 5), restartAfter = 1.second))
      val supervisor: ActorRef =
        if (events) projections.start[GapNoteEvents]("gap-notes", version, "gap-notes-" + nodes) { build =>
          new GapNoteEvents(subscriptions, new PostgresDocumentStore[NoteLog](build.documentStore("gap_notes"), mpjsons, new NoopDocumentStoreCache))
        } else projections.start[GapNoteViews]("gap-notes", version, "gap-notes-" + nodes) { build =>
          new GapNoteViews(subscriptions, new PostgresDocumentStore[NoteView](build.documentStore("gap_notes"), mpjsons, new NoopDocumentStoreCache))
        }
      bus ! RegisterSubscribers(Seq(supervisor))

      def create(title: String, lines: String*): AggregateId = {
        val id = notes.askFor[CustomCommandResponse[_]](CreateBuildNote(userId, title, 0)).asInstanceOf[SuccessResponse].aggregateId
        lines.zipWithIndex.foreach { case (line, i) => notes.askFor[CustomCommandResponse[_]](AddBuildNoteLine(userId, id, AggregateVersion(i + 1), line)) }
        id
      }
      def stop(): Unit = {
        Singletons.stop(system, supervisor)
        Seq(bus, notes).foreach(system.stop)
      }
    }

    /** A note at versions 1 to 9 built by version 1; then version 6 deleted from the store (as by hand), and version 2 built. */
    def gapBuild(events: Boolean): (Node, AggregateId) = {
      val first = new Node(1, events)
      val note = first.create("gap", (1 to 8).map("line " + _): _*)
      first.create("other", "x")
      awaitAssert(builds(first.supervisor).building mustBe None, 60.seconds, 100.millis)
      first.stop()
      DB.autoCommit(implicit session => sql"DELETE FROM events WHERE aggregate_id = ${note.asLong} AND version = 6".update())
      eventStore.watched = note.asLong
      (new Node(2, events), note)
    }
  }

  private def builds(supervisor: ActorRef): Builds = supervisor.askFor[Builds](GetBuilds, 5.seconds)
  private def buildStatus(supervisor: ActorRef): ProjectionStatus = supervisor.askFor[ProjectionStatus](ToBuildInProgress(GetProjectionStatus), 5.seconds)
  private def switchedTo(supervisor: ActorRef, build: Int): Unit =
    awaitAssert(builds(supervisor).active.map(_.number) mustBe Some(build), 60.seconds, 200.millis)

  private def failsAtTheGap(installation: Installation, node: Installation#Node, note: AggregateId): Unit = {
    awaitAssert(buildStatus(node.supervisor).failures.map(f => (f.aggregateId, f.version, f.error)) mustBe
      Seq((note, AggregateVersion(6), "versions from 6 to 6 are missing from the store")), 30.seconds, 200.millis)
    awaitAssert(builds(node.supervisor).caughtUp mustBe true, 30.seconds, 200.millis)
    val reads = installation.eventStore.partReads.get()
    Thread.sleep(2000) // nothing reads the note again meanwhile: no loop
    installation.eventStore.partReads.get() mustBe reads
    reads must be < 10
    builds(node.supervisor).active.map(_.number) mustBe Some(1)
    installation.eventStore.restoreFailures(RestoreFailureFilter(projection = Some("gap-notes"))).map(r => (r.aggregateId, r.errorClass)) mustBe
      Seq((note, "missing versions"))
  }

  Feature("A version missing from the store (CR-6)") {

    Scenario("a state-with-events build fails the aggregate at the missing version, reads it a bounded number of times, and does not switch; past it, it switches") {
      TestDatabase.inSchema("gap_views") {
        val installation = new Installation
        val (node, note) = installation.gapBuild(events = false)
        failsAtTheGap(installation, node, note)

        node.supervisor.askFor[FailedSubscriptionsHandled](ToBuildInProgress(SkipFailedVersion(note, pastMissing = true))).count mustBe 1

        switchedTo(node.supervisor, 2)
        awaitAssert(node.supervisor.askFor[Option[NoteView]](GetGapNote(note), 5.seconds).map(_.version) mustBe Some(9), 10.seconds)
        awaitAssert(installation.eventStore.restoreFailures(RestoreFailureFilter(projection = Some("gap-notes"))).map(_.skipped.isDefined) mustBe Seq(true), 5.seconds)
        node.stop()
      }
    }

    Scenario("an events build fails the aggregate the same way; past it, the later versions are applied") {
      TestDatabase.inSchema("gap_events") {
        val installation = new Installation
        val (node, note) = installation.gapBuild(events = true)
        failsAtTheGap(installation, node, note)

        node.supervisor.askFor[FailedSubscriptionsHandled](ToBuildInProgress(SkipFailedVersion(note, pastMissing = true))).count mustBe 1

        switchedTo(node.supervisor, 2)
        awaitAssert(node.supervisor.askFor[Option[NoteLog]](GetGapNote(note), 5.seconds).map(_.entries) mustBe
          Some(Vector("1", "2", "3", "4", "5", "7", "8", "9")), 10.seconds)
        node.stop()
      }
    }

    Scenario("a whole-aggregate skip lets the build switch, as before") {
      TestDatabase.inSchema("gap_skip") {
        val installation = new Installation
        val (node, note) = installation.gapBuild(events = true)
        awaitAssert(buildStatus(node.supervisor).failures.map(_.aggregateId) mustBe Seq(note), 30.seconds, 200.millis)

        node.supervisor.askFor[AggregateSkipped](ToBuildInProgress(SkipAggregate(note))).upTo mustBe AggregateVersion(9)

        switchedTo(node.supervisor, 2)
        node.stop()
      }
    }

    Scenario("the restore check lists the gap, and a filter by source lists one source's records") {
      TestDatabase.inSchema("gap_check") {
        val installation = new Installation
        val (node, note) = installation.gapBuild(events = true)
        awaitAssert(installation.eventStore.restoreFailures().map(_.aggregateId) mustBe Seq(note), 30.seconds, 200.millis)

        val report = RestoreCheck.run(installation.eventStore, Seq(new BuildNoteContext))

        report.gaps mustBe Seq(VersionGap(note, AggregateVersion(6), 1))
        report.failedTotal mustBe 0L // it restores: only the gap shows
        installation.eventStore.restoreFailures().size mustBe 2 // the build's and the check's
        installation.eventStore.restoreFailures(RestoreFailureFilter(source = Some(RestoreFailureSource.Check))).map(r => (r.aggregateId, r.errorClass, r.version)) mustBe
          Seq((note, "missing versions", Some(AggregateVersion(6))))

        And("chapter 18's query lists it too")
        val chapter = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get("docs", "book", "18-production.md")), "UTF-8").replace("\r\n", "\n")
        val section = chapter.substring(chapter.indexOf("### Versions missing from the store"))
        val query = section.substring(section.indexOf("```sql\n") + 7, section.indexOf("\n```", section.indexOf("```sql\n"))).stripSuffix(";")
        DB.readOnly(implicit session => SQL(query).map(rs => (rs.long(1), rs.long(3))).list()) mustBe Seq((note.asLong, 1L))
        node.stop()
      }
    }
  }

  Feature("The status of a build (CR-7)") {

    Scenario("is answered at once while the builder is held in a long read, with the last values and their time") {
      TestDatabase.inSchema("status_held") {
        val installation = new Installation
        import installation._
        val first = new Node(1, events = true)
        (1 to 30).foreach(i => first.create("note " + i, "x"))
        awaitAssert(builds(first.supervisor).building mustBe None, 60.seconds, 100.millis)
        first.stop()

        eventStore.held.set(true)
        val node = new Node(2, events = true)
        eventStore.holding.await(30, TimeUnit.SECONDS) mustBe true

        val started = System.nanoTime()
        val status = node.supervisor.askFor[BuildStatus](GetBuildStatus, 1.second)
        (System.nanoTime() - started) must be < 1.second.toNanos
        status.reportedAt.isDefined mustBe true
        status.phase mustBe "Bulk"

        eventStore.held.set(false)
        eventStore.release.countDown()
        switchedTo(node.supervisor, 2)
        node.stop()
      }
    }
  }
}

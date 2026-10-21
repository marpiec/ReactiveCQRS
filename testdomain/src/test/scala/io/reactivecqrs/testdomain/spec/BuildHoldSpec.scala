package io.reactivecqrs.testdomain.spec

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.cluster.Singletons
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.metrics.{ReactiveMetrics, RecordingMetrics}
import io.reactivecqrs.core.projection._
import io.reactivecqrs.core.rebuild.ProjectionBuilder.{BuildStatus, BuilderSettings, GetBuildStatus}
import io.reactivecqrs.core.rebuild.ProjectionSupervisor._
import io.reactivecqrs.core.rebuild._
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import scalikejdbc._

import scala.concurrent.duration._

// CR-2, D89: full builds wait until the store is ready for them; nothing else does.
class BuildHoldSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val userId = UserId(1)
  private var nodes = 0

  /** An installation of its own: the hold reads the store's state, which the spec changes. */
  private class Installation {
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
    val registry = new PostgresProjectionRegistry().initSchema()
    val uidGenerator: ActorRef = system.actorOf(Props(new UidGeneratorActor(
      new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))

    /** As after an upgrade of a large installation: the events from before have no keys, the index is not built. */
    def notReady(): Unit = DB.autoCommit { implicit session =>
      sql"DELETE FROM schema_steps WHERE name = 'event_keys'".update()
      sql"DELETE FROM event_keys".update()
      sql"DROP INDEX IF EXISTS aggregates_own_type_idx".execute()
    }

    class Node(hold: Option[BuildHold])(start: Projections => ActorRef) {
      nodes += 1
      val bus: ActorRef = system.actorOf(Props(new EventsBusActor()), "hold-bus-" + nodes)
      val notes: ActorRef = system.actorOf(AggregateCommandBusActor(new BuildNoteContext, uidGenerator, eventStore, responses, bus))
      val projections = new Projections(system, ProjectionsSettings(eventStore, subscriptions, registry, Seq(new BuildNoteContext),
        BuilderSettings(pageSize = 20, chunkEvents = 10), restartAfter = 1.second, holdBuilds = hold))
      val supervisor: ActorRef = start(projections)
      bus ! RegisterSubscribers(Seq(supervisor))

      def send(command: AnyRef): SuccessResponse = notes.askFor[CustomCommandResponse[_]](command) match {
        case success: SuccessResponse => success
        case other => fail("command failed: " + other)
      }
      def create(title: String, lines: String*): AggregateId = {
        val id = send(CreateBuildNote(userId, title, 0)).aggregateId
        lines.zipWithIndex.foreach { case (line, i) => send(AddBuildNoteLine(userId, id, AggregateVersion(i + 1), line)) }
        id
      }
      def stop(): Unit = {
        Singletons.stop(system, supervisor)
        Seq(bus, notes).foreach(system.stop)
      }
    }

    def notesProjection(projections: Projections, version: Int): ActorRef =
      projections.start[ManagedNotes]("held-notes", version, "held-notes-" + nodes) { build =>
        new ManagedNotes(subscriptions, new PostgresDocumentStore[NoteLines](build.documentStore("held_notes"), mpjsons, new NoopDocumentStoreCache), shout = version > 1)
      }
  }

  private def builds(supervisor: ActorRef): Builds = supervisor.askFor[Builds](GetBuilds, 5.seconds)
  private def linesOf(supervisor: ActorRef, id: AggregateId): Option[NoteLines] = supervisor.askFor[Option[NoteLines]](GetNoteLines(id), 5.seconds)

  Feature("A hold of full builds (CR-2)") {

    Scenario("a version change waits, shown held, until the store is ready; the active build serves live meanwhile") {
      TestDatabase.inSchema("build_hold") {
        val installation = new Installation
        import installation._
        val metrics = new RecordingMetrics
        ReactiveMetrics(system).use(metrics)

        Given("version 1, built, on a store just upgraded: its events without keys, its index of aggregates by type missing")
        val first = new Node(None)(projections => notesProjection(projections, 1))
        val note = first.create("note", "one")
        awaitAssert(builds(first.supervisor).building mustBe None, 60.seconds, 100.millis) // the first build serves while built: built
        awaitAssert(linesOf(first.supervisor, note) mustBe Some(NoteLines("note", Vector("one"))), 10.seconds)
        first.stop()
        notReady()
        eventStore.buildReadiness() mustBe io.reactivecqrs.core.eventstore.StoreReadiness(eventKeys = false, ownTypeIndex = false)

        When("version 2 starts with full builds held until the store is ready")
        val second = new Node(Some(BuildHold.untilStoreReady(eventStore, checkEvery = 300.millis)))(projections => notesProjection(projections, 2))

        Then("its build waits, held, and says why, in Builds and in BuildStatus")
        awaitAssert(builds(second.supervisor).heldBy.getOrElse("") must include("event keys"), 10.seconds, 100.millis)
        builds(second.supervisor).building.map(_.number) mustBe Some(2)
        val status = second.supervisor.askFor[BuildStatus](GetBuildStatus, 5.seconds)
        (status.phase, status.eventsSent) mustBe (("Waiting", 0L))
        status.heldBy.getOrElse("") must include("aggregates_own_type_idx")
        awaitAssert(metrics.gauged("reactivecqrs.builds.held") mustBe Seq(1L), 5.seconds)

        And("version 1 serves meanwhile and takes live events (ServeLive), and a targeted rebuild is not held")
        second.send(AddBuildNoteLine(userId, note, AggregateVersion(2), "two"))
        awaitAssert(linesOf(second.supervisor, note) mustBe Some(NoteLines("note", Vector("ONE", "TWO"))), 10.seconds) // by the code of version 2
        second.supervisor.askFor[AggregatesRebuilt](RebuildAggregates(RebuildTarget.OneAggregate(note)), 30.seconds).aggregates mustBe 1
        builds(second.supervisor).active.map(_.number) mustBe Some(1)
        second.supervisor.askFor[BuildStatus](GetBuildStatus, 5.seconds).phase mustBe "Waiting"

        When("the online jobs make the store ready")
        eventStore.backfillEventKeys()
        eventStore.buildIndexesConcurrently()
        eventStore.buildReadiness().ready mustBe true

        Then("the build starts within a check and switches")
        awaitAssert(builds(second.supervisor).active.map(_.number) mustBe Some(2), 60.seconds, 100.millis)
        awaitAssert(linesOf(second.supervisor, note) mustBe Some(NoteLines("note", Vector("ONE", "TWO"))), 10.seconds)
        builds(second.supervisor).heldBy mustBe None
        awaitAssert(metrics.gauged("reactivecqrs.builds.held") mustBe Seq(0L), 5.seconds)
        second.stop()
        ReactiveMetrics(system).use(io.reactivecqrs.core.metrics.Metrics.NoOp)
      }
    }

    Scenario("without a hold a build starts at once on a store that is not ready, as before") {
      TestDatabase.inSchema("build_no_hold") {
        val installation = new Installation
        import installation._
        notReady()

        val node = new Node(None)(projections => notesProjection(projections, 1))
        awaitAssert(builds(node.supervisor).active.map(_.number) mustBe Some(1), 60.seconds, 100.millis)
        builds(node.supervisor).heldBy mustBe None
        val note = node.create("note", "one")
        awaitAssert(linesOf(node.supervisor, note) mustBe Some(NoteLines("note", Vector("one"))), 10.seconds)
        node.stop()
      }
    }
  }
}

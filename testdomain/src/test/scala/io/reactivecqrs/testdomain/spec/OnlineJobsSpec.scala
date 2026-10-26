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
import io.reactivecqrs.core.jobs.OnlineJobs._
import io.reactivecqrs.core.jobs.{OnlineJobs, OnlineJobsSettings}
import io.reactivecqrs.core.projection._
import io.reactivecqrs.core.rebuild.ProjectionBuilder.BuilderSettings
import io.reactivecqrs.core.rebuild.ProjectionSupervisor._
import io.reactivecqrs.core.rebuild._
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import scalikejdbc._

import scala.concurrent.duration._

// CR-3, D91: the online jobs run in the background inside the application, in order, resume after a stop, and the hold
// of full builds follows them.
class OnlineJobsSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val userId = UserId(1)
  private var nodes = 0

  private class Installation {
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
    val registry = new PostgresProjectionRegistry().initSchema()
    val uidGenerator: ActorRef = system.actorOf(Props(new UidGeneratorActor(
      new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))

    /** As a large store just upgraded: no event has a key, the indexes are not built, two aggregates have no space. */
    def needsTheJobs(withoutSpace: Seq[AggregateId]): Unit = DB.autoCommit { implicit session =>
      sql"DELETE FROM schema_steps WHERE name = 'event_keys'".update()
      sql"UPDATE events SET type_id = NULL, space_id = NULL".update()
      sql"DROP INDEX IF EXISTS events_type_idx".execute()
      sql"DROP INDEX IF EXISTS events_space_idx".execute()
      sql"DROP INDEX IF EXISTS aggregates_own_type_idx".execute()
      sql"DROP INDEX IF EXISTS aggregates_space_id_idx".execute()
      withoutSpace.foreach(id => sql"UPDATE aggregates SET space_id = -1 WHERE id = ${id.asLong}".update())
    }

    /** The event keys' job as saved: its state and position. */
    def savedKeys(): (String, Long) = DB.readOnly { implicit session =>
      sql"SELECT state, position FROM online_jobs WHERE name = 'event-keys'".map(rs => (rs.string(1), rs.long(2))).single().get
    }

    def keyed(id: Long): Boolean = DB.readOnly(implicit session => sql"SELECT type_id IS NOT NULL FROM events WHERE id = $id".map(_.boolean(1)).single().get)

    class Node(version: Int, hold: Option[BuildHold]) {
      nodes += 1
      val bus: ActorRef = system.actorOf(Props(new EventsBusActor()), "jobs-bus-" + nodes)
      val notes: ActorRef = system.actorOf(AggregateCommandBusActor(new BuildNoteContext, uidGenerator, eventStore, responses, bus))
      val projections = new Projections(system, ProjectionsSettings(eventStore, subscriptions, registry, Seq(new BuildNoteContext),
        BuilderSettings(pageSize = 20, chunkEvents = 10), restartAfter = 1.second, holdBuilds = hold))
      val supervisor: ActorRef = projections.start[ManagedNotes]("jobs-notes", version, "jobs-notes-" + nodes) { build =>
        new ManagedNotes(subscriptions, new PostgresDocumentStore[NoteLines](build.documentStore("jobs_notes"), mpjsons, new NoopDocumentStoreCache),
          shout = version > 1)
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

    /** Version 1 built over notes; then the store needs the jobs, and version 2 starts with its full build held. */
    def heldBuild(notes: Int = 3): (Node, Seq[AggregateId]) = {
      val first = new Node(1, None)
      val ids = (1 to notes).map(i => first.create("note " + i, "line"))
      awaitAssert(builds(first.supervisor).building mustBe None, 60.seconds, 100.millis)
      first.stop()
      needsTheJobs(ids.take(2))
      val second = new Node(2, Some(BuildHold.untilStoreReady(eventStore, checkEvery = 300.millis)))
      awaitAssert(builds(second.supervisor).heldBy.isDefined mustBe true, 10.seconds, 100.millis)
      (second, ids)
    }

    def runner(settings: OnlineJobsSettings = OnlineJobsSettings(eventStore, batchSize = 2, pause = Duration.Zero, checkEvery = 300.millis)): ActorRef = {
      nodes += 1
      OnlineJobs.start(system, settings, "online-jobs-" + nodes)
    }
  }

  private def builds(supervisor: ActorRef): Builds = supervisor.askFor[Builds](GetBuilds, 5.seconds)
  private def jobs(runner: ActorRef): OnlineJobsStatus = runner.askFor[OnlineJobsStatus](GetOnlineJobsStatus, 5.seconds)
  private def stateOf(runner: ActorRef): Seq[(String, String)] = jobs(runner).jobs.map(j => j.name -> j.state.toString)
  private def switchedTo(supervisor: ActorRef, build: Int): Unit =
    awaitAssert(builds(supervisor).active.map(_.number) mustBe Some(build), 60.seconds, 200.millis)

  Feature("The online jobs, run in the background (CR-3)") {

    Scenario("a store that needs every job gets them in order, and the held build then runs") {
      TestDatabase.inSchema("online_jobs_all") {
        val installation = new Installation
        import installation._
        val (node, ids) = heldBuild()
        eventStore.buildReadiness().ready mustBe false

        When("the runner starts")
        val jobsRunner = runner()

        Then("the indexes, the event keys and the spaces are done, in that order, with what they did")
        awaitAssert(stateOf(jobsRunner) mustBe Seq(Indexes -> "Done", EventKeys -> "Done", Spaces -> "Done"), 60.seconds, 200.millis)
        val status = jobs(jobsRunner)
        val Seq(indexes, keys, spaces) = status.jobs: @unchecked
        indexes.finished.get.isAfter(keys.started.get) mustBe false
        keys.finished.get.isAfter(spaces.started.get) mustBe false
        keys.counts("written") mustBe DB.readOnly(implicit s => sql"SELECT count(*) FROM events".map(_.long(1)).single().get)
        keys.counts("missing") mustBe 0L
        (spaces.counts("filled"), spaces.counts("left")) mustBe ((2L, 0L))
        awaitAssert(jobs(jobsRunner).readiness.exists(_.ready) mustBe true, 5.seconds)

        And("the hold is over: the build of version 2 runs and switches")
        switchedTo(node.supervisor, 2)
        node.supervisor.askFor[Option[NoteLines]](GetNoteLines(ids.head), 5.seconds) mustBe Some(NoteLines("note 1", Vector("LINE")))
        Singletons.stop(system, jobsRunner)
        node.stop()
      }
    }

    Scenario("a runner stopped while it keys the events, started again, continues where it stopped") {
      TestDatabase.inSchema("online_jobs_resumed") {
        val installation = new Installation
        import installation._
        val (node, _) = heldBuild(notes = 30)
        val slow = OnlineJobsSettings(eventStore, batchSize = 1, pause = 100.millis, checkEvery = 300.millis)

        Given("a runner that stops in the middle of the event keys")
        val first = runner(slow)
        awaitAssert(jobs(first).jobs(1).counts.getOrElse("position", 0L) must be > 0L, 30.seconds, 50.millis)
        Singletons.stop(system, first)
        awaitAssert({ // nothing moves any more
          val before = savedKeys()
          Thread.sleep(300)
          savedKeys() mustBe before
        }, 10.seconds, 10.millis)
        val (state, position) = savedKeys()
        state mustBe "Running"
        eventStore.buildReadiness().ready mustBe false

        And("an event before its position whose key went meanwhile, which only a walk from the start would key again")
        val firstEvent = DB.readOnly(implicit s => sql"SELECT min(id) FROM events".map(_.long(1)).single().get)
        firstEvent must be <= position
        DB.autoCommit(implicit s => sql"UPDATE events SET type_id = NULL, space_id = NULL WHERE id = $firstEvent".update())

        When("a runner starts again")
        val second = runner()

        Then("it continues from the position: the jobs are done, and the event before it still has no key")
        awaitAssert(stateOf(second) mustBe Seq(Indexes -> "Done", EventKeys -> "Done", Spaces -> "Done"), 60.seconds, 200.millis)
        keyed(firstEvent) mustBe false
        switchedTo(node.supervisor, 2)
        Singletons.stop(system, second)
        node.stop()
      }
    }

    Scenario("a failed job keeps the builds held and says why; the operator's override releases them") {
      TestDatabase.inSchema("online_jobs_failed") {
        val installation = new Installation
        import installation._
        val (node, _) = heldBuild()

        Given("an object of the name of an index the job drops, which fails the index job")
        DB.autoCommit(implicit s => sql"CREATE TABLE aggregates_space_idx (n INT)".execute())
        val jobsRunner = runner()

        Then("the index job fails, the others run, and the build stays held, saying why")
        awaitAssert(stateOf(jobsRunner) mustBe Seq(Indexes -> "Failed", EventKeys -> "Done", Spaces -> "Done"), 60.seconds, 200.millis)
        jobs(jobsRunner).jobs.head.detail.getOrElse("") must include("SQLState")
        awaitAssert(builds(node.supervisor).heldBy.getOrElse("") must include("the online job indexes failed"), 10.seconds, 100.millis)
        builds(node.supervisor).active.map(_.number) mustBe Some(1)

        When("an operator releases the hold")
        eventStore.releaseBuildHold()

        Then("the build runs and switches, although the store is not ready")
        switchedTo(node.supervisor, 2)
        eventStore.buildReadiness().ready mustBe false
        jobs(jobsRunner).buildHoldReleased mustBe true

        And("once the cause is gone, the failed job runs again on request")
        DB.autoCommit(implicit s => sql"DROP TABLE aggregates_space_idx".execute())
        jobsRunner.askFor[OnlineJobsStatus](RetryOnlineJobs)
        awaitAssert(stateOf(jobsRunner).head mustBe (Indexes -> "Done"), 60.seconds, 200.millis)
        eventStore.buildReadiness().ready mustBe true
        Singletons.stop(system, jobsRunner)
        node.stop()
      }
    }
  }
}

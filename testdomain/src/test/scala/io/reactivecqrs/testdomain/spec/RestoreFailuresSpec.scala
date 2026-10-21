package io.reactivecqrs.testdomain.spec

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.cluster.Singletons
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.{PostgresEventStoreState, RestoreFailure, RestoreFailureFilter, RestoreFailureSource}
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
import scala.util.Failure

/** Whether the handlers of `PoisonNoteContext` take the line "poison": the code fix of CR-4's specs. */
object PoisonedNotes {
  @volatile var fixed = true
}

/** Notes whose handler throws on the line "poison" while not `fixed`: stored data the code no longer accepts. */
class PoisonNoteContext extends BuildNoteContext {
  override def eventHandlers = (userId, time, note) => {
    case BuildNoteLineAdded("poison") if !PoisonedNotes.fixed => throw new IllegalStateException("a poisoned line")
    case event => super.eventHandlers(userId, time, note)(event)
  }
}

// CR-4: aggregates that cannot be restored are recorded, block the builds that meet them visibly, and are resolved by an
// operator: the code fixed, the aggregate removed by hand (chapter 18), or skipped in one build.
class RestoreFailuresSpec extends CommonSpec {

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

    class Node(version: Int, settings: BuilderSettings = BuilderSettings(pageSize = 20, chunkEvents = 10), buildDelayMillis: Long = 0) {
      nodes += 1
      val bus: ActorRef = system.actorOf(Props(new EventsBusActor()), "rf-bus-" + nodes)
      val notes: ActorRef = system.actorOf(AggregateCommandBusActor(new PoisonNoteContext, uidGenerator, eventStore, responses, bus))
      val projections = new Projections(system, ProjectionsSettings(eventStore, subscriptions, registry, Seq(new PoisonNoteContext), settings,
        restartAfter = 1.second))
      val supervisor: ActorRef = projections.start[ManagedNotes]("rf-notes", version, "rf-notes-" + nodes) { build =>
        new ManagedNotes(subscriptions, new PostgresDocumentStore[NoteLines](build.documentStore("rf_notes"), mpjsons, new NoopDocumentStoreCache),
          shout = version > 1, buildDelayMillis)
      }
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

    /** Version 1 built from notes the code took; then the code stops taking the line "poison", and version 2 is built. */
    def blockedBuild(fillers: Int = 0, settings: BuilderSettings = BuilderSettings(pageSize = 20, chunkEvents = 10),
                     buildDelayMillis: Long = 0): (Node, AggregateId, AggregateId) = {
      PoisonedNotes.fixed = true
      val first = new Node(1)
      val (good, bad) = (first.create("good", "x"), first.create("bad", "poison", "after"))
      (1 to fillers).foreach(i => first.create("filler " + i))
      awaitAssert(builds(first.supervisor).building mustBe None, 60.seconds, 100.millis) // the first build serves while built: built
      awaitAssert(linesOf(first.supervisor, bad) mustBe Some(NoteLines("bad", Vector("poison", "after"))), 10.seconds)
      first.stop()
      PoisonedNotes.fixed = false
      (new Node(2, settings, buildDelayMillis), good, bad)
    }

    def eventIdOf(id: AggregateId, version: Int): Long = DB.readOnly { implicit session =>
      sql"SELECT id FROM events WHERE aggregate_id = ${id.asLong} AND version = $version".map(_.long(1)).single().get
    }
  }

  private def builds(supervisor: ActorRef): Builds = supervisor.askFor[Builds](GetBuilds, 5.seconds)
  private def linesOf(supervisor: ActorRef, id: AggregateId): Option[NoteLines] = supervisor.askFor[Option[NoteLines]](GetNoteLines(id), 5.seconds)
  private def buildStatus(supervisor: ActorRef): ProjectionStatus = supervisor.askFor[ProjectionStatus](ToBuildInProgress(GetProjectionStatus), 5.seconds)

  private def blockedBy(supervisor: ActorRef, bad: AggregateId): Unit = awaitAssert({
    builds(supervisor).caughtUp mustBe true
    buildStatus(supervisor).failures.map(_.aggregateId) mustBe Seq(bad)
  }, 60.seconds, 200.millis)

  private def switchedTo(supervisor: ActorRef, build: Int): Unit =
    awaitAssert(builds(supervisor).active.map(_.number) mustBe Some(build), 90.seconds, 200.millis)

  /** Chapter 18's statements that remove one aggregate by hand, for `id`. */
  private def removalStatements(id: AggregateId): Seq[String] = {
    val chapter = new String(Files.readAllBytes(Paths.get("docs", "book", "18-production.md")), UTF_8).replace("\r\n", "\n")
    val section = chapter.substring(chapter.indexOf("### Removing an aggregate by hand"))
    val block = section.substring(section.indexOf("```sql\n") + 7, section.indexOf("\n```", section.indexOf("```sql\n")))
    block.split("\n").map(_.replaceAll("--.*$", "").trim).filter(_.nonEmpty).mkString(" ").split(";").map(_.trim).filter(_.nonEmpty)
      .map(_.replace("4711", id.asLong.toString)).toSeq
  }

  Feature("Aggregates that cannot be restored (CR-4)") {

    Scenario("the owner and the build record it with its cause; the build does not switch, and the owner says why at once") {
      TestDatabase.inSchema("restore_failures_recorded") {
        val installation = new Installation
        import installation._
        val (node, _, bad) = blockedBuild()

        Then("the build in progress has caught up and is blocked by the aggregate, its failure naming the cause")
        blockedBy(node.supervisor, bad)
        val failure = buildStatus(node.supervisor).failures.head
        failure.error must include("the aggregate cannot be restored")
        failure.error must include("a poisoned line")
        builds(node.supervisor).active.map(_.number) mustBe Some(1)

        And("the build recorded it: the event it failed at, the cause, where")
        val build = registry.projection("rf-notes").get.building.get
        val recorded = eventStore.restoreFailures(RestoreFailureFilter(projection = Some("rf-notes")))
        recorded.map(r => (r.aggregateId, r.version, r.eventId, r.errorClass, r.source)) mustBe
          Seq((bad, Some(AggregateVersion(2)), Some(eventIdOf(bad, 2)), classOf[IllegalStateException].getName, RestoreFailureSource.Build("rf-notes", 2, build.subscriber)))
        recorded.head.error must include("a poisoned line")
        recorded.head.aggregateType mustBe AggregateType.of[BuildNote]

        And("so does its owner, when asked for it")
        node.notes.askFor[Any](GetAggregate(bad)) mustBe a[Failure[_]]
        awaitAssert(eventStore.restoreFailures().map(r => (r.aggregateId, r.source)) must contain ((bad, RestoreFailureSource.Owner)), 5.seconds)
        eventStore.restoreFailuresCount() mustBe 1L

        When("the failure is retried, the build asks the owner for the aggregate")
        node.supervisor.askFor[FailedSubscriptionsHandled](ToBuildInProgress(RetryFailedSubscription(bad))).count mustBe 1

        Then("the owner answers that it cannot restore it, and the failure says so at once, not after the gap's time")
        awaitAssert({
          val status = buildStatus(node.supervisor)
          status.retrying mustBe empty
          val error = status.failures.map(_.error).mkString
          error must include("Applying " + classOf[BuildNoteLineAdded].getName + " at version 2 of aggregate " + bad.asLong)
          error must include("a poisoned line")
        }, 15.seconds, 200.millis)
        builds(node.supervisor).active.map(_.number) mustBe Some(1)
        node.stop()
      }
    }

    Scenario("after the documented removal and a retry of all failures, the build switches") {
      TestDatabase.inSchema("restore_failures_removed") {
        val installation = new Installation
        import installation._
        val (node, good, bad) = blockedBuild()
        blockedBy(node.supervisor, bad)
        node.notes.askFor[Any](GetAggregate(bad)) mustBe a[Failure[_]]

        Given("the statements of chapter 18 name every table of the library that holds an aggregate's rows")
        val statements = removalStatements(bad)
        val tablesWithAggregates = DB.readOnly { implicit session =>
          sql"""SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema = current_schema() AND column_name = 'aggregate_id'"""
            .map(_.string(1)).list()
        } ++ Seq("aggregates", "noop_events")
        tablesWithAggregates.filterNot(table => statements.exists(_.startsWith("DELETE FROM " + table + " "))) mustBe empty

        When("an operator removes the aggregate with them, in one transaction")
        DB.localTx { implicit session =>
          statements.filterNot(s => s == "BEGIN" || s == "COMMIT").foreach { statement =>
            if (statement.startsWith("SELECT")) SQL(statement).map(_.long(1)).list() mustBe empty
            else SQL(statement).update()
          }
        }
        tablesWithAggregates.filterNot(_ == "noop_events").map(table => table -> DB.readOnly { implicit session =>
          val column = if (table == "aggregates") "id" else "aggregate_id"
          SQL(s"SELECT count(*) FROM $table WHERE $column = ?").bind(bad.asLong).map(_.long(1)).single().get
        }).filter(_._2 > 0) mustBe empty
        eventStore.restoreFailures() mustBe empty

        And("retries every failure of the build")
        node.supervisor.askFor[FailedSubscriptionsRetried](ToBuildInProgress(RetryAllFailedSubscriptions)).count mustBe 1

        Then("the failure clears, as for an aggregate deleted for good, and the build switches without a restart")
        switchedTo(node.supervisor, 2)
        linesOf(node.supervisor, good) mustBe Some(NoteLines("good", Vector("X")))
        linesOf(node.supervisor, bad) mustBe None
        node.stop()
      }
    }

    Scenario("after a code fix and a retry, the build switches with the aggregate's data, and the records go") {
      TestDatabase.inSchema("restore_failures_fixed") {
        val installation = new Installation
        import installation._
        val (node, _, bad) = blockedBuild()
        blockedBy(node.supervisor, bad)
        node.notes.askFor[Any](GetAggregate(bad)) mustBe a[Failure[_]]
        RestoreCheck.run(eventStore, Seq(new PoisonNoteContext)).failed mustBe Seq(bad)
        awaitAssert(eventStore.restoreFailures().map(_.source.getClass.getSimpleName).toSet.size mustBe 3, 5.seconds) // owner, build, check

        When("the handler takes the line again (a deploy), and the failures are retried")
        PoisonedNotes.fixed = true
        node.notes ! io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate(bad) // as a deploy of the fix starts new owners
        node.supervisor.askFor[FailedSubscriptionsRetried](ToBuildInProgress(RetryAllFailedSubscriptions)).count mustBe 1

        Then("the build applies the aggregate and switches")
        switchedTo(node.supervisor, 2)
        awaitAssert(linesOf(node.supervisor, bad) mustBe Some(NoteLines("bad", Vector("POISON", "AFTER"))), 10.seconds)

        And("the records of the owner and of the build are gone, and the check's goes at its next run")
        awaitAssert(eventStore.restoreFailures().map(_.source) mustBe Seq(RestoreFailureSource.Check), 15.seconds)
        RestoreCheck.run(eventStore, Seq(new PoisonNoteContext)).failedTotal mustBe 0L
        eventStore.restoreFailures() mustBe empty
        node.stop()
      }
    }

    Scenario("a whole aggregate skipped in the build after it caught up lets it switch, and its record says skipped") {
      TestDatabase.inSchema("restore_failures_skipped") {
        val installation = new Installation
        import installation._
        val (node, good, bad) = blockedBuild()
        blockedBy(node.supervisor, bad)

        node.supervisor.askFor[AggregateSkipped](ToBuildInProgress(SkipAggregate(bad))) mustBe AggregateSkipped(bad, AggregateVersion(3), 1)

        switchedTo(node.supervisor, 2)
        linesOf(node.supervisor, good) mustBe Some(NoteLines("good", Vector("X")))
        linesOf(node.supervisor, bad) mustBe None
        val skipped: Seq[RestoreFailure] = eventStore.restoreFailures(RestoreFailureFilter(projection = Some("rf-notes")))
        skipped.map(r => (r.aggregateId, r.skipped.isDefined)) mustBe Seq((bad, true))
        node.stop()
      }
    }

    Scenario("a whole aggregate skipped while the bulk pass runs lets the build switch once built") {
      TestDatabase.inSchema("restore_failures_skipped_bulk") {
        val installation = new Installation
        import installation._
        val (node, _, bad) = blockedBuild(fillers = 40, settings = BuilderSettings(pageSize = 1, chunkEvents = 1), buildDelayMillis = 100)

        Given("the bulk pass met the aggregate, and goes on")
        awaitAssert(eventStore.restoreFailures().map(_.aggregateId) mustBe Seq(bad), 30.seconds, 100.millis)
        node.supervisor.askFor[BuildStatus](GetBuildStatus).phase mustBe "Bulk"

        When("an operator skips it in the build in progress")
        node.supervisor.askFor[AggregateSkipped](ToBuildInProgress(SkipAggregate(bad))).upTo mustBe AggregateVersion(3)
        node.supervisor.askFor[BuildStatus](GetBuildStatus).phase mustBe "Bulk"

        Then("the build switches once built, without the aggregate, and its record says skipped")
        switchedTo(node.supervisor, 2)
        linesOf(node.supervisor, bad) mustBe None
        eventStore.restoreFailures(RestoreFailureFilter(projection = Some("rf-notes"))).map(_.skipped.isDefined) mustBe Seq(true)
        node.stop()
      }
    }
  }
}

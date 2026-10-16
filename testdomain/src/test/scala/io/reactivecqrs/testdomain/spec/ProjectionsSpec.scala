package io.reactivecqrs.testdomain.spec

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.documentstore.{Document, DocumentStore, NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.SubscribableProjectionActor.{SubscribedForProjectionUpdates, SubscriptionUpdated}
import io.reactivecqrs.core.projection._
import io.reactivecqrs.core.rebuild.ProjectionBuilder.BuilderSettings
import io.reactivecqrs.core.rebuild.ProjectionSupervisor._
import io.reactivecqrs.core.rebuild._
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.core.util.BlockingDispatcher
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe
import scalikejdbc._

import scala.concurrent.duration._

case class GetNoteLines(id: AggregateId)
case object GetAllNoteLines
case class SubscribeToNotes(subscriptionId: String, listener: ActorRef)

/** The lines of each note, from its state; `shout` (a later version's code) keeps them in capitals. Pushes each update to
  * its subscribers, unless it is being rebuilt. */
class ManagedNotes(val subscriptionsState: SubscriptionsState, store: DocumentStore[NoteLines], shout: Boolean) extends SubscribableProjectionActor {
  protected val listeners = List(AggregateListener[BuildNote] { (id, _, created, note) => implicit session =>
    note match {
      case Some(n) =>
        val lines = NoteLines(n.title, if (shout) n.lines.map(_.toUpperCase) else n.lines)
        if (created) store.insertDocument(0, id.asLong, lines) else store.overwriteDocument(id.asLong, lines)
        if (!rebuilding) sendUpdate[NoteLines](lines)
      case None => store.removeDocument(id.asLong)
    }
  })
  override protected def receiveQuery: Receive = {
    case GetNoteLines(id) => sender() ! store.getDocument(id.asLong).map(_.document)
    case GetAllNoteLines => sender() ! store.findAll().map { case (id, doc) => AggregateId(id) -> doc.document }
  }
  override protected def receiveSubscriptionRequest: Receive = {
    case SubscribeToNotes(subscriptionId, listener) => handleSubscribe[NoteLines, NoteLines](subscriptionId, listener, lines => Some(lines))
  }
  override protected def onClearProjectionData(): Unit = ()
  override protected val projectionName: String = "ManagedNotes"
  override protected val version: Int = 1
}

/** Every event of each note, from the events only: its builds take no state, so a context version leaves it alone. */
class ManagedNoteEvents(val subscriptionsState: SubscriptionsState, store: DocumentStore[NoteLog]) extends ProjectionActor {
  protected val listeners = List(EventsListener[BuildNote] { (id, events) => implicit session =>
    store.updateDocument(0, id.asLong, existing => Document(NoteLog(existing.map(_.document.entries).getOrElse(Vector()) ++ events.map(_.version.asInt.toString))))
  })
  override protected def receiveQuery: Receive = PartialFunction.empty
  override protected def onClearProjectionData(): Unit = ()
  override protected val projectionName: String = "ManagedNoteEvents"
  override protected val version: Int = 1
}

/** The same projection, of a class of its own: the class names the subscriptions of a projection 0.12 built. */
class LegacyNotes(subscriptionsState: SubscriptionsState, store: DocumentStore[NoteLines]) extends ManagedNotes(subscriptionsState, store, shout = false)

class BuildNoteContextV2 extends BuildNoteContext {
  override val version: Int = 2
}

// D57, D70, D63: managed projections are built in tables of their own and switch over in one update.
class ProjectionsSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val subscriptions = new PostgresSubscriptionsState(typesNamesState).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
  private val registry = new PostgresProjectionRegistry().initSchema()
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val userId = UserId(1)
  private var systems = 0

  /** One system on a node: an event bus, a command bus of notes and the managed projections `start` makes. */
  private class Node(context: AggregateContext[BuildNote] = new BuildNoteContext, settings: BuilderSettings = BuilderSettings(pageSize = 20, chunkEvents = 10))
                    (start: Projections => Seq[ActorRef]) {
    systems += 1
    val bus: ActorRef = system.actorOf(Props(new EventsBusActor()), "m4p-bus-" + systems)
    val notes: ActorRef = system.actorOf(AggregateCommandBusActor(context, uidGenerator, eventStore, responses, bus, eventsReplayMode = false))
    val projections = new ProjectionsOf(new Projections(system, ProjectionsSettings(eventStore, subscriptions, registry, bus, Seq(context), settings,
      restartAfter = 1.second)), systems)
    val supervisors: Seq[ActorRef] = start(projections.runtime)
    bus ! RegisterSubscribers(supervisors)

    def send(command: AnyRef): SuccessResponse = notes.askActor[CustomCommandResponse[_]](command)(10.seconds) match {
      case success: SuccessResponse => success
      case other => fail("command failed: " + other)
    }
    def create(title: String, lines: String*): AggregateId = {
      val id = send(CreateBuildNote(userId, title)).aggregateId
      lines.zipWithIndex.foreach { case (line, i) => send(AddBuildNoteLine(userId, id, AggregateVersion(i + 1), line)) }
      id
    }

    def stop(): Unit = (supervisors ++ Seq(bus, notes)).foreach(system.stop)
  }

  /** The runtime with names unique to its node, as each node of the test runs in one actor system. */
  private class ProjectionsOf(val runtime: Projections, node: Int)

  private def notesProjection(projections: Projections, name: String, version: Int, table: String, whileBuilding: WhileBuilding = ServeLive): ActorRef =
    projections.start[ManagedNotes](name, version, name + "-" + systems, whileBuilding) { build =>
      new ManagedNotes(subscriptions, new PostgresDocumentStore[NoteLines](build.documentStore(table), mpjsons, new NoopDocumentStoreCache), shout = version > 1)
    }

  private def builds(supervisor: ActorRef): Builds = supervisor.askActor[Builds](GetBuilds)(5.seconds)

  private def switchedTo(supervisor: ActorRef, build: Int): Unit =
    awaitAssert({
      val now = builds(supervisor)
      (now.active.map(_.number), now.building) mustBe ((Some(build), None))
    }, 60.seconds, 100.millis)

  private def linesOf(supervisor: ActorRef, id: AggregateId): Option[NoteLines] = supervisor.askActor[Option[NoteLines]](GetNoteLines(id))(5.seconds)

  private def tableExists(table: String): Boolean = DB.readOnly(implicit s => sql"SELECT to_regclass($table) IS NOT NULL".map(_.boolean(1)).single().get)

  Feature("A managed projection (D57, D70)") {

    Scenario("a new projection is built from the store first, serving meanwhile, and is live afterwards") {
      Given("notes stored before the projection exists")
      var supervisor: ActorRef = null
      val node = new Node()(projections => { supervisor = notesProjection(projections, "m4p-new", 1, "m4p_new"); Seq(supervisor) })
      val (a, b) = (node.create("a", "x"), node.create("b", "y", "z"))

      Then("its first build gets them and switches; the registry names it, its tables have the build's suffix")
      switchedTo(supervisor, 1)
      awaitAssert(linesOf(supervisor, b) mustBe Some(NoteLines("b", Vector("y", "z"))), 10.seconds)
      linesOf(supervisor, a) mustBe Some(NoteLines("a", Vector("x")))
      registry.projection("m4p-new").get.active.map(b => (b.suffix, b.subscriber)) mustBe Some(("_v1", "projection:m4p-new_v1"))
      tableExists("projection_m4p_new_v1") mustBe true

      And("events after the build reach it live")
      node.send(AddBuildNoteLine(userId, a, AggregateVersion(2), "later"))
      awaitAssert(linesOf(supervisor, a) mustBe Some(NoteLines("a", Vector("x", "later"))), 10.seconds)
      node.stop()
    }

    Scenario("a new version is built in its own tables while the active one serves, then takes over in one update and the old tables go") {
      Given("version 1, built and live")
      var v1: ActorRef = null
      val first = new Node()(projections => { v1 = notesProjection(projections, "m4p-bump", 1, "m4p_bump"); Seq(v1) })
      val note = first.create("note", "one")
      switchedTo(v1, 1)
      awaitAssert(linesOf(v1, note) mustBe Some(NoteLines("note", Vector("one"))), 10.seconds)
      val oldSubscriber = registry.projection("m4p-bump").get.active.get.subscriber
      first.stop()

      When("the code at version 2 starts, building slowly")
      var v2: ActorRef = null
      val second = new Node(settings = BuilderSettings(pageSize = 1, chunkEvents = 1))(projections => {
        v2 = notesProjection(projections, "m4p-bump", 2, "m4p_bump"); Seq(v2)
      })
      awaitAssert(builds(v2).building.map(_.number) mustBe Some(2), 10.seconds)

      Then("version 1's tables answer meanwhile, kept live by the new code (ServeLive)")
      linesOf(v2, note) mustBe Some(NoteLines("note", Vector("one")))

      And("version 2 replaces it once caught up: its own data, the old tables and subscriptions dropped")
      switchedTo(v2, 2)
      awaitAssert(linesOf(v2, note) mustBe Some(NoteLines("note", Vector("ONE"))), 10.seconds)
      awaitAssert(tableExists("projection_m4p_bump_v1") mustBe false, 10.seconds)
      tableExists("projection_m4p_bump_v2") mustBe true
      subscriptions.hasSubscriptions(oldSubscriber) mustBe false
      second.send(AddBuildNoteLine(userId, note, AggregateVersion(2), "two"))
      awaitAssert(linesOf(v2, note) mustBe Some(NoteLines("note", Vector("ONE", "TWO"))), 10.seconds)
      second.stop()
    }

    Scenario("a build continues after a stop from the position it saved") {
      Given("many notes, and a build stopped midway")
      var built: ActorRef = null
      val slow = BuilderSettings(pageSize = 2, chunkEvents = 2)
      val first = new Node(settings = slow)(projections => { built = notesProjection(projections, "m4p-resume", 1, "m4p_resume"); Seq(built) })
      val ids = (1 to 20).map(n => first.create("note " + n, "line"))
      awaitAssert(registry.projection("m4p-resume").get.building.exists(_.position.position > 0) mustBe true, 30.seconds, 20.millis)
      first.stop()
      val stoppedAt = registry.projection("m4p-resume").get.building.get.position

      When("the projection starts again")
      var again: ActorRef = null
      val second = new Node()(projections => { again = notesProjection(projections, "m4p-resume", 1, "m4p_resume"); Seq(again) })

      Then("the same build goes on from where it was, and ends with every note")
      stoppedAt.bulkDone mustBe false
      switchedTo(again, 1)
      ids.foreach(id => linesOf(again, id).map(_.lines) mustBe Some(Vector("line")))
      second.stop()
    }

    Scenario("a projection 0.12 built is adopted with its tables and subscription rows, not built again (D81)") {
      Given("a projection run unmanaged, as by 0.12, with its version recorded by the 0.12 rebuild")
      val legacyStore = new PostgresDocumentStore[NoteLines]("m4p_legacy", mpjsons, new NoopDocumentStoreCache)
      val unmanaged = system.actorOf(Props(new LegacyNotes(subscriptions, legacyStore)).withDispatcher(BlockingDispatcher.id))
      val old = new Node()(_ => Seq(unmanaged))
      val note = old.create("legacy", "kept")
      awaitAssert(legacyStore.getDocument(note.asLong).map(_.document) mustBe Some(NoteLines("legacy", Vector("kept"))), 10.seconds)
      old.stop()
      system.stop(unmanaged)
      new PostgresVersionsState().initSchema().saveVersionForProjection("m4p-legacy", 1)

      When("it starts managed, at the same version")
      var adopted: ActorRef = null
      val node = new Node()(projections => {
        adopted = projections.start[LegacyNotes]("m4p-legacy", 1, "m4p-legacy-" + systems) { build =>
          new LegacyNotes(subscriptions, new PostgresDocumentStore[NoteLines](build.documentStore("m4p_legacy"), mpjsons, new NoopDocumentStoreCache))
        }
        Seq(adopted)
      })

      Then("build 0 serves its table and goes on from its versions; no build runs")
      awaitAssert(builds(adopted).active.map(b => (b.number, b.suffix, b.subscriber)) mustBe Some((0, "", classOf[LegacyNotes].getName)), 10.seconds)
      builds(adopted).building mustBe None
      linesOf(adopted, note) mustBe Some(NoteLines("legacy", Vector("kept")))
      node.send(AddBuildNoteLine(userId, note, AggregateVersion(2), "more"))
      awaitAssert(linesOf(adopted, note) mustBe Some(NoteLines("legacy", Vector("kept", "more"))), 10.seconds)
      node.stop()
    }

    Scenario("push subscriptions move to the new build at the switch") {
      Given("a subscriber of a projection that is rebuilt on request")
      var supervisor: ActorRef = null
      val node = new Node()(projections => { supervisor = notesProjection(projections, "m4p-push", 1, "m4p_push"); Seq(supervisor) })
      val note = node.create("pushed", "a")
      switchedTo(supervisor, 1)
      val listener = TestProbe()
      supervisor ! SubscribeToNotes("subscription", listener.ref)
      listener.expectMsg(5.seconds, SubscribedForProjectionUpdates("subscription"))

      supervisor.askActor[RebuildStarted](RebuildProjection)(5.seconds) mustBe RebuildStarted(2)
      switchedTo(supervisor, 2)

      Then("an update after the switch reaches it from the new build")
      node.send(AddBuildNoteLine(userId, note, AggregateVersion(2), "b"))
      listener.fishForSpecificMessage(10.seconds) { case SubscriptionUpdated("subscription", NoteLines("pushed", Vector("a", "b"))) => () }
      node.stop()
    }

    Scenario("a context version bump rebuilds the projections that receive its states, and only them (D63)") {
      Given("a state projection and an events projection of notes, built")
      def started(projections: Projections): Seq[ActorRef] = Seq(
        notesProjection(projections, "m4p-states", 1, "m4p_states"),
        projections.start[ManagedNoteEvents]("m4p-events", 1, "m4p-events-" + systems) { build =>
          new ManagedNoteEvents(subscriptions, new PostgresDocumentStore[NoteLog](build.documentStore("m4p_events"), mpjsons, new NoopDocumentStoreCache))
        })
      var first: Seq[ActorRef] = Nil
      val one = new Node()(projections => { first = started(projections); first })
      one.create("context", "line")
      first.foreach(switchedTo(_, 1))
      one.stop()

      When("the notes' context is at version 2")
      var second: Seq[ActorRef] = Nil
      val two = new Node(new BuildNoteContextV2)(projections => { second = started(projections); second })

      Then("the state projection is built again at its version; the events projection keeps its build")
      switchedTo(second.head, 2)
      registry.projection("m4p-states").get.active.map(b => (b.version, b.contexts.get(classOf[BuildNote].getName.replace('$', '.')))) mustBe Some((1, Some(2)))
      builds(second(1)).active.map(_.number) mustBe Some(1)
      two.stop()
    }

    Scenario("retiring a projection removed from the code drops its builds and failure records (F94)") {
      var supervisor: ActorRef = null
      val node = new Node()(projections => { supervisor = notesProjection(projections, "m4p-retired", 1, "m4p_retired"); Seq(supervisor) })
      val note = node.create("retired", "a")
      switchedTo(supervisor, 1)
      val subscriber = registry.projection("m4p-retired").get.active.get.subscriber
      subscriptions.recordFailure(subscriber, SubscriptionFailure(SubscriptionType.AGGREGATES, AggregateType.of[BuildNote], note, AggregateVersion(2), "failed",
        java.time.Instant.now))
      node.stop()

      node.projections.runtime.retire("m4p-retired")

      registry.projection("m4p-retired") mustBe None
      tableExists("projection_m4p_retired_v1") mustBe false
      subscriptions.failures(subscriber) mustBe empty
      subscriptions.hasSubscriptions(subscriber) mustBe false
    }
  }
}

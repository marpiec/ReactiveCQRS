package io.reactivecqrs.testdomain.spec

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.cluster.Singletons
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.documentstore.{Document, DocumentStore, NoopDocumentStoreCache, PostgresDocumentStore, UniqueTextIndex}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.{AggregateScope, EventStoreState, PostgresEventStoreState, StoredAggregate}
import io.reactivecqrs.core.projection.SubscribableProjectionActor.{SubscribedForProjectionUpdates, SubscriptionUpdated}
import io.reactivecqrs.core.projection._
import io.reactivecqrs.core.rebuild.ProjectionBuilder.{BuildStatus, BuilderSettings, GetBuildStatus}
import io.reactivecqrs.core.rebuild.ProjectionSupervisor._
import io.reactivecqrs.core.rebuild._
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.core.util.BlockingDispatcher
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase, TestIds}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe
import scalikejdbc._

import scala.concurrent.duration._

case class GetNoteLines(id: AggregateId)
case object GetInstanceName
case object GetAllNoteLines
case class SubscribeToNotes(subscriptionId: String, listener: ActorRef)

/** The lines of each note, from its state; `shout` (a later version's code) keeps them in capitals. Pushes each update to
  * its subscribers, unless it is being rebuilt. */
// `buildDelayMillis`: each note takes that long while the projection is built, so a build can be watched at work.
class ManagedNotes(val subscriptionsState: SubscriptionsState, store: DocumentStore[NoteLines], shout: Boolean, buildDelayMillis: Long = 0)
  extends SubscribableProjectionActor {
  protected val listeners = List(AggregateListener[BuildNote] { (id, _, created, note) => implicit session =>
    note match {
      case Some(n) =>
        if (rebuilding && buildDelayMillis > 0) Thread.sleep(buildDelayMillis)
        val lines = NoteLines(n.title, if (shout) n.lines.map(_.toUpperCase) else n.lines)
        if (created) store.insertDocument(n.space, id.asLong, lines) else store.overwriteDocument(id.asLong, lines)
        if (!rebuilding) sendUpdate[NoteLines](lines)
      case None => store.removeDocument(id.asLong)
    }
  })

  // D63: what a targeted rebuild clears before the events come again.
  override protected def clearForRebuild(target: RebuildTarget)(implicit session: DBSession): Unit = target match {
    case RebuildTarget.OneAggregate(id) => store.removeDocument(id.asLong)
    case RebuildTarget.OneSpace(space) => store.clearSpace(space.asLong)
    case RebuildTarget.OneType(_) => store.clearAllData()
  }
  override protected def receiveQuery: Receive = {
    case GetNoteLines(id) => sender() ! store.getDocument(id.asLong).map(_.document)
    case GetInstanceName => sender() ! self.path.name
    case GetAllNoteLines => sender() ! store.findAll().map { case (id, doc) => AggregateId(id) -> doc.document }
  }
  override protected def receiveSubscriptionRequest: Receive = {
    case SubscribeToNotes(subscriptionId, listener) => handleSubscribe[NoteLines, NoteLines](subscriptionId, listener, lines => Some(lines))
  }
  override protected val projectionName: String = "ManagedNotes"
}

/** Every event of each note, from the events only: its builds take no state, so a context version leaves it alone. */
class ManagedNoteEvents(val subscriptionsState: SubscriptionsState, store: DocumentStore[NoteLog]) extends ProjectionActor {
  protected val listeners = List(EventsListener[BuildNote] { (id, events) => implicit session =>
    store.updateDocument(0, id.asLong, existing => Document(NoteLog(existing.map(_.document.entries).getOrElse(Vector()) ++ events.map(_.version.asInt.toString))))
  })
  override protected def receiveQuery: Receive = PartialFunction.empty
  override protected val projectionName: String = "ManagedNoteEvents"
}

/** The same projection, of a class of its own: the class names the subscriptions of a projection 0.12 built. */
class LegacyNotes(subscriptionsState: SubscriptionsState, store: DocumentStore[NoteLines]) extends ManagedNotes(subscriptionsState, store, shout = false)

/** The store, but a listing of space 77 in pages of 1 000 (a targeted rebuild's) waits after its last page, the empty one,
  * while `armed`: it counts `listed` down, then waits for `resume`. */
class PausingListing(mpjsons: MPJsons, typesNamesState: PostgresTypesNamesState) extends PostgresEventStoreState(mpjsons, typesNamesState) {
   var armed = false
  val listed = new java.util.concurrent.CountDownLatch(1)
  val resume = new java.util.concurrent.CountDownLatch(1)
  override def aggregatesOf(types: Seq[AggregateType], after: AggregateId, limit: Int, scope: AggregateScope): Seq[StoredAggregate] = {
    val page = super.aggregatesOf(types, after, limit, scope)
    if (armed && limit == 1000 && page.isEmpty && scope == AggregateScope.InSpace(io.reactivecqrs.api.id.SpaceId(77))) {
      armed = false
      listed.countDown()
      resume.await(10, java.util.concurrent.TimeUnit.SECONDS)
    }
    page
  }
}

case class MemoText(text: String)
case class GetMemoText(id: AggregateId)

/** Each memo's text, from its events; a memo deleted for good loses its document. While it is built, each memo takes
  * `buildDelayMillis`, so a build can be seen at work. */
class MemoTexts(val subscriptionsState: SubscriptionsState, store: DocumentStore[MemoText], buildDelayMillis: Long = 0) extends ProjectionActor {
  protected val listeners = List(EventsListener[Memo] { (id, events) => implicit session =>
    if (rebuilding && buildDelayMillis > 0) Thread.sleep(buildDelayMillis)
    events.foreach(_.event match {
      case MemoWritten(text) => store.insertDocument(0, id.asLong, MemoText(text))
      case _: MemoErased => store.removeDocument(id.asLong)
      case _ => ()
    })
  })
  override protected def receiveQuery: Receive = {
    case GetMemoText(id) => sender() ! store.getDocument(id.asLong).map(_.document)
  }
  override protected val projectionName: String = "MemoTexts"
}

case class GetNoteLog(id: AggregateId)

/** Every version of each note; a line "stop" makes the instance stop itself, as a fatal error would. */
class StoppingNotes(val subscriptionsState: SubscriptionsState, store: DocumentStore[NoteLog], stopped: java.util.concurrent.CountDownLatch) extends ProjectionActor {
  protected val listeners = List(EventsListener[BuildNote] { (id, events) => implicit session =>
    store.updateDocument(0, id.asLong, existing => Document(NoteLog(existing.map(_.document.entries).getOrElse(Vector()) ++ events.map(_.version.asInt.toString))))
    if (events.exists(_.event == BuildNoteLineAdded("stop"))) context.stop(self)
  })
  override def postStop(): Unit = {
    super.postStop()
    stopped.countDown()
  }
  override protected def receiveQuery: Receive = {
    case GetNoteLog(id) => sender() ! store.getDocument(id.asLong).map(_.document)
  }
  override protected val projectionName: String = "StoppingNotes"
}

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
  private class Node(context: AggregateContext[BuildNote] = new BuildNoteContext, settings: BuilderSettings = BuilderSettings(pageSize = 20, chunkEvents = 10),
                     registryOf: ProjectionRegistry = registry, storeOf: EventStoreState = eventStore)(start: Projections => Seq[ActorRef]) {
    systems += 1
    val bus: ActorRef = system.actorOf(Props(new EventsBusActor()), "m4p-bus-" + systems)
    val notes: ActorRef = system.actorOf(AggregateCommandBusActor(context, uidGenerator, eventStore, responses, bus))
    val projections = new ProjectionsOf(new Projections(system, ProjectionsSettings(storeOf, subscriptions, registryOf, Seq(context), settings,
      restartAfter = 1.second)), systems)
    val supervisors: Seq[ActorRef] = start(projections.runtime)
    bus ! RegisterSubscribers(supervisors)

    def send(command: AnyRef): SuccessResponse = notes.askFor[CustomCommandResponse[_]](command) match {
      case success: SuccessResponse => success
      case other => fail("command failed: " + other)
    }
    def create(title: String, lines: String*): AggregateId = createIn(0, title, lines: _*)
    def createIn(space: Long, title: String, lines: String*): AggregateId = {
      val id = send(CreateBuildNote(userId, title, space)).aggregateId
      lines.zipWithIndex.foreach { case (line, i) => send(AddBuildNoteLine(userId, id, AggregateVersion(i + 1), line)) }
      id
    }

    def stop(): Unit = {
      supervisors.foreach(Singletons.stop(system, _)) // in a cluster, the singleton too
      Seq(bus, notes).foreach(system.stop)
    }
  }

  /** The runtime with names unique to its node, as each node of the test runs in one actor system. */
  private class ProjectionsOf(val runtime: Projections, node: Int)

  private def notesProjection(projections: Projections, name: String, version: Int, table: String, whileBuilding: WhileBuilding = ServeLive,
                              buildDelayMillis: Long = 0): ActorRef =
    projections.start[ManagedNotes](name, version, name + "-" + systems, whileBuilding) { build =>
      new ManagedNotes(subscriptions, new PostgresDocumentStore[NoteLines](build.documentStore(table), mpjsons, new NoopDocumentStoreCache), shout = version > 1,
        buildDelayMillis)
    }

  private def builds(supervisor: ActorRef): Builds = supervisor.askFor[Builds](GetBuilds, 5.seconds)

  private def switchedTo(supervisor: ActorRef, build: Int): Unit =
    awaitAssert({
      val now = builds(supervisor)
      (now.active.map(_.number), now.building) mustBe ((Some(build), None))
    }, 60.seconds, 100.millis)

  private def linesOf(supervisor: ActorRef, id: AggregateId): Option[NoteLines] = supervisor.askFor[Option[NoteLines]](GetNoteLines(id), 5.seconds)

  private def tableExists(table: String): Boolean = DB.readOnly(implicit s => sql"SELECT to_regclass($table) IS NOT NULL".map(_.boolean(1)).single().get)

  Feature("A managed projection (D57, D70)") {

    Scenario("F258 the instances of two projections have names of their own, which name the projection") {
      var supervisors = Seq.empty[ActorRef]
      val node = new Node()(projections => {
        supervisors = Seq(notesProjection(projections, "m4p-named-a", 1, "m4p_named_a"), notesProjection(projections, "m4p-named-b", 1, "m4p_named_b"))
        supervisors
      })
      supervisors.foreach(switchedTo(_, 1))

      val names = supervisors.map(_.askFor[String](GetInstanceName, 5.seconds))
      names.distinct.size mustBe 2
      names.head must startWith("m4p-named-a-")
      node.stop()
    }

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
      // Enough notes that version 2's build, 30 ms each, takes seconds: on a cluster the first question through the
      // singleton's proxy waits about a second for it to be found (found on a cluster of one).
      (1 to 100).foreach(i => first.create("filler " + i))
      switchedTo(v1, 1)
      awaitAssert(linesOf(v1, note) mustBe Some(NoteLines("note", Vector("one"))), 10.seconds)
      val oldSubscriber = registry.projection("m4p-bump").get.active.get.subscriber
      first.stop()

      When("the code at version 2 starts, building slowly")
      var v2: ActorRef = null
      val second = new Node(settings = BuilderSettings(pageSize = 1, chunkEvents = 1))(projections => {
        v2 = notesProjection(projections, "m4p-bump", 2, "m4p_bump", buildDelayMillis = 30); Seq(v2)
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

    Scenario("a registry that fails at the switch: the switch is tried again, and the projection goes on taking events (AR7)") {
      Given("a registry whose first switch fails")
      val failing = new PostgresProjectionRegistry {
        private val failures = new java.util.concurrent.atomic.AtomicInteger(1)
        override def switch(name: String, build: Int): Boolean =
          if (failures.getAndDecrement() > 0) throw new IllegalStateException("database unavailable") else super.switch(name, build)
      }
      var supervisor: ActorRef = null
      val node = new Node(registryOf = failing)(projections => { supervisor = notesProjection(projections, "m4p-flaky", 1, "m4p_flaky"); Seq(supervisor) })
      val note = node.create("note", "one")

      Then("it switches at a later try, and events after it reach the projection")
      switchedTo(supervisor, 1)
      node.send(AddBuildNoteLine(userId, note, AggregateVersion(2), "two"))
      awaitAssert(linesOf(supervisor, note) mustBe Some(NoteLines("note", Vector("one", "two"))), 10.seconds)
      node.stop()
    }

    Scenario("a build whose instance cannot start leaves the active build serving and taking events, and starts again (F110)") {
      Given("version 1, built and live")
      var v1: ActorRef = null
      val first = new Node()(projections => { v1 = notesProjection(projections, "m4p-broken", 1, "m4p_broken"); Seq(v1) })
      val note = first.create("note", "one")
      switchedTo(v1, 1)
      first.stop()

      When("version 2 starts, and its build's instance fails to start until its store is repaired")
      val repaired = new java.util.concurrent.atomic.AtomicBoolean(false)
      val attempts = new java.util.concurrent.atomic.AtomicInteger(0)
      var v2: ActorRef = null
      val second = new Node()(projections => {
        v2 = projections.start[ManagedNotes]("m4p-broken", 2, "m4p-broken-" + systems) { build =>
          if (build.number > 1 && attempts.incrementAndGet() > 0 && !repaired.get()) throw new IllegalStateException("its store cannot be made")
          new ManagedNotes(subscriptions, new PostgresDocumentStore[NoteLines](build.documentStore("m4p_broken"), mpjsons, new NoopDocumentStoreCache), shout = build.version > 1)
        }
        Seq(v2)
      })

      Then("version 1 serves, and takes events, while the build fails and is tried again")
      awaitAssert(attempts.get() must be >= 2, 10.seconds, 10.millis)
      second.send(AddBuildNoteLine(userId, note, AggregateVersion(2), "two"))
      awaitAssert(linesOf(v2, note) mustBe Some(NoteLines("note", Vector("one", "two"))), 10.seconds, 20.millis)

      And("version 2 is built once its store is repaired, and takes over")
      repaired.set(true)
      switchedTo(v2, 2)
      awaitAssert(linesOf(v2, note) mustBe Some(NoteLines("note", Vector("ONE", "TWO"))), 10.seconds)
      second.stop()
    }

    Scenario("a new version that declares an index the active build's documents violate leaves the active build serving with its own indexes (F189)") {
      Given("version 1, built and live, with two notes of one title")
      var v1: ActorRef = null
      val first = new Node()(projections => { v1 = notesProjection(projections, "m4p-index", 1, "m4p_index"); Seq(v1) })
      val (a, b) = (first.create("same", "one"), first.create("same", "other"))
      switchedTo(v1, 1)
      awaitAssert(linesOf(v1, b) mustBe Some(NoteLines("same", Vector("other"))), 10.seconds)
      first.stop()

      When("version 2 starts, its store declaring titles unique")
      var v2: ActorRef = null
      val second = new Node()(projections => {
        v2 = projections.start[ManagedNotes]("m4p-index", 2, "m4p-index-" + systems) { build =>
          new ManagedNotes(subscriptions, new PostgresDocumentStore[NoteLines](build.documentStore("m4p_index"), mpjsons,
            new NoopDocumentStoreCache, Seq(UniqueTextIndex(1, Seq("title")))), shout = build.version > 1)
        }
        Seq(v2)
      })

      Then("the supervisor answers, version 1 serves and takes events, and its table has not got version 2's index")
      awaitAssert(builds(v2).building.map(_.number) mustBe Some(2), 10.seconds)
      linesOf(v2, a).map(_.title) mustBe Some("same")
      second.send(AddBuildNoteLine(userId, a, AggregateVersion(2), "two"))
      awaitAssert(linesOf(v2, a).map(_.lines.map(_.toLowerCase)) mustBe Some(Vector("one", "two")), 10.seconds)
      DB.readOnly(implicit s => sql"SELECT count(*) FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'projection_m4p_index_v1'"
        .map(_.int(1)).single().get) mustBe 1 // its primary key only
      second.stop()
    }

    Scenario("an instance that stops by itself loses no event: what it did not take stays in the outbox (F110, I3)") {
      Given("a projection, live, of two notes")
      val stopped = new java.util.concurrent.CountDownLatch(1)
      var supervisor: ActorRef = null
      val node = new Node()(projections => {
        supervisor = projections.start[StoppingNotes]("m4p-stopping", 1, "m4p-stopping-" + systems) { build =>
          new StoppingNotes(subscriptions, new PostgresDocumentStore[NoteLog](build.documentStore("m4p_stopping"), mpjsons, new NoopDocumentStoreCache), stopped)
        }
        Seq(supervisor)
      })
      val (a, c) = (node.create("a"), node.create("c"))
      switchedTo(supervisor, 1)
      def logOf(id: AggregateId): Option[NoteLog] = supervisor.askFor[Option[NoteLog]](GetNoteLog(id), 5.seconds)
      awaitAssert(logOf(c) mustBe Some(NoteLog(Vector("1"))), 10.seconds)

      When("its instance stops by itself, and an event of the other note is stored while it starts again")
      node.send(AddBuildNoteLine(userId, a, AggregateVersion(1), "stop"))
      stopped.await(10, java.util.concurrent.TimeUnit.SECONDS) mustBe true
      node.send(AddBuildNoteLine(userId, c, AggregateVersion(1), "late"))

      Then("once a new instance serves, that event is either applied or still waits in the outbox")
      awaitAssert(logOf(a).map(_.entries) mustBe Some(Vector("1", "2")), 10.seconds)
      (logOf(c).exists(_.entries.contains("2")) || eventStore.oldestVersionToPublish(c).contains(AggregateVersion(2))) mustBe true

      And("the projection gets it with the note's next event, from its owner")
      node.send(AddBuildNoteLine(userId, c, AggregateVersion(2), "next"))
      awaitAssert(logOf(c) mustBe Some(NoteLog(Vector("1", "2", "3"))), 10.seconds)
      node.stop()
    }

    Scenario("an aggregate deleted for good while a projection is built is not left in the new build (F111)") {
      Given("memos stored before the projection exists, and its build delivering them slowly")
      val memoContext = new MemoContext
      val bus = system.actorOf(Props(new EventsBusActor()))
      val memos = system.actorOf(AggregateCommandBusActor(memoContext, uidGenerator, eventStore, responses, bus))
      def send(command: AnyRef): Unit = memos.askFor[CustomCommandResponse[_]](command) mustBe a[SuccessResponse]
      val ids = (1 to 60).map(_ => AggregateId(TestIds.aggregate()))
      ids.foreach(id => send(WriteMemo(userId, id, "memo " + id.asLong)))
      val firstEvent = DB.readOnly(implicit s => sql"SELECT id FROM events WHERE aggregate_id = ${ids.head.asLong}".map(_.long(1)).single().get)
      val runtime = new Projections(system, ProjectionsSettings(eventStore, subscriptions, registry, Seq(memoContext),
        BuilderSettings(pageSize = 1, chunkEvents = 1), restartAfter = 1.second))
      val supervisor = runtime.start[MemoTexts]("m4p-memos", 1, "m4p-memos") { build =>
        new MemoTexts(subscriptions, new PostgresDocumentStore[MemoText](build.documentStore("m4p_memos"), mpjsons, new NoopDocumentStoreCache),
          buildDelayMillis = 30)
      }
      bus ! RegisterSubscribers(Seq(supervisor))
      awaitAssert(registry.projection("m4p-memos").get.building.exists(_.position.position >= firstEvent) mustBe true, 30.seconds, 5.millis)

      When("the first memo, which the build has, is deleted for good")
      send(EraseMemo(userId, ids.head, AggregateVersion(1)))

      Then("once built, the projection does not have it, and the memo is deleted")
      switchedTo(supervisor, 1)
      awaitAssert(supervisor.askFor[Option[MemoText]](GetMemoText(ids.head), 5.seconds) mustBe None, 10.seconds)
      supervisor.askFor[Option[MemoText]](GetMemoText(ids.last), 5.seconds) mustBe Some(MemoText("memo " + ids.last.asLong))
      awaitAssert(DB.readOnly(implicit s => sql"SELECT count(*) FROM events WHERE aggregate_id = ${ids.head.asLong}".map(_.int(1)).single().get) mustBe 0, 10.seconds)
      Singletons.stop(system, supervisor)
      Seq(bus, memos).foreach(system.stop)
    }

    Scenario("a build retired before a stop let it be dropped is dropped at the next start (F116)") {
      Given("a projection whose registry switched to another build, stopped before the old one was dropped")
      var first: ActorRef = null
      val node = new Node()(projections => { first = notesProjection(projections, "m4p-orphan", 1, "m4p_orphan"); Seq(first) })
      val note = node.create("note", "x")
      switchedTo(first, 1)
      awaitAssert(linesOf(first, note).isDefined mustBe true, 10.seconds)
      val orphan = registry.projection("m4p-orphan").get.active.get
      node.stop()
      registry.startBuild("m4p-orphan", 1, Map.empty)
      registry.switch("m4p-orphan", 2) mustBe true
      tableExists("projection_m4p_orphan_v1") mustBe true
      subscriptions.hasSubscriptions(orphan.subscriber) mustBe true

      When("the projection starts again")
      var again: ActorRef = null
      val next = new Node()(projections => { again = notesProjection(projections, "m4p-orphan", 1, "m4p_orphan"); Seq(again) })
      awaitAssert(builds(again).active.map(_.number) mustBe Some(2), 10.seconds)

      Then("the retired build's tables, versions and registry row are gone")
      awaitAssert(tableExists("projection_m4p_orphan_v1") mustBe false, 10.seconds)
      subscriptions.hasSubscriptions(orphan.subscriber) mustBe false
      registry.builds("m4p-orphan").map(_.number) mustBe Seq(2)
      next.stop()
    }

    Scenario("a build continues after a stop from the position it saved") {
      Given("many notes, and a build stopped midway")
      val writer = new Node()(_ => Nil)
      val ids = (1 to 20).map(n => writer.create("note " + n, "line"))
      writer.stop()
      var built: ActorRef = null
      val slow = BuilderSettings(pageSize = 2, chunkEvents = 2)
      val first = new Node(settings = slow)(projections => { built = notesProjection(projections, "m4p-resume", 1, "m4p_resume"); Seq(built) })
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

    Scenario("a host watches a build's progress through the supervisor: the builder's status, and the position it saved (F152)") {
      Given("notes, and a new projection building them slowly")
      val writer = new Node()(_ => Nil)
      (1 to 100).foreach(n => writer.create("note " + n, "line"))
      writer.stop()
      var built: ActorRef = null
      val node = new Node(settings = BuilderSettings(pageSize = 1, chunkEvents = 1))(projections => {
        built = notesProjection(projections, "m4p-watch", 1, "m4p_watch", buildDelayMillis = 30); Seq(built)
      })
      def status(): BuildStatus = built.askFor[BuildStatus](GetBuildStatus, 5.seconds)

      Then("while it builds, the status is the builder's, and GetBuilds gives the position the builder saved")
      awaitAssert({
        val now = status()
        now.phase mustBe "Bulk"
        now.position must be > 0L
      }, 30.seconds, 50.millis)
      awaitAssert(builds(built).building.map(_.position.position).getOrElse(0L) must be > 0L, 30.seconds, 50.millis)

      And("once it switched, no build runs")
      switchedTo(built, 1)
      status() mustBe BuildStatus.NoBuild
      node.stop()
    }

    Scenario("a build begun at run time with ServeStale leaves the active build unchanged until the switch, as one begun at a start does (F153)") {
      Given("build 1 live, serving stale while it is built again")
      var supervisor: ActorRef = null
      val node = new Node(settings = BuilderSettings(pageSize = 1, chunkEvents = 1))(projections => {
        supervisor = notesProjection(projections, "m4p-stale", 1, "m4p_stale", whileBuilding = ServeStale, buildDelayMillis = 30); Seq(supervisor)
      })
      val note = node.create("note", "one")
      switchedTo(supervisor, 1)
      awaitAssert(linesOf(supervisor, note) mustBe Some(NoteLines("note", Vector("one"))), 10.seconds)
      (1 to 100).foreach(i => node.create("filler " + i))

      When("a rebuild starts at run time, and the note changes meanwhile")
      supervisor.askFor[RebuildStarted](RebuildProjection, 5.seconds).build mustBe 2
      node.send(AddBuildNoteLine(userId, note, AggregateVersion(2), "two"))

      Then("the active build does not take the change while the new one builds")
      Thread.sleep(1000)
      linesOf(supervisor, note) mustBe Some(NoteLines("note", Vector("one")))
      builds(supervisor).building.map(_.number) mustBe Some(2)

      And("the new build has it once it took over")
      switchedTo(supervisor, 2)
      awaitAssert(linesOf(supervisor, note) mustBe Some(NoteLines("note", Vector("one", "two"))), 10.seconds)
      node.stop()
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
      DB.autoCommit { implicit s => // as the 0.12 rebuild recorded the versions it built
        sql"CREATE TABLE IF NOT EXISTS components_versions (component_name VARCHAR(255) NOT NULL, component_type SMALLINT NOT NULL, component_version INT NOT NULL)".execute()
        sql"INSERT INTO components_versions VALUES ('m4p-legacy', 2, 1)".update()
      }

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

      supervisor.askFor[RebuildStarted](RebuildProjection, 5.seconds) mustBe RebuildStarted(2)
      switchedTo(supervisor, 2)

      Then("an update after the switch reaches it from the new build")
      node.send(AddBuildNoteLine(userId, note, AggregateVersion(2), "b"))
      listener.fishForSpecificMessage(10.seconds) { case SubscriptionUpdated("subscription", NoteLines("pushed", Vector("a", "b"))) => () }
      node.stop()
    }

    Scenario("with the buses of two nodes (M5), the build replaced at a switch stops only once both removed it") {
      Given("a projection asked by two buses")
      val runtime = new Projections(system, ProjectionsSettings(eventStore, subscriptions, registry, Seq(new BuildNoteContext),
        BuilderSettings(pageSize = 20, chunkEvents = 10), restartAfter = 1.second))
      val supervisor = notesProjection(runtime, "m5-two-buses", 1, "m5_two_buses")
      val (busA, busB) = (TestProbe(), TestProbe())
      Seq(busA, busB).foreach { bus =>
        bus.send(supervisor, EventsBusActor.GetSubscriptionRequests(supervisor))
        bus.fishForSpecificMessage(10.seconds) { case EventsBusActor.SubscriptionRequests(_, registered) if registered == supervisor => () }
      }
      switchedTo(supervisor, 1)

      When("it is built again and switches: both buses are told to remove the build it replaces")
      supervisor.askFor[RebuildStarted](RebuildProjection, 5.seconds) mustBe RebuildStarted(2)
      val old = busA.fishForSpecificMessage(60.seconds) { case EventsBusActor.RemoveSubscriber(subscriber, false) => subscriber }
      busB.fishForSpecificMessage(10.seconds) { case EventsBusActor.RemoveSubscriber(subscriber, false) => subscriber } mustBe old
      val watcher = TestProbe()
      watcher.watch(old)

      Then("it keeps running while one bus still delivers to it, and stops once the other let it go too")
      busA.send(supervisor, EventsBusActor.SubscriberRemoved(old))
      watcher.expectNoMessage(1.second)
      busB.send(supervisor, EventsBusActor.SubscriberRemoved(old))
      watcher.expectTerminated(old, 10.seconds)
      Singletons.stop(system, supervisor)
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

    Scenario("a targeted rebuild of one aggregate changes its document only (D63)") {
      Given("two notes whose documents were damaged")
      var supervisor: ActorRef = null
      val node = new Node()(projections => { supervisor = notesProjection(projections, "m4p-target", 1, "m4p_target"); Seq(supervisor) })
      val (repaired, untouched) = (node.create("repaired", "a"), node.create("untouched", "b"))
      switchedTo(supervisor, 1)
      awaitAssert(linesOf(supervisor, untouched).isDefined mustBe true, 10.seconds)
      DB.autoCommit(implicit s => sql"""UPDATE projection_m4p_target_v1 SET document = '{"title":"damaged","lines":[]}'""".update())

      When("one of them is rebuilt")
      supervisor.askFor[AggregatesRebuilt](RebuildAggregates(RebuildTarget.OneAggregate(repaired)), 30.seconds) mustBe
        AggregatesRebuilt(RebuildTarget.OneAggregate(repaired), 1)

      Then("its document is right again, and the other one is as it was")
      linesOf(supervisor, repaired) mustBe Some(NoteLines("repaired", Vector("a")))
      linesOf(supervisor, untouched) mustBe Some(NoteLines("damaged", Vector()))
      registry.projection("m4p-target").flatMap(_.pendingTarget) mustBe None
      node.stop()
    }

    Scenario("a targeted rebuild of one organization rebuilds its aggregates only, after the space backfill (F49)") {
      Given("notes of two organizations, stored before aggregates had a space")
      var supervisor: ActorRef = null
      val node = new Node()(projections => { supervisor = notesProjection(projections, "m4p-space", 1, "m4p_space"); Seq(supervisor) })
      val (seven, eight) = (node.createIn(7, "seven", "x"), node.createIn(8, "eight", "y"))
      switchedTo(supervisor, 1)
      awaitAssert(linesOf(supervisor, eight).isDefined mustBe true, 10.seconds)
      DB.autoCommit { implicit s =>
        sql"UPDATE aggregates SET space_id = -1 WHERE id IN (${seven.asLong}, ${eight.asLong})".update()
        sql"""UPDATE projection_m4p_space_v1 SET document = '{"title":"damaged","lines":[]}'""".update()
      }

      When("the backfill gives them their spaces, and organization 7 is rebuilt")
      val backfill = eventStore.backfillSpaces()
      backfill.filled must be >= 2
      DB.readOnly(implicit s => sql"SELECT space_id FROM aggregates WHERE id = ${seven.asLong}".map(_.long(1)).single()) mustBe Some(7L)
      supervisor.askFor[AggregatesRebuilt](RebuildAggregates(RebuildTarget.OneSpace(io.reactivecqrs.api.id.SpaceId(7))), 30.seconds).aggregates mustBe 1

      Then("only organization 7's document is rebuilt")
      linesOf(supervisor, seven) mustBe Some(NoteLines("seven", Vector("x")))
      linesOf(supervisor, eight) mustBe Some(NoteLines("damaged", Vector()))
      node.stop()
    }

    Scenario("a targeted rebuild keeps an aggregate made while its target is listed (F112)") {
      Given("notes of organization 77, live, and a listing of the organization that waits after its first page")
      val pausing = new PausingListing(mpjsons, typesNamesState)
      var supervisor: ActorRef = null
      val node = new Node(storeOf = pausing)(projections => { supervisor = notesProjection(projections, "m4p-race", 1, "m4p_race"); Seq(supervisor) })
      val first = node.createIn(77, "first", "a")
      switchedTo(supervisor, 1)
      awaitAssert(linesOf(supervisor, first).isDefined mustBe true, 10.seconds)
      pausing.armed = true
      val requester = TestProbe()
      requester.send(supervisor, RebuildAggregates(RebuildTarget.OneSpace(io.reactivecqrs.api.id.SpaceId(77))))
      pausing.listed.await(10, java.util.concurrent.TimeUnit.SECONDS) mustBe true

      When("a note of the organization is made while the listing waits")
      val made = try {
        val made = node.createIn(77, "made", "b")
        scala.util.Try(awaitAssert(linesOf(supervisor, made).isDefined mustBe true, 2.seconds)) // applied meanwhile, unless the projection lists
        made
      } finally pausing.resume.countDown()

      Then("once the rebuild ends, the note is there with its line, as is the first")
      requester.expectMsgType[AggregatesRebuilt](30.seconds)
      awaitAssert(linesOf(supervisor, made) mustBe Some(NoteLines("made", Vector("b"))), 10.seconds)
      linesOf(supervisor, first) mustBe Some(NoteLines("first", Vector("a")))
      node.stop()
    }

    Scenario("an organization is not rebuilt while aggregates of its types have no space (F49, F112)") {
      var supervisor: ActorRef = null
      val node = new Node()(projections => { supervisor = notesProjection(projections, "m4p-no-space", 1, "m4p_no_space"); Seq(supervisor) })
      val note = node.createIn(5, "five", "x")
      switchedTo(supervisor, 1)
      DB.autoCommit(implicit s => sql"UPDATE aggregates SET space_id = -1 WHERE id = ${note.asLong}".update())
      try {
        supervisor.askFor[RebuildRefused](RebuildAggregates(RebuildTarget.OneSpace(io.reactivecqrs.api.id.SpaceId(5)))).reason must include("backfillSpaces")
      } finally DB.autoCommit(implicit s => sql"UPDATE aggregates SET space_id = 5 WHERE id = ${note.asLong}".update())
      node.stop()
    }

    Scenario("the space backfill asks the host for an aggregate whose first event does not know its space") {
      val node = new Node()(_ => Nil)
      val note = node.create("unknown space", "x")
      DB.autoCommit { implicit s =>
        sql"UPDATE aggregates SET space_id = -1 WHERE id = ${note.asLong}".update()
        sql"""UPDATE events SET event = '{"title":"unknown space","space":-1}' WHERE aggregate_id = ${note.asLong} AND version = 1""".update()
      }

      eventStore.backfillSpaces((_, id, _) => if (id == note) Some(io.reactivecqrs.api.id.SpaceId(42)) else None)

      DB.readOnly(implicit s => sql"SELECT space_id FROM aggregates WHERE id = ${note.asLong}".map(_.long(1)).single()) mustBe Some(42L)
      node.stop()
    }

    Scenario("a projection that does not clear for a targeted rebuild refuses it") {
      var events: ActorRef = null
      val node = new Node()(projections => {
        events = projections.start[ManagedNoteEvents]("m4p-no-target", 1, "m4p-no-target-" + systems) { build =>
          new ManagedNoteEvents(subscriptions, new PostgresDocumentStore[NoteLog](build.documentStore("m4p_no_target"), mpjsons, new NoopDocumentStoreCache))
        }
        Seq(events)
      })
      val note = node.create("note", "x")
      switchedTo(events, 1)

      events.askFor[RebuildRefused](RebuildAggregates(RebuildTarget.OneAggregate(note))).reason must include("clearForRebuild")
      registry.projection("m4p-no-target").flatMap(_.pendingTarget) mustBe None
      node.stop()
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

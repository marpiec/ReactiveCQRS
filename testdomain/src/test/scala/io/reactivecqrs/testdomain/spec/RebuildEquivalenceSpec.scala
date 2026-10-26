package io.reactivecqrs.testdomain.spec

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.cluster.Singletons
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.documentstore._
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection._
import io.reactivecqrs.core.rebuild.ProjectionBuilder.BuilderSettings
import io.reactivecqrs.core.rebuild.ProjectionSupervisor._
import io.reactivecqrs.core.rebuild._
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.core.util.BlockingDispatcher
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import scalikejdbc._

import scala.concurrent.duration._
import scala.util.Random

// Two aggregate types of this spec only, so a build reads nothing of the other specs. A list with items, undo,
// duplication, deletion and squash; a tag that is renamed.

case class EqList(title: String, items: Vector[String], space: Long)

case class EqListCreated(title: String, space: Long) extends FirstEvent[EqList] {
  def spaceId: SpaceId = SpaceId(space)
}
case class EqItemAdded(item: String) extends Event[EqList]
case class EqListRetitled(title: String) extends Event[EqList]
case class EqListUndone(eventsCount: Int) extends UndoEvent[EqList]
case class EqListDuplicated(spaceId: SpaceId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion) extends DuplicationEvent[EqList]
case class EqListDeleted() extends Event[EqList]
case class EqListSquashed(spaceId: SpaceId, title: String, items: Vector[String]) extends FirstEvent[EqList]

case class CreateEqList(userId: UserId, title: String, space: Long) extends FirstCommand[EqList, CustomCommandResponse[_]]
case class AddEqItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, item: String) extends Command[EqList, CustomCommandResponse[_]]
case class RetitleEqList(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, title: String) extends Command[EqList, CustomCommandResponse[_]]
case class UndoEqList(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, count: Int) extends Command[EqList, CustomCommandResponse[_]]
case class DuplicateEqList(userId: UserId, baseId: AggregateId, baseVersion: AggregateVersion) extends FirstCommand[EqList, CustomCommandResponse[_]]
case class DeleteEqList(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion) extends Command[EqList, CustomCommandResponse[_]]

class EqListContext extends AggregateContext[EqList] {
  override val version: Int = 1
  override def initialAggregateRoot: EqList = EqList("", Vector(), 0)
  override def commandHandlers = _ => {
    case c: CreateEqList => CommandSuccess(EqListCreated(c.title, c.space))
    case c: AddEqItem => CommandSuccess(EqItemAdded(c.item))
    case c: RetitleEqList => CommandSuccess(EqListRetitled(c.title))
    case c: UndoEqList => CommandSuccess(EqListUndone(c.count))
    case c: DuplicateEqList => CommandSuccess(EqListDuplicated(SpaceId(0), c.baseId, c.baseVersion))
    case _: DeleteEqList => CommandSuccess(EqListDeleted())
  }
  override def eventHandlers = (_, _, list) => {
    case e: EqListCreated => EqList(e.title, Vector(), e.space)
    case e: EqItemAdded => list.copy(items = list.items :+ e.item)
    case e: EqListRetitled => list.copy(title = e.title)
    case _: EqListUndone => list
    case _: EqListDuplicated => list
    case _: EqListDeleted => null
    case e: EqListSquashed => EqList(e.title, e.items, e.spaceId.asLong)
  }
  override val squash: Option[SquashPolicy[EqList]] = Some(SquashPolicy(list => EqListSquashed(SpaceId(list.space), list.title, list.items)))
}

case class EqTag(name: String, space: Long)

case class EqTagCreated(name: String, space: Long) extends FirstEvent[EqTag] {
  def spaceId: SpaceId = SpaceId(space)
}
case class EqTagRenamed(name: String) extends Event[EqTag]

case class CreateEqTag(userId: UserId, name: String, space: Long) extends FirstCommand[EqTag, CustomCommandResponse[_]]
case class RenameEqTag(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, name: String) extends Command[EqTag, CustomCommandResponse[_]]

class EqTagContext extends AggregateContext[EqTag] {
  override val version: Int = 1
  override def initialAggregateRoot: EqTag = EqTag("", 0)
  override def commandHandlers = _ => {
    case c: CreateEqTag => CommandSuccess(EqTagCreated(c.name, c.space))
    case c: RenameEqTag => CommandSuccess(EqTagRenamed(c.name))
  }
  override def eventHandlers = (_, _, tag) => {
    case e: EqTagCreated => EqTag(e.name, e.space)
    case e: EqTagRenamed => tag.copy(name = e.name)
  }
}

// One document per space that both types write, so a group or a chunk updates it several times in one transaction, and
// an index of each aggregate's space. Rows are sorted by aggregate, so a document does not depend on the order in which
// different aggregates were applied, only on each aggregate's own sequence.

case class EqListRow(id: Long, title: String, items: Vector[String], version: Int)
case class EqTagRow(id: Long, names: Vector[String])
case class EqSpace(lists: Vector[EqListRow], tags: Vector[EqTagRow])
case class EqWhere(space: Long)

class EqOverview(val subscriptionsState: SubscriptionsState, spaces: DocumentStore[EqSpace], index: DocumentStore[EqWhere]) extends ProjectionActor {

  private def updateSpace(space: Long, change: EqSpace => EqSpace)(implicit session: DBSession): Unit =
    spaces.updateDocument(space, space, existing => Document(change(existing.map(_.document).getOrElse(EqSpace(Vector(), Vector())))))

  private def spaceOf(id: AggregateId)(implicit session: DBSession): Option[Long] = index.getDocument(id.asLong).map(_.document.space)

  protected val listeners = List(
    // With its events, so a build replays each list whole: undo, duplication and a start at a squash event (D60, D96, F98).
    AggregateWithEventsListener[EqList] { (id, version, list, _) => implicit session =>
      list match {
        case Some(l) =>
          // A state listener sees the state after a batch, not each step (chapter 8): an undo of the creation moves the
          // list to space 0, live after its row was written in its space, in a build often before. The row moves with it.
          spaceOf(id) match {
            case None => index.insertDocument(l.space, id.asLong, EqWhere(l.space))
            case Some(old) if old != l.space =>
              updateSpace(old, s => s.copy(lists = s.lists.filterNot(_.id == id.asLong)))
              index.removeDocument(id.asLong)
              index.insertDocument(l.space, id.asLong, EqWhere(l.space))
            case Some(_) => ()
          }
          updateSpace(l.space, s => s.copy(lists = (s.lists.filterNot(_.id == id.asLong) :+ EqListRow(id.asLong, l.title, l.items, version.asInt)).sortBy(_.id)))
        case None => spaceOf(id).foreach { space =>
          updateSpace(space, s => s.copy(lists = s.lists.filterNot(_.id == id.asLong)))
          index.removeDocument(id.asLong)
        }
      }
    },
    EventsListener[EqTag] { (id, events) => implicit session =>
      events.foreach(_.event match {
        case EqTagCreated(name, space) =>
          index.insertDocument(space, id.asLong, EqWhere(space))
          updateSpace(space, s => s.copy(tags = (s.tags :+ EqTagRow(id.asLong, Vector(name))).sortBy(_.id)))
        case EqTagRenamed(name) => spaceOf(id).foreach(space =>
          updateSpace(space, s => s.copy(tags = s.tags.map(t => if (t.id == id.asLong) t.copy(names = t.names :+ name) else t))))
        case _ => ()
      })
    })

  override protected def receiveQuery: Receive = PartialFunction.empty
  override protected val projectionName: String = "EqOverview"
}

/** A document cache in a map, as a host keeps one: the builds read their documents through it, so its updates after
  * commit meet groups and chunks of many aggregates (D61). */
class MapDocumentCache[D <: AnyRef] extends DocumentStoreCache[D] {
  private val entries = new java.util.concurrent.ConcurrentHashMap[Long, Option[VersionedDocument[D]]]()
  override def put(key: Long, value: Option[VersionedDocument[D]]): Unit = entries.put(key, value)
  override def get(key: Long): CacheEntry[VersionedDocument[D]] = Option(entries.get(key)) match {
    case Some(Some(document)) => InCache(document)
    case Some(None) => InCacheEmpty
    case None => NotInCache
  }
  override def getAll(keys: Set[Long]): Map[Long, CacheEntry[VersionedDocument[D]]] = keys.map(key => key -> get(key)).toMap
  override def remove(key: Long): Unit = entries.remove(key)
  override def clear(): Unit = entries.clear()
}

/** The same projection, unmanaged: it takes every event live from the start, the reference the builds are compared with. */
class EqOverviewReference(subscriptionsState: SubscriptionsState, spaces: DocumentStore[EqSpace], index: DocumentStore[EqWhere])
  extends EqOverview(subscriptionsState, spaces, index)

// M4 verification: for a random history of two aggregate types, with undo, duplication, deletion and squash, the
// projection applied live, the one built while the active one serves and events keep coming, and the one built after a
// stop midway hold the same rows.
class RebuildEquivalenceSpec extends CommonSpec {

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
  private val listContext = new EqListContext
  private val tagContext = new EqTagContext
  private val name = "m4e-overview"
  private var nodes = 0

  private def spacesStore(table: String, cache: DocumentStoreCache[EqSpace]) = new PostgresDocumentStore[EqSpace](table, mpjsons, cache)
  private def indexStore(table: String, cache: DocumentStoreCache[EqWhere]) = new PostgresDocumentStore[EqWhere](table, mpjsons, cache)

  private val referenceSpaces = spacesStore("m4e_ref_spaces", new NoopDocumentStoreCache)
  private val referenceIndex = indexStore("m4e_ref_index", new NoopDocumentStoreCache)

  /** One system: an event bus, a command bus per type, the managed projection at `version` and, when asked, the
    * reference. */
  private class Node(version: Int, settings: BuilderSettings = BuilderSettings(pageSize = 20, chunkEvents = 10), reference: Boolean = false) {
    nodes += 1
    val bus: ActorRef = system.actorOf(Props(new EventsBusActor()), "m4e-bus-" + nodes)
    val lists: ActorRef = system.actorOf(AggregateCommandBusActor(listContext, uidGenerator, eventStore, responses, bus))
    val tags: ActorRef = system.actorOf(AggregateCommandBusActor(tagContext, uidGenerator, eventStore, responses, bus))
    private val runtime = new Projections(system, ProjectionsSettings(eventStore, subscriptions, registry, Seq(listContext, tagContext), settings,
      restartAfter = 1.second))
    val supervisor: ActorRef = runtime.start[EqOverview](name, version, name + "-" + nodes) { build =>
      new EqOverview(subscriptions, spacesStore(build.documentStore("m4e_spaces"), new MapDocumentCache), indexStore(build.documentStore("m4e_index"), new MapDocumentCache))
    }
    private val referenceProjection = if (reference) {
      Seq(system.actorOf(Props(new EqOverviewReference(subscriptions, referenceSpaces, referenceIndex)).withDispatcher(BlockingDispatcher.id)))
    } else Nil
    bus ! RegisterSubscribers(supervisor +: referenceProjection)

    def stop(): Unit = {
      Singletons.stop(system, supervisor) // in a cluster, the singleton too
      (Seq(bus, lists, tags) ++ referenceProjection).foreach(system.stop)
    }
  }

  /** A random history: each command is drawn from `random`; a refused one (an undo or a duplication below a squash
    * point) changes nothing. `ids` are the aggregates it wrote. */
  private class History(random: Random) {
    private var listVersions = Map[AggregateId, Int]()
    private var tagVersions = Map[AggregateId, Int]()
    private var squashed = Map[AggregateId, Int]()
    private var steps = 0
    /** How many commands of each kind were stored, and squashes done: the history must have them all. */
    var done = Map[String, Int]().withDefaultValue(0)

    def ids: Seq[AggregateId] = (listVersions.keys ++ tagVersions.keys ++ deleted).toSeq
    private var deleted = Set[AggregateId]()

    private def ask(bus: ActorRef, command: AnyRef): Any = bus.askFor[Any](command)

    private def onList(command: AnyRef, node: Node): Unit = ask(node.lists, command) match {
      case SuccessResponse(id, version) =>
        listVersions += id -> version.asInt // a duplicate's answer names the new list
        done += command.getClass.getSimpleName -> (done(command.getClass.getSimpleName) + 1)
      case _: EventHandlingError | _: FailureResponse => ()
      case other => fail("Unexpected answer to " + command + ": " + other)
    }

    def run(node: Node, count: Int): Unit = (1 to count).foreach { _ =>
      steps += 1
      val lists = listVersions.keys.toVector.sortBy(_.asLong)
      val tags = tagVersions.keys.toVector.sortBy(_.asLong)
      random.nextInt(20) match {
        case 0 | 1 => onList(CreateEqList(userId, "list " + steps, random.nextInt(3)), node)
        case 2 => ask(node.tags, CreateEqTag(userId, "tag " + steps, random.nextInt(3))) match {
          case SuccessResponse(id, version) => tagVersions += id -> version.asInt
          case other => fail("Unexpected answer: " + other)
        }
        case 3 | 4 if tags.nonEmpty =>
          val id = tags(random.nextInt(tags.size))
          ask(node.tags, RenameEqTag(userId, id, AggregateVersion(tagVersions(id)), "name " + steps)) match {
            case SuccessResponse(_, version) => tagVersions += id -> version.asInt
            case other => fail("Unexpected answer: " + other)
          }
        case n if lists.nonEmpty =>
          val id = lists(random.nextInt(lists.size))
          val version = AggregateVersion(listVersions(id))
          n match {
            case 5 => onList(UndoEqList(userId, id, version, 1 + random.nextInt(2)), node)
            case 6 => onList(DuplicateEqList(userId, id, version), node)
            case 7 => onList(RetitleEqList(userId, id, version, "title " + steps), node)
            case 8 if random.nextInt(4) == 0 =>
              onList(DeleteEqList(userId, id, version), node)
              listVersions -= id
              deleted += id
            case 9 if version.asInt > 2 && !squashed.get(id).contains(version.asInt) =>
              delivered(ids) // a squash waits for the events at or below its point to leave the outbox
              ask(node.lists, SquashAggregate(id, version)) match {
                case _: AggregateSquashed =>
                  squashed += id -> version.asInt
                  done += "squash" -> (done("squash") + 1)
                case _: SquashRefused => ()
                case other => fail("Unexpected answer: " + other)
              }
            case _ => onList(AddEqItem(userId, id, version, "item " + steps), node)
          }
        case _ => onList(CreateEqList(userId, "list " + steps, random.nextInt(3)), node)
      }
    }
  }

  /** Waits until every subscriber took every event of these aggregates: their outbox rows are gone. */
  private def delivered(ids: Seq[AggregateId]): Unit =
    awaitAssert(ids.foreach(id => eventStore.oldestVersionToPublish(id) mustBe None), 30.seconds, 100.millis)

  private def builds(supervisor: ActorRef): Builds = supervisor.askFor[Builds](GetBuilds, 5.seconds)

  private def switchedTo(supervisor: ActorRef, build: Int): Unit =
    awaitAssert({
      val now = builds(supervisor)
      (now.active.map(_.number), now.building) mustBe ((Some(build), None))
    }, 90.seconds, 100.millis)

  /** A table's rows as (id, document): JSONB text, so key order does not matter; the document version is left out, as
    * it counts writes, which differ between live delivery and a build. */
  private def rows(table: String): Seq[(Long, String)] = DB.readOnly { implicit session =>
    SQL(s"SELECT id, document::text FROM projection_$table ORDER BY id").map(rs => (rs.long(1), rs.string(2))).list()
  }

  private def activeSuffix: String = registry.projection(name).get.active.get.suffix

  private def sameAsReference(): Unit = awaitAssert({
    rows("m4e_spaces" + activeSuffix) mustBe rows("m4e_ref_spaces")
    rows("m4e_index" + activeSuffix) mustBe rows("m4e_ref_index")
  }, 30.seconds, 200.millis)

  Feature("A projection rebuilt is the projection applied live (M4 verification)") {

    Scenario("for a random history of two types with undo, duplication, deletion and squash: built live, rebuilt while serving, and rebuilt after a stop") {
      val history = new History(new Random(20261017))

      Given("the projection, started before any event, and a reference applying every event live")
      val first = new Node(version = 1, settings = BuilderSettings(pageSize = 4, chunkEvents = 3), reference = true)
      switchedTo(first.supervisor, 1)

      When("a random history is written")
      history.run(first, 200)
      delivered(history.ids)

      Then("the build applied live holds the reference's rows")
      rows("m4e_ref_spaces").size must be > 0
      sameAsReference()

      When("it is rebuilt from the store while it serves and more of the history is written")
      first.supervisor.askFor[Any](RebuildProjection, 5.seconds) mustBe RebuildStarted(2)
      history.run(first, 200)

      Then("the new build, once switched, holds the reference's rows")
      switchedTo(first.supervisor, 2)
      delivered(history.ids)
      sameAsReference()
      Seq("UndoEqList", "DuplicateEqList", "DeleteEqList", "squash").foreach(kind => withClue(kind + ": ")(history.done(kind) must be > 0))
      first.stop()

      When("a new version starts building, slowly, and stops midway")
      val stopped = new Node(version = 2, settings = BuilderSettings(pageSize = 2, chunkEvents = 2))
      awaitAssert(registry.projection(name).get.building.exists(b => b.position.position > 0 && !b.position.bulkDone) mustBe true, 30.seconds, 20.millis)
      stopped.stop()

      Then("the build started again goes on from where it was and holds the reference's rows")
      val again = new Node(version = 2)
      switchedTo(again.supervisor, 3)
      sameAsReference()
      again.stop()
    }
  }
}

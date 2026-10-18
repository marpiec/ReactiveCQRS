package io.reactivecqrs.testdomain.spec

import java.util.concurrent.atomic.AtomicBoolean

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.documentstore.{DocumentStore, NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.{MessageAck, RegisterSubscribers}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection._
import io.reactivecqrs.core.saga.{PostgresSagaState, SagaDefinition, SagaHandlingStatus, SagaInternalOrder, SagaOrder, SagaRunner}
import io.reactivecqrs.core.space.{PurgeHook, PurgeReport, SpacePurge}
import io.reactivecqrs.core.tools.StoredDataCheck
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.core.util.BlockingDispatcher
import io.reactivecqrs.testdomain.shoppingcart.ClientKey
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.testkit.TestProbe
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.util.Try

// R17: an organization's folders, copied within and across organizations.
case class Folder(space: SpaceId, name: String, notes: Vector[String])
case class FolderCreated(spaceId: SpaceId, name: String) extends FirstEvent[Folder]
case class FolderCopied(spaceId: SpaceId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion) extends DuplicationEvent[Folder]
case class NoteAdded(text: String) extends Event[Folder]
case class FolderSquashed(spaceId: SpaceId, name: String, notes: Vector[String]) extends FirstEvent[Folder]

case class CreateFolder(userId: UserId, space: SpaceId, name: String) extends FirstCommand[Folder, CustomCommandResponse[_]]
case class CopyFolder(userId: UserId, space: SpaceId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion)
  extends FirstCommand[Folder, CustomCommandResponse[_]]
case class AddNote(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, text: String, idempotencyId: Option[ClientKey] = None)
  extends Command[Folder, CustomCommandResponse[_]] with IdempotentCommand[ClientKey]

class FolderContext extends AggregateContext[Folder] {
  override val version: Int = 1
  override def initialAggregateRoot: Folder = Folder(SpaceId(-1), "", Vector.empty)
  override def commandHandlers = _ => {
    case c: CreateFolder => CommandSuccess(FolderCreated(c.space, c.name))
    case c: CopyFolder => CommandSuccess(FolderCopied(c.space, c.baseAggregateId, c.baseAggregateVersion))
    case c: AddNote => CommandSuccess(NoteAdded(c.text))
  }
  override def eventHandlers = (_, _, folder) => {
    case e: FolderCreated => Folder(e.spaceId, e.name, Vector.empty)
    case e: FolderCopied => folder.copy(space = e.spaceId)
    case e: NoteAdded => folder.copy(notes = folder.notes :+ e.text)
    case e: FolderSquashed => Folder(e.spaceId, e.name, e.notes)
  }
  override val squash: Option[SquashPolicy[Folder]] =
    Some(SquashPolicy(folder => FolderSquashed(folder.space, folder.name, folder.notes), keep = Set(classOf[NoteAdded])))
  override val snapshots: Option[SnapshotPolicy] = Some(SnapshotPolicy(every = 2))
}

case class FolderDoc(name: String, notes: Vector[String])
case class GetFolderDoc(id: AggregateId)

/** The folders, in documents of their organizations' spaces. */
class FoldersProjection(val subscriptionsState: SubscriptionsState, store: DocumentStore[FolderDoc]) extends ProjectionActor {
  protected val listeners = List(AggregateListener[Folder] { (id, _, created, folder) => implicit session =>
    folder match {
      case Some(f) if created => store.insertDocument(f.space.asLong, id.asLong, FolderDoc(f.name, f.notes))
      case Some(f) => store.overwriteDocument(id.asLong, FolderDoc(f.name, f.notes))
      case None => store.removeDocument(id.asLong)
    }
  })
  override protected def clearForRebuild(target: RebuildTarget)(implicit session: DBSession): Unit = target match {
    case RebuildTarget.OneSpace(space) => store.clearSpace(space.asLong)
    case RebuildTarget.OneAggregate(id) => store.removeDocument(id.asLong)
    case RebuildTarget.OneType(_) => store.clearAllData()
  }
  override protected def receiveQuery: Receive = {
    case GetFolderDoc(id) => sender() ! store.getDocument(id.asLong).map(_.document)
  }
  override protected val projectionName: String = "FoldersProjection"
}

/** The same documents, from a projection that cannot clear a space. */
class UnclearableFolders(val subscriptionsState: SubscriptionsState) extends ProjectionActor {
  protected val listeners = List(AggregateListener[Folder] { (_, _, _, _) => _ => () })
  override protected def receiveQuery: Receive = PartialFunction.empty
  override protected val projectionName: String = "UnclearableFolders"
}

/** A saga that waits forever in its first step, so its row stays, tagged with the order's organization. */
case class WaitInSpace(userId: UserId, space: SpaceId) extends SagaOrder
class WaitingSaga extends SagaDefinition {
  override val name = "PurgeSpecWaitingSaga"
  override def handleOrder(sagaStep: SagaStep): ReceiveOrder = { case _: WaitInSpace => Promise[SagaHandlingStatus]().future }
  override def handleRevert(sagaStep: SagaStep): ReceiveRevert = PartialFunction.empty
  override def spaceOf(order: SagaInternalOrder): Option[SpaceId] = order match {
    case w: WaitInSpace => Some(w.space)
    case _ => None
  }
}

class PurgeSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
  private val subscriptionsState = new PostgresSubscriptionsState(typesNamesState).initSchema()
  private val sagaState = new PostgresSagaState(mpjsons, typesNamesState)
  sagaState.initSchema()
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  private val folders = system.actorOf(Props(new FoldersProjection(subscriptionsState,
    new PostgresDocumentStore[FolderDoc]("purge_folders", mpjsons, new NoopDocumentStoreCache))).withDispatcher(BlockingDispatcher.id))
  eventBus ! RegisterSubscribers(List(folders))
  private val commandBus: ActorRef = system.actorOf(AggregateCommandBusActor(new FolderContext, uidGenerator, eventStore, responses, eventBus))
  private val folderType = AggregateType.of[Folder]
  private val saga = system.actorOf(SagaRunner.props(new WaitingSaga, uidGenerator, sagaState))

  DB.autoCommit { implicit session => sql"CREATE TABLE IF NOT EXISTS purge_spec_rows (aggregate_id BIGINT NOT NULL)".execute() }
  private val hook = new PurgeHook {
    override def name: String = "purge_spec_rows"
    override def purge(space: SpaceId, aggregateIds: Seq[AggregateId])(implicit session: DBSession): Long =
      sql"DELETE FROM purge_spec_rows WHERE aggregate_id IN (${aggregateIds.map(_.asLong)})".update().toLong
  }

  private val userId = UserId(1)
  private implicit val timeout: Timeout = Timeout(30.seconds)
  private var spaces = (System.nanoTime() % 1000000L) * 10 + 7000000000L

  private def newSpace(): SpaceId = { spaces += 1; SpaceId(spaces) }

  private def sent(command: AnyRef): CustomCommandResponse[_] = Await.result((commandBus ? command).mapTo[CustomCommandResponse[_]], 30.seconds)

  private def created(command: AnyRef): AggregateId = sent(command) match {
    case SuccessResponse(id, _) => id
    case other => fail("Expected a success, got " + other)
  }

  private def purgeOf(space: SpaceId, projections: Seq[ActorRef] = Seq(folders), buses: Map[AggregateType, ActorRef] = Map(folderType -> commandBus),
                      hooks: Seq[PurgeHook] = Seq(hook)): PurgeReport =
    Await.result(new SpacePurge(eventStore, buses, projections, hooks).purge(space), 2.minutes)

  private def docOf(id: AggregateId): Option[FolderDoc] = Await.result((folders ? GetFolderDoc(id)).mapTo[Option[FolderDoc]], 10.seconds)

  private def count(table: String, column: String, value: Long): Long = DB.readOnly { implicit session =>
    SQL(s"SELECT count(*) FROM $table WHERE $column = ?").bind(value).map(_.long(1)).single().get
  }

  private def rowsOf(id: AggregateId): Map[String, Long] =
    Seq("events", "events_archive", "events_to_publish", "snapshots", "subscriptions", "commands_responses", "purge_spec_rows")
      .map(table => table -> count(table, "aggregate_id", id.asLong)).toMap ++
      Map("aggregates" -> count("aggregates", "id", id.asLong), "aggregates read" -> count("aggregates", "base_id", id.asLong))

  private def stateOf(id: AggregateId): Option[Folder] =
    Await.result((commandBus ? GetAggregate(id)).mapTo[Try[Aggregate[Folder]]], 10.seconds).toOption.flatMap(_.aggregateRoot)

  private def outboxEmpty(ids: AggregateId*): Unit =
    awaitAssert(ids.foreach(id => count("events_to_publish", "aggregate_id", id.asLong) mustBe 0L), 10.seconds, 50.millis)

  Feature("Purging an organization (R17)") {

    Scenario("removes everything stored for the organization's aggregates, its sagas and its documents, and nothing of another") {
      Given("an organization with a folder, its copy, an idempotent response, an archive, a snapshot, a saga and host rows")
      val (space, other) = (newSpace(), newSpace())
      val folder = created(CreateFolder(userId, space, "Alpha"))
      sent(AddNote(userId, folder, AggregateVersion(1), "one", Some(ClientKey("purge-" + System.nanoTime())))) mustBe SuccessResponse(folder, AggregateVersion(2))
      sent(AddNote(userId, folder, AggregateVersion(2), "two")) mustBe SuccessResponse(folder, AggregateVersion(3))
      val copy = created(CopyFolder(userId, space, folder, AggregateVersion(2)))
      And("another organization with a copy of the copy (it reads both) and a folder of its own")
      val theirCopy = created(CopyFolder(userId, other, copy, AggregateVersion(1)))
      val theirs = created(CreateFolder(userId, other, "Beta"))
      outboxEmpty(folder, copy, theirCopy, theirs)
      Await.result(commandBus ? SquashAggregate(folder, AggregateVersion(2), userId), 10.seconds) mustBe AggregateSquashed(folder, AggregateVersion(2))
      commandBus ! PassivateAggregate(folder)
      awaitAssert(count("snapshots", "aggregate_id", folder.asLong) mustBe 1L, 10.seconds, 50.millis)
      DB.autoCommit { implicit session =>
        Seq(folder, copy, theirs).foreach(id => sql"INSERT INTO purge_spec_rows (aggregate_id) VALUES (${id.asLong})".update())
      }
      saga ! WaitInSpace(userId, space)
      saga ! WaitInSpace(userId, other)
      awaitAssert(Seq(space, other).map(s => count("sagas", "space_id", s.asLong)) mustBe Seq(1L, 1L), 10.seconds, 50.millis)
      awaitAssert(Seq(folder, copy, theirCopy, theirs).map(docOf(_).map(_.name)) mustBe Seq(Some("Alpha"), Some("Alpha"), Some("Alpha"), Some("Beta")),
        10.seconds, 50.millis)
      rowsOf(folder).filter(_._2 > 0).keySet mustBe Set("events", "events_archive", "snapshots", "subscriptions", "commands_responses",
        "purge_spec_rows", "aggregates", "aggregates read")

      StoredDataCheck.spaceRows(space.asLong).filter(_._2 > 0).keySet mustBe Set("aggregates", "event_keys", "sagas", "projection_purge_folders")

      When("the organization is purged")
      val report = purgeOf(space)

      Then("the purge is complete: two aggregates in two passes, the other organization's copy detached, the projection cleared")
      report.complete mustBe true
      (report.passes, report.aggregates, report.detached, report.projections) mustBe ((2, 2, Seq(theirCopy), Seq("FoldersProjection")))
      report.rows("purge_spec_rows") mustBe 2L
      report.rows("sagas") mustBe 1L
      And("nothing is stored for its aggregates any more, in any table")
      Seq(folder, copy).foreach(id => rowsOf(id).filter(_._2 > 0) mustBe empty)
      count("event_keys", "space_id", space.asLong) mustBe 0L
      count("sagas", "space_id", space.asLong) mustBe 0L
      Seq(folder, copy).map(docOf) mustBe Seq(None, None)
      StoredDataCheck.spaceRows(space.asLong).filter(_._2 > 0) mustBe empty
      Seq(folder, copy).map(stateOf) mustBe Seq(None, None)
      And("the other organization keeps everything, its copy now standing on its own with the same state")
      stateOf(theirCopy) mustBe Some(Folder(other, "Alpha", Vector("one")))
      stateOf(theirs) mustBe Some(Folder(other, "Beta", Vector.empty))
      (rowsOf(theirCopy)("aggregates"), rowsOf(theirs)("purge_spec_rows"), count("sagas", "space_id", other.asLong)) mustBe ((1L, 1L, 1L))
      Seq(theirCopy, theirs).map(docOf(_).map(_.name)) mustBe Seq(Some("Alpha"), Some("Beta"))
    }

    Scenario("refuses before it deletes anything when a projection cannot clear the space, or a type has no command bus here") {
      val space = newSpace()
      val folder = created(CreateFolder(userId, space, "Gamma"))
      outboxEmpty(folder)
      val unclearable = system.actorOf(Props(new UnclearableFolders(subscriptionsState)))

      val refusedByProjection = purgeOf(space, projections = Seq(folders, unclearable))
      refusedByProjection.refused mustBe Seq("projection UnclearableFolders does not implement clearForRebuild")
      val refusedByBus = purgeOf(space, buses = Map.empty)
      refusedByBus.refused mustBe Seq("aggregates of type " + folderType.typeName + " are in the space or read it, and no command bus of the type was given")

      Seq(refusedByProjection, refusedByBus).map(r => (r.complete, r.aggregates, r.remaining)) mustBe Seq((false, 0, 1), (false, 0, 1))
      rowsOf(folder)("events") mustBe 1L
      docOf(folder).map(_.name) mustBe Some("Gamma")
    }

    Scenario("a delivery still on its way after the purge is acknowledged and writes nothing") {
      val space = newSpace()
      val folder = created(CreateFolder(userId, space, "Delta"))
      outboxEmpty(folder)
      purgeOf(space).complete mustBe true

      val bus = TestProbe()
      folders.tell(AggregateWithType(folderType, folder, AggregateVersion(1), Seq(AggregateVersion(1)), Some(Folder(space, "Delta", Vector.empty))), bus.ref)
      bus.expectMsgType[MessageAck].aggregateId mustBe folder
      docOf(folder) mustBe None
    }

    Scenario("an aggregate created in the organization during the purge goes in a later pass") {
      val space = newSpace()
      val folder = created(CreateFolder(userId, space, "Epsilon"))
      outboxEmpty(folder)
      val once = new AtomicBoolean(false)
      var late = Option.empty[AggregateId]
      val creatingHook = new PurgeHook {
        override def name: String = "creating"
        override def purge(s: SpaceId, aggregateIds: Seq[AggregateId])(implicit session: DBSession): Long = {
          if (once.compareAndSet(false, true)) late = Some(created(CreateFolder(userId, space, "Late")))
          0L
        }
      }

      val report = purgeOf(space, hooks = Seq(creatingHook))
      (report.complete, report.passes, report.aggregates) mustBe ((true, 3, 2))
      late.map(rowsOf).map(_.filter(_._2 > 0)) mustBe Some(Map.empty)
      eventStore.spaceSize(space) mustBe 0
    }
  }
}

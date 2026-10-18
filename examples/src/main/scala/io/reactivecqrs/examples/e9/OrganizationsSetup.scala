package io.reactivecqrs.examples.e9

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.space.{ExportReport, ImportReport, PurgeReport, SpaceExport, SpaceImport, SpacePurge}
import io.reactivecqrs.core.tools.StoredDataCheck
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.examples.e1.BasicSetup
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/**
  * E9, organizations (chapter 16): memos of two organizations in one installation; one organization is exported, imported
  * into another, empty space (here of the same installation, so every id it brings is taken and moves), and then
  * purged, leaving the other organization as it was. Run it with `sbt "examples/runMain
  * io.reactivecqrs.examples.e9.OrganizationsSetup"`; `OrganizationsSpec` runs the same code against the test database.
  */
object OrganizationsSetup {

  final case class Installation(eventStore: PostgresEventStoreState, uidGenerator: ActorRef, memos: ActorRef)

  def start(system: ActorSystem): Installation = {
    val mpjsons = new MPJsons
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
    val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
    val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
    val memos = system.actorOf(AggregateCommandBusActor(new MemoContext, uidGenerator, eventStore, responses, eventBus), "memos")
    eventBus ! EventsBusActor.RegisterSubscribers(Nil)
    system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus)), "outboxPoller")
    Installation(eventStore, uidGenerator, memos)
  }

  /** What the move left: the reports, a moved memo as it reads in its new space, and what both spaces hold after. */
  final case class Move(exported: ExportReport, imported: ImportReport, movedMemo: Memo, purged: PurgeReport,
                        leftInSource: Map[String, Long], otherMemo: Memo)

  def moveAndPurge(installation: Installation, user: UserId, source: SpaceId, other: SpaceId, target: SpaceId)
                  (implicit system: ActorSystem, timeout: Timeout): Move = {
    def send(command: AnyRef): SuccessResponse = Await.result((installation.memos ? command).mapTo[CustomCommandResponse[_]], timeout.duration) match {
      case success: SuccessResponse => success
      case refused => throw new IllegalStateException("Refused: " + refused)
    }
    def memo(id: AggregateId): Memo =
      Await.result((installation.memos ? GetAggregate(id)).mapTo[Try[Aggregate[Memo]]], timeout.duration).get.aggregateRoot.get

    val first = send(WriteMemo(user, source, "Order paper"))
    send(EditMemo(user, first.aggregateId, first.aggregateVersion, "Order paper and toner"))
    send(WriteMemo(user, source, "Water the plants"))
    val others = send(WriteMemo(user, other, "Another organization's memo"))

    // #export
    val file = Files.createTempFile("organization-", ".jsonl")
    val writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)
    val exported = try new SpaceExport(installation.eventStore, contexts = Seq(new MemoContext)).write(source, writer) finally writer.close()
    // #export
    // #import
    val imported = new SpaceImport(installation.eventStore, installation.uidGenerator).read(file, into = target)
    // #import
    Files.delete(file)
    // #purge
    val purged = Await.result(new SpacePurge(installation.eventStore,
      commandBuses = Map(AggregateType.of[Memo] -> installation.memos), projections = Nil).purge(source), 2.minutes)
    // #purge
    Move(exported, imported, memo(imported.remapped(first.aggregateId)), purged, StoredDataCheck.spaceRows(source.asLong), memo(others.aggregateId))
  }

  def main(args: Array[String]): Unit = {
    BasicSetup.connect(sys.props.getOrElse("reactivecqrs.example.url", "jdbc:postgresql://localhost:5432/reactivecqrs"),
      sys.props.getOrElse("reactivecqrs.example.user", "reactivecqrs"), sys.props.getOrElse("reactivecqrs.example.password", "reactivecqrs"))
    implicit val system: ActorSystem = ActorSystem("e9")
    try {
      implicit val timeout: Timeout = Timeout(70.seconds)
      val first = System.currentTimeMillis() // spaces of their own on every run: an import needs an empty one
      val move = moveAndPurge(start(system), UserId(1), SpaceId(first), SpaceId(first + 1), SpaceId(first + 2))
      println("Exported: " + move.exported)
      println("Imported: " + move.imported.aggregates + " memos, ids moved: " + move.imported.remapped.size)
      println("A moved memo: " + move.movedMemo)                  // Memo(SpaceId(first + 2), Order paper and toner)
      println("Purged: " + move.purged.complete + ", left: " + move.leftInSource.values.sum)
      println("The other organization's: " + move.otherMemo)
    } finally Await.ready(system.terminate(), 30.seconds)
  }
}

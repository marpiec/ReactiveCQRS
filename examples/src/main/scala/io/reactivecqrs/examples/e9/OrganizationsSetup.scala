package io.reactivecqrs.examples.e9

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{SpaceId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.delivery.OutboxPollerActor
import io.reactivecqrs.core.documentstore.{MultipleLongIndex, NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.{PostgresSubscriptionsState, RebuildTarget}
import io.reactivecqrs.core.rebuild.ProjectionSupervisor.{AggregatesRebuilt, RebuildAggregates}
import io.reactivecqrs.core.rebuild.{PostgresProjectionRegistry, Projections, ProjectionsSettings}
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
import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * E9, organizations (chapter 16): the carts of two shops in one installation, and each shop's cart list, a managed
  * projection whose documents carry their shop's space. One shop is exported, imported into another, empty space (here
  * of the same installation, so every id it brings is taken and moves), its list rebuilt there, and then the shop is
  * purged where it was, leaving the other shop as it was. Run it with `sbt "examples/runMain
  * io.reactivecqrs.examples.e9.OrganizationsSetup"`; `OrganizationsSpec` runs the same code against the test database.
  */
object OrganizationsSetup {

  final case class Installation(eventStore: PostgresEventStoreState, uidGenerator: ActorRef, carts: ActorRef, shopCarts: ActorRef)

  def start(system: ActorSystem): Installation = {
    val mpjsons = new MPJsons
    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
    val subscriptionsState = new PostgresSubscriptionsState(typesNames).initSchema()
    val uidGenerator = system.actorOf(UidGeneratorActor.postgres(system), "uidGenerator")
    val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
    val carts = system.actorOf(AggregateCommandBusActor(new ShopCartContext, uidGenerator, eventStore, responses, eventBus), "carts")
    val projections = new Projections(system, ProjectionsSettings(eventStore, subscriptionsState,
      new PostgresProjectionRegistry().initSchema(), contexts = Seq(new ShopCartContext)))
    // #start-list
    val shopCarts: ActorRef = projections.start[ShopCartsList]("shop-carts", 1, "shopCarts") { build =>
      new ShopCartsList(subscriptionsState, new PostgresDocumentStore[ShopCartSummary](build.documentStore("shop_carts"),
        mpjsons, new NoopDocumentStoreCache, Seq(MultipleLongIndex(1, Seq("shop")))))
    }
    // #start-list
    eventBus ! EventsBusActor.RegisterSubscribers(List(shopCarts))
    system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus)), "outboxPoller")
    Installation(eventStore, uidGenerator, carts, shopCarts)
  }

  /** What the move left: the reports, the moved shop's list once rebuilt in its new space, and what the old space and the
    * other shop hold afterwards. */
  final case class Move(exported: ExportReport, imported: ImportReport, rebuilt: AggregatesRebuilt, movedCarts: Set[ShopCartSummary],
                        purged: PurgeReport, leftInSource: Map[String, Long], otherCarts: Set[ShopCartSummary])

  def moveAndPurge(installation: Installation, user: UserId, source: SpaceId, other: SpaceId, target: SpaceId)
                  (implicit system: ActorSystem, timeout: Timeout): Move = {
    def send(command: AnyRef): SuccessResponse = Await.result((installation.carts ? command).mapTo[CustomCommandResponse[_]], timeout.duration) match {
      case success: SuccessResponse => success
      case refused => throw new IllegalStateException("Refused: " + refused)
    }
    // A shop's carts once the list has `count` of them: the list is filled after the commands are answered.
    @tailrec def cartsOf(shop: SpaceId, count: Int, deadline: Deadline = timeout.duration.fromNow): Set[ShopCartSummary] = {
      val carts = Await.result((installation.shopCarts ? GetShopCarts(shop)).mapTo[Vector[ShopCartSummary]], timeout.duration).toSet
      if (carts.size == count || deadline.isOverdue()) carts else {
        Thread.sleep(100)
        cartsOf(shop, count, deadline)
      }
    }

    val groceries = send(OpenShopCart(user, source, "Groceries"))
    send(AddShopItem(user, groceries.aggregateId, groceries.aggregateVersion, "apples"))
    send(OpenShopCart(user, source, "Garden"))
    send(OpenShopCart(user, other, "Another shop's cart"))
    cartsOf(source, 2) // the shop is quiet: its events reached the list

    // #export
    val file = Files.createTempFile("shop-", ".jsonl")
    val writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)
    val exported = try new SpaceExport(installation.eventStore, contexts = Seq(new ShopCartContext)).write(source, writer)
                   finally writer.close()
    // ExportReport(space, aggregates, events, archived, detached, waitingInOutbox, refused)
    // #export
    // #import
    val imported = new SpaceImport(installation.eventStore, installation.uidGenerator).read(file, into = target)
    // ImportReport(space, aggregates, events, archived, remapped, unknownReferences, someUnknown, refused)
    // #import
    Files.delete(file)
    // #rebuild
    val rebuilt = Await.result(installation.shopCarts.ask(RebuildAggregates(RebuildTarget.OneSpace(target)))(Timeout(10.minutes)),
      10.minutes) // a whole organization can take a while
    // AggregatesRebuilt(RebuildTarget.OneSpace(target), aggregates), or RebuildRefused(reason)
    // #rebuild
    // #purge
    val purge = new SpacePurge(installation.eventStore,
      commandBuses = Map(AggregateType.of[ShopCart] -> installation.carts), // this node's bus of every type the shop has
      projections = Seq(installation.shopCarts))                            // every projection with its documents
    val purged = Await.result(purge.purge(source), 2.minutes)
    // #purge
    rebuilt match {
      case done: AggregatesRebuilt =>
        Move(exported, imported, done, cartsOf(target, 2), purged, StoredDataCheck.spaceRows(source.asLong), cartsOf(other, 1))
      case refused => throw new IllegalStateException("Not rebuilt: " + refused)
    }
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
      println("Imported: " + move.imported.aggregates + " carts, ids moved: " + move.imported.remapped.size)
      println("Rebuilt: " + move.rebuilt + "; the moved shop's list: " + move.movedCarts)
      println("Purged: " + move.purged.complete + ", left: " + move.leftInSource.values.sum)
      println("The other shop's list: " + move.otherCarts)
    } finally Await.ready(system.terminate(), 30.seconds)
  }
}

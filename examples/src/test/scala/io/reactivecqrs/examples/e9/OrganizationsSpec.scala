package io.reactivecqrs.examples.e9

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{SpaceId, UserId}
import io.reactivecqrs.core.projection.RebuildTarget
import io.reactivecqrs.core.rebuild.ProjectionSupervisor.AggregatesRebuilt
import io.reactivecqrs.core.space.SpacePurge
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase, TestSystems}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.concurrent.Await
import scala.concurrent.duration._

// E9 as its main runs it, against the test database. Then chapter 16's purge hook and the steps of an upgrade, each a
// region followed by the lines that check it.
class OrganizationsSpec extends CommonSpec {

  TestDatabase.init()

  private implicit val timeout: Timeout = Timeout(70.seconds)

  Feature("E9: organizations") {

    Scenario("a shop exported, imported into an empty space under new ids, its list rebuilt there, and purged; the other shop untouched") {
      val first = 9000000000L + System.nanoTime() % 1000000L
      val (source, other, target) = (SpaceId(first), SpaceId(first + 1), SpaceId(first + 2))
      val move = OrganizationsSetup.moveAndPurge(OrganizationsSetup.start(system), UserId(1), source, other, target)

      (move.exported.complete, move.exported.aggregates, move.exported.events) mustBe ((true, 2, 3L))
      (move.imported.complete, move.imported.aggregates, move.imported.remapped.size) mustBe ((true, 2, 2))
      move.rebuilt mustBe AggregatesRebuilt(RebuildTarget.OneSpace(target), 2)
      move.movedCarts mustBe Set(ShopCartSummary(target.asLong, "Groceries"), ShopCartSummary(target.asLong, "Garden"))
      (move.purged.complete, move.purged.aggregates) mustBe ((true, 2))
      move.leftInSource.values.sum mustBe 0L
      move.otherCarts mustBe Set(ShopCartSummary(other.asLong, "Another shop's cart"))
    }

    Scenario("the host's own rows go with a purge, through a hook; nothing of the shop is left") {
      DB.autoCommit(implicit session => sql"CREATE TABLE IF NOT EXISTS receipts (cart_id BIGINT NOT NULL)".execute())
      val app = TestSystems.newSystem("e9-hook")
      try {
        val installation = OrganizationsSetup.start(app)
        val cornerShop = SpaceId(42)
        val cartId = Await.result((installation.carts ? OpenShopCart(UserId(1), cornerShop, "Corner")).mapTo[CustomCommandResponse[_]],
          timeout.duration) match {
          case SuccessResponse(id, _) => id
          case refused => fail("Not opened: " + refused)
        }
        DB.autoCommit(implicit session => sql"INSERT INTO receipts (cart_id) VALUES (${cartId.asLong})".update())

        // #purgeHook
        import io.reactivecqrs.api.id.{AggregateId, SpaceId}
        import io.reactivecqrs.core.space.PurgeHook
        import scalikejdbc._

        val receiptsHook = new PurgeHook {
          override def name: String = "receipts"
          override def purge(space: SpaceId, aggregateIds: Seq[AggregateId])(implicit session: DBSession): Long =
            sql"DELETE FROM receipts WHERE cart_id IN (${aggregateIds.map(_.asLong)})".update().toLong
        }
        // #purgeHook
        val purged = Await.result(new SpacePurge(installation.eventStore, Map(AggregateType.of[ShopCart] -> installation.carts),
          Seq(installation.shopCarts), hooks = Seq(receiptsHook))(app).purge(cornerShop), 2.minutes)
        (purged.complete, purged.rows.get("receipts")) mustBe ((true, Some(1L)))

        // #spaceRows
        import io.reactivecqrs.core.tools.StoredDataCheck

        val left = StoredDataCheck.spaceRows(42L) // Map(aggregates -> 0, events -> 0, sagas -> 0, projection_shop_carts_v1 -> 0)
        // #spaceRows
        left.values.sum mustBe 0L
        left.keySet must contain allOf ("aggregates", "events", "projection_shop_carts_v1")
      } finally Await.ready(app.terminate(), 30.seconds)
    }

    Scenario("an upgrade's steps for organizations: aggregates without a space counted, indexes built, spaces and keys given") {
      val app = TestSystems.newSystem("e9-upgrade")
      try {
        val eventStore = OrganizationsSetup.start(app).eventStore
        val mpjsons = new MPJsons
        val typesNames = new PostgresTypesNamesState().initSchema()

        // #withoutSpace
        import io.reactivecqrs.core.tools.StoredDataCheck

        val report = StoredDataCheck.run(mpjsons, typesNames)
        println(report.aggregatesWithoutSpace)
        // #withoutSpace
        report.aggregatesWithoutSpace mustBe 0L

        // #buildIndexes
        import io.reactivecqrs.core.documentstore.PostgresDocumentStore

        eventStore.buildIndexesConcurrently()
        PostgresDocumentStore.buildSpaceIndexesConcurrently()
        // #buildIndexes

        // #backfillSpaces
        import io.reactivecqrs.core.eventstore.SpaceBackfill

        val spaces: SpaceBackfill = eventStore.backfillSpaces()  // SpaceBackfill(filled, left)
        // #backfillSpaces
        spaces mustBe SpaceBackfill(0, Nil) // every aggregate of a 1.0 store has its space

        locally {
          // #backfillByEvent
          val spaces = eventStore.backfillSpaces((aggregateType, aggregateId, firstEvent) => firstEvent match {
            case e: LegacyCartCreated => Some(SpaceId(e.shopNumber))
            case _ => None
          })
          // #backfillByEvent
          spaces mustBe SpaceBackfill(0, Nil)
        }

        // #backfillKeys
        import io.reactivecqrs.core.eventstore.EventKeysBackfill

        val keys: EventKeysBackfill = eventStore.backfillEventKeys()  // EventKeysBackfill(written, missing, orphans)
        // #backfillKeys
        keys mustBe EventKeysBackfill(0, 0) // every event of a 1.0 store has its key
      } finally Await.ready(app.terminate(), 30.seconds)
    }
  }
}

/** An old first event of a shop's cart, from before spaces: it keeps the shop in a number of its own. */
case class LegacyCartCreated(shopNumber: Long) extends FirstEvent[ShopCart] {
  def spaceId: SpaceId = SpaceId.unknown
}

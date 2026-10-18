package io.reactivecqrs.examples.e9

import io.reactivecqrs.api.id.{SpaceId, UserId}
import io.reactivecqrs.core.projection.RebuildTarget
import io.reactivecqrs.core.rebuild.ProjectionSupervisor.AggregatesRebuilt
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.util.Timeout

import scala.concurrent.duration._

// E9 as its main runs it, against the test database.
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
  }
}

package io.reactivecqrs.examples.e3

import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.util.Backoff
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.util.Timeout

import scala.concurrent.duration._

// E3 as its main runs it, against the test database.
class ProjectionsSpec extends CommonSpec {

  TestDatabase.init()

  private implicit val timeout: Timeout = Timeout(70.seconds)
  private val shop = ProjectionsSetup.start(system, Backoff(20.millis, 50.millis, maxAttempts = 3))
  private val user = UserId(1)

  Feature("E3: projections") {

    Scenario("the three kinds of listener fill one document, which a delayed query reads after its own write") {
      val (cartId, version) = ProjectionsSetup.cart(shop, user, "Groceries", "apples", "pears")
      ProjectionsSetup.activityAtLeast(shop, cartId, version).map(a => (a.name, a.itemCount, a.version)) mustBe Some(("Groceries", 2, 3))
      awaitAssert(ProjectionsSetup.activityAtLeast(shop, cartId, version) mustBe
        Some(CartActivity("Groceries", 2, 3, Vector("created as Groceries", "added apples", "added pears"), Some("pears"))), 5.seconds, 50.millis)
    }

    Scenario("a listener that keeps failing on one cart is recorded and shown in the status; the others go on, and a skip clears it") {
      val (broken, _) = ProjectionsSetup.cart(shop, user, "Broken")
      awaitAssert(ProjectionsSetup.status(shop).failures.map(_.aggregateId) must contain (broken), 10.seconds, 50.millis)
      awaitAssert(ProjectionsSetup.activityAtLeast(shop, broken, AggregateVersion(0)).map(_.log) mustBe
        Some(Vector("created as Broken")), 5.seconds, 50.millis)

      ProjectionsSetup.skip(shop, broken).count mustBe 1
      ProjectionsSetup.status(shop).failures.map(_.aggregateId) must not contain broken
    }
  }
}

package io.reactivecqrs.examples.e11

// E11, testing (chapter 19): the host's own wiring (E1's) against PostgreSQL, with the library's measures recorded.

// #postgres
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.metrics.{ReactiveMetrics, RecordingMetrics}
import io.reactivecqrs.examples.e1.{BasicSetup, CartSummary}
import io.reactivecqrs.testkit.TestDatabase
import org.apache.pekko.util.Timeout

import scala.concurrent.duration._

class CartsOnPostgresSpec extends ShopSpec {

  TestDatabase.init() // the connection pool, on a schema of its own for this test run

  private val metrics = new RecordingMetrics
  ReactiveMetrics(system).use(metrics)
  private val shop = BasicSetup.start(system) // what the host starts

  Feature("Carts in PostgreSQL") {

    Scenario("a cart is stored, reaches the read model through the outbox, and is measured") {
      implicit val timeout: Timeout = Timeout(30.seconds)
      BasicSetup.fillCart(shop, UserId(1)) mustBe Some(CartSummary("Groceries", 1, 2))
      metrics.counted("reactivecqrs.events.stored", Map("type" -> "ShoppingCart")) mustBe 2L
    }
  }
}
// #postgres

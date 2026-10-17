package io.reactivecqrs.examples.e1

import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.util.Timeout

import scala.concurrent.duration._

// E1 as its main runs it, against the test database.
class BasicSetupSpec extends CommonSpec {

  TestDatabase.init()

  Feature("E1: a basic single-node setup") {

    Scenario("a cart created and filled is in the read model, which the query waits for") {
      implicit val timeout: Timeout = Timeout(70.seconds)
      BasicSetup.fillCart(BasicSetup.start(system), UserId(1)) mustBe Some(CartSummary("Groceries", 1, 2))
    }
  }
}

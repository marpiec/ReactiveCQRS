package io.reactivecqrs.examples.e4

import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.examples.e4.CheckoutSaga.{CheckedOut, CheckoutRefused}
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.util.Timeout

import scala.concurrent.duration._

// E4 as its main runs it, against the test database.
class CheckoutSagaSpec extends CommonSpec {

  TestDatabase.init()

  private implicit val timeout: Timeout = Timeout(70.seconds)
  private val shop = SagaSetup.start(system)

  Feature("E4: a checkout saga") {

    Scenario("within the limit, the items are reserved and the customer charged") {
      val (answer, stockId) = SagaSetup.checkout(shop, UserId(1), items = 3, limitCents = 10000, amountCents = 2500)
      answer mustBe a[CheckedOut]
      SagaSetup.available(shop, stockId) mustBe 7
    }

    Scenario("over the limit, the checkout is refused and the reserved items go back") {
      val (answer, stockId) = SagaSetup.checkout(shop, UserId(1), items = 3, limitCents = 1000, amountCents = 2500)
      answer mustBe CheckoutRefused(List("Over the limit"))
      awaitAssert(SagaSetup.available(shop, stockId) mustBe 10, 10.seconds, 100.millis)
    }
  }
}

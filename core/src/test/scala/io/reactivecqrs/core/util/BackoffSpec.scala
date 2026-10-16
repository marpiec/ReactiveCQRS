package io.reactivecqrs.core.util

import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.duration._

class BackoffSpec extends AnyFeatureSpec with Matchers {

  Feature("Waiting between attempts") {

    Scenario("doubles from the first wait up to the maximum") {
      val backoff = Backoff(100.millis, 1.second, maxAttempts = 10)

      (2 to 7).map(backoff.delay) mustBe Seq(100.millis, 200.millis, 400.millis, 800.millis, 1.second, 1.second)
    }

    Scenario("with jitter, waits between half of the plain wait and all of it") {
      val backoff = Backoff(100.millis, 1.second, maxAttempts = 10, jitter = true)

      for (attempt <- 2 to 6; _ <- 1 to 50) {
        val plain = Backoff(100.millis, 1.second, 10).delay(attempt)
        backoff.delay(attempt) must (be >= plain / 2 and be <= plain)
      }
    }

    Scenario("does not overflow for large attempt numbers") {
      Backoff(1.second, 30.seconds, maxAttempts = Int.MaxValue).delay(1000) mustBe 30.seconds
    }
  }

  Feature("The attempt limit") {

    Scenario("counts every attempt, the first one included") {
      val backoff = Backoff(10.millis, 1.second, maxAttempts = 3)

      (1 to 4).map(backoff.exhausted) mustBe Seq(false, false, true, true)
    }
  }
}

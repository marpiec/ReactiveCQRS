package io.reactivecqrs.core.uid

import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

class IdPoolSpec extends AnyFeatureSpec with Matchers {

  Feature("Ids from pools fetched ahead (F23)") {

    Scenario("the first pool is asked for at once, and ids come in order from it") {
      val pool = new IdPool
      pool.available mustBe false
      pool.shouldAsk mustBe true
      pool.shouldAsk mustBe false // asked already
      pool.received(1000, 10)
      pool.available mustBe true
      (1 to 3).map(_ => pool.take()) mustBe Seq(1000, 1001, 1002)
    }

    Scenario("the next pool is asked for when a fifth is left, and taken over when this one runs out") {
      val pool = new IdPool
      pool.shouldAsk
      pool.received(1000, 10)
      (1 to 7).foreach(_ => pool.take())
      pool.shouldAsk mustBe false // 3 left
      pool.take()
      pool.shouldAsk mustBe true // 2 left
      pool.received(5000, 10)
      pool.shouldAsk mustBe false // one is in reserve
      (1 to 3).map(_ => pool.take()) mustBe Seq(1008, 1009, 5000)
    }

    Scenario("a pool that did not come is asked for again") {
      val pool = new IdPool
      pool.shouldAsk mustBe true
      pool.failed()
      pool.available mustBe false
      pool.shouldAsk mustBe true
      an[IllegalStateException] must be thrownBy pool.take()
    }
  }
}

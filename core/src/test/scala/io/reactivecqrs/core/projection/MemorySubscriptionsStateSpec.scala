package io.reactivecqrs.core.projection

import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import scalikejdbc.NoSession

// F14: the in-memory subscriptions state must behave like PostgresSubscriptionsState
class MemorySubscriptionsStateSpec extends AnyFeatureSpec with GivenWhenThen with Matchers {

  private val aggregateId = AggregateId(1)

  Feature("Clearing one subscriber's progress") {
    Scenario("other subscribers keep their progress") {
      Given("two subscribers that both processed version 3 of an aggregate")
      val state = new MemorySubscriptionsState
      Seq("ProjectionA", "ProjectionB").foreach { subscriber =>
        state.newVersionForEventsSubscription(subscriber, aggregateId, AggregateVersion.ZERO, AggregateVersion(3))(NoSession)
      }

      When("the first subscriber's progress is cleared")
      state.clearSubscriptionsInfo("ProjectionA")

      Then("the first starts again from zero and the second keeps its version")
      state.lastVersionForEventsSubscription("ProjectionA", aggregateId) mustBe AggregateVersion.ZERO
      state.lastVersionForEventsSubscription("ProjectionB", aggregateId) mustBe AggregateVersion(3)
    }
  }

  Feature("Advancing a subscription is a compare-and-set") {
    Scenario("an update from the expected version is stored without a prior read") {
      val state = new MemorySubscriptionsState
      state.newVersionForAggregatesSubscription("Projection", aggregateId, AggregateVersion.ZERO, AggregateVersion(2))(NoSession)
      state.lastVersionForAggregateSubscription("Projection", aggregateId) mustBe AggregateVersion(2)
    }

    Scenario("an update from a stale version is rejected") {
      Given("a subscription at version 2")
      val state = new MemorySubscriptionsState
      state.newVersionForAggregatesWithEventsSubscription("Projection", aggregateId, AggregateVersion.ZERO, AggregateVersion(2))(NoSession)

      Then("advancing it from version 1 fails and the version stays at 2")
      an[OptimisticLockingFailed] must be thrownBy {
        state.newVersionForAggregatesWithEventsSubscription("Projection", aggregateId, AggregateVersion(1), AggregateVersion(3))(NoSession)
      }
      state.lastVersionForAggregatesWithEventsSubscription("Projection", aggregateId) mustBe AggregateVersion(2)
    }
  }
}

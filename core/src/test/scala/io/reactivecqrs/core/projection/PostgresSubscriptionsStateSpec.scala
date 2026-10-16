package io.reactivecqrs.core.projection

import java.time.Instant

import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testutils.TestDatabase
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import scalikejdbc._

// F29, F6, F77: in live mode the subscription versions are cached, but the database decides. D20a: failures.
class PostgresSubscriptionsStateSpec extends AnyFeatureSpec with GivenWhenThen with Matchers {

  TestDatabase.init()

  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val projection = "PostgresSubscriptionsStateSpecProjection"

  private def newState(maxCachedAggregates: Int = 10000) =
    new PostgresSubscriptionsState(typesNamesState, keepInMemory = false, maxCachedAggregates = maxCachedAggregates).initSchema()

  private def newAggregateId() = AggregateId(System.nanoTime())

  private def advance(state: PostgresSubscriptionsState, subscriber: String, id: AggregateId, from: Int, to: Int): Unit =
    state.localTx { implicit session => state.newVersionForEventsSubscription(subscriber, id, AggregateVersion(from), AggregateVersion(to)) }

  Feature("The version cache in live mode") {

    Scenario("keeps at most the configured number of aggregates (F29)") {
      val state = newState(maxCachedAggregates = 3)
      (1 to 5).foreach(_ => advance(state, projection, newAggregateId(), 0, 1))

      state.cachedAggregates mustBe 3
    }

    Scenario("an aggregate dropped from the cache is read from the database again") {
      val state = newState(maxCachedAggregates = 1)
      val (a, b) = (newAggregateId(), newAggregateId())
      advance(state, projection, a, 0, 2)
      advance(state, projection, b, 0, 1)

      state.lastVersionForEventsSubscription(projection, a) mustBe AggregateVersion(2)
    }

    Scenario("clearing a subscriber's subscriptions also clears its cached versions (F6)") {
      val state = newState()
      val cleared = "PostgresSubscriptionsStateSpecCleared"
      val id = newAggregateId()
      advance(state, cleared, id, 0, 3)
      advance(state, projection, id, 0, 2)

      state.clearSubscriptionsInfo(cleared)

      state.lastVersionForEventsSubscription(cleared, id) mustBe AggregateVersion.ZERO
      state.lastVersionForEventsSubscription(projection, id) mustBe AggregateVersion(2)
    }

    Scenario("a version that moved in the database without this cache is read again after the failed update (F77)") {
      Given("a subscription at version 3, which another writer moved to 5")
      val state = newState()
      val id = newAggregateId()
      advance(state, projection, id, 0, 3)
      DB.autoCommit { implicit session => sql"UPDATE subscriptions SET aggregate_version = 5 WHERE aggregate_id = ${id.asLong}".update() }

      When("an update from the cached version fails")
      an[OptimisticLockingFailed] must be thrownBy advance(state, projection, id, 3, 4)

      Then("the next read sees the stored version, and updates from it succeed")
      state.lastVersionForEventsSubscription(projection, id) mustBe AggregateVersion(5)
      advance(state, projection, id, 5, 6)
      state.lastVersionForEventsSubscription(projection, id) mustBe AggregateVersion(6)
    }
  }

  Feature("Failures (D20a)") {

    val since = Instant.parse("2026-10-16T10:00:00Z")

    Scenario("are recorded, listed for their subscriber, and cleared") {
      val state = newState()
      val id = newAggregateId()
      val failure = SubscriptionFailure(SubscriptionType.EVENTS, id, AggregateVersion(4), "listener failed", since)

      state.recordFailure(projection, failure)
      state.failures(projection).filter(_.aggregateId == id) mustBe Seq(failure)
      newState().failures(projection).filter(_.aggregateId == id) mustBe Seq(failure)

      state.localTx { implicit session => state.clearFailure(projection, SubscriptionType.EVENTS, id) }
      state.failures(projection).filter(_.aggregateId == id) mustBe empty
    }

    Scenario("a later failure of the same subscription replaces the earlier one") {
      val state = newState()
      val id = newAggregateId()
      state.recordFailure(projection, SubscriptionFailure(SubscriptionType.AGGREGATES, id, AggregateVersion(4), "first", since))
      state.recordFailure(projection, SubscriptionFailure(SubscriptionType.AGGREGATES, id, AggregateVersion(6), "second", since.plusSeconds(60)))

      state.failures(projection).filter(_.aggregateId == id).map(f => (f.version.asInt, f.error)) mustBe Seq((6, "second"))
    }

    Scenario("clearing a subscriber's subscriptions also clears its failures") {
      val state = newState()
      val cleared = "PostgresSubscriptionsStateSpecClearedFailures"
      state.recordFailure(cleared, SubscriptionFailure(SubscriptionType.EVENTS, newAggregateId(), AggregateVersion(1), "failed", since))

      state.clearSubscriptionsInfo(cleared)

      state.failures(cleared) mustBe empty
    }
  }
}

package io.reactivecqrs.core.projection

import java.time.Instant

import io.reactivecqrs.api.{AggregateType, AggregateVersion}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testkit.{TestDatabase, TestIds}
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import scalikejdbc._

// F29, F6, F77: the subscription versions are cached, but the database decides. D20a: failures. D61: batches of many
// aggregates.
class PostgresSubscriptionsStateSpec extends AnyFeatureSpec with GivenWhenThen with Matchers {

  TestDatabase.init()

  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val projection = "PostgresSubscriptionsStateSpecProjection"
  private val failedType = AggregateType("io.reactivecqrs.core.projection.FailedAggregate")

  private def newState(maxCachedAggregates: Int = 10000) =
    new PostgresSubscriptionsState(typesNamesState, maxCachedAggregates = maxCachedAggregates).initSchema()

  private def newAggregateId() = AggregateId(TestIds.aggregate()) // unique: nanoTime can repeat within one expression

  private def advance(state: PostgresSubscriptionsState, subscriber: String, id: AggregateId, from: Int, to: Int): Unit =
    state.localTx { implicit session => state.newVersion(subscriber, SubscriptionType.EVENTS, id, AggregateVersion(from), AggregateVersion(to)) }

  Feature("The version cache") {

    Scenario("keeps at most the configured number of aggregates (F29)") {
      val state = newState(maxCachedAggregates = 3)
      (1 to 5).foreach { _ =>
        val id = newAggregateId()
        state.lastVersion(projection, SubscriptionType.EVENTS, id) // a read caches the aggregate's versions
        advance(state, projection, id, 0, 1)
      }

      state.cachedAggregates mustBe 3
    }

    Scenario("versions of many aggregates are read at once, from the cache for those cached") {
      val state = newState()
      val (cached, stored, none) = (newAggregateId(), newAggregateId(), newAggregateId())
      state.lastVersion(projection, SubscriptionType.EVENTS, cached)
      advance(state, projection, cached, 0, 2)
      advance(state, projection, stored, 0, 3)

      state.lastVersions(projection, SubscriptionType.EVENTS, Seq(cached, stored, none)) mustBe
        Map(cached -> AggregateVersion(2), stored -> AggregateVersion(3), none -> AggregateVersion.ZERO)
      state.cachedAggregates mustBe 1 // only the one read before: a batch read fills nothing
    }

    Scenario("a read that a commit changed while it ran is not cached (F115)") {
      @volatile var whileReading: () => Unit = () => ()
      val state = new PostgresSubscriptionsState(typesNamesState) {
        override protected def storedVersions(aggregateIds: Seq[AggregateId]): List[(AggregateId, String, AggregateVersion)] = {
          val read = super.storedVersions(aggregateIds)
          whileReading()
          read
        }
      }.initSchema()
      val id = newAggregateId()
      advance(state, projection, id, 0, 3)

      whileReading = () => state.localTx(implicit session => state.resetVersions(projection, Seq(id))) // a targeted rebuild resets it
      state.lastVersions(projection, SubscriptionType.EVENTS, Seq(id), cache = true) mustBe Map(id -> AggregateVersion(3))
      whileReading = () => ()

      state.lastVersion(projection, SubscriptionType.EVENTS, id) mustBe AggregateVersion.ZERO
    }

    Scenario("versions of many aggregates read for live deliveries are cached, with every subscriber's") {
      val state = newState()
      val (a, b) = (newAggregateId(), newAggregateId())
      advance(state, projection, a, 0, 2)
      advance(state, "PostgresSubscriptionsStateSpecOther", a, 0, 5)

      state.lastVersions(projection, SubscriptionType.EVENTS, Seq(a, b), cache = true) mustBe Map(a -> AggregateVersion(2), b -> AggregateVersion.ZERO)
      state.cachedAggregates mustBe 2

      DB.autoCommit { implicit session => sql"DELETE FROM subscriptions WHERE aggregate_id = ${a.asLong}".update() }
      state.lastVersion("PostgresSubscriptionsStateSpecOther", SubscriptionType.EVENTS, a) mustBe AggregateVersion(5) // from the cache
    }

    Scenario("an aggregate dropped from the cache is read from the database again") {
      val state = newState(maxCachedAggregates = 1)
      val (a, b) = (newAggregateId(), newAggregateId())
      advance(state, projection, a, 0, 2)
      advance(state, projection, b, 0, 1)

      state.lastVersion(projection, SubscriptionType.EVENTS, a) mustBe AggregateVersion(2)
    }

    Scenario("clearing a subscriber's subscriptions also clears its cached versions (F6)") {
      val state = newState()
      val cleared = "PostgresSubscriptionsStateSpecCleared"
      val id = newAggregateId()
      advance(state, cleared, id, 0, 3)
      advance(state, projection, id, 0, 2)

      state.clearSubscriptionsInfo(cleared)

      state.lastVersion(cleared, SubscriptionType.EVENTS, id) mustBe AggregateVersion.ZERO
      state.lastVersion(projection, SubscriptionType.EVENTS, id) mustBe AggregateVersion(2)
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
      state.lastVersion(projection, SubscriptionType.EVENTS, id) mustBe AggregateVersion(5)
      advance(state, projection, id, 5, 6)
      state.lastVersion(projection, SubscriptionType.EVENTS, id) mustBe AggregateVersion(6)
    }
  }

  Feature("Several moves of one subscription in one transaction (D61)") {

    def moves(state: PostgresSubscriptionsState, id: AggregateId, versions: (Int, Int)*)(implicit session: DBSession): Unit =
      versions.foreach { case (from, to) => state.newVersion(projection, SubscriptionType.EVENTS, id, AggregateVersion(from), AggregateVersion(to)) }

    Scenario("each move starts where the one before it ended, and the cache shows the last after the commit") {
      val state = newState()
      val id = newAggregateId()
      state.lastVersion(projection, SubscriptionType.EVENTS, id) mustBe AggregateVersion.ZERO // cached at zero

      state.localTx(implicit session => moves(state, id, 0 -> 1, 1 -> 3, 3 -> 4))

      state.lastVersion(projection, SubscriptionType.EVENTS, id) mustBe AggregateVersion(4)
      newState().lastVersion(projection, SubscriptionType.EVENTS, id) mustBe AggregateVersion(4)
    }

    Scenario("a move that does not start where the one before it ended fails") {
      val state = newState()
      val id = newAggregateId()
      an[OptimisticLockingFailed] must be thrownBy state.localTx(implicit session => moves(state, id, 0 -> 2, 0 -> 1))
      state.lastVersion(projection, SubscriptionType.EVENTS, id) mustBe AggregateVersion.ZERO
    }

    Scenario("a rolled-back transaction leaves the versions as they were, and the next one starts from them") {
      val state = newState()
      val id = newAggregateId()
      advance(state, projection, id, 0, 2)

      an[IllegalStateException] must be thrownBy state.localTx { implicit session =>
        moves(state, id, 2 -> 3, 3 -> 5)
        throw new IllegalStateException("the listener failed")
      }

      state.lastVersion(projection, SubscriptionType.EVENTS, id) mustBe AggregateVersion(2)
      state.localTx(implicit session => moves(state, id, 2 -> 3, 3 -> 4))
      state.lastVersion(projection, SubscriptionType.EVENTS, id) mustBe AggregateVersion(4)
    }
  }

  Feature("Failures (D20a)") {

    val since = Instant.parse("2026-10-16T10:00:00Z")

    Scenario("are recorded, listed for their subscriber, and cleared") {
      val state = newState()
      val id = newAggregateId()
      val failure = SubscriptionFailure(SubscriptionType.EVENTS, failedType, id, AggregateVersion(4), "listener failed", since)

      state.recordFailure(projection, failure)
      state.failures(projection).filter(_.aggregateId == id) mustBe Seq(failure)
      newState().failures(projection).filter(_.aggregateId == id) mustBe Seq(failure)

      state.localTx { implicit session => state.clearFailure(projection, SubscriptionType.EVENTS, id) }
      state.failures(projection).filter(_.aggregateId == id) mustBe empty
    }

    Scenario("a later failure of the same subscription replaces the earlier one") {
      val state = newState()
      val id = newAggregateId()
      state.recordFailure(projection, SubscriptionFailure(SubscriptionType.AGGREGATES, failedType, id, AggregateVersion(4), "first", since))
      state.recordFailure(projection, SubscriptionFailure(SubscriptionType.AGGREGATES, failedType, id, AggregateVersion(6), "second", since.plusSeconds(60)))

      state.failures(projection).filter(_.aggregateId == id).map(f => (f.version.asInt, f.error)) mustBe Seq((6, "second"))
    }

    Scenario("clearing a subscriber's subscriptions also clears its failures") {
      val state = newState()
      val cleared = "PostgresSubscriptionsStateSpecClearedFailures"
      state.recordFailure(cleared, SubscriptionFailure(SubscriptionType.EVENTS, failedType, newAggregateId(), AggregateVersion(1), "failed", since))

      state.clearSubscriptionsInfo(cleared)

      state.failures(cleared) mustBe empty
    }
  }
}

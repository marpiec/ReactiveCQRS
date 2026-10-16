package io.reactivecqrs.core.projection

import io.mpjsons.MPJsons
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.documentstore.{InCache, MapDocumentStoreCache, Note, PostgresDocumentStore, VersionedDocument}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testutils.TestDatabase
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

// F18, F16: a projection update runs in subscriptionsState.localTx; caches must not keep what a rolled-back transaction wrote.
class CachesAfterRollbackSpec extends AnyFeatureSpec with GivenWhenThen with Matchers {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val subscriptionsState = new PostgresSubscriptionsState(typesNamesState).initSchema()

  private def newAggregateId() = AggregateId(System.nanoTime())

  private def failingTransaction(work: scalikejdbc.DBSession => Unit): Unit = {
    an[IllegalStateException] must be thrownBy subscriptionsState.localTx { session =>
      work(session)
      throw new IllegalStateException("the transaction fails after the work was done")
    }
  }

  Feature("Subscription progress (F18)") {

    Scenario("a rolled-back transaction leaves the subscription at its previous version") {
      Given("an aggregate whose subscription is at version zero")
      val aggregateId = newAggregateId()

      When("a transaction advances it to version 3 and then rolls back")
      failingTransaction { implicit session =>
        subscriptionsState.newVersion("CachesAfterRollbackProjection", SubscriptionType.EVENTS, aggregateId, AggregateVersion.ZERO, AggregateVersion(3))
      }

      Then("the subscription is still at version zero, so the events are delivered again")
      subscriptionsState.lastVersion("CachesAfterRollbackProjection", SubscriptionType.EVENTS, aggregateId) mustBe AggregateVersion.ZERO
    }

    Scenario("a committed transaction advances the subscription") {
      val aggregateId = newAggregateId()
      subscriptionsState.localTx { implicit session =>
        subscriptionsState.newVersion("CachesAfterRollbackProjection", SubscriptionType.EVENTS, aggregateId, AggregateVersion.ZERO, AggregateVersion(3))
      }
      subscriptionsState.lastVersion("CachesAfterRollbackProjection", SubscriptionType.EVENTS, aggregateId) mustBe AggregateVersion(3)
      new PostgresSubscriptionsState(typesNamesState)
        .lastVersion("CachesAfterRollbackProjection", SubscriptionType.EVENTS, aggregateId) mustBe AggregateVersion(3)
    }
  }

  Feature("Document cache (F16)") {

    Scenario("a document inserted by a rolled-back transaction is not cached") {
      val cache = new MapDocumentStoreCache[Note]
      val store = new PostgresDocumentStore[Note]("rollback_test", mpjsons, cache)
      try {
        failingTransaction { implicit session => store.insertDocument(0, 1, Note("a")) }

        store.getDocument(1) mustBe None
      } finally store.dropTable()
    }

    Scenario("an update rolled back after commit of the original keeps the committed document") {
      val cache = new MapDocumentStoreCache[Note]
      val store = new PostgresDocumentStore[Note]("rollback_test", mpjsons, cache)
      try {
        Given("a committed document 'a'")
        subscriptionsState.localTx { implicit session => store.insertDocument(0, 1, Note("a")) }

        When("a transaction changes it to 'b' and rolls back")
        failingTransaction { implicit session => store.updateDocument(0, 1, _ => io.reactivecqrs.core.documentstore.Document(Note("b"))) }

        Then("reads still see 'a'")
        store.getDocument(1).map(_.document) mustBe Some(Note("a"))
      } finally store.dropTable()
    }

    Scenario("a committed write is cached") {
      val cache = new MapDocumentStoreCache[Note]
      val store = new PostgresDocumentStore[Note]("rollback_test", mpjsons, cache)
      try {
        subscriptionsState.localTx { implicit session => store.insertDocument(0, 1, Note("a")) }
        cache.get(1) mustBe InCache(VersionedDocument(1, Note("a")))
      } finally store.dropTable()
    }
  }
}

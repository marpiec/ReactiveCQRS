package io.reactivecqrs.core.util

import java.util.concurrent.{Callable, Executors, TimeUnit}

import io.mpjsons.MPJsons
import io.reactivecqrs.core.commandhandler.PostgresCommandResponseState
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.rebuild.PostgresProjectionRegistry
import io.reactivecqrs.core.saga.{PostgresSagaSchemaInitializer, PostgresSagaState}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testkit.TestDatabase
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.util.Try

// F63, D69: nodes that start together initialize the schema one at a time.
class SchemaLockSpec extends AnyFeatureSpec with Matchers {

  TestDatabase.init()

  private val mpjsons = new MPJsons

  private def initializeAll(): Unit = {
    val typesNames = new PostgresTypesNamesState().initSchema()
    new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    new PostgresSubscriptionsState(typesNames).initSchema()
    new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
    new PostgresProjectionRegistry().initSchema()
    new PostgresSagaSchemaInitializer().initSchema()
    new PostgresSagaState(mpjsons, typesNames).initSchema()
  }

  Feature("Schema initialization (F63)") {

    Scenario("eight nodes that initialize the schema at once all succeed, ten times over") {
      val pool = Executors.newFixedThreadPool(8)
      try {
        val failures = (1 to 10).flatMap { _ =>
          val runs = (1 to 8).map(_ => pool.submit(new Callable[Try[Unit]] { def call(): Try[Unit] = Try(initializeAll()) }))
          runs.map(_.get(2, TimeUnit.MINUTES)).filter(_.isFailure).map(_.failed.get.getMessage)
        }
        failures mustBe empty
      } finally pool.shutdown()
    }

    Scenario("an initialization inside another one runs at once, on the same thread") {
      SchemaLock(SchemaLock(42)) mustBe 42
    }
  }
}

package io.reactivecqrs.core.commandhandler

import java.time.Instant
import java.util.UUID

import io.mpjsons.MPJsons
import io.reactivecqrs.api.{AggregateVersion, CustomSuccessResponse, FailureResponse, SuccessResponse}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testutils.TestDatabase
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import scalikejdbc._

import scala.concurrent.duration._

case class Receipt(item: String, count: Int)

// F9, F64, R7: stored responses of idempotent commands, in both adapters.
class CommandResponseStateSpec extends AnyFeatureSpec with Matchers {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()

  private var now = Instant.now
  private val memoryState = new MemoryCommandResponseState(retention = 1.day, clock = () => now)
  private val postgresState = new PostgresCommandResponseState(mpjsons, typesNamesState, retention = 1.day).initSchema()

  private def newKey(): String = "test-" + UUID.randomUUID()

  private def store(state: CommandResponseState, key: String, response: io.reactivecqrs.api.CustomCommandResponse[_]): Unit = {
    DB.localTx { implicit session => state.storeResponse(key, response) }
  }

  /** Makes a stored response look older than the retention period. */
  private def age(state: CommandResponseState, key: String): Unit = state match {
    case _: MemoryCommandResponseState => now = now.plusSeconds(2 * 24 * 3600)
    case _ => DB.autoCommit { implicit session =>
      sql"UPDATE commands_responses SET handling_timestamp = handling_timestamp - INTERVAL '2 days' WHERE key = $key".update()
    }
  }

  for ((name, state) <- List("memory" -> memoryState, "PostgreSQL" -> postgresState)) {

    Feature(s"Stored responses ($name)") {

      Scenario("a stored response is found by its key") {
        val key = newKey()
        store(state, key, SuccessResponse(AggregateId(7), AggregateVersion(3)))

        state.responseByKey(key) mustBe Some(SuccessResponse(AggregateId(7), AggregateVersion(3)))
        state.responseByKey(newKey()) mustBe None
      }

      Scenario("a response with custom info keeps its info (F64)") {
        val key = newKey()
        val response = CustomSuccessResponse(AggregateId(8), AggregateVersion(2), Receipt("apple", 3))
        store(state, key, response)

        state.responseByKey(key) mustBe Some(response)
      }

      Scenario("a failure response is stored as it is") {
        val key = newKey()
        store(state, key, FailureResponse(List("no stock")))

        state.responseByKey(key) mustBe Some(FailureResponse(List("no stock")))
      }

      Scenario("a second response for the same key is refused, and the first one stays") {
        val key = newKey()
        store(state, key, SuccessResponse(AggregateId(9), AggregateVersion(1)))

        an[IdempotencyKeyTakenException] must be thrownBy store(state, key, SuccessResponse(AggregateId(10), AggregateVersion(1)))
        state.responseByKey(key) mustBe Some(SuccessResponse(AggregateId(9), AggregateVersion(1)))
      }

      Scenario("responses older than the retention period are removed, newer ones stay") {
        val oldKey = newKey()
        store(state, oldKey, SuccessResponse(AggregateId(11), AggregateVersion(1)))
        age(state, oldKey)
        val newerKey = newKey()
        store(state, newerKey, SuccessResponse(AggregateId(12), AggregateVersion(1)))

        state.removeExpired() must be >= 1

        state.responseByKey(oldKey) mustBe None
        state.responseByKey(newerKey) mustBe Some(SuccessResponse(AggregateId(12), AggregateVersion(1)))
      }
    }
  }
}

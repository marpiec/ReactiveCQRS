package io.reactivecqrs.testdomain.spec

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId, SagaId, UserId}
import io.reactivecqrs.core.eventstore.NewEvents
import io.reactivecqrs.core.commandhandler.PostgresCommandResponseState
import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.saga.PostgresSagaState
import io.reactivecqrs.core.tools.StoredDataCheck
import io.reactivecqrs.core.tools.StoredDataCheck.ProjectionTable
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testdomain.shoppingcart.MultipleCartCreatorSaga.CreateMultipleCarts
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.ActorRef
import scalikejdbc._

// D78, F52: the dry run reads every stored JSON value and reports per class which rows cannot be read.
class StoredDataCheckSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
  private val sagas = new PostgresSagaState(mpjsons, typesNamesState)
  sagas.initSchema()
  private val documents = new PostgresDocumentStore[String]("stored_data_check", mpjsons, new NoopDocumentStoreCache[String, AnyRef])

  private val userId = UserId(1)

  private def check(): StoredDataCheck.Report =
    StoredDataCheck.run(mpjsons, typesNamesState, Seq(ProjectionTable[String]("stored_data_check")))

  private def result(report: StoredDataCheck.Report, table: String, className: String): StoredDataCheck.ClassResult =
    report.results.find(r => r.table == table && r.className == className).getOrElse(StoredDataCheck.ClassResult(table, className, 0, 0, Nil))

  Feature("Checking that every stored value can be read") {

    Scenario("reports the rows of every kind of stored data, and which of them cannot be read") {
      Given("a cart with two events, a saga, an idempotent response and a projection document")
      val cartId = AggregateId(System.nanoTime())
      eventStore.localTx { session =>
        eventStore.persistEvents(cartId, NewEvents[ShoppingCart](userId, AggregateVersion.ZERO,
          Instant.now, Seq(ShoppingCartCreated("Checked"), ItemAdded("apple"))))(session)
      }.get
      sagas.createSaga("StoredDataCheckSpec", SagaId(System.nanoTime()), "respondTo", CreateMultipleCarts(userId, "carts", 2))
      DB.localTx { implicit session =>
        responses.storeResponse("stored-data-check-" + System.nanoTime(), CustomSuccessResponse(cartId, AggregateVersion(2), ItemReceipt("apple")))
        documents.insertDocument(0, cartId.asLong, "Checked")
      }
      val before = check()

      When("one of the events is damaged and the cart has no space")
      DB.autoCommit { implicit session =>
        sql"UPDATE events SET event = 'not json' WHERE aggregate_id = ${cartId.asLong} AND version = 2".update()
        sql"UPDATE aggregates SET space_id = -1 WHERE id = ${cartId.asLong}".update()
      }
      val after = try check() finally {
        // Later specs in this schema read every stored event.
        val itemAdded = mpjsons.serialize(ItemAdded("apple"), classOf[ItemAdded].getName)
        DB.autoCommit { implicit session =>
          sql"UPDATE events SET event = $itemAdded WHERE aggregate_id = ${cartId.asLong} AND version = 2".update()
          sql"UPDATE aggregates SET space_id = 0 WHERE id = ${cartId.asLong}".update()
        }
      }

      Then("every kind of stored data was read")
      result(after, "events", classOf[ShoppingCartCreated].getName).rows must be >= 1L
      result(after, "sagas", classOf[CreateMultipleCarts].getName).rows must be >= 1L
      result(after, "commands_responses", classOf[CustomSuccessResponse[_]].getName).rows must be >= 1L
      result(after, "projection_stored_data_check", "java.lang.String").rows must be >= 1L

      And("exactly the damaged event became unreadable, with an example naming it")
      after.unreadable mustBe before.unreadable + 1
      val itemsAdded = result(after, "events", classOf[ItemAdded].getName)
      itemsAdded.unreadable mustBe result(before, "events", classOf[ItemAdded].getName).unreadable + 1
      itemsAdded.examples must not be empty

      And("the pre-check counts see the cart without a space")
      after.aggregatesWithoutSpace mustBe before.aggregatesWithoutSpace + 1
      after.eventsByTypeVersion.get(0).exists(_ >= 2) mustBe true
      after.text must include("unreadable")
    }
  }
}

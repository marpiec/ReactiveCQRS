package io.reactivecqrs.testdomain.spec

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SagaId, SpaceId, UserId}
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
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase, TestIds}
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
      val cartId = AggregateId(TestIds.aggregate())
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

    // F181: an application without sagas never makes the saga table.
    Scenario("F198, F197 names the versions stored twice, and the aggregate types no context given has") {
      TestDatabase.inSchema("repeated") {
        val typesNames = new PostgresTypesNamesState().initSchema()
        val store = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
        val (cartId, folderId) = (AggregateId(TestIds.aggregate()), AggregateId(TestIds.aggregate()))
        store.localTx(session => store.persistEvents(cartId, NewEvents[ShoppingCart](userId, AggregateVersion.ZERO, Instant.now,
          Seq(ShoppingCartCreated("Twice"), ItemAdded("apple"))))(session)).get
        store.localTx(session => store.persistEvents(folderId, NewEvents[Folder](userId, AggregateVersion.ZERO, Instant.now,
          Seq(FolderCreated(SpaceId(0), "Folder"))))(session)).get
        DB.autoCommit { implicit session => // as an import of 1.0.0 could leave it, before the index was unique
          sql"DROP INDEX events_aggregate_version_uidx".execute()
          sql"""INSERT INTO events (id, user_id, aggregate_id, event_time, version, event_type_id, event_type_version, event)
               | SELECT nextval('events_seq'), user_id, aggregate_id, event_time, version, event_type_id, 0, event FROM events
               | WHERE aggregate_id = ${cartId.asLong} AND version = 2""".stripMargin.update()
        }

        val report = StoredDataCheck.run(mpjsons, typesNames, contexts = Seq(new ShoppingCartAggregateContext))

        report.repeatedVersions mustBe Seq(StoredDataCheck.RepeatedVersion(cartId.asLong, 2, 2))
        report.typesWithoutContext mustBe Map(classOf[Folder].getName -> 1L)
        report.text must include("Versions stored twice")
      }
    }

    Scenario("checks a database that lacks some tables, and names the tables it did not find") {
      TestDatabase.inSchema("without_sagas") {
        Given("the tables of the event store only, with a cart")
        val typesNames = new PostgresTypesNamesState().initSchema()
        val store = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
        store.localTx { session =>
          store.persistEvents(AggregateId(TestIds.aggregate()), NewEvents[ShoppingCart](userId, AggregateVersion.ZERO,
            Instant.now, Seq(ShoppingCartCreated("Without sagas"))))(session)
        }.get

        When("the check runs, with a projection table that was never made among the listed ones")
        val report = StoredDataCheck.run(mpjsons, typesNames, Seq(ProjectionTable[String]("never_made")))

        Then("it reads the events, and names the tables it did not find")
        result(report, "events", classOf[ShoppingCartCreated].getName).rows mustBe 1L
        report.missingTables mustBe Seq("sagas", "commands_responses", "projection_never_made")
        report.text must include("Tables not found, so not read: sagas, commands_responses, projection_never_made")
      }
    }

    // G3: a document class generic in its content, as a store of AggregateWrapper[Kanban] holds, is read as the store
    // reads it (by its full type), not as an erased class whose type parameter mpjsons cannot bind.
    Scenario("G3 a table of a generic document class reads, its class named with its type arguments") {
      val generic = new PostgresDocumentStore[Wrapped[Item]]("stored_data_check_generic", mpjsons, new NoopDocumentStoreCache[Wrapped[Item], AnyRef])
      try {
        DB.localTx(implicit session => generic.insertDocument(0, TestIds.aggregate(), Wrapped(Item(1, "apple"))))

        val report = StoredDataCheck.run(mpjsons, typesNamesState, Seq(ProjectionTable[Wrapped[Item]]("stored_data_check_generic")))

        val read = report.results.filter(_.table == "projection_stored_data_check_generic")
        read.map(r => (r.rows, r.unreadable)) mustBe Seq((1L, 0L))
        read.head.className must include("Wrapped[")
      } finally generic.dropTable()
    }

    // F342 (CR-1): events whose aggregate lost its own row are read by nothing; the check names them.
    Scenario("F342 lists the events whose aggregate has no row of its own") {
      TestDatabase.inSchema("orphans") {
        val typesNames = new PostgresTypesNamesState().initSchema()
        val store = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
        val (kept, orphan) = (AggregateId(TestIds.aggregate()), AggregateId(TestIds.aggregate()))
        Seq(kept, orphan).foreach(id => store.localTx(session => store.persistEvents(id, NewEvents[ShoppingCart](userId, AggregateVersion.ZERO, Instant.now,
          Seq(ShoppingCartCreated("Orphan"), ItemAdded("apple"), ItemAdded("pear"))))(session)).get)
        DB.autoCommit(implicit session => sql"DELETE FROM aggregates WHERE id = ${orphan.asLong}".update())

        val report = StoredDataCheck.run(mpjsons, typesNames)

        (report.orphans.events, report.orphans.aggregates) mustBe ((3L, 1L))
        report.orphans.listed.map(o => (o.aggregateId, o.events, o.eventClasses)) mustBe
          Seq((orphan, 3L, Seq(classOf[ItemAdded].getName, classOf[ShoppingCartCreated].getName)))
        report.text must include("aggregate " + orphan.asLong + ": 3 events")
      }
    }
  }
}

/** G3: a generic document, as Neula's AggregateWrapper[X]. */
case class Wrapped[A](value: A)

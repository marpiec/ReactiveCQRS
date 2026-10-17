package io.reactivecqrs.core.documentstore

import io.mpjsons.MPJsons
import org.scalatest._
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import io.reactivecqrs.testutils.TestDatabase
import scalikejdbc.DB

class PostgresDocumentStoreSpec extends AnyFeatureSpec with GivenWhenThen with BeforeAndAfter with Matchers {

  TestDatabase.init()

  val mpjsons = new MPJsons

  val testTable = "test_table"

  after {

  }

  feature("can find documents by path with one array") {
    scenario("value exists") {
      Given("document store with some values")
      val documentStore = new PostgresDocumentStore[ComplexType](testTable, mpjsons, new NoopDocumentStoreCache)
      try {
        DB.localTx { implicit connection =>

          documentStore.insertDocument(0, 1, ComplexType(List(SimpleType(1), SimpleType(2))))
          documentStore.insertDocument(0, 2, ComplexType(List(SimpleType(1))))
          documentStore.insertDocument(0, 3, ComplexType(List(SimpleType(2), SimpleType(3))))

          When("document store is searched by array value")
          val result = documentStore.findDocumentByObjectInArray(List("simpleArray"), Seq("field"), 2, 10000)

          Then("correct documents are retrieved")
          result.keySet mustBe Set(1, 3)
        }
      } finally {
        // Outside the transaction: DROP TABLE on another connection would wait for its locks forever.
        documentStore.dropTable()
      }
    }
  }

  feature("can find by option value") {
    scenario("option value exists") {
      Given("document store with one value")
      val documentStore = new PostgresDocumentStore[OptionIntType](testTable, mpjsons, new NoopDocumentStoreCache)
      try {
        DB.localTx { implicit connection =>
          documentStore.insertDocument(0, 1, OptionIntType(Some(13)))

          When("searching by option value")
          val result = documentStore.findDocumentByPath(Seq("option", "value"), "13")

          Then("correct document is retrieved")
          result.keySet mustBe Set(1)
        }
      } finally {
        // Outside the transaction: DROP TABLE on another connection would wait for its locks forever.
        documentStore.dropTable()
      }
    }
  }

  feature("i can has auto id") {
    scenario("inserting document and getting auto id back") {
      Given("empty document store")
      val documentStore = new PostgresDocumentStoreAutoId[SimpleType](testTable, mpjsons, new NoopDocumentStoreCache)

      try {
        DB.localTx { implicit connection =>
          When("document is inserted")
          val result = documentStore.insertDocument(0, SimpleType(42))

          Then("id must be 1")
          result mustBe 1

          When("another document is inserted")
          val another = documentStore.insertDocument(0, SimpleType(44))

          Then("another id must be 2")
          another mustBe 2
        }
      } finally {
        // Outside the transaction: DROP TABLE on another connection would wait for its locks forever.
        documentStore.dropTable()
      }
    }
  }

  // F118: PostgreSQL cuts names past 63 characters; only the names a store makes count.
  feature("table and index names") {
    scenario("a table of 63 characters is accepted when the store has no index") {
      val table = "t" * (63 - "projection_".length)
      val documentStore = new PostgresDocumentStore[SimpleType](table, mpjsons, new NoopDocumentStoreCache)
      documentStore.dropTable()
    }

    scenario("a table whose index name would pass 63 characters is refused") {
      val table = "t" * (63 - "projection_".length - "_idx_1".length + 1)
      an[IllegalArgumentException] must be thrownBy new PostgresDocumentStore[SimpleType](table, mpjsons, new NoopDocumentStoreCache,
        Seq(MultipleLongIndex(1, Seq("field"))))
    }
  }

}

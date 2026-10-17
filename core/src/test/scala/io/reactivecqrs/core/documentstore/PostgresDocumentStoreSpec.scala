package io.reactivecqrs.core.documentstore

import io.mpjsons.MPJsons
import org.scalatest._
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import io.reactivecqrs.testutils.TestDatabase
import scalikejdbc._

case class Label(name: String)
case class Labels(labels: List[Label])

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

  // F158: the value is a parameter of the query, not a part of its text.
  feature("searching by an object in an array") {
    scenario("F158 a value with quotes finds its document, and a value made to change the query finds nothing") {
      val documentStore = new PostgresDocumentStore[Labels](testTable, mpjsons, new NoopDocumentStoreCache)
      try {
        DB.localTx { implicit session =>
          documentStore.insertDocument(0, 1, Labels(List(Label("O'Brien"), Label("plain"))))
          documentStore.insertDocument(0, 2, Labels(List(Label("other"), Label("back\\slash \"quoted\""))))

          documentStore.findDocumentByObjectInArray(List("labels"), Seq("name"), "O'Brien", 100).keySet mustBe Set(1)
          documentStore.findDocumentByObjectInArray(List("labels"), Seq("name"), "back\\slash \"quoted\"", 100).keySet mustBe Set(2)
          documentStore.findDocumentByObjectInArray(List("labels"), Seq("name"), "x\"}]' OR '1'='1' OR '[{\"name\":\"x", 100) mustBe empty
        }
      } finally {
        documentStore.dropTable()
      }
    }
  }

  // F159: the queries a store builds are valid with no condition, sorted by numbers, and without a limit.
  feature("queries without conditions, and queries of parts") {
    scenario("F159 a query without conditions finds every document; parts sort by numbers without a limit") {
      val documentStore = new PostgresDocumentStore[Item](testTable, mpjsons, new NoopDocumentStoreCache)
      try {
        DB.localTx { implicit session =>
          documentStore.insertDocument(0, 1, Item("a", 3, 30L, active = true, Nil, None))
          documentStore.insertDocument(0, 2, Item("b", 1, 10L, active = true, Nil, None))
          documentStore.insertDocument(0, 3, Item("c", 2, 20L, active = false, Nil, None))

          documentStore.findDocument(DocumentStoreQuery(Nil, Seq(SortAscInt(Seq("count"))), 0, -1)).map(_._1) mustBe Seq(2, 3, 1)
          documentStore.findDocumentPartByPaths[String](Seq("name"),
            DocumentStoreQuery(Seq(ExpectedSingleBooleanValue(Seq("active"), true)), Seq(SortDescInt(Seq("count"))), 0, -1)) mustBe Seq(1L -> "a", 2L -> "b")
          documentStore.findDocumentPartByPaths[String](Seq("name"),
            DocumentStoreQuery(Seq(ExpectedSingleBooleanValue(Seq("active"), true)), Seq(SortAscLong(Seq("total"))), 1, 1)) mustBe Seq(1L -> "a")
        }
      } finally {
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


  Feature("Finders read in the caller's transaction (F160)") {
    Scenario("a document written in a listener's transaction is found by the finders that take expected values") {
      val documentStore = new PostgresDocumentStore[OptionIntType](testTable, mpjsons, new NoopDocumentStoreCache)
      try {
        DB.localTx { implicit session =>
          documentStore.insertDocument(0, 1, OptionIntType(Some(13)))
          val thirteen = ExpectedSingleTextValue(Seq("option", "value"), "13")

          documentStore.findDocumentByPath(thirteen).keySet mustBe Set(1)
          documentStore.findDocumentByPaths(thirteen).keySet mustBe Set(1)
        }
      } finally {
        documentStore.dropTable()
      }
    }

    Scenario("outside a transaction they read on their own") {
      val documentStore = new PostgresDocumentStore[OptionIntType](testTable, mpjsons, new NoopDocumentStoreCache)
      try {
        DB.localTx { implicit session => documentStore.insertDocument(0, 1, OptionIntType(Some(13))) }
        val thirteen = ExpectedSingleTextValue(Seq("option", "value"), "13")

        documentStore.findDocumentByPath(thirteen).keySet mustBe Set(1)
        documentStore.findDocumentByPaths(thirteen).keySet mustBe Set(1)
      } finally {
        documentStore.dropTable()
      }
    }
  }

  Feature("Indexes whose definition changed (F171)") {
    def definitionOf(index: String): Option[String] = DB.readOnly { implicit session =>
      SQL(s"SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND indexname = '$index'").map(_.string(1)).single()
    }
    def oidOf(index: String): Option[Long] = DB.readOnly { implicit session =>
      SQL(s"SELECT '$index'::regclass::oid::bigint").map(_.long(1)).single()
    }

    Scenario("an index declared with the same id and a new path is built again on the new path; an unchanged one is kept") {
      val first = new PostgresDocumentStore[OptionIntType](testTable, mpjsons, new NoopDocumentStoreCache,
        Seq(MultipleTextIndex(1, Seq("option", "value")), UniqueLongIndex(2, Seq("number")), MultipleCombinedIndex(3, Seq(TextIndexPath(Seq("a")), IntIndexPath(Seq("b"))))))
      try {
        definitionOf("projection_test_table_idx_1").get must include ("'{option,value}'")
        val (kept, combined) = (oidOf("projection_test_table_idx_2"), oidOf("projection_test_table_idx_3"))

        new PostgresDocumentStore[OptionIntType](testTable, mpjsons, new NoopDocumentStoreCache,
          Seq(MultipleTextIndex(1, Seq("renamed")), UniqueLongIndex(2, Seq("number")), MultipleCombinedIndex(3, Seq(TextIndexPath(Seq("a")), IntIndexPath(Seq("b"))))))

        definitionOf("projection_test_table_idx_1").get must include ("'{renamed}'")
        oidOf("projection_test_table_idx_2") mustBe kept
        oidOf("projection_test_table_idx_3") mustBe combined
      } finally {
        first.dropTable()
      }
    }
  }
}

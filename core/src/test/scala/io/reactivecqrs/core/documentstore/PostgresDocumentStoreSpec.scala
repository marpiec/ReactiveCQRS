package io.reactivecqrs.core.documentstore

import io.mpjsons.MPJsons
import org.scalatest._
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import io.reactivecqrs.testkit.TestDatabase
import scalikejdbc._

case class Label(name: String)
case class Labels(labels: List[Label])
case class Attributes(values: Map[String, String])

class PostgresDocumentStoreSpec extends AnyFeatureSpec with GivenWhenThen with BeforeAndAfter with Matchers {

  TestDatabase.init()

  val mpjsons = new MPJsons

  val testTable = "test_table"

  after {

  }

  Feature("can find documents by path with one array") {
    Scenario("value exists") {
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

  Feature("can find by option value") {
    Scenario("option value exists") {
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

  Feature("i can has auto id") {
    Scenario("inserting document and getting auto id back") {
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
  Feature("searching by an object in an array") {
    Scenario("F158 a value with quotes finds its document, and a value made to change the query finds nothing") {
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
  Feature("queries without conditions, and queries of parts") {
    Scenario("F159 a query without conditions finds every document; parts sort by numbers without a limit") {
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
  Feature("table and index names") {
    Scenario("a table of 63 characters is accepted when the store has no index") {
      val table = "t" * (63 - "projection_".length)
      val documentStore = new PostgresDocumentStore[SimpleType](table, mpjsons, new NoopDocumentStoreCache)
      documentStore.dropTable()
    }

    Scenario("a table whose index name would pass 63 characters is refused") {
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

  Feature("Paths with any characters (F210)") {
    Scenario("F210 a key with a comma, a quote or a brace is one key, in conditions, sorts, parts and indexes") {
      val (odd, other) = ("it's {a,b}", "a,b")
      val store = new PostgresDocumentStore[Attributes]("test_paths", mpjsons, new NoopDocumentStoreCache, Seq(MultipleTextIndex(1, Seq("values", odd))))
      try {
        val built = DB.readOnly(implicit session => sql"SELECT 'projection_test_paths_idx_1'::regclass::oid::bigint".map(_.long(1)).single())
        new PostgresDocumentStore[Attributes]("test_paths", mpjsons, new NoopDocumentStoreCache, Seq(MultipleTextIndex(1, Seq("values", odd))))
        DB.readOnly(implicit session => sql"SELECT 'projection_test_paths_idx_1'::regclass::oid::bigint".map(_.long(1)).single()) mustBe built

        DB.localTx { implicit session =>
          // JSON objects with such keys, as a host's own JSON may have them (mpjsons writes a map as pairs)
          Seq(1L -> """{"name": "n", "values": {"a,b": "x", "it's {a,b}": "b"}}""", 2L -> """{"name": "n", "values": {"it's {a,b}": "a"}}""", 3L -> """{"name": "n", "values": {"a": "x"}}""")
            .foreach { case (id, json) => sql"INSERT INTO projection_test_paths (space_id, id, version, document) VALUES (0, $id, 1, $json::jsonb)".update() }
          def keys(where: ExpectedValue*): Set[Long] = store.findDocumentPartByPaths[String](Seq("name"), DocumentStoreQuery.basic(where)).map(_._1).toSet

          keys(ExpectedSingleTextValue(Seq("values", other), "x")) mustBe Set(1L)
          keys(ExpectedSingleTextValue(Seq("values", odd), "a")) mustBe Set(2L)
          keys(ExpectedSingleTextValue(Seq("values", "x}' = '' OR '1'='1"), "x")) mustBe empty
          store.findDocumentPartByPaths[String](Seq("values", odd), DocumentStoreQuery(Seq(ExpectedSingleTextValueLike(Seq("values", odd), "%")),
            Seq(SortAscText(Seq("values", odd))), 0, 10)) mustBe Seq(2L -> "a", 1L -> "b")
        }
      } finally {
        store.dropTable()
      }
    }
  }

  Feature("Queries without a limit (F211)") {
    Scenario("F211 a query without a limit fails beyond the store's maxRows; with a limit, Unbounded or findAll it does not") {
      val store = new PostgresDocumentStore[SimpleType]("test_cap", mpjsons, new NoopDocumentStoreCache, maxRows = 2)
      try {
        DB.localTx { implicit session =>
          (1 to 3).foreach(i => store.insertDocument(0, i, SimpleType(1)))
          a[TooManyDocumentsException] must be thrownBy store.findDocumentByPath(Seq("field"), "1")
          a[TooManyDocumentsException] must be thrownBy store.findDocumentPartByPaths[Int](Seq("field"), DocumentStoreQuery.basic(Nil))
          store.findDocument(DocumentStoreQuery(Nil, Nil, 0, 2)).size mustBe 2
          store.findDocument(DocumentStoreQuery(Nil, Nil, 0, DocumentStoreQuery.Unbounded)).size mustBe 3
          store.findAll().size mustBe 3
        }
      } finally {
        store.dropTable()
      }
    }
  }

  Feature("Order, offset and limit, as the memory store keeps them (F273)") {
    Scenario("F273 sorted, paged queries") {
      val store = new PostgresDocumentStore[Item]("test_pages", mpjsons, new NoopDocumentStoreCache)
      try {
        DB.localTx { implicit session =>
          OrderedItems.all.foreach { case (key, item) => store.insertDocument(0, key, item) }
          OrderedItems.queries.foreach { case (query, keys) => store.findDocument(query).map(_._1) mustBe keys }
        }
      } finally {
        store.dropTable()
      }
    }
  }

  Feature("The index on space_id (F217)") {
    Scenario("F217 a new table gets it; an older one gets it from the tool, built concurrently, once") {
      val store = new PostgresDocumentStore[Label]("f217_spaces", mpjsons, new NoopDocumentStoreCache)
      try {
        def indexes = DB.readOnly(implicit session => PostgresDocumentStoreTrait.spaceIndexes("projection_f217_spaces"))
        indexes.map(_._2) mustBe List(true)

        Given("a table created before 1.0.1, without it")
        DB.autoCommit(implicit session => SQL("DROP INDEX " + indexes.head._1).execute.apply())
        indexes mustBe empty

        Then("the tool builds it, and leaves it the next time")
        PostgresDocumentStore.buildSpaceIndexesConcurrently() must contain("projection_f217_spaces")
        indexes.map(_._2) mustBe List(true)
        PostgresDocumentStore.buildSpaceIndexesConcurrently() must not contain "projection_f217_spaces"
      } finally store.dropTable()
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

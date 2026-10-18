package io.reactivecqrs.core.documentstore

import org.scalatest.{BeforeAndAfter, GivenWhenThen}
import org.scalatest.featurespec.AnyFeatureSpecLike
import org.scalatest.matchers.must.Matchers._
import scalikejdbc.{DBSession, NoSession}

case class SimpleType(field: Int)

case class ComplexType(simpleArray: List[SimpleType])

case class OptionType(option: Option[SimpleType])

case class OptionIntType(option: Option[Int])

class MemoryDocumentStoreSpec extends AnyFeatureSpecLike with GivenWhenThen with BeforeAndAfter {

  Feature("can find documents by path with one array") {
    Scenario("value exists") {
      Given("document store with some values")
      val documentStore = new MemoryDocumentStore[ComplexType]()
      implicit val session = NoSession
      documentStore.insertDocument(0, 1, ComplexType(List(SimpleType(1), SimpleType(2))))
      documentStore.insertDocument(0, 2, ComplexType(List(SimpleType(1))))
      documentStore.insertDocument(0, 3, ComplexType(List(SimpleType(2), SimpleType(3))))

      When("document store is searched by array value")
      val result = documentStore.findDocumentByObjectInArray(List("simpleArray"), Seq("field"), 2, 10000)

      Then("correct documents are retrieved")
      result.keySet mustBe Set(1, 3)
    }
  }

  Feature("can find by option value") {
    Scenario("option value exists") {
      Given("document store with one value")
      val documentStore = new MemoryDocumentStore[OptionIntType]()
      implicit val session = NoSession
      documentStore.insertDocument(0, 1, OptionIntType(Some(13)))

      When("searching by option value")
      val result = documentStore.findDocumentByPath(Seq("option", "value"), "13")

      Then("correct document is retrieved")
      result.keySet mustBe Set(1)
    }
  }

  Feature("Order, offset and limit, as PostgreSQL keeps them (F273)") {
    Scenario("F273 sorted, paged queries, and a query without a limit beyond maxRows") {
      val store = new MemoryDocumentStore[Item](maxRows = 4)
      OrderedItems.all.foreach { case (key, item) => store.insertDocument(0, key, item)(NoSession) }
      OrderedItems.queries.foreach { case (query, keys) => assert(store.findDocument(query).map(_._1) == keys, query) }
      store.insertDocument(0, 5, OrderedItems.all.head._2)(NoSession)
      assertThrows[TooManyDocumentsException](store.findDocumentByPaths()(NoSession))
    }
  }

  // F14: every query condition the PostgreSQL store supports must work here too
  Feature("Query conditions") {

    def itemsStore(): MemoryDocumentStore[Item] = {
      val documentStore = new MemoryDocumentStore[Item]()
      documentStore.insertDocuments(0, Seq(
        DocumentToInsert(1, Item("apple", 1, 10L, active = true, List("red", "green"), Some("fresh"))),
        DocumentToInsert(2, Item("Application", 2, 20L, active = false, List("blue"), None)),
        DocumentToInsert(3, Item("banana", 3, 30L, active = true, List.empty, None))))(NoSession)
      documentStore
    }

    def keysFor(condition: ExpectedValue): Set[Long] = itemsStore().findDocument(DocumentStoreQuery.basic(Seq(condition))).map(_._1).toSet

    Scenario("numbers and booleans compare by value") {
      keysFor(ExpectedSingleIntValue(Seq("count"), 2)) mustBe Set(2)
      keysFor(ExpectedSingleLongValue(Seq("total"), 30L)) mustBe Set(3)
      keysFor(ExpectedSingleBooleanValue(Seq("active"), value = true)) mustBe Set(1, 3)
      keysFor(ExpectedGreaterThanIntValue(Seq("count"), 1)) mustBe Set(2, 3)
      keysFor(ExpectedLessThanIntValue(Seq("count"), 3)) mustBe Set(1, 2)
    }

    Scenario("a missing value matches an empty option") {
      keysFor(ExpectedNoValue(Seq("note", "value"))) mustBe Set(2, 3)
    }

    Scenario("a like pattern is matched case-insensitively, as ILIKE does") {
      keysFor(ExpectedSingleTextValueLike(Seq("name"), "app%")) mustBe Set(1, 2)
      keysFor(ExpectedSingleTextValueLike(Seq("name"), "b_nana")) mustBe Set(3)
    }

    Scenario("array elements are searched by text value") {
      keysFor(ExpectedSingleTextValueInArray(Seq("tags"), "red")) mustBe Set(1)
      keysFor(ExpectedMultipleTextValuesInArray(Seq("tags"), Seq("green", "blue"))) mustBe Set(1, 2)
    }
  }

  // F156: a test of clearing an organization against the memory store tests what PostgreSQL does.
  Feature("Spaces (F156)") {

    Scenario("clearing a space removes its documents only, however they were written") {
      implicit val session: DBSession = NoSession
      val store = new MemoryDocumentStore[SimpleType]()
      store.insertDocument(1, 10, SimpleType(1))
      store.insertDocuments(1, Seq(DocumentToInsert(11, SimpleType(2))))
      store.updateDocument(2, 20, _ => Document(SimpleType(3)))
      store.insertDocument(2, 21, SimpleType(4))
      store.updateDocument(5, 10, existing => Document(SimpleType(existing.get.document.field + 1))) // keeps space 1, as SQL does
      store.overwriteDocument(21, SimpleType(5))

      store.clearSpace(1)

      store.findAll().keySet mustBe Set(20, 21)
      store.clearSpace(2)
      store.findAll() mustBe empty
    }

    Scenario("the same in the store with ids of its own") {
      implicit val session: DBSession = NoSession
      val store = new MemoryDocumentStoreAutoId[SimpleType]()
      val kept = store.insertDocument(2, SimpleType(1))
      store.insertDocument(1, SimpleType(2))
      store.insertDocuments(1, Seq(SimpleType(3)))

      store.clearSpace(1)

      store.findAll().keySet mustBe Set(kept)
    }
  }
}

case class Item(name: String, count: Int, total: Long, active: Boolean, tags: List[String], note: Option[String])

/** F273: items, and sorted, paged queries of them with the keys both stores find. */
object OrderedItems {
  val all: Seq[(Long, Item)] = Seq(
    1L -> Item("b", 2, 200L, active = true, Nil, Some("x")),
    2L -> Item("a", 1, 100L, active = true, Nil, None),
    3L -> Item("d", 4, 400L, active = false, Nil, Some("z")),
    4L -> Item("c", 3, 300L, active = true, Nil, Some("y")))
  val queries: Seq[(DocumentStoreQuery, Seq[Long])] = Seq(
    DocumentStoreQuery(Nil, Seq(SortAscInt("count")), 0, -1) -> Seq(2L, 1L, 4L, 3L),
    DocumentStoreQuery(Nil, Seq(SortDescLong("total")), 1, 2) -> Seq(4L, 1L),
    DocumentStoreQuery(Seq(ExpectedSingleBooleanValue(Seq("active"), value = true)), Seq(SortDescText("name")), 0, 2) -> Seq(4L, 1L),
    DocumentStoreQuery(Nil, Seq(SortAscText(Seq("note", "value"))), 0, -1) -> Seq(1L, 4L, 3L, 2L),
    DocumentStoreQuery(Nil, Seq(SortDescText(Seq("note", "value"))), 0, 1) -> Seq(2L))
}

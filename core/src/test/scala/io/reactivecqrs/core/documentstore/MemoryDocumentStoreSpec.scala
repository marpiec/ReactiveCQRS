package io.reactivecqrs.core.documentstore

import org.scalatest.{BeforeAndAfter, GivenWhenThen, FeatureSpecLike}
import org.scalatest.MustMatchers._

case class NothingMetadata()

case class SimpleType(field: Int)

case class ComplexType(simpleArray: List[SimpleType])

case class OptionType(option: Option[SimpleType])

case class OptionIntType(option: Option[Int])

class MemoryDocumentStoreSpec extends FeatureSpecLike with GivenWhenThen with BeforeAndAfter {

  feature("can find documents by path with one array") {
    scenario("value exists") {
      Given("document store with some values")
      val documentStore = new MemoryDocumentStore[ComplexType, NothingMetadata]()
      documentStore.insertDocument(1, ComplexType(List(SimpleType(1), SimpleType(2))), NothingMetadata())
      documentStore.insertDocument(2, ComplexType(List(SimpleType(1))), NothingMetadata())
      documentStore.insertDocument(3, ComplexType(List(SimpleType(2), SimpleType(3))), NothingMetadata())

      When("document store is searched by array value")
      val result = documentStore.findDocumentByObjectInArray(List("simpleArray"), Seq("field"), 2)

      Then("correct documents are retrieved")
      result.keySet mustBe Set(1, 3)
    }
  }

  feature("can find by option value") {
    scenario("option value exists") {
      Given("document store with one value")
      val documentStore = new MemoryDocumentStore[OptionIntType, NothingMetadata]()
      documentStore.insertDocument(1, OptionIntType(Some(13)), NothingMetadata())

      When("searching by option value")
      val result = documentStore.findDocumentByPath(Seq("option", "value"), "13")

      Then("correct document is retrieved")
      result.keySet mustBe Set(1)
    }
  }
}

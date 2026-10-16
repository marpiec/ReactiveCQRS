package io.reactivecqrs.testdomain.spec

import java.nio.file.Paths

import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.api.{AggregateVersion, FailureResponse, SuccessResponse}
import io.reactivecqrs.testdomain.shoppingcart.MultipleCartCreatorSaga.CreateMultipleCarts
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.SchemaReference
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

// D78: stored formats (events, idempotent command responses, saga orders) must stay readable across codec changes.
// CustomSuccessResponse is not covered yet: it cannot be serialized by class name at all (F64).
class StoredFormatsSpec extends AnyFeatureSpec with Matchers {

  // Tests run from the build root; references are committed next to this spec.
  private val referenceDir = Paths.get("testdomain", "src", "test", "resources", "schema-reference")

  private val samples: Seq[AnyRef] = Seq(
    ShoppingCartCreated("Groceries"),
    ShoppingCartDuplicated(SpaceId(0), AggregateId(1001), AggregateVersion(3)),
    ItemAdded("apple"),
    ItemRemoved(1),
    ShoppingCartDeleted(),
    ShoppingCartChangesUndone(2),
    CartNameRewritten("Food"),
    SuccessResponse(AggregateId(1001), AggregateVersion(2)),
    FailureResponse(List("Cannot have more than 5 items in your cart")),
    CreateMultipleCarts(UserId(1), "Groceries", 3))

  Feature("Stored formats stay readable") {
    Scenario("events, command responses and saga orders match their committed references") {
      SchemaReference.verify(new MPJsons, referenceDir, samples) mustBe empty
    }
  }

  Feature("F51: stored JSON holds only the class's own data") {
    Scenario("an event carries no reflection data") {
      val mpjsons = new MPJsons
      mpjsons.serialize(ItemAdded("apple"), classOf[ItemAdded].getName) must not include "evidence"
    }
  }
}

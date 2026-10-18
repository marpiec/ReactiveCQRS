package io.reactivecqrs.examples.e11

// E11, testing (chapter 19): the golden test of the stored formats of every example.

// #formats
import java.nio.file.Paths

import io.mpjsons.MPJsons
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.examples.e1.{ItemAdded, ItemRemoved, ShoppingCartCreated}
import io.reactivecqrs.examples.e4.CheckoutSaga.{Charge, Checkout}
import io.reactivecqrs.examples.e4.{ItemsReleased, ItemsReserved, PaymentCharged, PaymentOpened, StockOpened}
import io.reactivecqrs.examples.e5.{ChangesUndone, ItemWritten, ListCopied, ListCreated, ListSquashed, ShoppingList}
import io.reactivecqrs.examples.e6.{ConcertScheduled, SeatBooked}
import io.reactivecqrs.examples.e7.{NoteAdded, TicketOpened}
import io.reactivecqrs.examples.e9.{ShopCartOpened, ShopCartSquashed, ShopItemAdded}
import io.reactivecqrs.testkit.SchemaReference
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

class StoredFormatsSpec extends AnyFeatureSpec with Matchers {

  // One JSON file per class, committed next to this spec; the tests run from the build's root.
  private val references = Paths.get("examples", "src", "test", "resources", "schema-reference")

  // A sample of every class the examples store: events, saga orders, and the states of snapshots.
  private val samples: Seq[AnyRef] = Seq(
    ShoppingCartCreated("Groceries"), ItemAdded("apples"), ItemRemoved(1),                            // E1
    StockOpened(10), ItemsReserved(2), ItemsReleased(2), PaymentOpened(10000), PaymentCharged(2500),  // E4
    Checkout(UserId(1), AggregateId(1), AggregateId(2), 2, 2500),
    Charge(UserId(1), AggregateId(1), AggregateId(2), 2, 2500),
    ListCreated("Camping"), ItemWritten("tent"), ChangesUndone(1),                                    // E5
    ListCopied(SpaceId(0), AggregateId(1), AggregateVersion(2)), ListSquashed(SpaceId(0), "Camping", Vector("tent")),
    ShoppingList("Camping", Vector("tent")),
    ConcertScheduled("Jazz night", 60), SeatBooked("customer 1"),                                     // E6
    TicketOpened("Printer jam"), NoteAdded("Paper reloaded"),                                         // E7
    ShopCartOpened(SpaceId(42), "Groceries"), ShopItemAdded("apples"),                                // E9
    ShopCartSquashed(SpaceId(42), "Groceries", Vector("apples")))

  Feature("Stored formats") {

    Scenario("every stored class reads its committed reference, and what it writes now reads back") {
      SchemaReference.verify(new MPJsons, references, samples) mustBe empty
    }
  }
}
// #formats

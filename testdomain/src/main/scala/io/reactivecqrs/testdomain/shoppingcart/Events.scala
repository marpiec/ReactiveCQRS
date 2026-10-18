package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api.id.{AggregateId, SpaceId}
import io.reactivecqrs.api._

// The regions (`// #name`) are code blocks of the book, docs/book (BookSnippetsSpec checks them).

// #created
case class ShoppingCartCreated(name: String) extends FirstEvent[ShoppingCart] {
  def spaceId: SpaceId = SpaceId(0)
}
// #created

// #duplicated
case class ShoppingCartDuplicated(spaceId: SpaceId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion)
  extends DuplicationEvent[ShoppingCart]
// #duplicated

case class ItemAdded(name: String) extends Event[ShoppingCart]

case class ItemRemoved(id: Int) extends Event[ShoppingCart]

case class ShoppingCartDeleted() extends Event[ShoppingCart]

// #undone
case class ShoppingCartChangesUndone(eventsCount: Int) extends UndoEvent[ShoppingCart]
// #undone

case class CartNameRewritten(name: String) extends Event[ShoppingCart]
/** The squash event of a cart (D48): its state at the squash point. */
case class CartSquashed(spaceId: SpaceId, name: String, items: Vector[Item]) extends FirstEvent[ShoppingCart]

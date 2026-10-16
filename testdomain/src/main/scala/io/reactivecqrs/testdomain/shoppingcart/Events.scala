package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api.id.{AggregateId, SpaceId}
import io.reactivecqrs.api._



case class ShoppingCartCreated(name: String) extends FirstEvent[ShoppingCart] {
  def spaceId: SpaceId = SpaceId(0)
}

case class ShoppingCartDuplicated(spaceId: SpaceId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion) extends DuplicationEvent[ShoppingCart]

case class ItemAdded(name: String) extends Event[ShoppingCart]

case class ItemRemoved(id: Int) extends Event[ShoppingCart]

case class ShoppingCartDeleted() extends Event[ShoppingCart]

case class ShoppingCartChangesUndone(eventsCount: Int) extends UndoEvent[ShoppingCart]

case class CartNameRewritten(name: String) extends Event[ShoppingCart]
/** The squash event of a cart (D48): its state at the squash point. */
case class CartSquashed(spaceId: SpaceId, name: String, items: Vector[Item]) extends FirstEvent[ShoppingCart]

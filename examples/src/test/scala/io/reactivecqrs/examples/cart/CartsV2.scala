package io.reactivecqrs.examples.cart

// Chapter 17's version 2.0.0 of the carts: a new event class, written once every node runs that version.

// #contextV2
import io.reactivecqrs.api._
import io.reactivecqrs.testdomain.shoppingcart._

case class ItemAddedV2(name: String, source: String) extends Event[ShoppingCart]

class ShoppingCartContextV2 extends ShoppingCartAggregateContext {
  override val writtenFrom: Map[Class[_], String] = Map(classOf[ItemAddedV2] -> "2.0.0")

  override def commandHandlers = cart => {
    case c: AddItemConcurrently if everyNodeRuns("2.0.0") => CommandSuccess(ItemAddedV2(c.name, "version 2"))
    case other => super.commandHandlers(cart)(other) // ItemAdded, as in 1.0.0
  }

  override def eventHandlers = (userId, timestamp, cart) => {
    case ItemAddedV2(name, _) => EventsHandlers.itemAdded(cart, ItemAdded(name))
    case other => super.eventHandlers(userId, timestamp, cart)(other)
  }
}
// #contextV2

package io.reactivecqrs.examples.e1

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}

// E1's domain: a shopping cart, its events, its commands, and the context that ties them together.

// #domain
case class ShoppingCart(name: String, items: Vector[String])

case class ShoppingCartCreated(name: String) extends FirstEvent[ShoppingCart] {
  def spaceId: SpaceId = SpaceId(0) // the organization the cart belongs to; 0 is the platform
}
case class ItemAdded(name: String) extends Event[ShoppingCart]

case class CreateShoppingCart(userId: UserId, name: String)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]]
case class AddItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, name: String)
  extends Command[ShoppingCart, CustomCommandResponse[_]]

class ShoppingCartContext extends AggregateContext[ShoppingCart] {
  override val version: Int = 1
  override def initialAggregateRoot: ShoppingCart = ShoppingCart("", Vector.empty)

  override def commandHandlers = cart => {
    case c: CreateShoppingCart => CommandSuccess(ShoppingCartCreated(c.name))
    case c: AddItem =>
      if (cart.items.size >= 5) CommandFailure("Cannot have more than 5 items in your cart")
      else CommandSuccess(ItemAdded(c.name))
  }

  override def eventHandlers = (userId, timestamp, cart) => {
    case e: ShoppingCartCreated => ShoppingCart(e.name, Vector.empty)
    case e: ItemAdded => cart.copy(items = cart.items :+ e.name)
  }
}
// #domain

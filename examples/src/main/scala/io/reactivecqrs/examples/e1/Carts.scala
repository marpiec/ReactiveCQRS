package io.reactivecqrs.examples.e1

// E1's domain (chapter 3): a shopping cart, its events, its commands, and the context that ties them together.

// #domain
// #imports
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
// #imports

// #state
case class ShoppingCart(name: String, items: Vector[Item])

case class Item(id: Int, name: String)
// #state

// #events
case class ShoppingCartCreated(name: String) extends FirstEvent[ShoppingCart] {
  def spaceId: SpaceId = SpaceId(0)
}

case class ItemAdded(name: String) extends Event[ShoppingCart]

case class ItemRemoved(id: Int) extends Event[ShoppingCart]
// #events

// #commands
case class CreateShoppingCart(userId: UserId, name: String)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]]

case class AddItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                   name: String) extends Command[ShoppingCart, CustomCommandResponse[_]]

case class RemoveItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                      id: Int) extends Command[ShoppingCart, CustomCommandResponse[_]]
// #commands

// #context
class ShoppingCartContext extends AggregateContext[ShoppingCart] {

  override val version: Int = 1

  override def initialAggregateRoot: ShoppingCart = ShoppingCart("", Vector.empty)

  // #rules
  override def commandHandlers = cart => {
    case c: CreateShoppingCart =>
      CommandSuccess(ShoppingCartCreated(c.name))
    // #addItemRule
    case c: AddItem =>
      if (cart.items.size >= 5) CommandFailure("Cannot have more than 5 items in your cart")
      else CommandSuccess(ItemAdded(c.name))
    // #addItemRule
    case c: RemoveItem =>
      if (cart.items.exists(_.id == c.id)) CommandSuccess(ItemRemoved(c.id))
      else CommandFailure("There is no item " + c.id + " in the cart")
  }
  // #rules

  // #handlers
  override def eventHandlers = (userId, timestamp, cart) => {
    case e: ShoppingCartCreated =>
      ShoppingCart(e.name, Vector.empty)
    case e: ItemAdded =>
      val nextId = cart.items.foldLeft(0)((maxId, item) => math.max(maxId, item.id)) + 1
      cart.copy(items = cart.items :+ Item(nextId, e.name))
    case e: ItemRemoved =>
      cart.copy(items = cart.items.filterNot(_.id == e.id))
  }
  // #handlers
}
// #context
// #domain

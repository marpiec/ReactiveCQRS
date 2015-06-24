package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api._

import CommandsHandlers._
import EventsHandlers._

class ShoppingCartCommandBus extends AggregateCommandBus[ShoppingCart] {


  override def commandHandlers = (shoppingCart: ShoppingCart) => {
    case command: CreateShoppingCart => createShoppingCart(command)
    case command: AddItem => addItem(shoppingCart)(command)
    case command: RemoveItem => removeItem(command)
    case command: DeleteShoppingCart => deleteShoppingCart()(command)
  }

  override def eventHandlers = (shoppingCart: ShoppingCart) => {
    case event: ShoppingCartCreated => shoppingCartCreated(event)
    case event: ItemAdded => itemAdded(shoppingCart, event)
    case event: ItemRemoved => itemRemoved(shoppingCart, event)
  }

  override val initialAggregateRoot: ShoppingCart = ShoppingCart("", Vector())

}

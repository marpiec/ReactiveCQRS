package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api._

import CommandsHandlers._
import EventsHandlers._

class ShoppingCartCommandBus extends AggregateCommandBus[ShoppingCart] {

  def isAllowed(next: => CommandHandlingResult[Any]) = {
    if(true) {
      next
    } else {
      Failure(CommandFailure("User not allowed"))
    }
  }

  override def commandHandlers = (shoppingCart: ShoppingCart) => {
    case command: CreateShoppingCart => isAllowed(createShoppingCart(command))
    case command: AddItem => addItem(shoppingCart)(command)
    case command: RemoveItem => removeItem(command)
    case command: DeleteShoppingCart => deleteShoppingCart()(command)
  }

  override def eventHandlers = (shoppingCart: ShoppingCart) => {
    case event: ShoppingCartCreated => shoppingCartCreated(event)
    case event: ItemAdded => itemAdded(shoppingCart, event)
    case event: ItemRemoved => itemRemoved(shoppingCart, event)
  }

  override val initialState: ShoppingCart = ShoppingCart("", Vector())

}

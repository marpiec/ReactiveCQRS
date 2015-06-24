package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api._

import CommandsHandlers._

class ShoppingCartCommandBus extends AggregateCommandBus[ShoppingCart] {
//
//  def isAllowed(function: CommandHandlingResult[Any]) = {
//
//    if(true) {
//      function
//    } else {
//      (a: ShoppingCart, a2: Any) => Failure()
//    }
//
//  }

  val commandHandlers:ShoppingCart => PartialFunction[Any, CommandHandlingResult[Any]] = (aggregateRoot: ShoppingCart) => {
    case command: CreateShoppingCart => createShoppingCart(aggregateRoot)(command)
    case command: AddItem => addItem(command)
    case command: RemoveItem => removeItem(command)
    case command: DeleteShoppingCart => deleteShoppingCart()(command)
  }

  override val eventsHandlers = Seq(
    ShoppingCartCreatedHandler,
    ItemAddedHandler,
    ItemRemovedHandler)

  override val initialState: ShoppingCart = ShoppingCart("", Vector())

}

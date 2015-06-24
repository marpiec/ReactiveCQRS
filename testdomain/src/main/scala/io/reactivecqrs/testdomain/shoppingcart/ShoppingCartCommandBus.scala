package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api._

import CommandsHandlers._

class ShoppingCartCommandBus extends AggregateCommandBus[ShoppingCart] {
//
//  addCommandHandler[CreateShoppingCart](createShoppingCart _)
//  addCommandHandler[AddItem](addItem() _)
//  addCommandHandler[RemoveItem](removeItem() _)
//  addCommandHandler[DeleteShoppingCart](deleteShoppingCart() _)


  def commandHandlers = {
    case _:CreateShoppingCart => createShoppingCart()
    case _:AddItem => addItem()
    case _:RemoveItem => removeItem()
    case _:DeleteShoppingCart => deleteShoppingCart()
  }

  override val eventsHandlers = Seq(
    ShoppingCartCreatedHandler,
    ItemAddedHandler,
    ItemRemovedHandler)
}

package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api._

import CommandsHandlers._
import EventsHandlers._

class ShoppingCartAggregateContext extends AggregateContext[ShoppingCart] {


  override def commandHandlers = (shoppingCart: ShoppingCart) => {
    case c: CreateShoppingCart => createShoppingCart(c.userId, c)
    case c: DuplicateShoppingCart => duplicateShoppingCart(c)
    case c: AddItem => addItem(c.userId, c.aggregateId, c.expectedVersion, shoppingCart)(c)
    case c: RemoveItem => removeItem(c)
    case c: DeleteShoppingCart => deleteShoppingCart(c)
    case c: UndoShoppingCartChange => undoShoppingCartChange(c)
  }

  override def eventHandlers = (shoppingCart: ShoppingCart) => {
    case e: ShoppingCartDuplicated => shoppingCart
    case e: ShoppingCartCreated => shoppingCartCreated(e)
    case e: ItemAdded => itemAdded(shoppingCart, e)
    case e: ItemRemoved => itemRemoved(shoppingCart, e)
    case e: ShoppingCartDeleted => null
    case e: ShoppingCartChangesUndone => shoppingCart // TODO - this event should not need to be handled
  }

  override def initialAggregateRoot: ShoppingCart = ShoppingCart("", Vector())

}

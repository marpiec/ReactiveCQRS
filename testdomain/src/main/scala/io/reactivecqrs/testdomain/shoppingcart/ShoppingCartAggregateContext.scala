package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api._
import CommandsHandlers._
import EventsHandlers._

class ShoppingCartAggregateContext extends AggregateContext[ShoppingCart] {

  override val version: Int = 1

  override val eventsVersions = EV[ShoppingCartCreated](0 -> classOf[ShoppingCartCreated]) :: Nil

  override def commandHandlers = shoppingCart => {
    case c: CreateShoppingCart => createShoppingCart(c.userId, c)
    case c: DuplicateShoppingCart => duplicateShoppingCart(c)
    case c: AddItem => addItem(c.userId, c.aggregateId, c.expectedVersion, shoppingCart)(c)
    case c: RemoveItem => removeItem(c)
    case c: DeleteShoppingCart => deleteShoppingCart(c)
    case c: UndoShoppingCartChange => undoShoppingCartChange(c)
  }

  override def rewriteHistoryCommandHandlers = (events, shoppingCart) => {
    case c: RewriteCartName => rewriteCartName(c, events, shoppingCart)
  }

  override def eventHandlers = (userId, timestamp, shoppingCart) => {
    case e: ShoppingCartDuplicated => shoppingCart
    case e: ShoppingCartCreated => shoppingCartCreated(e)
    case e: ItemAdded => itemAdded(shoppingCart, e)
    case e: ItemRemoved => itemRemoved(shoppingCart, e)
    case e: ShoppingCartDeleted => null
    case e: ShoppingCartChangesUndone => shoppingCart // TODO - this event should not need to be handled
    case e: CartNameRewritten => cartNameRewritten(shoppingCart, e)
  }

  override def initialAggregateRoot: ShoppingCart = ShoppingCart("", Vector())

}

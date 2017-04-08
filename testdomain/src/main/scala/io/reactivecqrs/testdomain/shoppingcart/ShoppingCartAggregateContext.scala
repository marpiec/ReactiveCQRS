package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api._
import CommandsHandlers._
import EventsHandlers._

import scala.concurrent.Future

class ShoppingCartAggregateContext extends AggregateContext[ShoppingCart] {

  override val eventsVersions = EV[ShoppingCartCreated](0 -> classOf[ShoppingCartCreated]) :: Nil

  override def commandHandlers = (shoppingCart) => {
    case c: CreateShoppingCart => createShoppingCart(c.userId, c)
    case c: DuplicateShoppingCart => Future.successful(duplicateShoppingCart(c))
    case c: AddItem => addItem(c.userId, c.aggregateId, c.expectedVersion, shoppingCart)(c)
    case c: RemoveItem => Future.successful(removeItem(c))
    case c: DeleteShoppingCart => Future.successful(deleteShoppingCart(c))
    case c: UndoShoppingCartChange => Future.successful(undoShoppingCartChange(c))
  }

  override def eventHandlers = (userId, timestamp, shoppingCart) => {
    case e: ShoppingCartDuplicated => shoppingCart
    case e: ShoppingCartCreated => shoppingCartCreated(e)
    case e: ItemAdded => itemAdded(shoppingCart, e)
    case e: ItemRemoved => itemRemoved(shoppingCart, e)
    case e: ShoppingCartDeleted => null
    case e: ShoppingCartChangesUndone => shoppingCart // TODO - this event should not need to be handled
  }

  override def initialAggregateRoot: ShoppingCart = ShoppingCart("", Vector())

}

package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.SpaceId
import CommandsHandlers._
import EventsHandlers._

class ShoppingCartAggregateContext extends AggregateContext[ShoppingCart] {

  override val version: Int = 1


  override def commandHandlers = shoppingCart => {
    case c: CreateShoppingCart => createShoppingCart(c.userId, c)
    case c: DuplicateShoppingCart => duplicateShoppingCart(c)
    case c: AddItem => addItem(c.userId, c.aggregateId, c.expectedVersion, shoppingCart)(c)
    case c: AddItemConcurrently => CommandSuccess(ItemAdded(c.name))
    case c: AddItems => CommandSuccess(c.names.map(ItemAdded))
    case c: AddItemOnBehalf => CommandSuccess(ItemAdded(c.name))
    case c: CreateShoppingCartWithId => CommandSuccess(ShoppingCartCreated(c.name))
    case c: AddItemWithReceipt => CommandSuccess(ItemAdded(c.name), ItemReceipt(c.name))
    case _: CheckCart => CommandSuccess(Seq.empty[Event[ShoppingCart]])
    case c: RemoveItem => removeItem(c)
    case c: DeleteShoppingCart => deleteShoppingCart(c)
    case c: UndoShoppingCartChange => undoShoppingCartChange(c)
  }

  override def rewriteHistoryCommandHandlers = (events, shoppingCart) => {
    case c: RewriteCartName => rewriteCartName(c.name, events)
    case c: RewriteCartNameConcurrently => rewriteCartName(c.name, events)
  }

  override def eventHandlers = (userId, timestamp, shoppingCart) => {
    case e: ShoppingCartDuplicated => shoppingCart
    case e: ShoppingCartCreated => shoppingCartCreated(e)
    case e: ItemAdded => itemAdded(shoppingCart, e)
    case e: ItemRemoved => itemRemoved(shoppingCart, e)
    case e: ShoppingCartDeleted => null
    case e: ShoppingCartChangesUndone => shoppingCart // TODO - this event should not need to be handled
    case e: CartNameRewritten => cartNameRewritten(shoppingCart, e)
    case e: CartSquashed => ShoppingCart(e.name, e.items)
  }

  override val squash: Option[SquashPolicy[ShoppingCart]] = Some(SquashPolicy(cart => CartSquashed(SpaceId(0), cart.name, cart.items)))

  override def initialAggregateRoot: ShoppingCart = ShoppingCart("", Vector())

}

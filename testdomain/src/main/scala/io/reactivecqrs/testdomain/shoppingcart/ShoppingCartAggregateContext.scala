package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.SpaceId
import CommandsHandlers._
import EventsHandlers._

// The regions (`// #name`) are code blocks of the book, docs/book (BookSnippetsSpec checks them).
class ShoppingCartAggregateContext extends AggregateContext[ShoppingCart] {

  override val version: Int = 1


  override def commandHandlers = shoppingCart => {
    case c: CreateShoppingCart => createShoppingCart(c.userId, c)
    // #duplicateHandler
    case c: DuplicateShoppingCart =>
      CommandSuccess(ShoppingCartDuplicated(SpaceId(0), c.baseAggregateId, c.baseAggregateVersion))
    // #duplicateHandler
    case c: AddItem => addItem(c.userId, c.aggregateId, c.expectedVersion, shoppingCart)(c)
    case c: AddItemConcurrently => CommandSuccess(ItemAdded(c.name))
    // #addItems
    case c: AddItems => CommandSuccess(c.names.map(ItemAdded))
    // #addItems
    case c: AddItemOnBehalf => CommandSuccess(ItemAdded(c.name))
    case c: CreateShoppingCartWithId => CommandSuccess(ShoppingCartCreated(c.name))
    // #receipt
    case c: AddItemWithReceipt => CommandSuccess(ItemAdded(c.name), ItemReceipt(c.name))
    // #receipt
    // #checkCart
    case _: CheckCart => CommandSuccess(Seq.empty[Event[ShoppingCart]])
    // #checkCart
    case c: RemoveItem => removeItem(c)
    case c: DeleteShoppingCart => deleteShoppingCart(c)
    // #undoHandler
    case c: UndoShoppingCartChange => CommandSuccess(ShoppingCartChangesUndone(c.stepsToUndo))
    // #undoHandler
  }

  override def rewriteHistoryCommandHandlers = (events, shoppingCart) => {
    case c: RewriteCartName => rewriteCartName(c.name, events)
    case c: RewriteCartNameConcurrently => rewriteCartName(c.name, events)
  }

  // #eventHandlers
  override def eventHandlers = (userId, timestamp, shoppingCart) => {
    // #duplicatedHandler
    case e: ShoppingCartDuplicated => shoppingCart // the state that arrives is already the copy
    // #duplicatedHandler
    case e: ShoppingCartCreated => shoppingCartCreated(e)
    case e: ItemAdded => itemAdded(shoppingCart, e)
    case e: ItemRemoved => itemRemoved(shoppingCart, e)
    // #deletedHandler
    case e: ShoppingCartDeleted => null
    // #deletedHandler
    case e: CartNameRewritten => cartNameRewritten(shoppingCart, e)
    case e: CartSquashed => ShoppingCart(e.name, e.items)
  }
  // #eventHandlers

  override val squash: Option[SquashPolicy[ShoppingCart]] = Some(SquashPolicy(cart => CartSquashed(SpaceId(0), cart.name, cart.items)))

  override def initialAggregateRoot: ShoppingCart = ShoppingCart("", Vector())

}

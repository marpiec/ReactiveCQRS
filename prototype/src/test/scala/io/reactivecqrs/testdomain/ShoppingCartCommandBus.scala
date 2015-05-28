package io.reactivecqrs.testdomain

import io.reactivecqrs.core._
import io.reactivecqrs.testdomain.api._


class ShoppingCartCommandBus extends AggregateCommandBus[ShoppingCart] {

  override val commandsHandlers = Seq(
    new CreateShoppingCartHandler(),
    new AddItemHandler(),
    new RemoveItemHandler(),
    new DeleteShoppingCartHandler())

  override val eventsHandlers = Seq(
    ShoppingCartCreatedHandler,
    ItemAddedHandler,
    ItemRemovedHandler)
}

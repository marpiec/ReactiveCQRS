package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api.AggregateCommandBus


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

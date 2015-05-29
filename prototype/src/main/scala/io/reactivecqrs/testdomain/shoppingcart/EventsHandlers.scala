package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api.{FirstEventHandler, EventHandler}



object ShoppingCartCreatedHandler extends FirstEventHandler[ShoppingCart, ShoppingCartCreated] {
  override def handle(event: ShoppingCartCreated): ShoppingCart = {
    ShoppingCart(event.name, Vector())
  }
}

object ItemAddedHandler extends EventHandler[ShoppingCart, ItemAdded] {
  override def handle(aggregateRoot: ShoppingCart, event: ItemAdded): ShoppingCart = {
    val itemId = aggregateRoot.items.foldLeft(0)((maxId, item) => math.max(maxId, item.id)) + 1
    val item = Item(itemId, event.name)
    aggregateRoot.copy(items = aggregateRoot.items :+ item)
  }
}

object ItemRemovedHandler extends EventHandler[ShoppingCart, ItemRemoved] {
  override def handle(aggregateRoot: ShoppingCart, event: ItemRemoved): ShoppingCart = {
    aggregateRoot.copy(items = aggregateRoot.items.filterNot(_.id == event.id))
  }
}
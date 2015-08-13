package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api.Event



case class ShoppingCartCreated(name: String) extends Event[ShoppingCart]

case class ItemAdded(name: String) extends Event[ShoppingCart]

case class ItemRemoved(id: Int) extends Event[ShoppingCart]

case class ShoppingCartDeleted() extends Event[ShoppingCart]

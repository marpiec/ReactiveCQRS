package io.reactivecqrs.testdomain.api

import io.reactivecqrs.core.{FirstEvent, Event}



case class ShoppingCartCreated(name: String) extends FirstEvent[ShoppingCart]

case class ItemAdded(name: String) extends Event[ShoppingCart]

case class ItemRemoved(id: Int) extends Event[ShoppingCart]

case class ShoppingCartDeleted() extends Event[ShoppingCart]

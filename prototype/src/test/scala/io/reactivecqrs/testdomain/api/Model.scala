package io.reactivecqrs.testdomain.api

case class ShoppingCart(name: String, items: Vector[Item])

case class Item(id: Int, name: String)

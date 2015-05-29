package io.reactivecqrs.testdomain.shoppingcart

case class ShoppingCart(name: String, items: Vector[Item])

case class Item(id: Int, name: String)

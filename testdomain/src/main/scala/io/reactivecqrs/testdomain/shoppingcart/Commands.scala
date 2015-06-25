package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api._


case class CreateShoppingCart(name: String) extends Command[ShoppingCart, CommandResponse]

case class AddItem(name: String) extends Command[ShoppingCart, CommandResponse]

case class RemoveItem(id: Int) extends Command[ShoppingCart, CommandResponse]

case class UndoShoppingCartChange(stepsToUndo: Int) extends Command[ShoppingCart, CommandResponse]

case class DeleteShoppingCart() extends Command[ShoppingCart, CommandResponse]


package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api._


case class CreateShoppingCart(name: String) extends FirstCommand[ShoppingCart, CommandResult]

case class AddItem(name: String) extends Command[ShoppingCart, CommandResult]

case class RemoveItem(id: Int) extends Command[ShoppingCart, CommandResult]

case class UndoShoppingCartChange(stepsToUndo: Int) extends Command[ShoppingCart, CommandResult]

case class DeleteShoppingCart() extends Command[ShoppingCart, CommandResult]


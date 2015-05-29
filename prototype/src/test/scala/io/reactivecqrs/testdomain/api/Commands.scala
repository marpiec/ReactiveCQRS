package io.reactivecqrs.testdomain.api

import io.reactivecqrs.api.{AggregateVersion, CommandResult, FirstCommand, Command}
import io.reactivecqrs.api.guid.AggregateId


case class CreateShoppingCart(name: String) extends FirstCommand[ShoppingCart, CommandResult]

case class AddItem(name: String) extends Command[ShoppingCart, CommandResult]

case class RemoveItem(id: Int) extends Command[ShoppingCart, CommandResult]

case class UndoShoppingCartChange(stepsToUndo: Int) extends Command[ShoppingCart, CommandResult]

case class DeleteShoppingCart() extends Command[ShoppingCart, CommandResult]


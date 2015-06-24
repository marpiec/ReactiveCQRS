package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api.Success

object CommandsHandlers {

//  val logCommand: AbstractCommand[ShoppingCart] => AbstractCommand[ShoppingCart] = command => {
//    println(command)
//    command
//  }

  val createShoppingCart = () => (command: CreateShoppingCart) => {
    Success(ShoppingCartCreated(command.name))
  }

  val addItem = () => (command: AddItem) => {
    Success(ItemAdded(command.name))
  }

  val removeItem = () => (command: RemoveItem) => {
    Success(ItemRemoved(command.id))
  }

  val deleteShoppingCart = () => (command: DeleteShoppingCart) => {
    Success(ShoppingCartDeleted())
  }

}


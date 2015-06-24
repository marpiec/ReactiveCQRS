package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api.Success

object CommandsHandlers {

//  val logCommand: AbstractCommand[ShoppingCart] => AbstractCommand[ShoppingCart] = command => {
//    println(command)
//    command
//  }

  val createShoppingCart = () => (aggregate: ShoppingCart, command: CreateShoppingCart) => {
    Success(ShoppingCartCreated(command.name))
  }

  val addItem = () => (aggregate: ShoppingCart, command: AddItem) => {
    Success(ItemAdded(command.name))
  }

  val removeItem = () => (aggregate: ShoppingCart, command: RemoveItem) => {
    Success(ItemRemoved(command.id))
  }

  val deleteShoppingCart = () => (aggregate: ShoppingCart, command: DeleteShoppingCart) => {
    Success(ShoppingCartDeleted())
  }

}


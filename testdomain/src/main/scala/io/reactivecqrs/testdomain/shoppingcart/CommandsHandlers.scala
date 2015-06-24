package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api.Success

object CommandsHandlers {

  def createShoppingCart(command: CreateShoppingCart) = {
    Success(ShoppingCartCreated(command.name))
  }

  def addItem()(command: AddItem) = {
    Success(ItemAdded(command.name))
  }

  def removeItem()(command: RemoveItem) = {
    Success(ItemRemoved(command.id))
  }

  def deleteShoppingCart()(command: DeleteShoppingCart) = {
    Success(ShoppingCartDeleted())
  }

}


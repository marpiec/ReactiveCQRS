package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api.{FailureResponse, Failure, Success}

object CommandsHandlers {


  def createShoppingCart(command: CreateShoppingCart) = {
    Success(ShoppingCartCreated(command.name))
  }

  def addItem(shoppingCart: ShoppingCart)(command: AddItem) = {
    if(shoppingCart.items.size > 5) {
      Failure(FailureResponse("Cannot have more than 5 items in your cart"))
    } else {
      Success(ItemAdded(command.name))
    }
  }

  def removeItem(command: RemoveItem) = {
    Success(ItemRemoved(command.id))
  }

  def deleteShoppingCart()(command: DeleteShoppingCart) = {
    Success(ShoppingCartDeleted())
  }

}


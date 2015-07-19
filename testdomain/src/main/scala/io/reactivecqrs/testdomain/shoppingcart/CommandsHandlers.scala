package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api.{AggregateVersion, FailureResponse, Failure, Success}

object CommandsHandlers {


  def createShoppingCart(userId: UserId, command: CreateShoppingCart) = {
    Success(ShoppingCartCreated(command.name))
  }

  def addItem(userId: UserId,
              aggregateId: AggregateId, expectedVersion: AggregateVersion,
              shoppingCart: ShoppingCart)(command: AddItem) = {
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


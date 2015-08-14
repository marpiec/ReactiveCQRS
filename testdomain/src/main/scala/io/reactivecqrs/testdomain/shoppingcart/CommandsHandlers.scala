package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api._

object CommandsHandlers {


  def createShoppingCart(userId: UserId, command: CreateShoppingCart) = {
    CommandSuccess(ShoppingCartCreated(command.name))
  }

  def addItem(userId: UserId,
              aggregateId: AggregateId, expectedVersion: AggregateVersion,
              shoppingCart: ShoppingCart)(command: AddItem) = {
    if(shoppingCart.items.size > 5) {
      CommandFailure(FailureResponse("Cannot have more than 5 items in your cart"))
    } else {
      CommandSuccess(ItemAdded(command.name))
    }
  }

  def removeItem(command: RemoveItem) = {
    CommandSuccess(ItemRemoved(command.id))
  }

  def deleteShoppingCart()(command: DeleteShoppingCart) = {
    CommandSuccess(ShoppingCartDeleted())
  }

}


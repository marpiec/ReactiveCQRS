package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api._

import scala.concurrent.Future

import scala.concurrent.ExecutionContext.Implicits.global

object CommandsHandlers {


  def createShoppingCart(userId: UserId, command: CreateShoppingCart) = {
    Future {
      Thread.sleep(200)
      if (command.name.endsWith("M 4")) {
        CommandFailure("Cannot add cart *M 4")
      } else {
        CommandSuccess(ShoppingCartCreated(command.name))
      }
    }
  }

  def duplicateShoppingCart(command: DuplicateShoppingCart) = {
    CommandSuccess(ShoppingCartDuplicated(command.baseAggregateId, command.baseAggregateVersion))
  }

  def addItem(userId: UserId,
              aggregateId: AggregateId, expectedVersion: AggregateVersion,
              shoppingCart: ShoppingCart)(command: AddItem) = {
    AsyncCommandResult(
      Future {
        if (shoppingCart.items.size > 5) {
          CommandFailure("Cannot have more than 5 items in your cart")
        } else {
          CommandSuccess(ItemAdded(command.name))
        }
      }
    )
  }

  def removeItem(command: RemoveItem) = {
    CommandSuccess(ItemRemoved(command.id))
  }

  def deleteShoppingCart(command: DeleteShoppingCart) = {
    CommandSuccess(ShoppingCartDeleted())
  }

  def undoShoppingCartChange(command: UndoShoppingCartChange) = {
    CommandSuccess(ShoppingCartChangesUndone(command.stepsToUndo))
  }

}


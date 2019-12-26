package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
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
    CommandSuccess(ShoppingCartDuplicated(SpaceId(0), command.baseAggregateId, command.baseAggregateVersion))
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

  def rewriteCartName(command: RewriteCartName, events: Iterable[EventWithVersion[ShoppingCart]], shoppingCart: ShoppingCart) = {
    val rewritten: Iterable[EventWithVersion[ShoppingCart]] = events.map(ev => ev.event match {
      case e: ShoppingCartCreated =>
        println(e)
        EventWithVersion(ev.version, e.copy(name = command.name))
      case e => println(e)
        ev
    })
    RewriteCommandSuccess(rewritten.toSeq, CartNameRewritten(command.name))
  }

}


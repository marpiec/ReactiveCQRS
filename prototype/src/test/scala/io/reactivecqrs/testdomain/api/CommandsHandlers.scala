package io.reactivecqrs.testdomain.api

import io.reactivecqrs.api.guid.AggregateId
import io.reactivecqrs.core._


class CreateShoppingCartHandler extends CommandHandler[ShoppingCart, CreateShoppingCart, CommandResult] {
  def handle(aggregateId: AggregateId, command: CreateShoppingCart) = {
    Success(ShoppingCartCreated(command.name))
  }
}

class AddItemHandler extends CommandHandler[ShoppingCart, AddItem, CommandResult] {
  def handle(aggregateId: AggregateId, command: AddItem) = {
    Success(List(ItemAdded(command.name)))
  }
}

class RemoveItemHandler extends CommandHandler[ShoppingCart, RemoveItem, CommandResult] {
  def handle(aggregateId: AggregateId, command: RemoveItem) = {
    Success(List(ItemRemoved(command.id)))
  }
}

class DeleteShoppingCartHandler extends CommandHandler[ShoppingCart, DeleteShoppingCart, CommandResult] {
  override def handle(aggregateId: AggregateId, command: DeleteShoppingCart) = {
    Success(List(ShoppingCartDeleted()))
  }
}



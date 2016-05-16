package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}


case class CreateShoppingCart(userId: UserId, name: String) extends FirstCommand[ShoppingCart, CustomCommandResponse[_]]

case class DuplicateShoppingCart(userId: UserId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion) extends FirstCommand[ShoppingCart, CustomCommandResponse[_]]

case class AddItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                   name: String) extends Command[ShoppingCart, CustomCommandResponse[_]]

case class RemoveItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                      id: Int) extends Command[ShoppingCart, CustomCommandResponse[_]]

case class UndoShoppingCartChange(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                                  stepsToUndo: Int) extends Command[ShoppingCart, CustomCommandResponse[_]]

case class DeleteShoppingCart(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion)
  extends Command[ShoppingCart, CustomCommandResponse[_]]



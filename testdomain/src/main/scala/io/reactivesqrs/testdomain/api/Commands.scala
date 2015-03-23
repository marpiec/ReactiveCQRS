package io.reactivesqrs.testdomain.api

import io.reactivecqrs.api.command.Command
import io.reactivecqrs.api.guid.AggregateId

case class DeleteUser(userId: AggregateId, expectedVersion: Int)
  extends Command[User, DeleteUserResult]
case class DeleteUserResult(success: Boolean)



case class RegisterUser(userId: AggregateId, name: String)
  extends Command[User, RegisterUserResult]
case class RegisterUserResult(success: Boolean)


case class ChangeUserAddress(userId: AggregateId, expectedVersion: Int, city: String, street: String, number: String)
  extends Command[User, ChangeUserAddressResult]
case class ChangeUserAddressResult(success: Boolean)


case class UndoUserChange(userId: AggregateId, expectedVersion: Int, stepsToUndo: Int)
  extends Command[User, UndoUserChangeResult]
case class UndoUserChangeResult(success: Boolean)
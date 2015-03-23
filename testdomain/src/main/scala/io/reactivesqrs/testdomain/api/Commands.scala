package io.reactivesqrs.testdomain.api

import io.reactivecqrs.api.command.Command
import io.reactivecqrs.api.guid.{AggregateVersion, AggregateId}

case class DeleteUser(userId: AggregateId, expectedVersion: AggregateVersion)
  extends Command[User, DeleteUserResult]
case class DeleteUserResult(success: Boolean)



case class RegisterUser(userId: AggregateId, name: String)
  extends Command[User, RegisterUserResult]
case class RegisterUserResult(success: Boolean)


case class ChangeUserAddress(userId: AggregateId, expectedVersion: AggregateVersion, city: String, street: String, number: String)
  extends Command[User, ChangeUserAddressResult]
case class ChangeUserAddressResult(success: Boolean)


case class UndoUserChange(userId: AggregateId, expectedVersion: AggregateVersion, stepsToUndo: Int)
  extends Command[User, UndoUserChangeResult]
case class UndoUserChangeResult(success: Boolean)
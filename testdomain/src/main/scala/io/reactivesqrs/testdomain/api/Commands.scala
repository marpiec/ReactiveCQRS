package io.reactivesqrs.testdomain.api

import io.reactivecqrs.api.command.{FirstCommand, FollowingCommand}
import io.reactivecqrs.api.guid.{AggregateId, AggregateVersion}


case class RegisterUser(name: String)
  extends FirstCommand[User, RegisterUserResult]

case class RegisterUserResult(success: Boolean, registeredUserId: AggregateId)


case class ChangeUserAddress(aggregateId: AggregateId, expectedVersion: AggregateVersion, city: String, street: String, number: String)
  extends FollowingCommand[User, ChangeUserAddressResult]

case class ChangeUserAddressResult(success: Boolean)


case class UndoUserChange(aggregateId: AggregateId, expectedVersion: AggregateVersion, stepsToUndo: Int)
  extends FollowingCommand[User, UndoUserChangeResult]

case class UndoUserChangeResult(success: Boolean)


case class DeleteUser(aggregateId: AggregateId, expectedVersion: AggregateVersion)
  extends FollowingCommand[User, DeleteUserResult]

case class DeleteUserResult(success: Boolean)


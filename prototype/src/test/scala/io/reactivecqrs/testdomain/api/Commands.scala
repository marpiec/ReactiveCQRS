package io.reactivecqrs.testdomain.api

import io.reactivecqrs.api.guid.{UserId, AggregateId}
import io.reactivecqrs.core.{FirstCommand, AggregateVersion, Command}


case class RegisterUser(name: String) extends FirstCommand[User, RegisterUserResult]

case class RegisterUserResult(registeredUserId: AggregateId)


case class ChangeUserAddress(userId: UserId,
                             aggregateId: AggregateId,
                             expectedVersion: AggregateVersion,
                             city: String,
                             street: String,
                             number: String) extends Command[User, CommandSucceed]


case class UndoUserChange(userId: UserId,
                          aggregateId: AggregateId,
                          expectedVersion: AggregateVersion,
                          stepsToUndo: Int) extends Command[User, CommandSucceed]


case class DeleteUser(userId: UserId,
                      aggregateId: AggregateId,
                      expectedVersion: AggregateVersion) extends Command[User, CommandSucceed]


case class CommandSucceed()


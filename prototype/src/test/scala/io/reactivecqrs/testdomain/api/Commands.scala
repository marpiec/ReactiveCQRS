package io.reactivecqrs.testdomain.api

import io.reactivecqrs.api.guid.AggregateId
import io.reactivecqrs.core.{FirstCommand, AggregateVersion, Command}


case class RegisterUser(name: String) extends FirstCommand[User, RegisterUserResult]

case class RegisterUserResult(registeredUserId: AggregateId)


case class ChangeUserAddress(aggregateId: AggregateId,
                             expectedVersion: AggregateVersion,
                             city: String,
                             street: String,
                             number: String) extends Command[User, CommandSucceed]


case class UndoUserChange(aggregateId: AggregateId,
                          expectedVersion: AggregateVersion,
                          stepsToUndo: Int) extends Command[User, CommandSucceed]


case class DeleteUser(aggregateId: AggregateId,
                      expectedVersion: AggregateVersion) extends Command[User, CommandSucceed]


case class CommandSucceed()


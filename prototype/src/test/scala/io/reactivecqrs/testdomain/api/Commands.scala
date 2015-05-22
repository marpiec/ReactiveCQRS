package io.reactivecqrs.testdomain.api

import io.reactivecqrs.api.guid.AggregateId
import io.reactivecqrs.core.{AggregateVersion, Command, FirstCommand}


case class RegisterUser(name: String) extends FirstCommand[User, RegisterUserResult]

case class RegisterUserResult(registeredUserId: AggregateId)


case class ChangeUserAddress(city: String,
                             street: String,
                             number: String) extends Command[User, CommandSucceed]


case class UndoUserChange(stepsToUndo: Int) extends Command[User, CommandSucceed]


case class DeleteUser() extends Command[User, CommandSucceed]


case class CommandSucceed(aggregateId: AggregateId, version: AggregateVersion)


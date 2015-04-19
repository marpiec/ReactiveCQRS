package io.reactivecqrs.testdomain.api

import io.reactivecqrs.core.{FirstCommand, AggregateVersion, Command, AggregateId}


case class RegisterUser(name: String) extends FirstCommand[User, RegisterUserResult]

case class RegisterUserResult(registeredUserId: AggregateId)


case class ChangeUserAddress(id: AggregateId,
                             expectedVersion: AggregateVersion,
                             city: String,
                             street: String,
                             number: String) extends Command[User, EmptyResult]


case class UndoUserChange(id: AggregateId,
                          expectedVersion: AggregateVersion,
                          stepsToUndo: Int) extends Command[User, EmptyResult]


case class DeleteUser(id: AggregateId,
                      expectedVersion: AggregateVersion) extends Command[User, EmptyResult]


case class EmptyResult()


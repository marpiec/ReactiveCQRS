package io.reactivecqrs.testdomain.api

import io.reactivecqrs.api.guid.AggregateId
import io.reactivecqrs.core.{CommandSuccessful, AggregateVersion, Command, FirstCommand}


case class RegisterUser(name: String) extends FirstCommand[User, CommandSuccessful]


case class ChangeUserAddress(city: String,
                             street: String,
                             number: String) extends Command[User, CommandSuccessful]


case class UndoUserChange(stepsToUndo: Int) extends Command[User, CommandSuccessful]


case class DeleteUser() extends Command[User, CommandSuccessful]


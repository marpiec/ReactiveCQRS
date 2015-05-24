package io.reactivecqrs.testdomain.api

import io.reactivecqrs.api.guid.AggregateId
import io.reactivecqrs.core._


class RegisterUserHandler extends CommandHandler[User, RegisterUser, CommandSuccessful] {
  def handle(aggregateId: AggregateId, command: RegisterUser) = {
    CommandHandlingResult(List(UserRegistered(command.name)), version => CommandSuccessful(aggregateId, version))
  }
}

class ChangeUserAddressHandler extends CommandHandler[User, ChangeUserAddress, CommandSuccessful] {
  def handle(aggregateId: AggregateId, command: ChangeUserAddress) = {
    CommandHandlingResult(List(UserAddressChanged(command.city, command.street, command.number)),
      version => CommandSuccessful(aggregateId, version))
  }
}

class DeleteUserHandler extends CommandHandler[User, DeleteUser, CommandSuccessful] {
  override def handle(aggregateId: AggregateId, command: DeleteUser) = {
    CommandHandlingResult(List(UserDeleted()),
      version => CommandSuccessful(aggregateId, version))
  }
}



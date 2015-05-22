package io.reactivecqrs.testdomain.api

import io.reactivecqrs.api.guid.AggregateId
import io.reactivecqrs.core._


class RegisterUserHandler extends CommandHandler[User, RegisterUser, RegisterUserResult] {
  def handle(aggregateId: AggregateId, command: RegisterUser) = {
    (UserRegistered(command.name),RegisterUserResult(aggregateId))
  }
}

class ChangeUserAddressHandler extends CommandHandler[User, ChangeUserAddress, CommandSucceed] {
  def handle(aggregateId: AggregateId, command: ChangeUserAddress) = {
    (UserAddressChanged(command.city, command.street, command.number),
      CommandSucceed())
  }
}

class DeleteUserHandler extends CommandHandler[User, DeleteUser, CommandSucceed] {
  override def handle(aggregateId: AggregateId, command: DeleteUser) = {
    (UserDeleted(),
      CommandSucceed())
  }
}



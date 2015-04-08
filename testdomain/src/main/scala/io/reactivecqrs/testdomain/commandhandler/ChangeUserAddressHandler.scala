package io.reactivecqrs.testdomain.commandhandler

import io.reactivecqrs.api.command._
import io.reactivecqrs.api.guid.{CommandId, UserId}
import io.reactivecqrs.utils.Success
import io.reactivecqrs.testdomain.api.{EmptyResult, ChangeUserAddress, User, UserAddressChanged}

class ChangeUserAddressHandler
  extends FollowingCommandHandler[User, ChangeUserAddress, EmptyResult] {

  override def handle(commandId: CommandId, userId: UserId, aggregateRoot: User, command: ChangeUserAddress, repository: RepositoryFollowingEventHandler[User]) = {
    repository.storeFollowingEvent(commandId, userId, command.aggregateId, command.expectedVersion,
      UserAddressChanged(command.city, command.street, command.number))
    Success(EmptyResult())
  }

}


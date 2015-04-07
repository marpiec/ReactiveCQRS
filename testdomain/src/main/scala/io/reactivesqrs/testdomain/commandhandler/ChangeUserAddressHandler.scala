package io.reactivesqrs.testdomain.commandhandler

import io.reactivecqrs.api.command._
import io.reactivecqrs.api.guid.{CommandId, UserId}
import io.reactivesqrs.testdomain.api.{ChangeUserAddress, ChangeUserAddressResult, User, UserAddressChanged}

class ChangeUserAddressHandler
  extends FollowingCommandHandler[User, ChangeUserAddress, ChangeUserAddressResult] {

  override def handle(commandId: CommandId, userId: UserId, aggregateRoot: User, command: ChangeUserAddress, repository: RepositoryHandler[User]): ChangeUserAddressResult = {
    repository.storeFirstEvent(commandId, userId,
      UserAddressChanged(command.city, command.street, command.number))
    ChangeUserAddressResult(success = true)
  }

  override def commandClass = classOf[ChangeUserAddress]
}


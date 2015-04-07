package io.reactivesqrs.testdomain.commandhandler

import io.reactivecqrs.api.command.{FollowingCommandHandler, RepositoryHandler}
import io.reactivecqrs.api.guid.{CommandId, UserId}
import io.reactivesqrs.testdomain.api.{DeleteUser, DeleteUserResult, User, UserRemoved}

class DeleteUserHandler extends FollowingCommandHandler[User, DeleteUser, DeleteUserResult] {

  override def handle(commandId: CommandId, userId: UserId, aggregateRoot: User, command: DeleteUser, repository: RepositoryHandler[User]): DeleteUserResult = {
    repository.storeFollowingEvent(commandId, userId, command.aggregateId, command.expectedVersion, UserRemoved())
    DeleteUserResult(success = true)
  }


  override def commandClass: Class[DeleteUser] = classOf[DeleteUser]

}
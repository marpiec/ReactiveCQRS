package io.reactivesqrs.testdomain.api.commandhandler

import io.reactivecqrs.api.command.{CommandEnvelope, FollowingCommandHandler, RepositoryHandler}
import io.reactivecqrs.api.guid.{UserId, CommandId}
import io.reactivesqrs.testdomain.api.{DeleteUser, DeleteUserResult, User, UserRemoved}

class DeleteUserHandler extends FollowingCommandHandler[User, DeleteUser, DeleteUserResult] {

  override def handle(commandId: CommandId, userId: UserId, aggregateRoot: User, command: DeleteUser, repository: RepositoryHandler[User]): DeleteUserResult = {
    repository.storeFollowingEvent(commandId, userId, command.aggregateId, command.expectedVersion, UserRemoved())
    DeleteUserResult(success = true)
  }


  override def commandClass: Class[DeleteUser] = classOf[DeleteUser]

}
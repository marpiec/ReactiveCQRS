package io.reactivesqrs.testdomain.commandhandler

import io.reactivecqrs.api.command.{RepositoryFollowingEventHandler, FollowingCommandHandler}
import io.reactivecqrs.api.guid.{CommandId, UserId}
import io.reactivecqrs.utils.Success
import io.reactivesqrs.testdomain.api._

class DeleteUserHandler extends FollowingCommandHandler[User, DeleteUser, SimpleResult] {

  override def handle(commandId: CommandId, userId: UserId, aggregateRoot: User, command: DeleteUser, repository: RepositoryFollowingEventHandler[User]) = {
    repository.storeFollowingEvent(commandId, userId, command.aggregateId, command.expectedVersion, UserRemoved())
    Success(SimpleResult())
  }


  override def commandClass: Class[DeleteUser] = classOf[DeleteUser]

}
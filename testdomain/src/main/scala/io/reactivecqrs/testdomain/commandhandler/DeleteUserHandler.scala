package io.reactivecqrs.testdomain.commandhandler

import io.reactivecqrs.api.command.{RepositoryFollowingEventHandler, FollowingCommandHandler}
import io.reactivecqrs.api.guid.{CommandId, UserId}
import io.reactivecqrs.utils.Success
import io.reactivecqrs.testdomain.api._

class DeleteUserHandler extends FollowingCommandHandler[User, DeleteUser, EmptyResult] {

  override def handle(commandId: CommandId, userId: UserId, aggregateRoot: User, command: DeleteUser, repository: RepositoryFollowingEventHandler[User]) = {
    repository.storeFollowingEvent(commandId, userId, command.aggregateId, command.expectedVersion, UserDeleted())
    Success(EmptyResult())
  }

}
package io.reactivesqrs.testdomain.commandhandler

import io.reactivecqrs.api.command.{RepositoryFollowingEventHandler, FollowingCommandHandler}
import io.reactivecqrs.api.guid.{CommandId, UserId}
import io.reactivecqrs.utils.Success
import io.reactivesqrs.testdomain.api._

class UndoUserChangeHandler extends FollowingCommandHandler[User, UndoUserChange, EmptyResult] {

  override def handle(commandId: CommandId, userId: UserId, aggregateRoot: User, command: UndoUserChange, repository: RepositoryFollowingEventHandler[User]) = {
    repository.storeFollowingEvent(commandId, userId, command.aggregateId, command.expectedVersion, new UserChangeUndone(command.stepsToUndo))
    Success(EmptyResult())
  }

}
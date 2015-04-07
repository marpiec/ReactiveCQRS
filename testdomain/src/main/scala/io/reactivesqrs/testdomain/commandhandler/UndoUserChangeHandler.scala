package io.reactivesqrs.testdomain.commandhandler

import io.reactivecqrs.api.command.{FollowingCommandHandler, RepositoryHandler}
import io.reactivecqrs.api.guid.{CommandId, UserId}
import io.reactivesqrs.testdomain.api._

class UndoUserChangeHandler extends FollowingCommandHandler[User, UndoUserChange, UndoUserChangeResult] {


  override def handle(commandId: CommandId, userId: UserId, aggregateRoot: User, command: UndoUserChange, repository: RepositoryHandler[User]): UndoUserChangeResult = {
    repository.storeFollowingEvent(commandId, userId, command.aggregateId, command.expectedVersion, new UserChangeUndone(command.stepsToUndo))
    UndoUserChangeResult(success = true)
  }

  override def commandClass: Class[DeleteUser] = classOf[DeleteUser]

}
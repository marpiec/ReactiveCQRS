package io.reactivesqrs.testdomain.commandhandler

import io.reactivecqrs.api.command.{FirstCommandHandler, FollowingCommandHandler, RepositoryHandler}
import io.reactivecqrs.api.guid.{CommandId, UserId}
import io.reactivesqrs.testdomain.api._


class RegisterUserHandler extends FirstCommandHandler[User, RegisterUser, RegisterUserResult] {


  override def handle(commandId: CommandId, userId: UserId, command: RegisterUser, repository: RepositoryHandler[User]): RegisterUserResult = {
    val result = repository.storeFirstEvent(commandId, userId, UserRegistered(command.name))
    RegisterUserResult(success = true, registeredUserId = result.aggregateId)
  }


  override def commandClass: Class[RegisterUser] = classOf[RegisterUser]

}
package io.reactivecqrs.testdomain.commandhandler

import io.reactivecqrs.api.command.{RepositoryFirstEventHandler, FirstCommandHandler}
import io.reactivecqrs.api.guid.{CommandId, UserId}
import io.reactivecqrs.utils.Success


class RegisterUserHandler(aggregateIdGenerator: AggregateIdGenerator) extends FirstCommandHandler[User, RegisterUser, RegisterUserResult] {

  override def handle(commandId: CommandId, userId: UserId, command: RegisterUser, repository: RepositoryFirstEventHandler[User]) = {
    val aggregateId = aggregateIdGenerator.nextAggregateId
    val result = repository.storeFirstEvent(commandId, userId, aggregateId, UserRegistered(command.name))

    Success(RegisterUserResult(aggregateId))
  }

}
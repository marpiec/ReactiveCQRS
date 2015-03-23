package io.reactivesqrs.testdomain.api.commandhandler

import io.reactivecqrs.api.command.{OnConcurrentModification, CommandHandlingResult, CommandHandler, AbstractCommandHandler}
import io.reactivecqrs.api.guid.{AggregateVersion, AggregateId, UserId, CommandId}
import io.reactivecqrs.core.RepositoryActorApi
import io.reactivesqrs.testdomain.api.{UserAddressChanged, User, ChangeUserAddress, ChangeUserAddressResult}


class ChangeUserAddressHandler(repository: RepositoryActorApi) extends CommandHandler[User, ChangeUserAddress, ChangeUserAddressResult] {

  def handle(commandId: CommandId, userId: UserId, command: ChangeUserAddress): Unit = {
    // validation

    // validation against aggregate state

    // validation against other services

    repository.storeEvent(commandId, userId, command.userId, command.expectedVersion, UserAddressChanged(command.city, command.street, command.number))

    // Retry if conflict

    // Side effects after success

    // Create response

    ChangeUserAddressResult(success = true)
  }

  override def validateCommand(userId: UserId,
                               aggregateId: AggregateId,
                               version: AggregateVersion,
                               currentAggregateRoot: User,
                               command: ChangeUserAddress): Option[ChangeUserAddressResult] = ???

  override def onConcurrentModification(): OnConcurrentModification = ???

  override def commandClass: Class[ChangeUserAddress] = ???

  override def handle(userId: UserId, command: ChangeUserAddress): CommandHandlingResult[User, ChangeUserAddressResult] = ???
}


package io.reactivesqrs.testdomain.api.commandhandler

import io.reactivecqrs.api.command._
import io.reactivecqrs.api.guid.{AggregateId, AggregateVersion, UserId}
import io.reactivesqrs.testdomain.api.{ChangeUserAddress, ChangeUserAddressResult, User, UserAddressChanged}


class ChangeUserAddressHandler
  extends CommandHandler[User, ChangeUserAddress, ChangeUserAddressResult] {


  // validation

  // validation against aggregate state

  // validation against other services

  // store events

  // Retry if conflict

  // Side effects after success

  // Create response


  override def validateCommand(userId: UserId,
                               aggregateId: AggregateId,
                               version: AggregateVersion,
                               currentAggregateRoot: User,
                               command: ChangeUserAddress): ValidationResult[ChangeUserAddressResult] = {
    ValidationSuccess()
  }

  override def commandClass = classOf[ChangeUserAddress]

  override def handle(userId: UserId, command: ChangeUserAddress) = {
    CommandHandlingResult(
      event = UserAddressChanged(command.city, command.street, command.number),
      response = ChangeUserAddressResult(success = true)
    )
  }
}


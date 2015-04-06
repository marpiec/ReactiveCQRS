package io.reactivesqrs.testdomain.api.commandhandler

import io.reactivecqrs.api.AggregateIdGenerator
import io.reactivecqrs.api.command._
import io.reactivecqrs.api.guid.{CommandId, UserId}
import io.reactivecqrs.core.RepositoryActorApi
import io.reactivesqrs.testdomain.api.{ChangeUserAddress, ChangeUserAddressResult, User, UserAddressChanged}


class ChangeUserAddressHandler(aggregateIdGenerator: AggregateIdGenerator, repository: RepositoryActorApi[User])
  extends CommandHandler[User, ChangeUserAddress, ChangeUserAddressResult] {

  override def handle(commandId: CommandId, userId: UserId, aggregateRoot: User, command: ChangeUserAddress): Unit = {

    repository.storeFirstEvent(commandId, userId, aggregateIdGenerator.nextAggregateId,
      UserAddressChanged(command.city, command.street, command.number))

    ChangeUserAddressResult(success = true)

  }

  override def commandClass = classOf[ChangeUserAddress]

}


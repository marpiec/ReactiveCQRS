package io.reactivesqrs.testdomain

import java.time.Clock

import akka.actor.ActorRef
import io.reactivecqrs.api.{AggregateIdGenerator, CommandIdGenerator}
import io.reactivecqrs.core.{CommandBus, CommandLogActorApi}
import io.reactivesqrs.testdomain.api.User
import io.reactivesqrs.testdomain.commandhandler.{ChangeUserAddressHandler, DeleteUserHandler, RegisterUserHandler, UndoUserChangeHandler}

class UserCommandBus(clock: Clock,
                     commandIdGenerator: CommandIdGenerator,
                     aggregateIdGenerator: AggregateIdGenerator,
                     commandLog: CommandLogActorApi,
                     aggregateRepositoryActor: ActorRef)
  extends CommandBus[User](clock, commandIdGenerator,
    aggregateIdGenerator,
    commandLog,
    aggregateRepositoryActor,
    Array(
      new RegisterUserHandler(aggregateIdGenerator),
      new ChangeUserAddressHandler,
      new UndoUserChangeHandler,
      new DeleteUserHandler
    )) {


}

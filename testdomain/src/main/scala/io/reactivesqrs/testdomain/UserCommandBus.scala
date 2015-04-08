package io.reactivesqrs.testdomain

import java.time.Clock

import akka.actor.ActorRef
import io.reactivecqrs.api.{AggregateIdGenerator, CommandIdGenerator}
import io.reactivecqrs.core.{CommandBus, CommandLogActorApi}
import io.reactivesqrs.testdomain.api.User
import io.reactivesqrs.testdomain.commandhandler.{ChangeUserAddressHandler, DeleteUserHandler, RegisterUserHandler, UndoUserChangeHandler}

class UserCommandBus(protected val clock: Clock,
                     protected val commandIdGenerator: CommandIdGenerator,
                     protected val aggregateIdGenerator: AggregateIdGenerator,
                     protected val commandLog: CommandLogActorApi,
                     protected val aggregateRepositoryActor: ActorRef)
  extends CommandBus[User](new RegisterUserHandler(aggregateIdGenerator),
                           new ChangeUserAddressHandler,
                           new UndoUserChangeHandler,
                           new DeleteUserHandler)
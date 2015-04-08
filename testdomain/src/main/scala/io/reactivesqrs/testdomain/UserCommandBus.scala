package io.reactivesqrs.testdomain

import java.time.Clock

import akka.actor.ActorRef
import io.reactivecqrs.api.command.{Command, CommandHandler}
import io.reactivecqrs.api.{AggregateIdGenerator, CommandIdGenerator}
import io.reactivecqrs.core.{RepositoryActorApi, CommandLogActorApi, CommandBus}
import io.reactivesqrs.testdomain.api.{RegisterUserResult, RegisterUser, User}
import io.reactivesqrs.testdomain.commandhandler.{DeleteUserHandler, UndoUserChangeHandler, ChangeUserAddressHandler, RegisterUserHandler}

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
                        new RegisterUserHandler,
                        new ChangeUserAddressHandler,
                        new UndoUserChangeHandler,
                        new DeleteUserHandler
                      ).asInstanceOf[Array[CommandHandler[User, Command[User, _], _]]]){


}

package io.reactivecqrs.core

import java.time.Clock

import akka.actor.Actor
import akka.util.Timeout
import io.reactivecqrs.api.{Aggregate, AggregateIdGenerator, CommandIdGenerator}
import io.reactivecqrs.api.command._
import io.reactivecqrs.api.exception._
import io.reactivecqrs.api.guid.{AggregateId, AggregateVersion, CommandId, UserId}
import io.reactivecqrs.utils.{Result, Failure, Success}

import scala.concurrent.duration._

abstract class CommandBus[AGGREGATE](clock: Clock,
                                     commandIdGenerator: CommandIdGenerator, //TODO make it an actor
                                     aggregateIdGenerator: AggregateIdGenerator,
                                     commandLog: CommandLogActorApi,
                                     aggregateRepository: RepositoryActorApi[AGGREGATE],
                                     handlers: Array[CommandHandler[AGGREGATE, _ <: Command[_, _], _]]) extends Actor {


  private val commandHandlers = handlers.map(handler => handler.commandClass -> handler).toMap

  implicit val akkaTimeout = Timeout(5 seconds)

  override def receive: Receive = {
    case command: FirstCommand[AGGREGATE, _] => submitFirstCommand(command)
    case command: FollowingCommand[AGGREGATE, _] => submitFollowingCommand(command)
    case message: _ => sender() ! IncorrectCommand(
      s"Received command of type ${message.getClass} but expected instance of ${classOf[Command[_, _]]}")
  }


  def submitFirstCommand[COMMAND <: Command[AGGREGATE, RESPONSE], RESPONSE](command: COMMAND): Unit = {

    val commandHandler = getProperCommandHandler

    commandHandler.handle(commandIdGenerator.nextCommandId, command)

    // implementation

    def getProperCommandHandler = {
      commandHandlers(command.asInstanceOf[Command[AGGREGATE, RESPONSE]].getClass).asInstanceOf[FirstCommandHandler[AGGREGATE, COMMAND, RESPONSE]]
    }

  }



  def submitFollowingCommand[COMMAND <: FollowingCommand[AGGREGATE, RESPONSE], RESPONSE](command: COMMAND): Unit = {

    val commandHandler = getProperCommandHandler

    handleCommand(commandHandler)

    // implementation

    def handleCommand(handler: FollowingCommandHandler[AGGREGATE, COMMAND, RESPONSE]) = {
      val aggregateState = loadLastAggregateState()
      aggregateState match {
        case Failure(exception) => sender ! exception
        case Success(aggregate) =>
          if (aggregate.version < command.expectedVersion) {
            sender ! IncorrectAggregateVersionException(s"Expected version ${command.expectedVersion} but was ${aggregate.version}")
          } else if (aggregate.version > command.expectedVersion) {
            handleConcurrentModification(aggregate.version)
          } else if (aggregate.aggregateRoot.isEmpty) {
            sender ! AggregateWasAlreadyDeletedException(s"Aggregate is already deleted. So no new commands are possible")
          } else {
            handler.handle(commandIdGenerator.nextCommandId, aggregate.aggregateRoot.get, command)
          }
      }
    }


    def loadLastAggregateState(): Result[Aggregate[AGGREGATE], AggregateDoesNotExistException] = {
      aggregateRepository.getAggregate(command.aggregateId)
    }

    def getProperCommandHandler = {
      commandHandlers(command.asInstanceOf[Command[AGGREGATE, RESPONSE]].getClass).asInstanceOf[FollowingCommandHandler[AGGREGATE, COMMAND, RESPONSE]]
    }

    def handleConcurrentModification(currentVersion: AggregateVersion): Unit = {
      ???
    }

  }


}

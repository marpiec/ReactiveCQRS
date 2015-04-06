package io.reactivecqrs.core

import java.time.Clock

import akka.actor.{ActorRef, Actor}
import akka.util.Timeout
import io.reactivecqrs.api.command._
import io.reactivecqrs.api.exception._
import io.reactivecqrs.api.guid.{AggregateVersion, UserId}
import io.reactivecqrs.api.{Aggregate, AggregateIdGenerator, CommandIdGenerator}
import io.reactivecqrs.utils.{Failure, Result, Success}

import scala.concurrent.duration._


abstract class CommandBus[AGGREGATE](clock: Clock,
                                     commandIdGenerator: CommandIdGenerator, //TODO make it an actor
                                     aggregateIdGenerator: AggregateIdGenerator,
                                     commandLog: CommandLogActorApi,
                                     aggregateRepositoryActor: ActorRef,
                                     aggregateRepository: RepositoryActorApi[AGGREGATE],
                                     handlers: Array[CommandHandler[AGGREGATE, _ <: Command[_, _], _]]) extends Actor {

  private val repositoryHandler = new RepositoryHandler[AGGREGATE](aggregateRepositoryActor)


  private val commandHandlers = handlers.map(handler => handler.commandClass -> handler).toMap

  implicit val akkaTimeout = Timeout(5 seconds)

  override def receive: Receive = {
    case CommandEnvelope(acknowledgeId, userId, command) => command match {
      case command: FirstCommand[AGGREGATE, _] => submitFirstCommand(acknowledgeId, userId, command)
      case command: FollowingCommand[AGGREGATE, _] => submitFollowingCommand(acknowledgeId, userId, command)
      case command: _ => sender() ! IncorrectCommand(
        s"Received command of type ${command.getClass} but expected instance of ${classOf[Command[_, _]]}")
    }
    case envelope: _ => sender() ! IncorrectCommand(
      s"Received commandEnvelope of type ${envelope.getClass} but expected instance of ${classOf[Command[_, _]]}")
  }


  def submitFirstCommand[COMMAND <: Command[AGGREGATE, RESPONSE], RESPONSE](acknowledgeId: String, userId: UserId, command: COMMAND): Unit = {

    val commandHandler = getProperCommandHandler

    commandHandler.handle(commandIdGenerator.nextCommandId, userId, command, repositoryHandler)

    // implementation

    def getProperCommandHandler = {
      commandHandlers(command.asInstanceOf[Command[AGGREGATE, RESPONSE]].getClass).asInstanceOf[FirstCommandHandler[AGGREGATE, COMMAND, RESPONSE]]
    }

  }


  def submitFollowingCommand[COMMAND <: FollowingCommand[AGGREGATE, RESPONSE], RESPONSE](acknowledgeId: String, userId: UserId, command: COMMAND): Unit = {

    val commandHandler = getProperCommandHandler

    handleCommand(commandHandler)

    // implementation

    def handleCommand(handler: FollowingCommandHandler[AGGREGATE, COMMAND, RESPONSE]) = {
      val aggregateState = loadLastAggregateState()
      aggregateState match {
        case Failure(exception) => sender ! exception
        case Success(aggregate) if aggregate.version < command.expectedVersion =>
          sender ! IncorrectAggregateVersionException(s"Expected version ${command.expectedVersion} but was ${aggregate.version}")
        case Success(aggregate) if aggregate.version > command.expectedVersion =>
          handleConcurrentModification(aggregate.version)
        case Success(aggregate) if aggregate.aggregateRoot.isEmpty =>
          sender ! AggregateWasAlreadyDeletedException(s"Aggregate is already deleted. So no new commands are possible")
        case Success(aggregate) =>
          handler.handle(commandIdGenerator.nextCommandId, userId, aggregate.aggregateRoot.get, command, repositoryHandler)
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

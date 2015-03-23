package io.reactivecqrs.core

import java.time.Clock

import akka.actor.Actor
import akka.util.Timeout
import io.reactivecqrs.api.{AggregateIdGenerator, CommandIdGenerator}
import io.reactivecqrs.api.command._
import io.reactivecqrs.api.exception._
import io.reactivecqrs.api.guid.{AggregateId, AggregateVersion, CommandId, UserId}

import scala.concurrent.duration._

abstract class CommandBus[AGGREGATE](clock: Clock,
                                     commandIdGenerator: CommandIdGenerator, //TODO make it an actor
                                     aggregateIdGenerator: AggregateIdGenerator,
                                     commandLog: CommandLogActorApi,
                                     aggregateRepository: RepositoryActorApi[AGGREGATE],
                                     handlers: Array[AbstractCommandHandler[AGGREGATE, _ <: Command[_, _], _]]) extends Actor {


  private val commandHandlers = handlers.map(handler => handler.commandClass -> handler).toMap

  implicit val akkaTimeout = Timeout(5 seconds)

  override def receive: Receive = {
    case ce: CommandEnvelope[AGGREGATE, _] => submitCommand(ce.acknowledgeId, ce.userId, ce.aggregateId, ce.expectedVersion, ce.command)
    case ce: FirstCommandEnvelope[AGGREGATE, _] => submitCommandForNewAggregate(ce.acknowledgeId, ce.userId, ce.command)
    case message: _ => sender() ! IncorrectCommand(
      s"Received command of type ${message.getClass} but expected instance of ${classOf[CommandEnvelope[_, _]]}")
  }

  def submitCommand[COMMAND <: Command[AGGREGATE, RESPONSE], RESPONSE](
                 messageId: String,
                 userId: UserId,
                 aggregateId: AggregateId,
                 expectedVersion: AggregateVersion,
                 command: COMMAND): Unit = {

    val commandHandler = getProperCommandHandler

    loadLastAggregateState() match {
      case Left(exception) => sender ! exception
      case Right(aggregate) =>

        if (aggregate.version < expectedVersion) {
          sender ! IncorrectAggregateVersionException(s"Expected version $expectedVersion but was ${aggregate.version}")
        } else if (aggregate.version > expectedVersion) {
          handleConcurrentModification(aggregate.version)
        } else if (aggregate.aggregateRoot.isEmpty) {
          sender ! AggregateWasAlreadyDeletedException(s"Aggregate is already deleted. So no new commands are possible")
        } else {
          validateAndHandleCommand(aggregate.aggregateRoot.get)
        }

    }


    // Implementation

    def validateAndHandleCommand(currentAggregateRoot: AGGREGATE): Unit = {
      commandHandler.validateCommand(userId, aggregateId, expectedVersion, currentAggregateRoot, command) match {
        case Some(response) => sender ! response
        case None => handleValidCommand(commandIdGenerator.nextCommandId)
      }
    }

    def loadLastAggregateState() = {
      aggregateRepository.getAggregate(aggregateId)
    }

    def getProperCommandHandler = {
      commandHandlers(command.getClass).asInstanceOf[CommandHandler[AGGREGATE, COMMAND, RESPONSE]]
    }

    def handleValidCommand(newCommandId: CommandId): Unit = {
      val CommandHandlingResult(event, response) = commandHandler.handle(userId, command)

      val storeEventResponse = aggregateRepository.storeEvent(newCommandId, userId, aggregateId, expectedVersion, event)

      if (storeEventResponse.success) {
        commandLog.logCommand(newCommandId, userId, clock.instant(), command)
        sender ! response
      } else {
        storeEventResponse.exception match {
          case ConcurrentAggregateModificationException(currentVersion, _) => handleConcurrentModification(currentVersion)
          case exception => sender ! exception
        }
      }
    }

    def handleConcurrentModification(currentVersion: AggregateVersion): Unit = {
      commandHandler.onConcurrentModification() match {
        case Retry => submitCommand(messageId, userId, aggregateId, currentVersion, command)
        case Fail => sender ! ConcurrentAggregateModificationException
      }
    }

  }


  def submitCommandForNewAggregate[COMMAND <: Command[AGGREGATE, RESPONSE], RESPONSE](
                 acknowledgeId: String,
                 userId: UserId,
                 command: COMMAND): Unit = {

    // Get proper command handler
    val commandHandler = getProperCommandHandler

    commandHandler.validateCommand(userId, command) match {
      case Some(response) => sender ! response
      case None => handleValidCommand(commandIdGenerator.nextCommandId)
    }

    // Implementation

    def getProperCommandHandler = {
      commandHandlers(command.getClass).asInstanceOf[FirstCommandHandler[AGGREGATE, COMMAND, RESPONSE]]
    }

    def handleValidCommand(newCommandId: CommandId): Unit = {
      val CommandHandlingResult(event, response) = commandHandler.handle(userId, command)


      val storeEventResponse = aggregateRepository.storeEvent(
        newCommandId,
        userId,
        aggregateIdGenerator.nextAggregateId,
        AggregateVersion.ZERO,
        event)

      if (storeEventResponse.success) {
        commandLog.logCommand(newCommandId, userId, clock.instant(), command)
        sender ! response
      } else {
        storeEventResponse.exception match {
          case exception => sender ! exception
        }
      }
    }


  }

}

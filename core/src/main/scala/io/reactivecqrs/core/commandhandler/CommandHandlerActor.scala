package io.reactivecqrs.core.commandhandler

import java.io.{PrintWriter, StringWriter}
import java.time.Instant

import akka.actor.{Actor, ActorRef, Props}
import io.reactivecqrs.api._
import io.reactivecqrs.api.command.{LogCommand, LogConcurrentCommand, LogFirstCommand}
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.{GetAggregateRoot, PersistEvents}
import io.reactivecqrs.core.commandhandler.CommandHandlerActor.{InternalCommandEnvelope, InternalConcurrentCommandEnvelope, InternalFirstCommandEnvelope, InternalFollowingCommandEnvelope}
import io.reactivecqrs.core.util.ActorLogging

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object CommandHandlerActor {

  sealed trait InternalCommandEnvelope[AGGREGATE_ROOT, +RESPONSE <: CustomCommandResponse[_]]

  case class InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]](respondTo: ActorRef, commandId: CommandId, commandEnvelope: FirstCommand[AGGREGATE_ROOT, RESPONSE])
    extends InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE]

  case class InternalConcurrentCommandEnvelope[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]](respondTo: ActorRef, commandId: CommandId, commandEnvelope: ConcurrentCommand[AGGREGATE_ROOT, RESPONSE])
    extends InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE]

  case class InternalFollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]](respondTo: ActorRef, commandId: CommandId, commandEnvelope: Command[AGGREGATE_ROOT, RESPONSE])
    extends InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE]

  case class NoAggregateExist()

}


class CommandHandlerActor[AGGREGATE_ROOT](aggregateId: AggregateId,
                                          repositoryActor: ActorRef,
                                          commandLogActor: ActorRef,
                                          commandHandlers: AGGREGATE_ROOT => PartialFunction[Any, CustomCommandResult[Any]],
                                          initialState: () => AGGREGATE_ROOT) extends Actor with ActorLogging {
  
  var resultAggregatorsCounter = 0

  val responseTimeout = 60.seconds // timeout for Response handler, we assume one minute is maximum for someone to wait for response

  private def waitingForCommand = logReceive {
    case commandEnvelope: InternalFirstCommandEnvelope[_, _] =>
      handleFirstCommand(commandEnvelope.asInstanceOf[InternalFirstCommandEnvelope[AGGREGATE_ROOT, CustomCommandResponse[_]]])
    case commandEnvelope: InternalConcurrentCommandEnvelope[_, _] =>
      requestAggregateForCommandHandling(commandEnvelope.asInstanceOf[InternalConcurrentCommandEnvelope[AGGREGATE_ROOT, CustomCommandResponse[_]]])
    case commandEnvelope: InternalFollowingCommandEnvelope[_, _] =>
      requestAggregateForCommandHandling(commandEnvelope.asInstanceOf[InternalFollowingCommandEnvelope[AGGREGATE_ROOT, CustomCommandResponse[_]]])
  }

  private def waitingForAggregate(command: InternalCommandEnvelope[AGGREGATE_ROOT, CustomCommandResponse[_]]) = logReceive {
    case s:Success[_] => handleFollowingCommand(command, s.get.asInstanceOf[Aggregate[AGGREGATE_ROOT]])
    case f:Failure[_] => throw new IllegalStateException("Error getting aggregate")
  }


  override def receive = waitingForCommand

  private def handleFirstCommand[COMMAND <: FirstCommand[AGGREGATE_ROOT, RESPONSE], RESPONSE <: CustomCommandResponse[_]](envelope: InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE]) = envelope match {
    case InternalFirstCommandEnvelope(respondTo, commandId, command) =>


      val resultTry:Try[CustomCommandResult[Any]] = Try {
        commandHandlers(initialState())(command.asInstanceOf[FirstCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]])
      }

      resultTry match {
        case  Success(result) => result match {
          case s: CommandSuccess[_, _] =>
            val success = s.asInstanceOf[CommandSuccess[AGGREGATE_ROOT, AnyRef]]
            val response = success.responseInfo match {
              case r: Nothing => SuccessResponse(aggregateId, AggregateVersion(s.events.size))
              case _ => CustomSuccessResponse(aggregateId, AggregateVersion(s.events.size), success.responseInfo)
            }
            val resultAggregator = context.actorOf(Props(new ResultAggregator[RESPONSE](respondTo, response.asInstanceOf[RESPONSE], responseTimeout)), nextResultAggregatorName)
            repositoryActor ! PersistEvents[AGGREGATE_ROOT](resultAggregator, commandId, command.userId, AggregateVersion.ZERO, Instant.now, success.events)
            commandLogActor ! LogFirstCommand(commandId, command)
          case failure: CommandFailure[_, _] =>
            respondTo ! failure.response
        }
        case Failure(exception) =>
          respondTo ! CommandHandlingError(command.getClass.getSimpleName, stackTraceToString(exception), commandId)
          log.error(exception, "Error handling command")
      }
  }

  private def stackTraceToString(e: Throwable) = {
    val sw = new StringWriter()
    e.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  private def nextResultAggregatorName[RESPONSE <: CustomCommandResponse[_], COMMAND <: Command[AGGREGATE_ROOT, RESPONSE]]: String = {
    resultAggregatorsCounter += 1
    "ResultAggregator_" + resultAggregatorsCounter
  }

  private def requestAggregateForCommandHandling[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], RESPONSE <: CustomCommandResponse[_]](commandEnvelope: InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE]): Unit = {
    context.become(waitingForAggregate(commandEnvelope))
    repositoryActor ! GetAggregateRoot(self)
  }

  // TODO handling concurrent command is not thread safe
  private def handleFollowingCommand[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], RESPONSE <: CustomCommandResponse[_]](envelope: InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE], aggregate: Aggregate[AGGREGATE_ROOT]): Unit = envelope match {
    case InternalConcurrentCommandEnvelope(respondTo, commandId, command) =>
      handleFollowingCommandVersionAware(aggregate, respondTo, command.userId, commandId, command, aggregate.version)
    case InternalFollowingCommandEnvelope(respondTo, commandId, command) =>
      handleFollowingCommandVersionAware(aggregate, respondTo, command.userId, commandId, command, command.expectedVersion)
    case e => throw new IllegalArgumentException(s"Unsupported envelope type [$e]")
  }

  private def handleFollowingCommandVersionAware[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], RESPONSE <: CustomCommandResponse[_]](aggregate: Aggregate[AGGREGATE_ROOT], respondTo: ActorRef,
                                                                                                                                     userId: UserId, commandId: CommandId, command: Any, expectedVersion: AggregateVersion): Unit = {

    val resultTry:Try[CustomCommandResult[Any]] = Try {
      commandHandlers(aggregate.aggregateRoot.get)(command)
    }

    resultTry match {
      case Success(result) => result match {
        case s: CommandSuccess[_, _] =>
          val success = s.asInstanceOf[CommandSuccess[AGGREGATE_ROOT, AnyRef]]
          val response = success.responseInfo match {
            case r: Nothing => SuccessResponse(aggregateId, expectedVersion.incrementBy(success.events.size))
            case _ => CustomSuccessResponse(aggregateId, expectedVersion.incrementBy(success.events.size), success.responseInfo)
          }
          val resultAggregator = context.actorOf(Props(new ResultAggregator[RESPONSE](respondTo, response.asInstanceOf[RESPONSE], responseTimeout)), nextResultAggregatorName)
          repositoryActor ! PersistEvents[AGGREGATE_ROOT](resultAggregator, commandId, userId, expectedVersion, Instant.now, success.events)
          command match {
            case c: Command[_, _] => commandLogActor ! LogCommand(commandId, c)
            case c: ConcurrentCommand[_, _] => commandLogActor ! LogConcurrentCommand(commandId, c)
          }

        case failure: CommandFailure[_, _] =>
          respondTo ! failure.response
      }
      case Failure(exception) =>
        respondTo ! CommandHandlingError(command.getClass.getSimpleName, stackTraceToString(exception), commandId)
        log.error(exception, "Error handling command")
    }

    context.become(waitingForCommand)
  }



}

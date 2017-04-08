package io.reactivecqrs.core.commandhandler

import java.io.{PrintWriter, StringWriter}
import java.time.Instant

import akka.actor.{Actor, ActorRef, Props}
import io.reactivecqrs.api._
import io.reactivecqrs.api.command.{LogCommand, LogConcurrentCommand, LogFirstCommand}
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.{GetAggregateRoot, IdempotentCommandInfo, PersistEvents}
import io.reactivecqrs.core.commandhandler.CommandHandlerActor.{InternalCommandEnvelope, InternalConcurrentCommandEnvelope, InternalFirstCommandEnvelope, InternalFollowingCommandEnvelope}
import io.reactivecqrs.core.util.ActorLogging

import scala.concurrent.Future
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
                                          commandResponseState: CommandResponseState,
                                          commandHandlers: AGGREGATE_ROOT => PartialFunction[Any, GenericCommandResult[Any]],
                                          initialState: () => AGGREGATE_ROOT) extends Actor with ActorLogging {

  private implicit val ec = context.dispatcher

  private var resultAggregatorsCounter = 0

  private val responseTimeout: FiniteDuration = 60.seconds // timeout for Response handler, we assume one minute is maximum for someone to wait for response

  private def waitingForCommand = logReceive {
    case commandEnvelope: InternalFirstCommandEnvelope[_, _] =>
      respondIfAlreadyHandled(commandEnvelope.respondTo, commandEnvelope.commandEnvelope) {
        handleFirstCommand(commandEnvelope.asInstanceOf[InternalFirstCommandEnvelope[AGGREGATE_ROOT, CustomCommandResponse[_]]])
      }
    case commandEnvelope: InternalConcurrentCommandEnvelope[_, _] =>
      respondIfAlreadyHandled(commandEnvelope.respondTo, commandEnvelope.commandEnvelope) {
        requestAggregateForCommandHandling(commandEnvelope.asInstanceOf[InternalConcurrentCommandEnvelope[AGGREGATE_ROOT, CustomCommandResponse[_]]])
      }
    case commandEnvelope: InternalFollowingCommandEnvelope[_, _] =>
      respondIfAlreadyHandled(commandEnvelope.respondTo, commandEnvelope.commandEnvelope) {
        requestAggregateForCommandHandling(commandEnvelope.asInstanceOf[InternalFollowingCommandEnvelope[AGGREGATE_ROOT, CustomCommandResponse[_]]])
      }
  }

  private def respondIfAlreadyHandled(respondTo: ActorRef, command: Any)(block: => Unit): Unit = {
    command match {
      case idm: IdempotentCommand[_] if idm.idempotencyId.isDefined =>
        val key = idm.idempotencyId.get.asDbKey
        commandResponseState.responseByKey(key) match {
          case Some(response) =>
            println("Command repeated " + idm)
            respondTo ! response
          case None => block
        }
      case _ => block
    }
  }

  private def waitingForAggregate(command: InternalCommandEnvelope[AGGREGATE_ROOT, CustomCommandResponse[_]]) = logReceive {
    case s:Success[_] => handleFollowingCommand(command, s.get.asInstanceOf[Aggregate[AGGREGATE_ROOT]])
    case f:Failure[_] => throw new IllegalStateException("Error getting aggregate")
  }


  override def receive = waitingForCommand

  private def handleFirstCommand[COMMAND <: FirstCommand[AGGREGATE_ROOT, RESPONSE], RESPONSE <: CustomCommandResponse[_]](envelope: InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE]) = {
    val InternalFirstCommandEnvelope(respondTo, commandId, command) = envelope
    try {
      commandHandlers(initialState())(command.asInstanceOf[FirstCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]]) match {
        case result: CustomCommandResult[_] => handleCommandResult(AggregateVersion.ZERO, respondTo, command.userId, commandId, command, Some(AggregateVersion.ZERO), result)
        case result: AsyncCommandResult[_] =>
          result.future.onSuccess {case result => handleCommandResult(AggregateVersion.ZERO, respondTo, command.userId, commandId, command, Some(AggregateVersion.ZERO), result)}
          result.future.onFailure {case exception => handleCommandHandlingException(respondTo, commandId, command, exception)}
      }
    } catch {
      case exception: Exception => handleCommandHandlingException(respondTo, commandId, command, exception)
    }

  }

  private def handleFollowingCommandVersionAware[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], RESPONSE <: CustomCommandResponse[_]](aggregate: Aggregate[AGGREGATE_ROOT], respondTo: ActorRef, userId: UserId, commandId: CommandId, command: Any, expectedVersion: Option[AggregateVersion]): Unit = {
    try {
      commandHandlers(aggregate.aggregateRoot.get)(command) match {
        case result: CustomCommandResult[_] =>
          handleCommandResult(aggregate.version, respondTo, userId, commandId, command, expectedVersion, result)
        case result: AsyncCommandResult[_] =>
          result.future.onFailure {case exception => handleCommandHandlingException(respondTo, commandId, command, exception)}
          result.future.onSuccess {case result => handleCommandResult(aggregate.version, respondTo, userId, commandId, command, expectedVersion, result)}
      }

    } catch {
      case exception: Exception => handleCommandHandlingException(respondTo, commandId, command, exception)
    }
  }


  private def handleCommandHandlingException[RESPONSE <: CustomCommandResponse[_]](respondTo: ActorRef, commandId: CommandId, command: Any, exception: Throwable) = {
    respondTo ! CommandHandlingError(command.getClass.getSimpleName, stackTraceToString(exception), commandId)
    log.error(exception, "Error handling command")
  }

  private def idempotentCommandInfo(command: Any, response: CustomCommandResponse[_]): Option[IdempotentCommandInfo] = {
    command match {
      case idm: IdempotentCommand[_] if idm.idempotencyId.isDefined => Some(IdempotentCommandInfo(idm, response))
      case _ => None
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
    context.become(waitingForAggregate(commandEnvelope)) // TODO FIGURE OUT, won't this ignore other commands? e.g. Concurrent Commands that might then not be delivered?
    repositoryActor ! GetAggregateRoot(self)
  }

  // TODO handling concurrent command is not thread safe
  private def handleFollowingCommand[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], RESPONSE <: CustomCommandResponse[_]](envelope: InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE], aggregate: Aggregate[AGGREGATE_ROOT]): Unit = envelope match {
    case InternalConcurrentCommandEnvelope(respondTo, commandId, command) =>
      handleFollowingCommandVersionAware(aggregate, respondTo, command.userId, commandId, command, None)
    case InternalFollowingCommandEnvelope(respondTo, commandId, command) =>
      handleFollowingCommandVersionAware(aggregate, respondTo, command.userId, commandId, command, Some(command.expectedVersion))
    case e => throw new IllegalArgumentException(s"Unsupported envelope type [$e]")
  }





  private def handleCommandResult[RESPONSE <: CustomCommandResponse[_], COMMAND <: Command[AGGREGATE_ROOT, RESPONSE]](
       version: AggregateVersion, respondTo: ActorRef, userId: UserId, commandId: CommandId, command: Any, expectedVersion: Option[AggregateVersion], result: CustomCommandResult[Any]): Unit = {
    result match {
      case s: CommandSuccess[_, _] =>
        val success = s.asInstanceOf[CommandSuccess[AGGREGATE_ROOT, AnyRef]]
        val response = success.responseInfo match {
          // if concurrent command then expected aggregate version plus events count will be sent to command originator,
          // that might be inaccurate if other command happened meantime, but this should not be a problem for concurrent command
          case r: Nothing => SuccessResponse(aggregateId, version.incrementBy(success.events.size))
          case _ => CustomSuccessResponse(aggregateId, version.incrementBy(success.events.size), success.responseInfo)
        }
        val resultAggregator = context.actorOf(Props(new ResultAggregator[RESPONSE](respondTo, response.asInstanceOf[RESPONSE], responseTimeout)), nextResultAggregatorName)
        repositoryActor ! PersistEvents[AGGREGATE_ROOT](resultAggregator, commandId, userId, expectedVersion, Instant.now, success.events, idempotentCommandInfo(command, response))
        command match {
          case c: FirstCommand[_, _] => commandLogActor ! LogFirstCommand(commandId, c)
          case c: Command[_, _] => commandLogActor ! LogCommand(commandId, c)
          case c: ConcurrentCommand[_, _] => commandLogActor ! LogConcurrentCommand(commandId, c)
        }
      case failure: CommandFailure[_, _] =>
        respondTo ! failure.response
    }

    context.become(waitingForCommand)
  }
}

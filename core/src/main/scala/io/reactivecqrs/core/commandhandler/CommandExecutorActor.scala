package io.reactivecqrs.core.commandhandler

import java.io.{PrintWriter, StringWriter}
import java.time.Instant

import akka.actor.{Actor, ActorContext, ActorRef, PoisonPill, Props}
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.api._
import io.reactivecqrs.api.command.{LogCommand, LogConcurrentCommand, LogFirstCommand}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.{IdempotentCommandInfo, PersistEvents}
import io.reactivecqrs.core.commandhandler.CommandHandlerActor.{InternalCommandEnvelope, InternalConcurrentCommandEnvelope, InternalFirstCommandEnvelope, InternalFollowingCommandEnvelope}
import io.reactivecqrs.core.util.ActorLogging

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

class CommandExecutorActor[AGGREGATE_ROOT]
                                          (parentContext: ActorContext,
                                            aggregateId: AggregateId,
                                           commandEnvelope: InternalCommandEnvelope[AGGREGATE_ROOT, CustomCommandResponse[_]],
                                           repositoryActor: ActorRef,
                                           commandLogActor: ActorRef,
                                           commandResponseState: CommandResponseState,
                                           resultAggregatorName: String,
                                           commandHandlers: AGGREGATE_ROOT => PartialFunction[Any, GenericCommandResult[Any]],
                                           initialState: () => AGGREGATE_ROOT) extends Actor with ActorLogging {

  private val responseTimeout: FiniteDuration = 60.seconds // timeout for Response handler, we assume one minute is maximum for someone to wait for response

  import context.dispatcher

  context.system.scheduler.scheduleOnce(responseTimeout, self, PoisonPill) // wait for maximum 60s for a response

  // Receiving aggregate from aggregate repository
  override def receive: Receive = logReceive {
    case s:Success[_] =>
      handleFollowingCommand(s.get.asInstanceOf[Aggregate[AGGREGATE_ROOT]])
      context.stop(self)
    case f:Failure[_] =>
      handleCommandHandlingException(f.exception)
      context.stop(self)
  }


  // TODO handling concurrent command is not thread safe
  private def handleFollowingCommand(aggregate: Aggregate[AGGREGATE_ROOT]): Unit = commandEnvelope match {
    case InternalFirstCommandEnvelope(respondTo, commandId, command) =>
      handleFirstCommand(respondTo, commandId, command.asInstanceOf[FirstCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]])
    case InternalConcurrentCommandEnvelope(respondTo, commandId, command) =>
      handleFollowingCommandVersionAware(aggregate, respondTo, command.userId, commandId, command, None)
    case InternalFollowingCommandEnvelope(respondTo, commandId, command) =>
      handleFollowingCommandVersionAware(aggregate, respondTo, command.userId, commandId, command, Some(command.expectedVersion))
    case e => log.error(s"Unsupported envelope type [$e]")

  }


  private def handleFollowingCommandVersionAware(aggregate: Aggregate[AGGREGATE_ROOT], respondTo: ActorRef, userId: UserId, commandId: CommandId,
                                                 command: Any, expectedVersion: Option[AggregateVersion]): Unit = {
    try {
      commandHandlers(aggregate.aggregateRoot.get)(command) match {
        case result: CustomCommandResult[_] =>
          handleCommandResult(aggregate.version, userId, expectedVersion, result)
        case result: AsyncCommandResult[_] =>
          result.future.onFailure {case exception => handleCommandHandlingException(exception)}
          result.future.onSuccess {case r => handleCommandResult(aggregate.version, userId, expectedVersion, r)}
      }

    } catch {
      case exception: Exception => handleCommandHandlingException(exception)
    }
  }

  private def handleFirstCommand(respondTo: ActorRef, commandId: CommandId, command: FirstCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]) = {
    try {
      commandHandlers(initialState())(command.asInstanceOf[FirstCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]]) match {
        case result: CustomCommandResult[_] => handleCommandResult(AggregateVersion.ZERO, command.userId, Some(AggregateVersion.ZERO), result)
        case result: AsyncCommandResult[_] =>
          result.future.onFailure { case exception => handleCommandHandlingException(exception) }
          result.future.onSuccess { case r => handleCommandResult(AggregateVersion.ZERO, command.userId, Some(AggregateVersion.ZERO), r) }
      }
    } catch {
      case exception: Exception => handleCommandHandlingException(exception)
    }
  }


    private def handleCommandResult(version: AggregateVersion, userId: UserId, expectedVersion: Option[AggregateVersion], result: CustomCommandResult[Any]): Unit = {
    result match {
      case s: CommandSuccess[_, _] =>
        val success = s.asInstanceOf[CommandSuccess[AGGREGATE_ROOT, AnyRef]]
        val response = success.responseInfo match {
          // if concurrent command then expected aggregate version plus events count will be sent to command originator,
          // that might be inaccurate if other command happened meantime, but this should not be a problem for concurrent command
          case r: Nothing => SuccessResponse(aggregateId, version.incrementBy(success.events.size))
          case _ => CustomSuccessResponse(aggregateId, version.incrementBy(success.events.size), success.responseInfo)
        }
        val resultAggregator = parentContext.actorOf(Props(new ResultAggregator[CustomCommandResponse[_]](commandEnvelope.respondTo, response, responseTimeout)), resultAggregatorName)
        repositoryActor ! PersistEvents[AGGREGATE_ROOT](resultAggregator, commandEnvelope.commandId, userId, expectedVersion, Instant.now, success.events, idempotentCommandInfo(commandEnvelope.command, response))
        commandEnvelope.command match {
          case c: FirstCommand[_, _] => commandLogActor ! LogFirstCommand(commandEnvelope.commandId, c)
          case c: Command[_, _] => commandLogActor ! LogCommand(commandEnvelope.commandId, c)
          case c: ConcurrentCommand[_, _] => commandLogActor ! LogConcurrentCommand(commandEnvelope.commandId, c)
        }
      case failure: CommandFailure[_, _] =>
        commandEnvelope.respondTo ! failure.response
    }

  }


  private def idempotentCommandInfo(command: Any, response: CustomCommandResponse[_]): Option[IdempotentCommandInfo] = {
    command match {
      case idm: IdempotentCommand[_] if idm.idempotencyId.isDefined => Some(IdempotentCommandInfo(idm, response))
      case _ => None
    }
  }

  private def handleCommandHandlingException(exception: Throwable) = {
    commandEnvelope.respondTo ! CommandHandlingError(commandEnvelope.command.getClass.getSimpleName, stackTraceToString(exception), commandEnvelope.commandId)
    log.error(exception, "Error handling command")
  }

  private def stackTraceToString(e: Throwable) = {
    val sw = new StringWriter()
    e.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

}

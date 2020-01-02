package io.reactivecqrs.core.commandhandler

import java.io.{PrintWriter, StringWriter}
import java.time.Instant

import akka.actor.{Actor, ActorRef, PoisonPill}
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.api._
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.{GetAggregateRootCurrentVersion, IdempotentCommandInfo, OverrideAndPersistEvents, PersistEvents}
import io.reactivecqrs.core.commandhandler.CommandExecutorActor.AggregateModified
import io.reactivecqrs.core.commandhandler.CommandHandlerActor.{InternalCommandEnvelope, InternalConcurrentCommandEnvelope, InternalFirstCommandEnvelope, InternalFollowingCommandEnvelope, InternalRewriteHistoryCommandEnvelope, InternalRewriteHistoryConcurrentCommandEnvelope}
import io.reactivecqrs.core.util.ActorLogging
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.AggregateWithSelectedEvents

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}
import scala.reflect._
import scala.reflect.runtime.universe._

object CommandExecutorActor {
  case object AggregateModified
}

class CommandExecutorActor[AGGREGATE_ROOT:ClassTag:TypeTag](aggregateId: AggregateId,
                                           commandEnvelope: InternalCommandEnvelope[AGGREGATE_ROOT, CustomCommandResponse[_]],
                                           repositoryActor: ActorRef,
                                           commandResponseState: CommandResponseState,
                                           resultAggregatorName: String,
                                           commandHandlers: AGGREGATE_ROOT => PartialFunction[Any, GenericCommandResult[Any]],
                                           rewriteHistoryCommandHandlers: (Iterable[EventWithVersion[AGGREGATE_ROOT]], AGGREGATE_ROOT) => PartialFunction[Any, GenericCommandResult[Any]],
                                           initialState: () => AGGREGATE_ROOT) extends Actor with ActorLogging {

  private val responseTimeout: FiniteDuration = 60.seconds // timeout for Response handler, we assume one minute is maximum for someone to wait for response

  import context.dispatcher

  context.system.scheduler.scheduleOnce(responseTimeout, self, PoisonPill) // wait for maximum 60s for a response

  override def receive = receiveAggregate

  // Receiving aggregate from aggregate repository
  private def receiveAggregate: Receive = logReceive {
    case Success(s) => s match {
      case s: Aggregate[_] => handleFollowingCommand(s.asInstanceOf[Aggregate[AGGREGATE_ROOT]], Iterable.empty)
      case s: AggregateWithSelectedEvents[_] => handleFollowingCommand(s.aggregate.asInstanceOf[Aggregate[AGGREGATE_ROOT]], s.events.asInstanceOf[Iterable[EventWithVersion[AGGREGATE_ROOT]]])
    }
//      println("Received aggregate " + aggregateId.asLong+" of version " + s.get.asInstanceOf[Aggregate[AGGREGATE_ROOT]].version.asInt)

    case f:Failure[_] =>
      handleCommandHandlingExceptionAndStop(f.exception)
    case m =>
      log.error("receiveAggregate Unsupported message received" + m)
      unhandled(m)
  }


  private def receiveFirstCommandHandlingResult(userId: UserId) = logReceive {
    case result: CustomCommandResult[_] => handleCommandResult(AggregateVersion.ZERO, userId, AggregateVersion.ZERO, result)
    case exception: Exception => handleCommandHandlingExceptionAndStop(exception)
    case m =>
      log.error("receiveFirstCommandHandlingResult Unsupported message received" + m)
      unhandled(m)
  }

  private def receiveCommandHandlingResult(currentVersion: AggregateVersion, expectedVersion: AggregateVersion, userId: UserId) = logReceive {
    case result: CustomCommandResult[_] => handleCommandResult(currentVersion, userId, expectedVersion, result)
    case exception: Exception => handleCommandHandlingExceptionAndStop(exception)
    case m =>
      log.error("receiveCommandHandlingResult Unsupported message received" + m)
      unhandled(m)
  }

  private def receiveEventsPersistResult(response: CustomCommandResponse[_]) = logReceive {
    case AggregateModified =>
      commandEnvelope.respondTo ! response
      context.stop(self)
    case e: AggregateConcurrentModificationError =>
      commandEnvelope match {
        case InternalConcurrentCommandEnvelope(respondTo, commandId, command) =>
          context.become(receiveAggregate)
          repositoryActor ! GetAggregateRootCurrentVersion(self)
        case InternalRewriteHistoryConcurrentCommandEnvelope(respondTo, commandId, command) =>
          context.become(receiveAggregate)
          repositoryActor ! GetAggregateRootCurrentVersion(self)
        case _ =>
          commandEnvelope.respondTo ! e
          context.stop(self)
      }
    case e: EventHandlingError =>
      log.error("EventHandlingError " + e.eventName +"\n" + e.stackTrace)
      commandEnvelope.respondTo ! e
      context.stop(self)
    case m =>
      log.error("receiveEventsPersistResult Unsupported message received" + m)
      unhandled(m)
  }


  // TODO handling concurrent command is not thread safe
  private def handleFollowingCommand(aggregate: Aggregate[AGGREGATE_ROOT], events: Iterable[EventWithVersion[AGGREGATE_ROOT]]): Unit = commandEnvelope match {
    case InternalFirstCommandEnvelope(respondTo, commandId, command) =>
      handleFirstCommand(respondTo, commandId, command.asInstanceOf[FirstCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]])
    case InternalConcurrentCommandEnvelope(respondTo, commandId, command) =>
      handleFollowingCommand(aggregate, respondTo, command.userId, commandId, command, aggregate.version)
    case InternalFollowingCommandEnvelope(respondTo, commandId, command) =>
      handleFollowingCommand(aggregate, respondTo, command.userId, commandId, command, command.expectedVersion)
    case InternalRewriteHistoryCommandEnvelope(respondTo, commandId, command) =>
      handleRewriteHistoryCommand(aggregate, events, respondTo, command.userId, commandId, command, command.expectedVersion)
    case InternalRewriteHistoryConcurrentCommandEnvelope(respondTo, commandId, command) =>
      handleRewriteHistoryCommand(aggregate, events, respondTo, command.userId, commandId, command, aggregate.version)
    case e =>
      log.error(s"Unsupported envelope type [$e]")
      context.stop(self)

  }


  private def handleFollowingCommand(aggregate: Aggregate[AGGREGATE_ROOT], respondTo: ActorRef, userId: UserId, commandId: CommandId,
                                                 command: Any, expectedVersion: AggregateVersion): Unit = {
    if(expectedVersion == aggregate.version) {
      context.become(receiveCommandHandlingResult(aggregate.version, expectedVersion, userId))
      try {
//        println(s"Handling command ${command.getClass.getSimpleName} for aggregate ${aggregate.id.asLong} of version ${aggregate.version.asInt}")
        commandHandlers(aggregate.aggregateRoot.get)(command) match {
          case result: CustomCommandResult[_] => self ! result
          case asyncResult: AsyncCommandResult[_] =>
            asyncResult.future.onFailure {case exception => self ! exception}
            asyncResult.future.onSuccess {case result => self ! result }
        }
      } catch {
        case exception: Exception => self ! exception
      }
    } else {
//      println("CommandExecutorActor AggregateConcurrentModificationError "+aggregateId.asLong+" expected " + expectedVersion.asInt+" was " + aggregate.version.asInt)
      self ! AggregateConcurrentModificationError(aggregateId, AggregateType(classTag[AGGREGATE_ROOT].toString), expectedVersion, aggregate.version)
    }
  }

  private def handleRewriteHistoryCommand(aggregate: Aggregate[AGGREGATE_ROOT], events: Iterable[EventWithVersion[AGGREGATE_ROOT]], respondTo: ActorRef, userId: UserId, commandId: CommandId,
                                     command: Any, expectedVersion: AggregateVersion): Unit = {
    if (expectedVersion == aggregate.version) {
      context.become(receiveCommandHandlingResult(aggregate.version, expectedVersion, userId))
      try {
        //        println(s"Handling command ${command.getClass.getSimpleName} for aggregate ${aggregate.id.asLong} of version ${aggregate.version.asInt}")
        rewriteHistoryCommandHandlers(events, aggregate.aggregateRoot.get)(command) match {
          case result: CustomCommandResult[_] => self ! result
          case asyncResult: AsyncCommandResult[_] =>
            asyncResult.future.onFailure { case exception => self ! exception }
            asyncResult.future.onSuccess { case result => self ! result }
        }
      } catch {
        case exception: Exception => self ! exception
      }
    } else {
      //      println("CommandExecutorActor AggregateConcurrentModificationError "+aggregateId.asLong+" expected " + expectedVersion.asInt+" was " + aggregate.version.asInt)
      self ! AggregateConcurrentModificationError(aggregateId, AggregateType(classTag[AGGREGATE_ROOT].toString), expectedVersion, aggregate.version)
    }
  }

  private def handleFirstCommand(respondTo: ActorRef, commandId: CommandId, command: FirstCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]) = {
    context.become(receiveFirstCommandHandlingResult(commandEnvelope.command.asInstanceOf[FirstCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]].userId))
    try {
      commandHandlers(initialState())(command.asInstanceOf[FirstCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]]) match {
        case result: CustomCommandResult[_] => self ! result
        case asyncResult: AsyncCommandResult[_] =>
          asyncResult.future.onFailure { case exception => self ! exception}
          asyncResult.future.onSuccess { case result => self ! result}
      }
    } catch {
      case exception: Exception => self ! exception
    }
  }


    private def handleCommandResult(version: AggregateVersion, userId: UserId, expectedVersion: AggregateVersion, result: CustomCommandResult[Any]): Unit = {
    result match {
      case s: CommandSuccess[_, _] =>
        val success = s.asInstanceOf[CommandSuccess[AGGREGATE_ROOT, AnyRef]]
        val response = success.responseInfo match {
          // if concurrent command then expected aggregate version plus events count will be sent to command originator,
          // that might be inaccurate if other command happened meantime, but this should not be a problem for concurrent command
          case r: Nothing => SuccessResponse(aggregateId, version.incrementBy(success.events.size))
          case _ => CustomSuccessResponse(aggregateId, version.incrementBy(success.events.size), success.responseInfo)
        }
        context.become(receiveEventsPersistResult(response))
        repositoryActor ! PersistEvents[AGGREGATE_ROOT](self, commandEnvelope.commandId, userId, expectedVersion, Instant.now, success.events, idempotentCommandInfo(commandEnvelope.command, response))
      case s: RewriteCommandSuccess[_, _] =>

        val success = s.asInstanceOf[RewriteCommandSuccess[AGGREGATE_ROOT, AnyRef]]
        val response = success.responseInfo match {
          // if concurrent command then expected aggregate version plus events count will be sent to command originator,
          // that might be inaccurate if other command happened meantime, but this should not be a problem for concurrent command
          case r: Nothing => SuccessResponse(aggregateId, version.incrementBy(success.events.size))
          case _ => CustomSuccessResponse(aggregateId, version.incrementBy(success.events.size), success.responseInfo)
        }
        context.become(receiveEventsPersistResult(response))
        repositoryActor ! OverrideAndPersistEvents[AGGREGATE_ROOT](success.eventsRewritten, PersistEvents[AGGREGATE_ROOT](self, commandEnvelope.commandId, userId, expectedVersion, Instant.now, success.events, idempotentCommandInfo(commandEnvelope.command, response)))


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

  private def handleCommandHandlingExceptionAndStop(exception: Throwable): Unit = {
    exception match {
      case e: AggregateInIncorrectVersionException => commandEnvelope.respondTo ! AggregateConcurrentModificationError(aggregateId, e.aggregateType, e.requestedVersion, e.currentVersion)
      case _ =>
        commandEnvelope.respondTo ! CommandHandlingError(commandEnvelope.command.getClass.getSimpleName, stackTraceToString(exception), commandEnvelope.commandId)
        log.error(exception, "Error handling command")
    }
    context.stop(self)
  }

  private def stackTraceToString(e: Throwable) = {
    val sw = new StringWriter()
    e.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

}

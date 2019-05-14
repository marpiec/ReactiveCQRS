package io.reactivecqrs.core.commandhandler

import akka.actor.{Actor, ActorRef, Props}
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor._
import io.reactivecqrs.core.commandhandler.CommandHandlerActor._
import io.reactivecqrs.core.util.ActorLogging

import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Success

object CommandHandlerActor {

  sealed trait InternalCommandEnvelope[AGGREGATE_ROOT, +RESPONSE <: CustomCommandResponse[_]] {
    val respondTo: ActorRef
    val commandId: CommandId
    val command: Any
  }

  case class InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]](respondTo: ActorRef, commandId: CommandId, command: FirstCommand[AGGREGATE_ROOT, RESPONSE])
    extends InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE]

  case class InternalConcurrentCommandEnvelope[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]](respondTo: ActorRef, commandId: CommandId, command: ConcurrentCommand[AGGREGATE_ROOT, RESPONSE])
    extends InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE]

  case class InternalFollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]](respondTo: ActorRef, commandId: CommandId, command: Command[AGGREGATE_ROOT, RESPONSE])
    extends InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE]

  case class InternalRewriteHistoryCommandEnvelope[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]](respondTo: ActorRef, commandId: CommandId, command: RewriteHistoryCommand[AGGREGATE_ROOT, RESPONSE])
    extends InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE]

  case class InternalRewriteHistoryConcurrentCommandEnvelope[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]](respondTo: ActorRef, commandId: CommandId, command: RewriteHistoryConcurrentCommand[AGGREGATE_ROOT, RESPONSE])
    extends InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE]

  case class NoAggregateExist()

}


class CommandHandlerActor[AGGREGATE_ROOT: TypeTag](aggregateId: AggregateId,
                                          repositoryActor: ActorRef,
                                          commandResponseState: CommandResponseState,
                                          commandHandlers: AGGREGATE_ROOT => PartialFunction[Any, GenericCommandResult[Any]],
                                          rewriteHistoryCommandHandlers: (Iterable[EventWithVersion[AGGREGATE_ROOT]], AGGREGATE_ROOT) => PartialFunction[Any, GenericCommandResult[Any]],
                                          initialState: () => AGGREGATE_ROOT)
                                         (implicit aggregateRootClassTag: ClassTag[AGGREGATE_ROOT])extends Actor with ActorLogging {

  private val aggregateTypeSimpleName = aggregateRootClassTag.runtimeClass.getSimpleName

  private var resultAggregatorsCounter = 0

  private val responseTimeout: FiniteDuration = 60.seconds // timeout for Response handler, we assume one minute is maximum for someone to wait for response

  override def receive = logReceive {
    case commandEnvelope: InternalFirstCommandEnvelope[_, _] =>
      respondIfAlreadyHandled(commandEnvelope.respondTo, commandEnvelope.command) {

        val commandExecutorActor = context.actorOf(Props(new CommandExecutorActor[AGGREGATE_ROOT](aggregateId, commandEnvelope.asInstanceOf[InternalCommandEnvelope[AGGREGATE_ROOT, CustomCommandResponse[_]]], repositoryActor,
          commandResponseState, nextResultAggregatorName, commandHandlers, rewriteHistoryCommandHandlers, initialState)), aggregateTypeSimpleName+"_CommandExecutor_" + aggregateId.asLong+"_"+commandEnvelope.commandId.asLong)

        // Pass initial state immediatelly to command executor
        commandExecutorActor ! Success(Aggregate(aggregateId, AggregateVersion.ZERO, Some(initialState())))

      }
    case commandEnvelope: InternalConcurrentCommandEnvelope[_, _] =>
      respondIfAlreadyHandled(commandEnvelope.respondTo, commandEnvelope.command) {
        val commandExecutorActor = context.actorOf(Props(new CommandExecutorActor[AGGREGATE_ROOT](aggregateId, commandEnvelope.asInstanceOf[InternalCommandEnvelope[AGGREGATE_ROOT, CustomCommandResponse[_]]], repositoryActor,
          commandResponseState, nextResultAggregatorName, commandHandlers, rewriteHistoryCommandHandlers, initialState)), aggregateTypeSimpleName+"_CommandExecutor_" + aggregateId.asLong+"_"+commandEnvelope.commandId.asLong)

        repositoryActor ! GetAggregateRootCurrentVersion(commandExecutorActor)
      }
    case commandEnvelope: InternalFollowingCommandEnvelope[_, _] =>
      respondIfAlreadyHandled(commandEnvelope.respondTo, commandEnvelope.command) {
        val commandExecutorActor = context.actorOf(Props(new CommandExecutorActor[AGGREGATE_ROOT](aggregateId, commandEnvelope.asInstanceOf[InternalCommandEnvelope[AGGREGATE_ROOT, CustomCommandResponse[_]]], repositoryActor,
          commandResponseState, nextResultAggregatorName, commandHandlers, rewriteHistoryCommandHandlers, initialState)), aggregateTypeSimpleName+"_CommandExecutor_" + aggregateId.asLong+"_"+commandEnvelope.commandId.asLong)

        repositoryActor ! GetAggregateRootExactVersion(commandExecutorActor, commandEnvelope.command.expectedVersion)
      }
    case commandEnvelope: InternalRewriteHistoryCommandEnvelope[_, _] =>
      respondIfAlreadyHandled(commandEnvelope.respondTo, commandEnvelope.command) {
        val commandExecutorActor = context.actorOf(Props(new CommandExecutorActor[AGGREGATE_ROOT](aggregateId, commandEnvelope.asInstanceOf[InternalCommandEnvelope[AGGREGATE_ROOT, CustomCommandResponse[_]]], repositoryActor,
          commandResponseState, nextResultAggregatorName, commandHandlers, rewriteHistoryCommandHandlers, initialState)), aggregateTypeSimpleName+"_CommandExecutor_" + aggregateId.asLong+"_"+commandEnvelope.commandId.asLong)

        repositoryActor ! GetAggregateRootWithEventsExactVersion(commandExecutorActor, commandEnvelope.command.expectedVersion, commandEnvelope.command.eventsTypes.map(_.getName))
      }
    case commandEnvelope: InternalRewriteHistoryConcurrentCommandEnvelope[_, _] =>
      respondIfAlreadyHandled(commandEnvelope.respondTo, commandEnvelope.command) {
        val commandExecutorActor = context.actorOf(Props(new CommandExecutorActor[AGGREGATE_ROOT](aggregateId, commandEnvelope.asInstanceOf[InternalCommandEnvelope[AGGREGATE_ROOT, CustomCommandResponse[_]]], repositoryActor,
          commandResponseState, nextResultAggregatorName, commandHandlers, rewriteHistoryCommandHandlers, initialState)), aggregateTypeSimpleName+"_CommandExecutor_" + aggregateId.asLong+"_"+commandEnvelope.commandId.asLong)

        repositoryActor ! GetAggregateRootWithEventsCurrentVersion(commandExecutorActor, commandEnvelope.command.eventsTypes.map(_.getName))
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




  private def nextResultAggregatorName[RESPONSE <: CustomCommandResponse[_], COMMAND <: Command[AGGREGATE_ROOT, RESPONSE]]: String = {
    resultAggregatorsCounter += 1
    aggregateTypeSimpleName+"_ResultAggregator_" + resultAggregatorsCounter
  }

}

package io.reactivecqrs.core.commandhandler

import akka.actor.{Actor, ActorRef, Props}
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.{GetAggregateRoot, PersistEvents}
import io.reactivecqrs.core.commandhandler.CommandHandlerActor.{InternalCommandEnvelope, InternalConcurrentCommandEnvelope, InternalFirstCommandEnvelope, InternalFollowingCommandEnvelope}
import io.reactivecqrs.core.util.ActorLogging

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object CommandHandlerActor {

  sealed trait InternalCommandEnvelope[AGGREGATE_ROOT, +RESPONSE]

  case class InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, commandId: CommandId, commandEnvelope: FirstCommand[AGGREGATE_ROOT, RESPONSE])
    extends InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE]

  case class InternalConcurrentCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, commandId: CommandId, commandEnvelope: ConcurrentCommand[AGGREGATE_ROOT, RESPONSE])
    extends InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE]

  case class InternalFollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, commandId: CommandId, commandEnvelope: Command[AGGREGATE_ROOT, RESPONSE])
    extends InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE]


  case class NoAggregateExist()

}


class CommandHandlerActor[AGGREGATE_ROOT](aggregateId: AggregateId,
                                          repositoryActor: ActorRef,
                                          commandHandlers: AGGREGATE_ROOT => PartialFunction[Any, CommandResult[Any]],
                                           initialState: () => AGGREGATE_ROOT) extends Actor with ActorLogging {
  
  var resultAggregatorsCounter = 0

  val responseTimeout = 5.seconds

  private def waitingForCommand = logReceive {
    case commandEnvelope: InternalFirstCommandEnvelope[_, _] =>
      handleFirstCommand(commandEnvelope.asInstanceOf[InternalFirstCommandEnvelope[AGGREGATE_ROOT, Any]])
    case commandEnvelope: InternalConcurrentCommandEnvelope[_, _] =>
      requestAggregateForCommandHandling(commandEnvelope.asInstanceOf[InternalConcurrentCommandEnvelope[AGGREGATE_ROOT, Any]])
    case commandEnvelope: InternalFollowingCommandEnvelope[_, _] =>
      requestAggregateForCommandHandling(commandEnvelope.asInstanceOf[InternalFollowingCommandEnvelope[AGGREGATE_ROOT, Any]])
  }

  private def waitingForAggregate(command: InternalCommandEnvelope[AGGREGATE_ROOT, _]) = logReceive {
    case s:Success[_] => handleFollowingCommand(command, s.get.asInstanceOf[Aggregate[AGGREGATE_ROOT]])
    case f:Failure[_] => throw new IllegalStateException("Error getting aggregate")
  }


  override def receive = waitingForCommand

  private def handleFirstCommand[COMMAND <: FirstCommand[AGGREGATE_ROOT, RESPONSE], RESPONSE](envelope: InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE]) = envelope match {
    case InternalFirstCommandEnvelope(respondTo, commandId, command) =>

      val result:CommandResult[Any] = commandHandlers(initialState())(command.asInstanceOf[FirstCommand[AGGREGATE_ROOT, Any]])

      result match {
        case s: CommandSuccess[_, _] =>
          val success: CommandSuccess[AGGREGATE_ROOT, RESPONSE] = s.asInstanceOf[CommandSuccess[AGGREGATE_ROOT, RESPONSE]]
          val resultAggregator = context.actorOf(Props(new ResultAggregator[RESPONSE](respondTo, success.response(aggregateId, AggregateVersion(s.events.size)), responseTimeout)), nextResultAggregatorName)
          repositoryActor ! PersistEvents[AGGREGATE_ROOT](resultAggregator, aggregateId, commandId, command.userId, AggregateVersion.ZERO, success.events)
        case failure: CommandFailure[_, _] =>
          respondTo ! failure.response
      }

  }

  private def nextResultAggregatorName[RESPONSE, COMMAND <: Command[AGGREGATE_ROOT, RESPONSE]]: String = {
    resultAggregatorsCounter += 1
    "ResultAggregator_" + resultAggregatorsCounter
  }

  private def requestAggregateForCommandHandling[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], RESPONSE](commandEnvelope: InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE]): Unit = {
    context.become(waitingForAggregate(commandEnvelope))
    repositoryActor ! GetAggregateRoot(self)
  }

  // TODO handling concurrent command is not thread safe
  private def handleFollowingCommand[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], RESPONSE](envelope: InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE], aggregate: Aggregate[AGGREGATE_ROOT]): Unit = envelope match {
    case InternalConcurrentCommandEnvelope(respondTo, commandId, command) =>
      handleFollowingCommandVersionAware(aggregate, respondTo, command.userId, commandId, command, aggregate.version)
    case InternalFollowingCommandEnvelope(respondTo, commandId, command) =>
      handleFollowingCommandVersionAware(aggregate, respondTo, command.userId, commandId, command, command.expectedVersion)
    case e => throw new IllegalArgumentException(s"Unsupported envelope type [$e]")
  }

  private def handleFollowingCommandVersionAware[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], RESPONSE](aggregate: Aggregate[AGGREGATE_ROOT], respondTo: ActorRef, userId: UserId,
                                                                                              commandId: CommandId, commandEnvelope: Any, expectedVersion: AggregateVersion): Unit = {
    val result = commandHandlers(aggregate.aggregateRoot.get)(commandEnvelope)

    result match {
      case s: CommandSuccess[_, _] =>
        val success = s.asInstanceOf[CommandSuccess[AGGREGATE_ROOT, RESPONSE]]
        val resultAggregator = context.actorOf(Props(new ResultAggregator[RESPONSE](respondTo, success.response(aggregateId, expectedVersion.incrementBy(success.events.size)), responseTimeout)), nextResultAggregatorName)
        repositoryActor ! PersistEvents[AGGREGATE_ROOT](resultAggregator, aggregateId, commandId, userId, expectedVersion, success.events)
      case failure: CommandFailure[_, _] =>
        respondTo ! failure.response
    }
    context.become(waitingForCommand)
  }



}

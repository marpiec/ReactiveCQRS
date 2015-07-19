package io.reactivecqrs.core.commandhandler

import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor
import AggregateRepositoryActor.{GetAggregateRoot, PersistEvents}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.{FirstCommandEnvelope, FollowingCommandEnvelope}
import io.reactivecqrs.core.commandhandler.CommandHandlerActor.{InternalFollowingCommandEnvelope, InternalFirstCommandEnvelope}

import scala.concurrent.duration._

object CommandHandlerActor {

  sealed trait InternalCommandEnvelope[AGGREGATE_ROOT, +RESPONSE]

  case class InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, commandId: CommandId, commandEnvelope: FirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE])
    extends InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE]

  case class InternalFollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, commandId: CommandId, commandEnvelope: FollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE])
    extends InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE]


  case class NoAggregateExist()

}


class CommandHandlerActor[AGGREGATE_ROOT](aggregateId: AggregateId,
                                          repositoryActor: ActorRef,
                                          commandHandlers: AGGREGATE_ROOT => PartialFunction[Any, CommandResult[Any]],
                                           initialState: () => AGGREGATE_ROOT) extends Actor {
  
  var resultAggregatorsCounter = 0

  val responseTimeout = 5.seconds

  private def waitingForCommand = LoggingReceive {
    case commandEnvelope: InternalFirstCommandEnvelope[_, _] =>
      handleFirstCommand(commandEnvelope.asInstanceOf[InternalFirstCommandEnvelope[AGGREGATE_ROOT, Any]])
    case commandEnvelope: InternalFollowingCommandEnvelope[_, _] =>
      requestAggregateForCommandHandling(commandEnvelope.asInstanceOf[InternalFollowingCommandEnvelope[AGGREGATE_ROOT, Any]])
  }

  private def waitingForAggregate(command: InternalFollowingCommandEnvelope[AGGREGATE_ROOT, _]) = LoggingReceive {
    case aggregate: Aggregate[_] => handleFollowingCommand(command, aggregate.asInstanceOf[Aggregate[AGGREGATE_ROOT]])
  }


  override def receive = waitingForCommand

  private def handleFirstCommand[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], RESPONSE](envelope: InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE]) = envelope match {
    case InternalFirstCommandEnvelope(respondTo, commandId, FirstCommandEnvelope(userId, command)) =>
//      val result = commandHandlers(command.getClass.getName).asInstanceOf[CommandHandlerF[AGGREGATE_ROOT]].apply(command.asInstanceOf[COMMAND])

      val result:CommandResult[Any] = commandHandlers(initialState())(command.asInstanceOf[Command[AGGREGATE_ROOT, Any]])

      result match {
        case s: Success[_, _] =>
          val success: Success[AGGREGATE_ROOT, RESPONSE] = s.asInstanceOf[Success[AGGREGATE_ROOT, RESPONSE]]
          val resultAggregator = context.actorOf(Props(new ResultAggregator[RESPONSE](respondTo, success.response(aggregateId, AggregateVersion(s.events.size)), responseTimeout)), nextResultAggregatorName)
          repositoryActor ! PersistEvents[AGGREGATE_ROOT](resultAggregator, aggregateId, commandId, userId, AggregateVersion.ZERO, success.events)
        case failure: Failure[_, _] =>
          ??? // TODO send failure message to requestor
      }

  }

  private def nextResultAggregatorName[RESPONSE, COMMAND <: Command[AGGREGATE_ROOT, RESPONSE]]: String = {
    resultAggregatorsCounter += 1
    "ResultAggregator_" + resultAggregatorsCounter
  }

  private def requestAggregateForCommandHandling[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], RESPONSE](commandEnvelope: InternalFollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE]): Unit = {
    context.become(waitingForAggregate(commandEnvelope))
    repositoryActor ! GetAggregateRoot(self)
  }

  private def handleFollowingCommand[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], RESPONSE](envelope: InternalFollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE], aggregate: Aggregate[AGGREGATE_ROOT]): Unit = envelope match {
    case InternalFollowingCommandEnvelope(respondTo, commandId, FollowingCommandEnvelope(userId, commandAggregateId, expectedVersion, command)) =>
//      val handler = commandHandlers(command.getClass.getName).asInstanceOf[CommandHandlerF[AGGREGATE_ROOT]]
//
//
//      val result = handler.apply(command.asInstanceOf[COMMAND])

      val result = commandHandlers(aggregate.aggregateRoot.get)(command.asInstanceOf[Command[AGGREGATE_ROOT, Any]])

      result match {
        case s: Success[_, _] =>
          val success = s.asInstanceOf[Success[AGGREGATE_ROOT, RESPONSE]]
          val resultAggregator = context.actorOf(Props(new ResultAggregator[RESPONSE](respondTo, success.response(aggregateId, expectedVersion.incrementBy(success.events.size)), responseTimeout)), nextResultAggregatorName)
          repositoryActor ! PersistEvents[AGGREGATE_ROOT](resultAggregator, aggregateId, commandId, userId, expectedVersion, success.events)
        case failure: Failure[_, _] =>
          ??? // TODO send failure message to requestor
      }
      context.become(waitingForCommand)
  }



}

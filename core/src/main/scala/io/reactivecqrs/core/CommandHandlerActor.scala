package io.reactivecqrs.core

import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import io.reactivecqrs.core.AggregateCommandBusActor.{FirstCommandEnvelope, FollowingCommandEnvelope}
import io.reactivecqrs.core.AggregateRepositoryActor.{PersistEvents, GetAggregateRoot}
import io.reactivecqrs.core.CommandHandlerActor._
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId}

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
                                          commandHandlers: Map[String, CommandHandler[AGGREGATE_ROOT, AbstractCommand[AGGREGATE_ROOT, Any], Any]]) extends Actor {
  
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

  private def handleFirstCommand[COMMAND <: FirstCommand[AGGREGATE_ROOT, RESPONSE], RESPONSE](envelope: InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE]) = envelope match {
    case InternalFirstCommandEnvelope(respondTo, commandId, FirstCommandEnvelope(userId, command)) =>
      val result = commandHandlers(command.getClass.getName).asInstanceOf[CommandHandler[AGGREGATE_ROOT, COMMAND, RESPONSE]].handle(aggregateId, command.asInstanceOf[COMMAND])

      result match {
        case s: Success[_, _] =>
          val success: Success[AGGREGATE_ROOT, RESPONSE] = s.asInstanceOf[Success[AGGREGATE_ROOT, RESPONSE]]
          val resultAggregator = context.actorOf(Props(new ResultAggregator[RESPONSE](respondTo, success.response(aggregateId, AggregateVersion(s.events.size)), responseTimeout)), nextResultAggregatorName)
          repositoryActor ! PersistEvents[AGGREGATE_ROOT](resultAggregator, aggregateId, commandId, userId, AggregateVersion.ZERO, success.events)
        case failure: Failure[_, _] =>
          ??? // TODO send failure message to requestor
      }

  }

  private def nextResultAggregatorName[RESPONSE, COMMAND <: FirstCommand[AGGREGATE_ROOT, RESPONSE]]: String = {
    resultAggregatorsCounter += 1
    "ResultAggregator_" + resultAggregatorsCounter
  }

  private def requestAggregateForCommandHandling[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], RESPONSE](commandEnvelope: InternalFollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE]): Unit = {
    context.become(waitingForAggregate(commandEnvelope))
    repositoryActor ! GetAggregateRoot(self)
  }

  private def handleFollowingCommand[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], RESPONSE](envelope: InternalFollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE], aggregate: Aggregate[AGGREGATE_ROOT]): Unit = envelope match {
    case InternalFollowingCommandEnvelope(respondTo, commandId, FollowingCommandEnvelope(userId, commandAggregateId, expectedVersion, command)) =>
      val handler = commandHandlers(command.getClass.getName).asInstanceOf[CommandHandler[AGGREGATE_ROOT, COMMAND, RESPONSE]]
      val result = handler.handle(aggregateId, command.asInstanceOf[COMMAND])

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

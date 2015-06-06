package io.reactivecqrs.core

import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import io.reactivecqrs.core.AggregateCommandBusActor.{FirstCommandEnvelope, FollowingCommandEnvelope}
import io.reactivecqrs.core.AggregateRepositoryActor.{PersistEvents, GetAggregateRoot}
import io.reactivecqrs.core.CommandHandlerActor._
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId}
import io.reactivecqrs.core._

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
  

  println(s"UserCommandHandler with $aggregateId created")


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
        case success: Success[_, _] =>
          val resultAggregator = context.actorOf(Props(new ResultAggregator[RESPONSE](respondTo, success.asInstanceOf[Success[AGGREGATE_ROOT, RESPONSE]].response(aggregateId, AggregateVersion(success.events.size)))), "ResultAggregator")

          println("Created persistence actor " + repositoryActor.path)
          repositoryActor ! PersistEvents[AGGREGATE_ROOT](resultAggregator, aggregateId, commandId, userId, AggregateVersion.ZERO, success.asInstanceOf[Success[AGGREGATE_ROOT, RESPONSE]].events)
          println("...sent " + PersistEvents[AGGREGATE_ROOT](resultAggregator, aggregateId, commandId, userId, AggregateVersion.ZERO, success.asInstanceOf[Success[AGGREGATE_ROOT, RESPONSE]].events))
        case failure: Failure[_, _] =>
          ??? // TODO send failure message to requestor
      }

  }

  private def requestAggregateForCommandHandling[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], RESPONSE](commandEnvelope: InternalFollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE]): Unit = {
    context.become(waitingForAggregate(commandEnvelope))
    repositoryActor ! GetAggregateRoot(self)
  }

  private def handleFollowingCommand[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], RESPONSE](envelope: InternalFollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE], aggregate: Aggregate[AGGREGATE_ROOT]): Unit = envelope match {
    case InternalFollowingCommandEnvelope(respondTo, commandId, FollowingCommandEnvelope(userId, commandAggregateId, expectedVersion, command)) =>
      val result = commandHandlers(command.getClass.getName).asInstanceOf[CommandHandler[AGGREGATE_ROOT, COMMAND, RESPONSE]]
        .handle(aggregateId, command.asInstanceOf[COMMAND])

      result match {
        case success: Success[_, _] =>
          val resultAggregator = context.actorOf(Props(new ResultAggregator[RESPONSE](respondTo, success.asInstanceOf[Success[AGGREGATE_ROOT, RESPONSE]].response(aggregateId, expectedVersion.incrementBy(success.events.size)))), "ResultAggregator")
          repositoryActor ! PersistEvents[AGGREGATE_ROOT](resultAggregator, aggregateId, commandId, userId, expectedVersion, success.asInstanceOf[Success[AGGREGATE_ROOT, RESPONSE]].events)
          println("...sent")
        case failure: Failure[_, _] =>
          ??? // TODO send failure message to requestor
      }
      context.become(waitingForCommand)
  }



}

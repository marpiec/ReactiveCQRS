package io.reactivecqrs.actor

import akka.actor.{ActorRef, Actor, Props}
import akka.event.LoggingReceive
import io.reactivecqrs.actor.AggregateCommandBusActor.{FollowingCommandEnvelope, FirstCommandEnvelope}
import io.reactivecqrs.actor.AggregateRepositoryActor.EventsEnvelope
import io.reactivecqrs.api.guid.{CommandId, AggregateId}
import io.reactivecqrs.core._

import scala.reflect.ClassTag

object CommandHandlerActor {

  case class InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, commandId: CommandId, commandEnvelope: FirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE])

  case class InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, commandId: CommandId, commandEnvelope: FollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE])


}

class CommandHandlerActor[AGGREGATE_ROOT](aggregateId: AggregateId,
                                          commandsHandlersSeq: Seq[CommandHandler[AGGREGATE_ROOT,AbstractCommand[AGGREGATE_ROOT, _],_]],
                                          eventsHandlers: Seq[AbstractEventHandler[AGGREGATE_ROOT, Event[AGGREGATE_ROOT]]])
                                         (implicit aggregateRootClassTag: ClassTag[AGGREGATE_ROOT]) extends Actor {

  import CommandHandlerActor._

  val commandsHandlers:Map[String, CommandHandler[AGGREGATE_ROOT,AbstractCommand[AGGREGATE_ROOT, Any],Any]] =
    commandsHandlersSeq.map(ch => (ch.commandClassName, ch.asInstanceOf[CommandHandler[AGGREGATE_ROOT,AbstractCommand[AGGREGATE_ROOT, Any],Any]])).toMap

  println(s"UserCommandHandler with $aggregateId created")

  override def receive: Receive = LoggingReceive {
    case ice: InternalCommandEnvelope[_, _] => handleCommand[Command[AGGREGATE_ROOT, Any], Any](ice.asInstanceOf[InternalCommandEnvelope[AGGREGATE_ROOT, Any]])
    case ifce: InternalFirstCommandEnvelope[_, _] => println("Received with id " +aggregateId);handleFirstCommand(ifce.asInstanceOf[InternalFirstCommandEnvelope[AGGREGATE_ROOT, Any]])
  }


  def handleCommand[COMMAND <: Command[AGGREGATE_ROOT, RESULT], RESULT](internalCommandEnvelope: InternalCommandEnvelope[AGGREGATE_ROOT, Any]): Unit = {
    println("Handling non first command")
    internalCommandEnvelope match {
      case InternalCommandEnvelope(respondTo, commandId, FollowingCommandEnvelope(userId, aggregateId, expectedVersion, command)) =>
        val result = commandsHandlers(command.getClass.getName).asInstanceOf[CommandHandler[AGGREGATE_ROOT, COMMAND, RESULT]].  handle(aggregateId, command.asInstanceOf[COMMAND])
        
        result match {
          case success: Success[_,_] =>
            val resultAggregator = context.actorOf(Props(new ResultAggregator[RESULT](respondTo, success.asInstanceOf[Success[AGGREGATE_ROOT, RESULT]].response(aggregateId, expectedVersion.incrementBy(success.events.size)))), "ResultAggregator")
            val repositoryActor = context.actorSelection("AggregateRepository" + aggregateId.asLong)
            repositoryActor ! EventsEnvelope[AGGREGATE_ROOT](resultAggregator, aggregateId, commandId, userId, expectedVersion, success.asInstanceOf[Success[AGGREGATE_ROOT, RESULT]].events)
            println("...sent")
          case failure: Failure[_,_] =>
            ???
        }
        

      case c => throw new IllegalArgumentException("Unsupported command " + c)
    }

  }

  def handleFirstCommand[COMMAND <: FirstCommand[AGGREGATE_ROOT, RESULT], RESULT](internalFirstCommandEnvelope: InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESULT]): Unit = {
    println("Handling first command")
    internalFirstCommandEnvelope match {
      case InternalFirstCommandEnvelope(respondTo, commandId, FirstCommandEnvelope(userId, command)) =>
        val result = commandsHandlers(command.getClass.getName).asInstanceOf[CommandHandler[AGGREGATE_ROOT, COMMAND, RESULT]].handle(aggregateId, command.asInstanceOf[COMMAND])

        result match {
          case success: Success[_,_] =>
            val resultAggregator = context.actorOf(Props(new ResultAggregator[RESULT](respondTo, success.asInstanceOf[Success[AGGREGATE_ROOT, RESULT]].response(aggregateId, AggregateVersion(success.events.size)))), "ResultAggregator")
            val newRepositoryActor = context.actorOf(Props(new AggregateRepositoryActor[AGGREGATE_ROOT](aggregateId, eventsHandlers)), "AggregateRepository" + aggregateId.asLong)
            println("Created persistence actor " +newRepositoryActor.path)
            newRepositoryActor ! EventsEnvelope[AGGREGATE_ROOT](resultAggregator, aggregateId, commandId, userId, AggregateVersion.ZERO, success.asInstanceOf[Success[AGGREGATE_ROOT, RESULT]].events)
            println("...sent " + EventsEnvelope[AGGREGATE_ROOT](resultAggregator, aggregateId, commandId, userId, AggregateVersion.ZERO, success.asInstanceOf[Success[AGGREGATE_ROOT, RESULT]].events))
          case failure: Failure[_,_] =>
            ???
        }

      case c => throw new IllegalArgumentException("Unsupported first command " + c)
    }
  }


}

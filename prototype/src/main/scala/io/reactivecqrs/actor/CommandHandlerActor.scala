package io.reactivecqrs.actor

import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import io.reactivecqrs.core._

import scala.reflect.ClassTag

class CommandHandlerActor[AGGREGATE_ROOT](aggregateId: AggregateId,
                                          commandsHandlersSeq: Seq[CommandHandler[AGGREGATE_ROOT,AbstractCommand[AGGREGATE_ROOT, _],_]],
                                          eventsHandlers: Seq[EventHandler[AGGREGATE_ROOT, Event[AGGREGATE_ROOT]]])
                                         (implicit aggregateRootClassTag: ClassTag[AGGREGATE_ROOT]) extends Actor {

  val commandsHandlers:Map[String, CommandHandler[AGGREGATE_ROOT,AbstractCommand[AGGREGATE_ROOT, Any],Any]] =
    commandsHandlersSeq.map(ch => (ch.commandClassName, ch.asInstanceOf[CommandHandler[AGGREGATE_ROOT,AbstractCommand[AGGREGATE_ROOT, Any],Any]])).toMap

  println(s"UserCommandHandler with $aggregateId created")

  override def receive: Receive = LoggingReceive {
    case CommandEnvelope(respondTo, command) => handleCommand[Command[AGGREGATE_ROOT, Any], Any](respondTo, command.asInstanceOf[Command[AGGREGATE_ROOT, Any]])
    case FirstCommandEnvelope(respondTo, firstCommand) => println("Received with id " +aggregateId);handleFirstCommand(respondTo, firstCommand.asInstanceOf[FirstCommand[AGGREGATE_ROOT, Any]])
  }


  def handleCommand[COMMAND <: Command[AGGREGATE_ROOT, RESULT], RESULT](respondTo: ActorRef, command: COMMAND): Unit = {
    command match {
      case c: Command[AGGREGATE_ROOT, RESULT] =>
        val result = commandsHandlers(command.getClass.getName).asInstanceOf[CommandHandler[AGGREGATE_ROOT, COMMAND, RESULT]].handle(aggregateId, c.asInstanceOf[COMMAND])
        val resultAggregator = context.actorOf(Props(new ResultAggregator[RESULT](respondTo, result._2)), "ResultAggregator")
        val newRepositoryActor = context.actorOf(Props(new AggregateRepositoryPersistentActor[AGGREGATE_ROOT](aggregateId, eventsHandlers)), "AggregateRepository" + aggregateId.asLong)
        newRepositoryActor ! EventEnvelope[AGGREGATE_ROOT](resultAggregator, AggregateVersion.ZERO, result._1)

      case c => throw new IllegalArgumentException("Unsupported command " + c)
    }

  }

  def handleFirstCommand[COMMAND <: FirstCommand[AGGREGATE_ROOT, RESULT], RESULT](respondTo: ActorRef, command: FirstCommand[AGGREGATE_ROOT, RESULT]): Unit = {
    command match {
      case c: FirstCommand[AGGREGATE_ROOT, RESULT] =>
        val result = commandsHandlers(command.getClass.getName).asInstanceOf[CommandHandler[AGGREGATE_ROOT, COMMAND, RESULT]].handle(aggregateId, c.asInstanceOf[COMMAND])
        val resultAggregator = context.actorOf(Props(new ResultAggregator(respondTo, result._2)), "ResultAggregator")
        val newRepositoryActor = context.actorOf(Props(new AggregateRepositoryPersistentActor[AGGREGATE_ROOT](aggregateId, eventsHandlers)), "AggregateRepository" + aggregateId.asLong)
        newRepositoryActor ! EventEnvelope[AGGREGATE_ROOT](resultAggregator, AggregateVersion.ZERO, result._1)
      case c => throw new IllegalArgumentException("Unsupported first command " + c)
    }
  }


}

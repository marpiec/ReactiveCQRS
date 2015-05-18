package io.reactivecqrs.actor

import akka.actor.{Actor, Props}
import akka.event.LoggingReceive
import io.reactivecqrs.core._

import scala.reflect.ClassTag

class AkkaCommandBus[AGGREGATE_ROOT](val commandsHandlers: Seq[CommandHandler[AGGREGATE_ROOT,AbstractCommand[AGGREGATE_ROOT, _],_]],
                                     val eventsHandlers: Seq[EventHandler[AGGREGATE_ROOT, Event[AGGREGATE_ROOT]]])
                                    (implicit aggregateRootClassTag: ClassTag[AGGREGATE_ROOT])extends Actor {



  var nextAggregateId = 1

  override def receive: Receive = LoggingReceive {
    case c: Command[_,_] => routeCommand(c.asInstanceOf[Command[AGGREGATE_ROOT, _]])
    case fc: FirstCommand[_,_] => routeFirstCommand(fc.asInstanceOf[FirstCommand[AGGREGATE_ROOT, _]])
    case GetAggregateRoot(id) => routeGetAggregateRoot(id)
  }

  def routeCommand[RESPONSE](command: Command[AGGREGATE_ROOT, RESPONSE]): Unit = {
    println("Routes non first command")
    val existingCommandHandlerActor = context.actorSelection("CommandHandler" + command.id.asLong)
    val respondTo = sender()
    existingCommandHandlerActor ! CommandEnvelope(respondTo, command)
  }

  def routeFirstCommand[RESPONSE](firstCommand: FirstCommand[AGGREGATE_ROOT, RESPONSE]): Unit = {
    println("Routes first command")
    val newAggregateId = nextAggregateId // Actor construction might be delayed so we need to store current aggregate id
    val respondTo = sender() // with sender this shouldn't be the case, but just to be sure
    val newCommandHandlerActor = context.actorOf(Props(new CommandHandlerActor[AGGREGATE_ROOT](AggregateId(newAggregateId), commandsHandlers, eventsHandlers)), "CommandHandler" + newAggregateId)
    newCommandHandlerActor ! FirstCommandEnvelope(respondTo, firstCommand)

    nextAggregateId += 1

  }

  def routeGetAggregateRoot(id: AggregateId): Unit = {
    println(s"Routes routeGetAggregateRoot $id")
    val respondTo = sender()
    val aggregate = context.actorSelection("CommandHandler" + id.asLong+"/" + "AggregateRepository"+id.asLong)
    aggregate ! ReturnAggregateRoot(respondTo)
  }
}

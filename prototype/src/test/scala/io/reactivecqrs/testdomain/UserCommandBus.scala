package io.reactivecqrs.testdomain

import akka.actor.{Props, Actor}
import io.reactivecqrs.core.{FirstCommandEnvelope, CommandEnvelope, FirstCommand, Command}

class UserCommandBus extends Actor {

  var nextAggregateId = 1

  override def receive: Receive = {
    case c: Command[_,_] => routeCommand(c)
    case fc: FirstCommand[_,_] => routeFirstCommand(fc)
  }
  
  def routeCommand[AGGREGATE_ROOT,RESPONSE](command: Command[AGGREGATE_ROOT, RESPONSE]): Unit = {
    val existingCommandHandlerActor = context.actorSelection("CommandHandler" + command.id.asLong)
    existingCommandHandlerActor ! CommandEnvelope(sender(), command)
  }

  def routeFirstCommand[AGGREGATE_ROOT,RESPONSE](firstCommand: FirstCommand[AGGREGATE_ROOT, RESPONSE]): Unit = {
    val newCommandHandlerActor = context.actorOf(Props(new UserCommandHandler), "CommandHandler" + nextAggregateId)
    nextAggregateId += 1

    newCommandHandlerActor ! FirstCommandEnvelope(sender(), firstCommand)

  }
}

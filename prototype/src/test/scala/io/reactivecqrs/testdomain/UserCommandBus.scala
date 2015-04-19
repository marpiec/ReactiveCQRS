package io.reactivecqrs.testdomain

import akka.actor.{Props, Actor}
import akka.event.LoggingReceive
import io.reactivecqrs.core._

class UserCommandBus extends Actor {

  var nextAggregateId = 1

  override def receive: Receive = LoggingReceive {
    case c: Command[_,_] => routeCommand(c)
    case fc: FirstCommand[_,_] => routeFirstCommand(fc)
    case GetAggregateRoot(id) => routeGetAggregateRoot(id)
  }

  def routeCommand[AGGREGATE_ROOT,RESPONSE](command: Command[AGGREGATE_ROOT, RESPONSE]): Unit = {
    val existingCommandHandlerActor = context.actorSelection("CommandHandler" + command.id.asLong)
    val respondTo = sender()
    existingCommandHandlerActor ! CommandEnvelope(respondTo, command)
  }

  def routeFirstCommand[AGGREGATE_ROOT,RESPONSE](firstCommand: FirstCommand[AGGREGATE_ROOT, RESPONSE]): Unit = {
    val newAggregateId = nextAggregateId // Actor construction might be delayed so we need to store current aggregate id
    val respondTo = sender() // with sender this shouldn't be the case, but just to be sure
    val newCommandHandlerActor = context.actorOf(Props(new UserCommandHandler(AggregateId(newAggregateId))), "CommandHandler" + newAggregateId)
    newCommandHandlerActor ! FirstCommandEnvelope(respondTo, firstCommand)

    nextAggregateId += 1

  }

  def routeGetAggregateRoot(id: AggregateId): Unit = {
    val respondTo = sender()
    val aggregate = context.actorSelection("CommandHandler" + id.asLong+"/" + "UserAggregate"+id.asLong)
    aggregate ! ReturnAggregateRoot(respondTo)
  }
}

package io.reactivecqrs.core.commandlog

import akka.actor.Actor
import io.reactivecqrs.api.command.{LogCommand, LogConcurrentCommand, LogFirstCommand}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.util.ActorLogging



class CommandLogActor[AGGREGATE_ROOT](aggregateId: AggregateId, commandLogState: CommandLogState) extends Actor with ActorLogging {

  override def receive: Receive = {
    case LogCommand(commandId, command) => commandLogState.storeCommand(commandId, aggregateId, command)
    case LogFirstCommand(commandId, command) => commandLogState.storeFirstCommand(commandId, aggregateId, command)
    case LogConcurrentCommand(commandId, command) => commandLogState.storeConcurrentCommand(commandId, aggregateId, command)
  }
}

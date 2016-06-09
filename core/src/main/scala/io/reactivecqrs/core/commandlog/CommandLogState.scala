package io.reactivecqrs.core.commandlog

import io.reactivecqrs.api.{Command, ConcurrentCommand, CustomCommandResponse, FirstCommand}
import io.reactivecqrs.api.id.{AggregateId, CommandId}

abstract class CommandLogState {
  def storeFirstCommand(commandId: CommandId, aggregateId: AggregateId, command: FirstCommand[_, _ <: CustomCommandResponse[_]]): Unit
  def storeCommand(commandId: CommandId, aggregateId: AggregateId, command: Command[_, _ <: CustomCommandResponse[_]]): Unit
  def storeConcurrentCommand(commandId: CommandId, aggregateId: AggregateId, command: ConcurrentCommand[_, _ <: CustomCommandResponse[_]]): Unit
}

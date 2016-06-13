package io.reactivecqrs.core.commandlog
import io.reactivecqrs.api.{Command, ConcurrentCommand, CustomCommandResponse, FirstCommand}
import io.reactivecqrs.api.id.{AggregateId, CommandId}


class MemoryCommandLogState extends CommandLogState {
  override def storeFirstCommand(commandId: CommandId, aggregateId: AggregateId, command: FirstCommand[_, _ <: CustomCommandResponse[_]]): Unit = ()

  override def storeConcurrentCommand(commandId: CommandId, aggregateId: AggregateId, command: ConcurrentCommand[_, _ <: CustomCommandResponse[_]]): Unit = ()

  override def storeCommand(commandId: CommandId, aggregateId: AggregateId, command: Command[_, _ <: CustomCommandResponse[_]]): Unit = ()
}

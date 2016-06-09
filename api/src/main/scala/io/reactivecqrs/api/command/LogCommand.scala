package io.reactivecqrs.api.command

import io.reactivecqrs.api.id.CommandId
import io.reactivecqrs.api.{Command, ConcurrentCommand, CustomCommandResponse, FirstCommand}

case class LogCommand[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]](commandId: CommandId, command: Command[AGGREGATE_ROOT, RESPONSE])

case class LogFirstCommand[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]](commandId: CommandId, command: FirstCommand[AGGREGATE_ROOT, RESPONSE])

case class LogConcurrentCommand[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]](commandId: CommandId, command: ConcurrentCommand[AGGREGATE_ROOT, RESPONSE])

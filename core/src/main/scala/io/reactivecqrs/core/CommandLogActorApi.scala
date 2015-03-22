package io.reactivecqrs.core

import java.time.Instant

import io.reactivecqrs.api.command.{CommandLogTransform, Command}
import io.reactivecqrs.api.guid.{UserId, CommandId}

case class LogCommand[AGGREGATE, RESPONSE](commandId: CommandId, userId: UserId, timestamp: Instant, command: Command[AGGREGATE, RESPONSE])

trait CommandLogActorApi {

  def logCommand[COMMAND <: Command[AGGREGATE, RESPONSE], AGGREGATE, RESPONSE](commandId: CommandId, userUid: UserId, timestamp: Instant, command: COMMAND): Unit = {
    addTransformedCommand(commandId, userUid, transformIfNeeded(command))
  }

  protected def addTransformedCommand[COMMAND <: Command[AGGREGATE, RESPONSE], AGGREGATE, RESPONSE](commandId: CommandId, userUid: UserId, command: COMMAND)

  private def transformIfNeeded[COMMAND <: Command[AGGREGATE, RESPONSE], AGGREGATE, RESPONSE](command: COMMAND) = command match {
    case transformableCommand: CommandLogTransform => transformableCommand.transform()
    case _ => command
  }

  def getCommandById[COMMAND <: Command[AGGREGATE, RESPONSE], AGGREGATE, RESPONSE](commandId: CommandId): CommandRow[RESPONSE]
}

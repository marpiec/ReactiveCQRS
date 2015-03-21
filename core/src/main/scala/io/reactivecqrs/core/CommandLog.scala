package io.reactivecqrs.core

import io.reactivecqrs.api.command.{CommandLogTransform, Command}
import io.reactivecqrs.api.guid.{UserId, CommandId}

trait CommandLog {

  def logCommand[COMMAND <: Command[AGGREGATE, RESPONSE], AGGREGATE, RESPONSE](commandId: CommandId, userUid: UserId, command: COMMAND): Unit = {
    addTransformedCommand(commandId, userUid, transformIfNeeded(command))
  }

  protected def addTransformedCommand[COMMAND <: Command[AGGREGATE, RESPONSE], AGGREGATE, RESPONSE](commandId: CommandId, userUid: UserId, command: COMMAND)

  private def transformIfNeeded[COMMAND <: Command[AGGREGATE, RESPONSE], AGGREGATE, RESPONSE](command: COMMAND) = command match {
    case transformableCommand: CommandLogTransform => transformableCommand.transform()
    case _ => command
  }

  def getCommandById[COMMAND <: Command[AGGREGATE, RESPONSE], AGGREGATE, RESPONSE](commandId: CommandId): CommandRow[RESPONSE]
}

package io.reactivecqrs.core

import java.time.Instant

import io.reactivecqrs.api.command.{Command, CommandLogTransform}
import io.reactivecqrs.api.guid.{UserId, CommandId}

case class LogCommand[AGGREGATE_ROOT, RESPONSE](commandId: CommandId, userId: UserId, timestamp: Instant, command: Command[AGGREGATE_ROOT, RESPONSE])

trait CommandLogActorApi {

  def logCommand[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], AGGREGATE_ROOT, RESPONSE](commandId: CommandId, userUid: UserId, timestamp: Instant, command: COMMAND): Unit = {
    addTransformedCommand[COMMAND, AGGREGATE_ROOT, RESPONSE](commandId, userUid, transformIfNeeded[COMMAND, AGGREGATE_ROOT, RESPONSE](command))
  }

  protected def addTransformedCommand[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], AGGREGATE_ROOT, RESPONSE](commandId: CommandId, userUid: UserId, command: COMMAND)

  private def transformIfNeeded[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], AGGREGATE_ROOT, RESPONSE](command: COMMAND): COMMAND = command match {
    case transformableCommand: CommandLogTransform[_, _] => transformableCommand.asInstanceOf[CommandLogTransform[AGGREGATE_ROOT, RESPONSE]].transform().asInstanceOf[COMMAND]
    case _ => command
  }

  def getCommandById[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], AGGREGATE_ROOT, RESPONSE](commandId: CommandId): CommandRow[RESPONSE]
}

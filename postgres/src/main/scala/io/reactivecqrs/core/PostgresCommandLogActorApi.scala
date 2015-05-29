package io.reactivecqrs.core

import io.reactivecqrs.api.command.Command
import io.reactivecqrs.api.id.{UserId, CommandId}

class PostgresCommandLogActorApi extends CommandLogActorApi {

  override protected def addTransformedCommand[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], AGGREGATE_ROOT, RESPONSE](commandId: CommandId, userUid: UserId, command: COMMAND): Unit = ???

  override def getCommandById[COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], AGGREGATE_ROOT, RESPONSE](commandId: CommandId): CommandRow[RESPONSE] = ???

}

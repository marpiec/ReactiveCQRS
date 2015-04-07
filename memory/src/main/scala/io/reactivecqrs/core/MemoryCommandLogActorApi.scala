package io.reactivecqrs.core

import io.reactivecqrs.api.command.Command
import io.reactivecqrs.api.guid.{UserId, CommandId}

class MemoryCommandLogActorApi extends CommandLogActorApi {

  override protected def addTransformedCommand[COMMAND <: Command[AGGREGATE, RESPONSE], AGGREGATE, RESPONSE](commandId: CommandId, userUid: UserId, command: COMMAND): Unit = ???

  override def getCommandById[COMMAND <: Command[AGGREGATE, RESPONSE], AGGREGATE, RESPONSE](commandId: CommandId): CommandRow[RESPONSE] = ???

}

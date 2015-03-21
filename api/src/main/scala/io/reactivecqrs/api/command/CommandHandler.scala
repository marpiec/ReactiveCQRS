package io.reactivecqrs.api.command

import io.reactivecqrs.api.guid.{UserId, CommandId}

/**
 * Responsible for handling command send by user.
 * @tparam COMMAND Type of command that can be handled.
 * @tparam RESPONSE Response send to user after attempt to handle command.
 */
trait CommandHandler[COMMAND <: Command[RESPONSE], RESPONSE] {

  def handle(commandId: CommandId, userId: UserId, command: COMMAND): RESPONSE

  def commandClass: Class[COMMAND] // TODO extract from declaration
}

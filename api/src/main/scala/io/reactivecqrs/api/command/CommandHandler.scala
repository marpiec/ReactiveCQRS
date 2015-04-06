package io.reactivecqrs.api.command

import akka.actor.ActorRef
import io.reactivecqrs.api.guid.{CommandId, UserId}


sealed abstract class CommandHandler[AGGREGATE, COMMAND <: Command[AGGREGATE, RESPONSE], RESPONSE] {




  def commandClass: Class[COMMAND] // TODO extract from declaration
}


trait FirstCommandHandler[AGGREGATE, COMMAND <: FirstCommand[AGGREGATE, RESPONSE], RESPONSE] extends CommandHandler[AGGREGATE, COMMAND, RESPONSE] {
  /**
   * validation
   * validation against aggregate state
   * validation against other services
   * store events
   * Retry if conflict
   * Side effects after success
   * Create response
   */
  def handle(commandId: CommandId,
             command: COMMAND): RESPONSE
}


trait FollowingCommandHandler[AGGREGATE, COMMAND <: FollowingCommand[AGGREGATE, RESPONSE], RESPONSE] extends CommandHandler[AGGREGATE, COMMAND, RESPONSE] {
  /**
   * validation
   * validation against aggregate state
   * validation against other services
   * store events
   * Retry if conflict
   * Side effects after success
   * Create response
   */
  def handle(commandId: CommandId,
             aggregateRoot: AGGREGATE,
             command: COMMAND): RESPONSE
}
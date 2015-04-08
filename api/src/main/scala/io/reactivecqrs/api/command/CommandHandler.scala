package io.reactivecqrs.api.command

import io.reactivecqrs.api.exception.CqrsException
import io.reactivecqrs.api.guid.{AggregateVersion, AggregateId, CommandId, UserId}
import io.reactivecqrs.utils.Result


sealed abstract class CommandHandler[AGGREGATE, COMMAND <: Command[AGGREGATE, RESPONSE], RESPONSE] {




  def commandClass: Class[COMMAND] // TODO extract from declaration
}


abstract class FirstCommandHandler[AGGREGATE, COMMAND <: FirstCommand[AGGREGATE, RESPONSE], RESPONSE] extends CommandHandler[AGGREGATE, COMMAND, RESPONSE] {
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
             userId: UserId,
             command: COMMAND,
             repository: RepositoryFirstEventHandler[AGGREGATE]): Result[RESPONSE, CqrsException]
}


abstract class FollowingCommandHandler[AGGREGATE, COMMAND <: FollowingCommand[AGGREGATE, RESPONSE], RESPONSE] extends CommandHandler[AGGREGATE, COMMAND, RESPONSE] {
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
             userId: UserId,
             aggregateRoot: AGGREGATE,
             command: COMMAND,
             repository: RepositoryFollowingEventHandler[AGGREGATE]): Result[RESPONSE, CqrsException]
}
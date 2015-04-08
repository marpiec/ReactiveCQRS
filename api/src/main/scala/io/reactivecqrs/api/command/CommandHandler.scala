package io.reactivecqrs.api.command

import io.reactivecqrs.api.exception.CqrsException
import io.reactivecqrs.api.guid.{CommandId, UserId}
import io.reactivecqrs.utils.Result


sealed abstract class CommandHandler[AGGREGATE, COMMAND <: Command[AGGREGATE, RESPONSE], RESPONSE](implicit ev: Manifest[COMMAND]) {
  val commandClass:Class[COMMAND] = ev.runtimeClass.asInstanceOf[Class[COMMAND]]
}


abstract class FirstCommandHandler[AGGREGATE, COMMAND <: FirstCommand[AGGREGATE, RESPONSE], RESPONSE](implicit ev: Manifest[COMMAND]) extends CommandHandler[AGGREGATE, COMMAND, RESPONSE] {
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


abstract class FollowingCommandHandler[AGGREGATE, COMMAND <: FollowingCommand[AGGREGATE, RESPONSE], RESPONSE](implicit ev: Manifest[COMMAND]) extends CommandHandler[AGGREGATE, COMMAND, RESPONSE] {
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
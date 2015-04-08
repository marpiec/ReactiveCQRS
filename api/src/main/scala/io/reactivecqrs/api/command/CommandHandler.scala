package io.reactivecqrs.api.command

import io.reactivecqrs.api.exception.CqrsException
import io.reactivecqrs.api.guid.{CommandId, UserId}
import io.reactivecqrs.utils.Result


sealed abstract class CommandHandler[AGGREGATE_ROOT, COMMAND <: Command[AGGREGATE_ROOT, RESPONSE], RESPONSE](implicit ev: Manifest[COMMAND]) {
  val commandClass:Class[COMMAND] = ev.runtimeClass.asInstanceOf[Class[COMMAND]]
}


abstract class FirstCommandHandler[AGGREGATE_ROOT, COMMAND <: FirstCommand[AGGREGATE_ROOT, RESPONSE], RESPONSE](implicit ev: Manifest[COMMAND]) extends CommandHandler[AGGREGATE_ROOT, COMMAND, RESPONSE] {
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
             repository: RepositoryFirstEventHandler[AGGREGATE_ROOT]): Result[RESPONSE, CqrsException]
}


abstract class FollowingCommandHandler[AGGREGATE_ROOT, COMMAND <: FollowingCommand[AGGREGATE_ROOT, RESPONSE], RESPONSE](implicit ev: Manifest[COMMAND]) extends CommandHandler[AGGREGATE_ROOT, COMMAND, RESPONSE] {
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
             aggregateRoot: AGGREGATE_ROOT,
             command: COMMAND,
             repository: RepositoryFollowingEventHandler[AGGREGATE_ROOT]): Result[RESPONSE, CqrsException]
}
package io.reactivecqrs.api.command

import io.reactivecqrs.api.guid.{AggregateVersion, AggregateId, UserId}


case class CommandEnvelope[COMMAND <: Command[_, _]](acknowledgeId: String, userId: UserId, command: COMMAND)

/**
 * Trait that should be implemented by command class that will be instantiated by user.
 * @tparam RESPONSE Type of response that will be send to user after command handling.
 */
sealed abstract class Command[AGGREGATE, RESPONSE]

abstract class FirstCommand[AGGREGATE, RESPONSE] extends Command[AGGREGATE, RESPONSE]

abstract class FollowingCommand[AGGREGATE, RESPONSE] extends Command[AGGREGATE, RESPONSE] {
  def aggregateId: AggregateId
  def expectedVersion: AggregateVersion
}
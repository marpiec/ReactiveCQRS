package io.reactivecqrs.api.command

import io.reactivecqrs.api.exception.CqrsException
import io.reactivecqrs.api.guid.{AggregateVersion, AggregateId, UserId}
import io.reactivecqrs.utils.Result


case class CommandEnvelope[COMMAND <: Command[_, _]](acknowledgeId: String, userId: UserId, command: COMMAND)

case class CommandResponseEnvelope[RESPONSE](acknowledgeId: String, response: Result[RESPONSE, CqrsException])

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
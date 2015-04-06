package io.reactivecqrs.api.command

import io.reactivecqrs.api.guid.{AggregateVersion, AggregateId, UserId}

/**
 * Trait that should be implemented by command class that will be instantiated by user.
 * @tparam RESPONSE Type of response that will be send to user after command handling.
 */
sealed abstract class Command[AGGREGATE, RESPONSE] {
  def acknowledgeId: String
  def userId: UserId
}

abstract class FirstCommand[AGGREGATE, RESPONSE] extends Command[AGGREGATE, RESPONSE]

abstract class FollowingCommand[AGGREGATE, RESPONSE] {
  def aggregateId: AggregateId
  def expectedVersion: AggregateVersion
}
package io.reactivecqrs.api.guid

/**
 * Globally unique id that identifies user of the application, originator of command.
 * It might be the same as AggregateId, if user is represented as Aggregate in the system.
 * @param asLong unique long identifier across users.
 */
case class UserId(asLong: Long)


object UserId {
  def fromAggregateId(id: AggregateId) = UserId(id.asLong)
}

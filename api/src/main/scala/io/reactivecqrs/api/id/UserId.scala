package io.reactivecqrs.api.id

/**
 * The user who sent a command, stored with each of its events. The library does not interpret it; it may be the id of
 * an aggregate that stands for the user.
 *
 * @param asLong the user's id.
 */
case class UserId(asLong: Long)

object UserId {
  /** The user that the aggregate `id` stands for. */
  def fromAggregateId(id: AggregateId): UserId = UserId(id.asLong)
}

package io.reactivecqrs.api.id

import io.reactivecqrs.api.AggregateVersion

/**
 * Globally unique id that identifies single aggregate in whole application.
 *
 * @param asLong unique long identifier across aggregates.
 */
case class AggregateId(asLong: Long)


case class AggregateIdWithVersion(id: AggregateId, version: AggregateVersion)

object SpaceId {
  def apply(id: AggregateId): SpaceId = SpaceId(id.asLong)
  val unknown = SpaceId(-1L)
}
case class SpaceId(asLong: Long)
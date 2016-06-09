package io.reactivecqrs.core.aggregaterepository

import java.time.Instant

import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api.{AggregateType, AggregateVersion, Event}

case class IdentifiableEventNoAggregateType[AGGREGATE_ROOT](eventId: Long, aggregateId: AggregateId, version: AggregateVersion, event: Event[AGGREGATE_ROOT], userId: UserId, timestamp: Instant)


case class IdentifiableEvent[AGGREGATE_ROOT](eventId: Long, aggregateType: AggregateType, aggregateId: AggregateId, version: AggregateVersion, event: Event[AGGREGATE_ROOT], userId: UserId, timestamp: Instant)

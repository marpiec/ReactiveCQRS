package io.reactivecqrs.api

import io.reactivecqrs.api.event.Event
import io.reactivecqrs.api.guid.AggregateId

case class AggregateUpdatedNotification[AGGREGATE_ROOT](aggregate: Aggregate[AGGREGATE_ROOT])

case class NewEventForAggregateNotification[AGGREGATE_ROOT](aggregateId: AggregateId, version: Int, event: Event[AGGREGATE_ROOT])

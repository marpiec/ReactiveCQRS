package io.reactivecqrs.api

import io.reactivecqrs.api.event.Event
import io.reactivecqrs.api.guid.AggregateId

case class AggregateUpdatedNotification[AGGREGATE](aggregate: Aggregate[AGGREGATE])

case class NewEventForAggregateNotification[AGGREGATE](aggregateId: AggregateId, version: Int, event: Event[AGGREGATE])

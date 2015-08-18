package io.reactivecqrs.core.aggregaterepository

import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.api.{AggregateType, AggregateVersion, Event}

case class IdentifiableEventNoAggregateType[AGGREGATE_ROOT](aggregateId: AggregateId, version: AggregateVersion, event: Event[AGGREGATE_ROOT])


case class IdentifiableEvent[AGGREGATE_ROOT](aggregateType: AggregateType, aggregateId: AggregateId, version: AggregateVersion, event: Event[AGGREGATE_ROOT])

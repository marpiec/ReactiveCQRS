package io.reactivecqrs.core.api

import io.reactivecqrs.api.{Event, AggregateVersion}
import io.reactivecqrs.api.id.AggregateId

case class IdentifiableEvent[AGGREGATE_ROOT](aggregateId: AggregateId, version: AggregateVersion, event: Event[AGGREGATE_ROOT])

package io.reactivecqrs.core.aggregaterepository

import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId

case class EventIdentifier(eventId: Long, aggregateId: AggregateId, version: AggregateVersion)

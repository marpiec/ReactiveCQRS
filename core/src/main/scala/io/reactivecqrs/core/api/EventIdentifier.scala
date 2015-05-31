package io.reactivecqrs.core.api

import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId

case class EventIdentifier(aggregateId: AggregateId, version: AggregateVersion)

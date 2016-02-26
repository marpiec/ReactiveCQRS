package io.reactivecqrs.core.errors

import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.api.{AggregateType, AggregateVersion}

case class AggregateConcurrentModificationError(aggregateType: AggregateType, aggregateId: AggregateId, eventTypes: Seq[String], expected: AggregateVersion, was: AggregateVersion)

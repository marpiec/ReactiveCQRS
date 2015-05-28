package io.reactivecqrs.api

import io.reactivecqrs.api.guid.AggregateId
import io.reactivecqrs.core.AggregateVersion

case class Aggregate[AGGREGATE_ROOT](id: AggregateId, version: AggregateVersion, aggregateRoot: Option[AGGREGATE_ROOT])

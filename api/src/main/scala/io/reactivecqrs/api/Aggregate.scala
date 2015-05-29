package io.reactivecqrs.api

import io.reactivecqrs.api.id.AggregateId

case class Aggregate[AGGREGATE_ROOT](id: AggregateId, version: AggregateVersion, aggregateRoot: Option[AGGREGATE_ROOT])

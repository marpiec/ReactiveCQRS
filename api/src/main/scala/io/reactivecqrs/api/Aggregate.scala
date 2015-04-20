package io.reactivecqrs.api

import io.reactivecqrs.api.guid.{AggregateVersion, AggregateId}


case class AggregateRoot[AGGREGATE_ROOT](id: AggregateId, version: AggregateVersion, aggregateRoot: Option[AGGREGATE_ROOT])

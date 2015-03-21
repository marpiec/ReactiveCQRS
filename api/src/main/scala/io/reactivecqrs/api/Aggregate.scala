package io.reactivecqrs.api

import io.reactivecqrs.api.guid.AggregateId


case class Aggregate[AGGREGATE](id: AggregateId, version: Int, aggregateRoot: Option[AGGREGATE])

package io.reactivecqrs.api

import io.reactivecqrs.api.guid.{AggregateVersion, AggregateId}


case class Aggregate[AGGREGATE](id: AggregateId, version: AggregateVersion, aggregateRoot: Option[AGGREGATE])

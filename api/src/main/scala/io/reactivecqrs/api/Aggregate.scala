package io.reactivecqrs.api

import java.time.Instant

import io.reactivecqrs.api.id.{AggregateId, UserId}

import scala.reflect.runtime.universe._


case class AggregateType(typeName: String) {
  def simpleName = typeName.substring(typeName.lastIndexOf(".") + 1)
}

case class EventType(typeName: String)

case class AggregateWithType[AGGREGATE_ROOT](aggregateType: AggregateType, id: AggregateId, version: AggregateVersion, aggregateRoot: Option[AGGREGATE_ROOT], eventId: Long)

case class AggregateWithTypeAndEvent[AGGREGATE_ROOT](aggregateType: AggregateType, id: AggregateId, version: AggregateVersion, aggregateRoot: Option[AGGREGATE_ROOT], event: Event[AGGREGATE_ROOT], eventId: Long, userId: UserId, timestamp: Instant)

case class Aggregate[AGGREGATE_ROOT: TypeTag](id: AggregateId, version: AggregateVersion, aggregateRoot: Option[AGGREGATE_ROOT])

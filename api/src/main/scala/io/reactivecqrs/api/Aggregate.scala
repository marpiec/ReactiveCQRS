package io.reactivecqrs.api

import io.reactivecqrs.api.id.AggregateId
import scala.reflect.runtime.universe._


case class AggregateType(typeName: String) {
  def simpleName = typeName.substring(typeName.lastIndexOf(".") + 1)
}

case class EventType(typeName: String)

case class AggregateWithType[AGGREGATE_ROOT](aggregateType: AggregateType, id: AggregateId, version: AggregateVersion, aggregateRoot: Option[AGGREGATE_ROOT])

case class AggregateWithTypeAndEvent[AGGREGATE_ROOT](aggregateType: AggregateType, id: AggregateId, version: AggregateVersion, aggregateRoot: Option[AGGREGATE_ROOT], event: Event[AGGREGATE_ROOT])

case class Aggregate[AGGREGATE_ROOT: TypeTag](id: AggregateId, version: AggregateVersion, aggregateRoot: Option[AGGREGATE_ROOT])

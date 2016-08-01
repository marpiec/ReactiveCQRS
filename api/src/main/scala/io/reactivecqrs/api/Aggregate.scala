package io.reactivecqrs.api

import java.time.Instant

import io.reactivecqrs.api.id.{AggregateId, UserId}

import scala.reflect.runtime.universe._


case class AggregateType(typeName: String) {
  def simpleName = typeName.substring(typeName.lastIndexOf(".") + 1)
}

case class EventType(typeName: String)

case class Aggregate[AGGREGATE_ROOT: TypeTag](id: AggregateId, version: AggregateVersion, aggregateRoot: Option[AGGREGATE_ROOT])




case class EventInfo[AGGREGATE_ROOT](version: AggregateVersion, event: Event[AGGREGATE_ROOT], userId: UserId, timestamp: Instant)

case class AggregateWithType[AGGREGATE_ROOT](aggregateType: AggregateType, id: AggregateId, version: AggregateVersion, eventsCount: Int, aggregateRoot: Option[AGGREGATE_ROOT])

case class AggregateWithTypeAndEvents[AGGREGATE_ROOT](aggregateType: AggregateType, id: AggregateId, aggregateRoot: Option[AGGREGATE_ROOT], events: Seq[EventInfo[AGGREGATE_ROOT]])

case class IdentifiableEvents[AGGREGATE_ROOT](aggregateType: AggregateType, aggregateId: AggregateId, events: Seq[EventInfo[AGGREGATE_ROOT]])




case class IdentifiableEventNoAggregateType[AGGREGATE_ROOT](aggregateId: AggregateId, version: AggregateVersion, event: Event[AGGREGATE_ROOT], userId: UserId, timestamp: Instant)

case class IdentifiableEvent[AGGREGATE_ROOT](aggregateType: AggregateType, aggregateId: AggregateId, version: AggregateVersion, event: Event[AGGREGATE_ROOT], userId: UserId, timestamp: Instant)



case class EventIdentifier(aggregateId: AggregateId, version: AggregateVersion)

case class EventsIdentifiers(aggregateId: AggregateId, versions: Seq[AggregateVersion])
package io.reactivecqrs.api

import java.time.Instant

import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}

import scala.reflect.runtime.universe._


case class AggregateType(typeName: String) {
  def simpleName = typeName.substring(typeName.lastIndexOf(".") + 1)
}

case class EventType(typeName: String)

case class Aggregate[AGGREGATE_ROOT: TypeTag](id: AggregateId, version: AggregateVersion, aggregateRoot: Option[AGGREGATE_ROOT])



/** Who acted and why, stored with every event of a command (R13, D32): the actor as the application encodes it (a
  * person, an agent, an application, a remote organization, someone acting for another), the correlation and causation
  * ids, the W3C trace context, the organization, and entries of the application's own. */
case class EventMetadata(actor: String,
                         correlationId: Option[String] = None,
                         causationId: Option[String] = None,
                         traceContext: Option[String] = None,
                         spaceId: Option[SpaceId] = None,
                         custom: Map[String, String] = Map.empty)

object EventMetadata {
  /** The envelope of an event stored without one (before 1.0, or by a command without metadata): its user. */
  def ofUser(userId: UserId): EventMetadata = EventMetadata(actor = "user:" + userId.asLong)
}

/** An event with its version and who stored it when. `storedMetadata`: the envelope stored with it, if any. */
case class EventInfo[AGGREGATE_ROOT](version: AggregateVersion, event: Event[AGGREGATE_ROOT], userId: UserId, timestamp: Instant,
                                     storedMetadata: Option[EventMetadata] = None) {
  /** The event's metadata envelope; for an event stored without one, its user as the actor (D32). */
  def metadata: EventMetadata = storedMetadata.getOrElse(EventMetadata.ofUser(userId))
}

case class AggregateWithType[AGGREGATE_ROOT](aggregateType: AggregateType, id: AggregateId, version: AggregateVersion, events: Seq[AggregateVersion], aggregateRoot: Option[AGGREGATE_ROOT], replayed: Boolean)

case class AggregateWithTypeAndEvents[AGGREGATE_ROOT](aggregateType: AggregateType, id: AggregateId, aggregateRoot: Option[AGGREGATE_ROOT], events: Seq[EventInfo[AGGREGATE_ROOT]], replayed: Boolean)

case class IdentifiableEvents[AGGREGATE_ROOT](aggregateType: AggregateType, aggregateId: AggregateId, events: Seq[EventInfo[AGGREGATE_ROOT]], replayed: Boolean)




case class IdentifiableEventNoAggregateType[AGGREGATE_ROOT](aggregateId: AggregateId, version: AggregateVersion, event: Event[AGGREGATE_ROOT], userId: UserId, timestamp: Instant)

case class IdentifiableEvent[AGGREGATE_ROOT](aggregateType: AggregateType, aggregateId: AggregateId, version: AggregateVersion, event: Event[AGGREGATE_ROOT], userId: UserId, timestamp: Instant)



case class EventWithIdentifier[AGGREGATE_ROOT](aggregateId: AggregateId, version: AggregateVersion, event: Event[AGGREGATE_ROOT])

case class EventIdentifier(aggregateId: AggregateId, version: AggregateVersion) {
  override def toString: String = s"${aggregateId.asLong}#${version.asInt}"
}

case class EventsIdentifiers(aggregateId: AggregateId, versions: Seq[AggregateVersion])

case class EventWithVersion[AGGREGATE_ROOT](version: AggregateVersion, event: Event[AGGREGATE_ROOT])
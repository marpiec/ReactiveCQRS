package io.reactivecqrs.api

import java.time.Instant

import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}

import scala.reflect.{ClassTag, classTag}

/** The key of an aggregate type: its root class's full name, under which the store keeps its aggregates. */
case class AggregateType(typeName: String) {
  /** The root class's name without its package. */
  def simpleName: String = typeName.substring(typeName.lastIndexOf(".") + 1)
}

object AggregateType {
  /** The key of an aggregate root class (D75, F50): its name as the store has always kept it, Scala's full name
    * (`Outer.Inner` for a class nested in another or in an object), taken from the class without runtime reflection. */
  def of[AGGREGATE_ROOT: ClassTag]: AggregateType = ofClass(classTag[AGGREGATE_ROOT].runtimeClass)
  /** The key of `rootClass`, as `of` gives it. */
  def ofClass(rootClass: Class[_]): AggregateType = AggregateType(rootClass.getName.replace('$', '.'))
}

/** An aggregate as a read answers it: its id, its version, and its root, `None` when it is deleted. */
case class Aggregate[AGGREGATE_ROOT](id: AggregateId, version: AggregateVersion, aggregateRoot: Option[AGGREGATE_ROOT])

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

// The delivery messages of a subscription (one per listener kind). `startsAtSquash`: the first version delivered is the
// aggregate's squash event, so a subscriber behind it starts there, as nothing before it exists any more (F98, D51).

/** A delivery to an aggregate listener: the aggregate's state at `version`, after `events` (their versions). */
case class AggregateWithType[AGGREGATE_ROOT](aggregateType: AggregateType, id: AggregateId, version: AggregateVersion, events: Seq[AggregateVersion], aggregateRoot: Option[AGGREGATE_ROOT],
                                             startsAtSquash: Boolean = false)

/** A delivery to an aggregate-with-events listener: the aggregate's state after `events`, and the events. */
case class AggregateWithTypeAndEvents[AGGREGATE_ROOT](aggregateType: AggregateType, id: AggregateId, aggregateRoot: Option[AGGREGATE_ROOT], events: Seq[EventInfo[AGGREGATE_ROOT]],
                                                      startsAtSquash: Boolean = false)

/** A delivery to an event listener: `events` of one aggregate, in order. */
case class IdentifiableEvents[AGGREGATE_ROOT](aggregateType: AggregateType, aggregateId: AggregateId, events: Seq[EventInfo[AGGREGATE_ROOT]],
                                              startsAtSquash: Boolean = false)

/** One event of an aggregate, with its version, user and time. */
case class IdentifiableEventNoAggregateType[AGGREGATE_ROOT](aggregateId: AggregateId, version: AggregateVersion, event: Event[AGGREGATE_ROOT], userId: UserId, timestamp: Instant)

/** An event with the version it was stored at: what a history-rewriting command's handler receives (the aggregate's
  * events of the classes the command names) and gives back changed (`RewriteCommandSuccess.eventsRewritten`). */
case class EventWithVersion[AGGREGATE_ROOT](version: AggregateVersion, event: Event[AGGREGATE_ROOT])

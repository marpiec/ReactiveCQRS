package io.reactivecqrs.core.delivery

import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import org.apache.pekko.actor.ActorRef

// Re-publication requests. The event bus sends them to the aggregate type's command bus, which passes them to the
// aggregate's owner. The owner re-publishes from the given version up to its current version, with its current
// state, so a subscriber that receives state always gets a consistent range (docs/design/M2-delivery.md).

/** Re-publish everything from the oldest version still in the outbox (D8: lost or unacknowledged deliveries). */
case class RepublishOutbox(aggregateId: AggregateId)

/** Re-publish `from` up to the current version to one subscriber only: its gap or its retry (D20c). */
case class RepublishRange(aggregateId: AggregateId, from: AggregateVersion, subscriber: ActorRef)

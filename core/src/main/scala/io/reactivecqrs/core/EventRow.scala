package io.reactivecqrs.core

import java.time.Instant

import io.reactivecqrs.api.event.Event
import io.reactivecqrs.api.guid.{AggregateId, UserId, CommandId}

/**
 *
 * @param commandId
 * @param userId
 * @param aggregateId
 * @param version version of aggregate after applying the event.
 * @param creationTimestamp
 * @param event
 * @tparam AGGREGATE
 */
case class EventRow[AGGREGATE](commandId: CommandId,
                               userId: UserId,
                               aggregateId: AggregateId,
                               version: Int,
                               creationTimestamp: Instant,
                               event: Event[AGGREGATE])
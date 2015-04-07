package io.reactivecqrs.core

import io.reactivecqrs.api.event.Event
import io.reactivecqrs.api.guid.{UserId, AggregateVersion, AggregateId, CommandId}


case class StoreFirstEvent[AGGREGATE](messageId: String, userId: UserId, commandId: CommandId, newAggregateId: AggregateId, event: Event[AGGREGATE])

case class StoreEvent[AGGREGATE](messageId: String, userId: UserId, commandId: CommandId, aggregateId: AggregateId, expectedVersion: AggregateVersion, event: Event[AGGREGATE])

case class GetAggregate(id: AggregateId)


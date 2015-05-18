package io.reactivecqrs.core

import io.reactivecqrs.api.AggregateRoot
import io.reactivecqrs.api.event.Event
import io.reactivecqrs.api.exception.{RepositoryException, CqrsException}
import io.reactivecqrs.api.guid.{UserId, AggregateVersion, AggregateId, CommandId}
import io.reactivecqrs.utils.Result


case class StoreFirstEvent[AGGREGATE_ROOT](messageId: String, userId: UserId, commandId: CommandId, newAggregateId: AggregateId, event: Event[AGGREGATE_ROOT])

case class StoreFollowingEvent[AGGREGATE_ROOT](messageId: String, userId: UserId, commandId: CommandId, aggregateId: AggregateId, expectedVersion: AggregateVersion, event: Event[AGGREGATE_ROOT])

case class StoreEventResponse(messageId: String, result: Result[Unit, CqrsException])

case class LoadAggregate(messageId: String, id: AggregateId)

case class LoadAggregateForVersion(messageId: String, id: AggregateId, version: AggregateVersion)

case class GetAggregateResponse[AGGREGATE_ROOT](messageId: String, result: Result[AggregateRoot[AGGREGATE_ROOT], RepositoryException])

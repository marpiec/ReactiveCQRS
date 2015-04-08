package io.reactivecqrs.api.command

import io.reactivecqrs.api.event.Event
import io.reactivecqrs.api.exception.CqrsException
import io.reactivecqrs.api.guid.{AggregateId, AggregateVersion, CommandId, UserId}
import io.reactivecqrs.utils.Result


trait RepositoryFirstEventHandler[AGGREGATE_ROOT] {
   def storeFirstEvent(commandId: CommandId, userId: UserId, newAggregateId: AggregateId, event: Event[AGGREGATE_ROOT]): Result[Unit, CqrsException]
 }


trait RepositoryFollowingEventHandler[AGGREGATE_ROOT] {
  def storeFollowingEvent(commandId: CommandId, userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, event: Event[AGGREGATE_ROOT]): Result[Unit, CqrsException]
}

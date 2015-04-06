package io.reactivecqrs.api.command

import akka.actor.ActorRef
import io.reactivecqrs.api.event.Event
import io.reactivecqrs.api.exception.CqrsException
import io.reactivecqrs.api.guid.{AggregateId, AggregateVersion, CommandId, UserId}

case class StoreEventsResponse(messageId: String, success: Boolean, aggregateId: AggregateId, exception: CqrsException)


class RepositoryHandler[AGGREGATE](aggregateRepositoryActor: ActorRef) {

  def storeFirstEvent(commandId: CommandId, userId: UserId, event: Event[AGGREGATE]): StoreEventsResponse = ???

  def storeFollowingEvent(commandId: CommandId, userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, event: Event[AGGREGATE]): StoreEventsResponse = ???

}

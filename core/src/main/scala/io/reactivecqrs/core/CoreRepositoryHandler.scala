package io.reactivecqrs.core

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import io.reactivecqrs.api.command.{RepositoryFollowingEventHandler, RepositoryFirstEventHandler}
import io.reactivecqrs.api.event.Event
import io.reactivecqrs.api.exception.CqrsException
import io.reactivecqrs.api.guid.{AggregateId, AggregateVersion, CommandId, UserId}
import io.reactivecqrs.utils.Result

import scala.concurrent.Await
import scala.concurrent.duration._

case class StoreEventsResponse(messageId: String, success: Boolean, aggregateId: AggregateId, exception: CqrsException)


class CoreRepositoryHandler[AGGREGATE_ROOT](aggregateRepositoryActor: ActorRef) extends RepositoryFirstEventHandler[AGGREGATE_ROOT] with RepositoryFollowingEventHandler[AGGREGATE_ROOT]{

  implicit val timeout = Timeout(1 second)

  override def storeFirstEvent(commandId: CommandId, userId: UserId, newAggregateId: AggregateId, event: Event[AGGREGATE_ROOT]): Result[Unit, CqrsException] = {

    val future = aggregateRepositoryActor ? StoreFirstEvent("123", userId, commandId, newAggregateId, event)

    val result = Await.result(future, 1 second).asInstanceOf[StoreEventResponse]

    result.result
  }

  override def storeFollowingEvent(commandId: CommandId, userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, event: Event[AGGREGATE_ROOT]): Result[Unit, CqrsException] = {
    val future = aggregateRepositoryActor ? StoreFollowingEvent("123", userId, commandId, aggregateId, expectedVersion, event)
    val result = Await.result(future, 1 second).asInstanceOf[StoreEventResponse]
    result.result
  }

}

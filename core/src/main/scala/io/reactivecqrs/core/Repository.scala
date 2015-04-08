package io.reactivecqrs.core

import java.time.Clock

import akka.actor.Actor
import akka.event.LoggingReceive
import io.reactivecqrs.api.event.{EventHandler, Event}
import io.reactivecqrs.api.exception.{AggregateAlreadyExistsException, AggregateDoesNotExistException, ConcurrentAggregateModificationException}
import io.reactivecqrs.api.guid.{AggregateId, AggregateVersion, CommandId, UserId}
import io.reactivecqrs.utils.{Failure, Success}

abstract class Repository[AGGREGATE](handlers: EventHandler[AGGREGATE, _ <: Event[AGGREGATE]]*) extends Actor {

  protected val clock: Clock
  protected val eventStore: EventStore[AGGREGATE]

  private val dataStore = new DataStore[AGGREGATE](handlers:_*)

  override def receive = LoggingReceive {
    case StoreFirstEvent(messageId, userId, commandId, newAggregateId, event) => storeFirstEvent(messageId, userId, commandId, newAggregateId, event.asInstanceOf[Event[AGGREGATE]])
    case StoreFollowingEvent(messageId, userId, commandId, aggregateId, expectedVersion, event) => storeFollowingEvent(messageId, userId, commandId, aggregateId, expectedVersion, event.asInstanceOf[Event[AGGREGATE]]);
    case GetAggregate(messageId, id) => getAggregate(messageId, id)
  }

  private def storeFirstEvent(messageId: String, userId: UserId, commandId: CommandId, newAggregateId: AggregateId, event: Event[AGGREGATE]): Unit = {
    val events = eventStore.getEvents(newAggregateId)
    if(events.isEmpty) {
      eventStore.putEvent(newAggregateId, new EventRow[AGGREGATE](commandId, userId, newAggregateId, 1, clock.instant(), event))
      sender ! StoreEventResponse(messageId, Success(Unit))
    } else {
      sender ! StoreEventResponse(messageId, Failure(AggregateAlreadyExistsException("Aggregate " + newAggregateId + " already exists")))
    }
  }

  private def storeFollowingEvent(messageId: String, userId: UserId, commandId: CommandId, aggregateId: AggregateId, expectedVersion: AggregateVersion, event: Event[AGGREGATE]): Unit = {
    val events = eventStore.getEvents(aggregateId)
    if(events.size == expectedVersion.version) {
      eventStore.putEvent(aggregateId, new EventRow[AGGREGATE](commandId, userId, aggregateId, expectedVersion.version + 1, clock.instant(), event))
      sender ! StoreEventResponse(messageId, Success(Unit))
    } else {
      sender ! StoreEventResponse(messageId, Failure(ConcurrentAggregateModificationException(expectedVersion, new AggregateVersion(events.size), "Aggregate concurrent modification")))
    }

  }

  private def getAggregate(messageId: String, id: AggregateId): Unit = {
    val events = eventStore.getEvents(id)
    if(events.isEmpty) {
      sender ! GetAggregateResponse(messageId, Failure(AggregateDoesNotExistException("No events for aggregate " + id)))
    } else {
      val aggregate = dataStore.buildAggregate(id, events)
      sender ! GetAggregateResponse(messageId, aggregate)
    }
  }

}

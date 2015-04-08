package io.reactivecqrs.core

import java.time.Clock

import akka.actor.Actor
import akka.event.LoggingReceive
import io.reactivecqrs.api.event.{EventHandler, Event}
import io.reactivecqrs.api.exception.{AggregateAlreadyExistsException, AggregateDoesNotExistException, ConcurrentAggregateModificationException}
import io.reactivecqrs.api.guid.{AggregateId, AggregateVersion, CommandId, UserId}
import io.reactivecqrs.utils.{Failure, Success}

abstract class Repository[AGGREGATE_ROOT](handlers: EventHandler[AGGREGATE_ROOT, _ <: Event[AGGREGATE_ROOT]]*) extends Actor {

  protected val clock: Clock
  protected val eventStore: EventStore[AGGREGATE_ROOT]

  private val dataStore = new DataStore[AGGREGATE_ROOT](handlers:_*)

  override def receive = LoggingReceive {
    case StoreFirstEvent(messageId, userId, commandId, newAggregateId, event) => storeFirstEvent(messageId, userId, commandId, newAggregateId, event.asInstanceOf[Event[AGGREGATE_ROOT]])
    case StoreFollowingEvent(messageId, userId, commandId, aggregateId, expectedVersion, event) => storeFollowingEvent(messageId, userId, commandId, aggregateId, expectedVersion, event.asInstanceOf[Event[AGGREGATE_ROOT]]);
    case LoadAggregate(messageId, id) => loadAggregate(messageId, id, None)
    case LoadAggregateForVersion(messageId, id, version) => loadAggregate(messageId, id, Some(version))
  }

  private def storeFirstEvent(messageId: String, userId: UserId, commandId: CommandId, newAggregateId: AggregateId, event: Event[AGGREGATE_ROOT]): Unit = {
    val events = eventStore.getEvents(newAggregateId)
    if(events.isEmpty) {
      eventStore.putEvent(newAggregateId, new EventRow[AGGREGATE_ROOT](commandId, userId, newAggregateId, 1, clock.instant(), event))
      sender ! StoreEventResponse(messageId, Success(Unit))
    } else {
      sender ! StoreEventResponse(messageId, Failure(AggregateAlreadyExistsException("Aggregate " + newAggregateId + " already exists")))
    }
  }

  private def storeFollowingEvent(messageId: String, userId: UserId, commandId: CommandId, aggregateId: AggregateId, expectedVersion: AggregateVersion, event: Event[AGGREGATE_ROOT]): Unit = {
    val events = eventStore.getEvents(aggregateId)
    if(events.size == expectedVersion.version) {
      eventStore.putEvent(aggregateId, new EventRow[AGGREGATE_ROOT](commandId, userId, aggregateId, expectedVersion.version + 1, clock.instant(), event))
      sender ! StoreEventResponse(messageId, Success(Unit))
    } else {
      sender ! StoreEventResponse(messageId, Failure(ConcurrentAggregateModificationException(expectedVersion, new AggregateVersion(events.size), "Aggregate concurrent modification")))
    }

  }
  
  private def loadAggregate(messageId: String, id: AggregateId, version: Option[AggregateVersion]): Unit = {
    val events = version match {
      case Some(v) => eventStore.getEventsToVersion(id, v.version)
      case None => eventStore.getEvents(id)
    } 
    
    if(events.isEmpty) {
      sender ! GetAggregateResponse(messageId, Failure(AggregateDoesNotExistException("No events for aggregate " + id)))
    } else {
      val aggregate = dataStore.buildAggregate(id, events)
      sender ! GetAggregateResponse(messageId, aggregate)
    }
  }

}

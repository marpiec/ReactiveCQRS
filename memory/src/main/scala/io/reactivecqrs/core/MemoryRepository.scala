package io.reactivecqrs.core

import java.time.Clock

import akka.actor.Actor
import io.reactivecqrs.api.event.Event
import io.reactivecqrs.api.exception.{AggregateDoesNotExistException, ConcurrentAggregateModificationException, AggregateAlreadyExistsException}
import io.reactivecqrs.api.guid.{UserId, AggregateVersion, AggregateId, CommandId}
import io.reactivecqrs.utils.{Failure, Success}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class MemoryCache[AGGREGATE] {

  private val events = mutable.Map[AggregateId, ListBuffer[EventRow[AGGREGATE]]]()

  def putEvent(id: AggregateId, eventRow: EventRow[AGGREGATE]): Unit = {
    val eventsForAggregate = events.getOrElseUpdate(id, new ListBuffer[EventRow[AGGREGATE]])
    eventsForAggregate += eventRow
  }

  def getEvents(id: AggregateId): Vector[EventRow[AGGREGATE]] = {
    events.get(id).map(_.toVector).getOrElse(Vector())
  }
}


class MemoryRepository[AGGREGATE](clock: Clock, memoryCache: MemoryCache[AGGREGATE], dataStore: DataStore[AGGREGATE]) extends Actor {

  override def receive = {
    case StoreFirstEvent(messageId, userId, commandId, newAggregateId, event) => storeFirstEvent(messageId, userId, commandId, newAggregateId, event.asInstanceOf[Event[AGGREGATE]])
    case StoreEvent(messageId, userId, commandId, aggregateId, expectedVersion, event) => storeEvent(messageId, userId, commandId, aggregateId, expectedVersion, event.asInstanceOf[Event[AGGREGATE]]);
    case GetAggregate(messageId, id) => getAggregate(messageId, id)
  }

  private def storeFirstEvent(messageId: String, userId: UserId, commandId: CommandId, newAggregateId: AggregateId, event: Event[AGGREGATE]): Unit = {
    val events = memoryCache.getEvents(newAggregateId)
    if(events.isEmpty) {
      memoryCache.putEvent(newAggregateId, new EventRow[AGGREGATE](commandId, userId, newAggregateId, 1, clock.instant(), event))
      sender ! StoreEventResponse(messageId, Success(Unit))
    } else {
      sender ! StoreEventResponse(messageId, Failure(AggregateAlreadyExistsException("Aggregate " + newAggregateId + " already exists")))
    }
  }

  private def storeEvent(messageId: String, userId: UserId, commandId: CommandId, aggregateId: AggregateId, expectedVersion: AggregateVersion, event: Event[AGGREGATE]): Unit = {
    val events = memoryCache.getEvents(aggregateId)
    if(events.size == expectedVersion.version) {
      memoryCache.putEvent(aggregateId, new EventRow[AGGREGATE](commandId, userId, aggregateId, expectedVersion.version + 1, clock.instant(), event))
      sender ! StoreEventResponse(messageId, Success(Unit))
    } else {
      sender ! StoreEventResponse(messageId, Failure(ConcurrentAggregateModificationException(expectedVersion, new AggregateVersion(events.size), "Aggregate concurrent modification")))
    }

  }

  private def getAggregate(messageId: String, id: AggregateId): Unit = {
    val events = memoryCache.getEvents(id)
    if(events.isEmpty) {
      sender ! GetAggregateResponse(messageId, Failure(AggregateDoesNotExistException("No events for aggregate " + id)))
    } else {
      val aggregate = dataStore.buildAggregate(id, events.toStream)
      sender ! GetAggregateResponse(messageId, aggregate)
    }
  }

}
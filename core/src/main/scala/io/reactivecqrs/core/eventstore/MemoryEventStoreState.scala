package io.reactivecqrs.core.eventstore

import java.time.Instant

import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api.{AggregateVersion, Event, UndoEvent}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.PersistEvents
import io.reactivecqrs.core.aggregaterepository.{EventIdentifier, IdentifiableEventNoAggregateType}

class MemoryEventStoreState extends EventStoreState {

  private var eventStore: Map[AggregateId, Vector[Event[_]]] = Map()
  private var eventsToPublish: Map[(AggregateId, Int), (UserId, Instant, Event[_], Long)] = Map()
  private var eventIdSeq: Long = 0


  override def persistEvents[AGGREGATE_ROOT](aggregateId: AggregateId, eventsEnvelope: PersistEvents[AGGREGATE_ROOT]): Seq[(Event[AGGREGATE_ROOT], Long)] = {

    var eventsForAggregate: Vector[Event[_]] = eventStore.getOrElse(aggregateId, Vector())

    if (eventsEnvelope.expectedVersion.asInt != eventsForAggregate.size) {
      throw new IllegalStateException("Incorrect version for event, expected " + eventsEnvelope.expectedVersion.asInt + " but was " + eventsForAggregate.size)
    }
    var versionsIncreased = 0
    val eventsWithIds = eventsEnvelope.events.map(event => {
      eventsForAggregate :+= event
      val key = (aggregateId, eventsEnvelope.expectedVersion.asInt + versionsIncreased)
      eventIdSeq += 1
      val value = (eventsEnvelope.userId, eventsEnvelope.timestamp, event, eventIdSeq)
      eventsToPublish += key -> value
      versionsIncreased += 1
      (event, eventIdSeq)
    })

    eventStore += aggregateId -> eventsForAggregate
    eventsWithIds
  }


  override def readAndProcessEvents[AGGREGATE_ROOT](aggregateId: AggregateId, upToVersion: Option[AggregateVersion])(eventHandler: (Event[AGGREGATE_ROOT], AggregateId, Boolean) => Unit): Unit = {
    var eventsForAggregate: Vector[Event[AGGREGATE_ROOT]] = eventStore.getOrElse(aggregateId, Vector()).asInstanceOf[Vector[Event[AGGREGATE_ROOT]]]

    if(upToVersion.isDefined) {
      eventsForAggregate = eventsForAggregate.take(upToVersion.get.asInt)
    }



    var undoEventsCount = 0
    val eventsWithNoop = eventsForAggregate.reverse.map(event => {
      if(undoEventsCount == 0) {
        event match {
          case e:UndoEvent[_] =>
            undoEventsCount += e.eventsCount
            (event, true)
          case _ =>
            (event, false)
        }
      } else {
        undoEventsCount -= 1
        (event, true)
      }
    }).reverse

    eventsWithNoop.foreach(eventWithNoop => eventHandler(eventWithNoop._1, aggregateId, eventWithNoop._2))
  }

  override def deletePublishedEventsToPublish(events: Seq[EventIdentifier]): Unit = {

    events.foreach { event =>
      eventsToPublish -= ((event.aggregateId, event.version.asInt))
    }

  }

  override def readAggregatesWithEventsToPublish(aggregateHandler: (AggregateId) => Unit): Unit = {
    eventsToPublish.keys.groupBy(_._1).keys.foreach(aggregateHandler)
  }

  override def readEventsToPublishForAggregate[AGGREGATE_ROOT](aggregateId: AggregateId): List[IdentifiableEventNoAggregateType[AGGREGATE_ROOT]] = {

    eventsToPublish.filterKeys(_._1 == aggregateId).toList.
      map(e => IdentifiableEventNoAggregateType[AGGREGATE_ROOT](e._2._4, e._1._1, AggregateVersion(e._1._2), e._2._3.asInstanceOf[Event[AGGREGATE_ROOT]], e._2._1, e._2._2))

  }
}

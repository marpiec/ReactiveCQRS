package io.reactivecqrs.core.eventstore

import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.api.{AggregateVersion, Event}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.PersistEvents
import io.reactivecqrs.core.aggregaterepository.{EventIdentifier, IdentifiableEventNoAggregateType}

class MemoryEventStoreState extends EventStoreState {

  private var eventStore: Map[AggregateId, List[Event[_]]] = Map()
  private var eventsToPublish: Map[(AggregateId, Int), Event[_]] = Map()

  
  def persistEvents[AGGREGATE_ROOT](aggregateId: AggregateId, eventsEnvelope: PersistEvents[AGGREGATE_ROOT]): Unit = {

    var eventsForAggregate: List[Event[_]] = eventStore.getOrElse(aggregateId, List())

    if (eventsEnvelope.expectedVersion.asInt != eventsForAggregate.size) {
      throw new IllegalStateException("Incorrect version for event, expected " + eventsEnvelope.expectedVersion.asInt + " but was " + eventsForAggregate.size)
    }
    var versionsIncreased = 0
    eventsEnvelope.events.foreach(event => {
      eventsForAggregate ::= event
      eventsToPublish += (aggregateId, eventsEnvelope.expectedVersion.asInt + versionsIncreased) -> event
      versionsIncreased += 1
    })

    eventStore += aggregateId -> eventsForAggregate

  }


  def readAndProcessAllEvents[AGGREGATE_ROOT](aggregateId: AggregateId)(eventHandler: (Event[AGGREGATE_ROOT], Boolean) => Unit): Unit = {
    val eventsForAggregate: List[Event[AGGREGATE_ROOT]] = eventStore.getOrElse(aggregateId, List()).asInstanceOf[List[Event[AGGREGATE_ROOT]]]
    eventsForAggregate.reverse.foreach(eventHandler(_, false))
  }

  def deletePublishedEventsToPublish(events: Seq[EventIdentifier]): Unit = {

    events.foreach { event =>
      eventsToPublish -= ((event.aggregateId, event.version.asInt))
    }

  }

  override def readAggregatesWithEventsToPublish(aggregateHandler: (AggregateId) => Unit): Unit = {
    eventsToPublish.keys.groupBy(_._1).keys.foreach(aggregateHandler)
  }

  override def readEventsToPublishForAggregate[AGGREGATE_ROOT](aggregateId: AggregateId): List[IdentifiableEventNoAggregateType[AGGREGATE_ROOT]] = {

    eventsToPublish.filterKeys(_._1 == aggregateId).toList.
      map(e => IdentifiableEventNoAggregateType[AGGREGATE_ROOT](e._1._1, AggregateVersion(e._1._2), e._2.asInstanceOf[Event[AGGREGATE_ROOT]]))

  }
}

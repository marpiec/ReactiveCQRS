package io.reactivecqrs.core.eventstore

import io.reactivecqrs.api.Event
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.PersistEvents
import io.reactivecqrs.core.aggregaterepository.EventIdentifier

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


  def readAndProcessAllEvents[AGGREGATE_ROOT](aggregateId: AggregateId)(eventHandler: Event[AGGREGATE_ROOT] => Unit): Unit = {
    val eventsForAggregate: List[Event[AGGREGATE_ROOT]] = eventStore.getOrElse(aggregateId, List()).asInstanceOf[List[Event[AGGREGATE_ROOT]]]
    eventsForAggregate.reverse.foreach(eventHandler)
  }

  def deletePublishedEventsToPublish(events: Seq[EventIdentifier]): Unit = {

    events.foreach { event =>
      eventsToPublish -= ((event.aggregateId, event.version.asInt))
    }

  }


}

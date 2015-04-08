package io.reactivecqrs.core

import io.reactivecqrs.api.guid.AggregateId

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class MemoryEventStore[AGGREGATE_ROOT] extends EventStore[AGGREGATE_ROOT] {

  private val events = mutable.Map[AggregateId, ListBuffer[EventRow[AGGREGATE_ROOT]]]()

  override def putEvent(id: AggregateId, eventRow: EventRow[AGGREGATE_ROOT]): Unit = {
    val eventsForAggregate = events.getOrElseUpdate(id, new ListBuffer[EventRow[AGGREGATE_ROOT]])
    eventsForAggregate += eventRow
  }

  override def getEvents(id: AggregateId) = {
    events.get(id).map(_.toStream).getOrElse(Stream())
  }
}



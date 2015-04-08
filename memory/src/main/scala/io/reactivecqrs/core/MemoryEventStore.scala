package io.reactivecqrs.core

import io.reactivecqrs.api.guid.AggregateId

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class MemoryEventStore[AGGREGATE] extends EventStore[AGGREGATE] {

  private val events = mutable.Map[AggregateId, ListBuffer[EventRow[AGGREGATE]]]()

  override def putEvent(id: AggregateId, eventRow: EventRow[AGGREGATE]): Unit = {
    val eventsForAggregate = events.getOrElseUpdate(id, new ListBuffer[EventRow[AGGREGATE]])
    eventsForAggregate += eventRow
  }

  override def getEvents(id: AggregateId) = {
    events.get(id).map(_.toStream).getOrElse(Stream())
  }
}



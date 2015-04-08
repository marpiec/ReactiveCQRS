package io.reactivecqrs.core

import io.reactivecqrs.api.guid.AggregateId

trait EventStore[AGGREGATE] {

  def putEvent(id: AggregateId, eventRow: EventRow[AGGREGATE]): Unit

  def getEvents(id: AggregateId): Stream[EventRow[AGGREGATE]]

}

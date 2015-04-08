package io.reactivecqrs.core

import io.reactivecqrs.api.guid.AggregateId

trait EventStore[AGGREGATE_ROOT] {

  def putEvent(id: AggregateId, eventRow: EventRow[AGGREGATE_ROOT]): Unit

  def getEvents(id: AggregateId): Stream[EventRow[AGGREGATE_ROOT]]

}

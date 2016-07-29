package io.reactivecqrs.core.eventstore

import java.time.Instant

import io.reactivecqrs.api.{AggregateType, AggregateVersion, Event}
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.PersistEvents
import io.reactivecqrs.core.aggregaterepository.{EventIdentifier, IdentifiableEventNoAggregateType}
import scalikejdbc.DBSession

abstract class EventStoreState {

  def countAllEvents(): Int
  def persistEvents[AGGREGATE_ROOT](aggregateId: AggregateId, eventsEnvelope: PersistEvents[AGGREGATE_ROOT])(implicit session: DBSession): Seq[(Event[AGGREGATE_ROOT], AggregateVersion)]
  def readAndProcessEvents[AGGREGATE_ROOT](aggregateId: AggregateId, version: Option[AggregateVersion])(eventHandler: (Event[AGGREGATE_ROOT], AggregateId, Boolean) => Unit)
  def readAndProcessAllEvents(eventHandler: (Event[_], AggregateId, AggregateVersion, AggregateType, UserId, Instant) => Unit): Unit
  def deletePublishedEventsToPublish(eventsIds: Seq[EventIdentifier]): Unit

  def readAggregatesWithEventsToPublish(oldOnly: Boolean)(aggregateHandler: AggregateId => Unit): Unit
  def readEventsToPublishForAggregate[AGGREGATE_ROOT](aggregateId: AggregateId): List[IdentifiableEventNoAggregateType[AGGREGATE_ROOT]]

  def localTx[A](block: DBSession => A): A
}



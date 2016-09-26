package io.reactivecqrs.core.eventstore

import java.time.Instant

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.PersistEvents
import scalikejdbc.DBSession

abstract class EventStoreState {

  def countAllEvents(): Int
  def persistEvents[AGGREGATE_ROOT](eventsVersionsMapReverse: Map[String, EventTypeVersion], aggregateId: AggregateId, eventsEnvelope: PersistEvents[AGGREGATE_ROOT])(implicit session: DBSession): Seq[(Event[AGGREGATE_ROOT], AggregateVersion)]
  def readAndProcessEvents[AGGREGATE_ROOT](eventsVersionsMap: Map[EventTypeVersion, String], aggregateId: AggregateId, version: Option[AggregateVersion])(eventHandler: (UserId, Instant, Event[AGGREGATE_ROOT], AggregateId, Boolean) => Unit)
  def readAndProcessAllEvents(eventsVersionsMap: Map[EventTypeVersion, String],
                              batchPerAggregate: Boolean, eventHandler: (Seq[EventInfo[_]], AggregateId, AggregateType) => Unit): Unit
  def deletePublishedEventsToPublish(eventsIds: Seq[EventIdentifier]): Unit

  def readAggregatesWithEventsToPublish(aggregateTypeName: String, oldOnly: Boolean)(aggregateHandler: AggregateId => Unit): Unit
  def readEventsToPublishForAggregate[AGGREGATE_ROOT](eventsVersionsMap: Map[EventTypeVersion, String], aggregateId: AggregateId): List[IdentifiableEventNoAggregateType[AGGREGATE_ROOT]]

  def localTx[A](block: DBSession => A): A
}



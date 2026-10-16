package io.reactivecqrs.core.eventstore

import java.time.Instant

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.PersistEvents
import scalikejdbc.DBSession

import scala.util.Try

abstract class EventStoreState {
  def countAllEvents(): Int
  def countEventsForAggregateTypes(strings: Seq[String]): Int
  def persistEvents[AGGREGATE_ROOT](eventsVersionsMapReverse: Map[String, EventTypeVersion], aggregateId: AggregateId, eventsEnvelope: PersistEvents[AGGREGATE_ROOT])(implicit session: DBSession): Try[Seq[(Event[AGGREGATE_ROOT], AggregateVersion)]]
  def overwriteEvents[AGGREGATE_ROOT](aggregateId: AggregateId, events: Iterable[EventWithVersion[AGGREGATE_ROOT]])(implicit session: DBSession): Unit
  def readAndProcessEvents[AGGREGATE_ROOT](eventsVersionsMap: Map[EventTypeVersion, String], aggregateId: AggregateId, version: Option[Either[AggregateVersion, Instant]])(eventHandler: (UserId, Instant, Event[AGGREGATE_ROOT], AggregateId, Int, Boolean) => Unit): Unit
  def readAndProcessAllEvents(eventsVersionsMap: Map[EventTypeVersion, String], aggregateType: String,
                              batchPerAggregate: Boolean, eventHandler: (Seq[EventInfo[_]], AggregateId, AggregateType) => Unit): Unit
  def deletePublishedEventsToPublish(aggregateId: AggregateId, eventsIds: Seq[EventWithIdentifier[_]]): Unit

  def readAggregatesWithEventsToPublish(aggregateTypeName: String, oldOnly: Boolean)(aggregateHandler: AggregateId => Unit): Unit
  def readEventsToPublishForAggregate[AGGREGATE_ROOT](eventsVersionsMap: Map[EventTypeVersion, String], aggregateId: AggregateId): List[IdentifiableEventNoAggregateType[AGGREGATE_ROOT]]

  /** The aggregate's own events with versions `from` to `to`, in order, undone ones included, as live delivery sent
    * them (D60): what a re-publication sends. */
  def readEventsRange[AGGREGATE_ROOT](eventsVersionsMap: Map[EventTypeVersion, String], aggregateId: AggregateId,
                                      from: AggregateVersion, to: AggregateVersion): Seq[EventInfo[AGGREGATE_ROOT]]

  /** The oldest version of the aggregate still waiting in the outbox, if any. */
  def oldestVersionToPublish(aggregateId: AggregateId): Option[AggregateVersion]

  def localTx[A](block: DBSession => A): A

  def readNotYetPublishedEvents(): Map[AggregateId, AggregateVersion]
}



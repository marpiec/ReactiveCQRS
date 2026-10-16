package io.reactivecqrs.core.eventstore

import java.time.Instant

import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api._
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.PersistEvents
import io.reactivecqrs.core.eventstore.MemoryEventStoreState.EventRow
import scalikejdbc.{DBSession, NoSession}

import scala.util.Try

object MemoryEventStoreState {
  case class EventRow(eventId: Long, aggregateId: AggregateId, aggregateVersion: AggregateVersion, aggregateType: AggregateType,
                      event: Event[_], userId: UserId, timestamp: Instant)
}

case class EventStoreEntry[AGGREGATE_ROOT](userId: UserId, timestamp: Instant, version: AggregateVersion, event: Event[AGGREGATE_ROOT])

class MemoryEventStoreState extends EventStoreState {

  private var eventsRows: List[EventRow] = List.empty
  private var eventStore: Map[AggregateId, Vector[EventStoreEntry[_]]] = Map.empty
  private var eventsToPublish: Map[(AggregateId, AggregateVersion), (UserId, Instant, Event[_], Long)] = Map.empty
  private var eventIdSeq: Long = 0


  override def persistEvents[AGGREGATE_ROOT](eventsVersionsMapReverse: Map[String, EventTypeVersion], aggregateId: AggregateId, eventsEnvelope: PersistEvents[AGGREGATE_ROOT])(implicit session: DBSession): Try[Seq[(Event[AGGREGATE_ROOT], AggregateVersion)]] = Try {

    var eventsForAggregate: Vector[EventStoreEntry[_]] = eventStore.getOrElse(aggregateId, Vector())

    if (eventsEnvelope.expectedVersion.asInt != eventsForAggregate.size) {
      throw new IllegalStateException("Incorrect version for event, expected " + eventsEnvelope.expectedVersion.asInt + " but was " + eventsForAggregate.size)
    }
    val eventsWithVersions = eventsEnvelope.events.map(event => {
      val version = AggregateVersion(eventsForAggregate.size + 1)
      eventsForAggregate :+= EventStoreEntry(eventsEnvelope.userId, eventsEnvelope.timestamp, version, event)
      eventIdSeq += 1
      eventsRows ::= EventRow(eventIdSeq, aggregateId, version,
                    AggregateType(event.aggregateRootType.toString), event, eventsEnvelope.userId, eventsEnvelope.timestamp)
      val key = (aggregateId, version)

      val value = (eventsEnvelope.userId, eventsEnvelope.timestamp, event, eventIdSeq)
      eventsToPublish += key -> value
      (event, key._2)
    })

    eventStore += aggregateId -> eventsForAggregate
    eventsWithVersions
  }


  override def readAndProcessEvents[AGGREGATE_ROOT](eventsVersionsMap: Map[EventTypeVersion, String], aggregateId: AggregateId, upToVersion: Option[Either[AggregateVersion, Instant]])(eventHandler: (UserId, Instant, Event[AGGREGATE_ROOT], AggregateId, Int, Boolean) => Unit): Unit = {
    var eventsForAggregate: Vector[EventStoreEntry[AGGREGATE_ROOT]] = eventStore.getOrElse(aggregateId, Vector()).asInstanceOf[Vector[EventStoreEntry[AGGREGATE_ROOT]]]

    if(upToVersion.isDefined) {
      upToVersion.get match {
        case Left(version) => eventsForAggregate = eventsForAggregate.take(version.asInt)
        case Right(instant) => throw new IllegalStateException("Memory event store by instant not yet supported")
      }
    }

    var undoEventsCount = 0
    val eventsWithNoop = eventsForAggregate.reverse.map(event => {
      if(undoEventsCount == 0) {
        event.event match {
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

    // A duplicated aggregate starts with its base's history up to the duplicated version, as the
    // aggregates chain does in PostgreSQL; base events are reported with the base's id.
    eventsForAggregate.headOption.map(_.event).foreach {
      case duplication: DuplicationEvent[_] =>
        readAndProcessEvents(eventsVersionsMap, duplication.baseAggregateId, Some(Left(duplication.baseAggregateVersion)))(eventHandler)
      case _ => ()
    }

    eventsWithNoop.foreach(eventWithNoop => eventHandler(eventWithNoop._1.userId, eventWithNoop._1.timestamp, eventWithNoop._1.event, aggregateId, eventWithNoop._1.version.asInt, eventWithNoop._2))
  }

  override def readAndProcessAllEvents(eventsVersionsMap: Map[EventTypeVersion, String], aggregateType: String, batchPerAggregate: Boolean, eventHandler: (Seq[EventInfo[_]], AggregateId, AggregateType) => Unit): Unit = {
    eventsRows.filter(_.aggregateType.typeName == aggregateType).foreach(row => {
      eventHandler(Seq(EventInfo(row.aggregateVersion, row.event, row.userId, row.timestamp)), row.aggregateId, row.aggregateType)
    })
  }

  // Like the SQL DELETE: deleting an event that is no longer waiting is a no-op.
  override def deletePublishedEventsToPublish(aggregateId: AggregateId, eventsIds: Seq[EventWithIdentifier[_]]): Unit = {
    eventsToPublish --= eventsIds.map(eventId => (eventId.aggregateId, eventId.version))
  }

  override def readAggregatesWithEventsToPublish(aggregateTypeName: String, oldOnly: Boolean)(aggregateHandler: (AggregateId) => Unit): Unit = {
    val olderThan = Instant.now().minusSeconds(if (oldOnly) 60 else 10)
    val aggregatesOfType = eventsRows.filter(_.aggregateType.typeName == aggregateTypeName).map(_.aggregateId).toSet
    eventsToPublish
      .filter { case ((aggregateId, _), (_, timestamp, _, _)) => aggregatesOfType.contains(aggregateId) && timestamp.isBefore(olderThan) }
      .keys.map(_._1).toSet
      .foreach(aggregateHandler)
  }

  override def readEventsToPublishForAggregate[AGGREGATE_ROOT](eventsVersionsMap: Map[EventTypeVersion, String], aggregateId: AggregateId): List[IdentifiableEventNoAggregateType[AGGREGATE_ROOT]] = {
    eventsToPublish.toList
      .filter { case ((eventAggregateId, _), _) => eventAggregateId == aggregateId }
      .sortBy { case ((_, version), _) => version.asInt }
      .map { case ((eventAggregateId, version), (userId, timestamp, event, _)) =>
        IdentifiableEventNoAggregateType[AGGREGATE_ROOT](eventAggregateId, version, event.asInstanceOf[Event[AGGREGATE_ROOT]], userId, timestamp)
      }
  }

  override def countAllEvents(): Int = eventsRows.size

  override def countEventsForAggregateTypes(aggregateTypes: Seq[String]): Int = {
    val types = aggregateTypes.toSet
    eventsRows.count(row => types.contains(row.aggregateType.typeName))
  }

  override def localTx[A](block: (DBSession) => A): A = {
    block(NoSession)
  }

  override def readNotYetPublishedEvents(): Map[AggregateId, AggregateVersion] = {
    eventsToPublish.keys.groupBy(_._1).map(a => a._1 -> AggregateVersion(a._2.map(_._2.asInt).min))
  }

  // The SQL version updates the stored event in place; restore and re-publication then read the new one.
  override def overwriteEvents[AGGREGATE_ROOT](aggregateId: AggregateId, events: Iterable[EventWithVersion[AGGREGATE_ROOT]])(implicit session: DBSession): Unit = {
    events.foreach { rewritten =>
      eventStore += aggregateId -> eventStore.getOrElse(aggregateId, Vector()).map(entry =>
        if (entry.version == rewritten.version) entry.asInstanceOf[EventStoreEntry[AGGREGATE_ROOT]].copy(event = rewritten.event) else entry)
      eventsRows = eventsRows.map(row =>
        if (row.aggregateId == aggregateId && row.aggregateVersion == rewritten.version) row.copy(event = rewritten.event) else row)
      eventsToPublish.get((aggregateId, rewritten.version)).foreach { case (userId, timestamp, _, eventId) =>
        eventsToPublish += (aggregateId, rewritten.version) -> (userId, timestamp, rewritten.event, eventId)
      }
    }
  }

}

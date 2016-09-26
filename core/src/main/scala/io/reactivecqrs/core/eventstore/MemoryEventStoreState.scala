package io.reactivecqrs.core.eventstore

import java.time.Instant

import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api._
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.PersistEvents
import io.reactivecqrs.core.eventstore.MemoryEventStoreState.EventRow
import scalikejdbc.{DBSession, NoSession}

object MemoryEventStoreState {
  case class EventRow(eventId: Long, aggregateId: AggregateId, aggregateVersion: AggregateVersion, aggregateType: AggregateType,
                      event: Event[_], userId: UserId, timestamp: Instant)
}

case class EventStoreEntry[AGGREGATE_ROOT](userId: UserId, timestamp: Instant, event: Event[AGGREGATE_ROOT])

class MemoryEventStoreState extends EventStoreState {

  private var eventsRows: List[EventRow] = List.empty
  private var eventStore: Map[AggregateId, Vector[EventStoreEntry[_]]] = Map.empty
  private var eventsToPublish: Map[(AggregateId, AggregateVersion), (UserId, Instant, Event[_], Long)] = Map.empty
  private var eventIdSeq: Long = 0


  override def persistEvents[AGGREGATE_ROOT](eventsVersionsMapReverse: Map[String, EventTypeVersion], aggregateId: AggregateId, eventsEnvelope: PersistEvents[AGGREGATE_ROOT])(implicit session: DBSession): Seq[(Event[AGGREGATE_ROOT], AggregateVersion)] = {

    var eventsForAggregate: Vector[EventStoreEntry[_]] = eventStore.getOrElse(aggregateId, Vector())

    if (eventsEnvelope.expectedVersion.isDefined && eventsEnvelope.expectedVersion.get.asInt != eventsForAggregate.size) {
      throw new IllegalStateException("Incorrect version for event, expected " + eventsEnvelope.expectedVersion.get.asInt + " but was " + eventsForAggregate.size)
    }
    var versionsIncreased = 0
    val eventsWithVersions = eventsEnvelope.events.map(event => {
      eventsForAggregate :+= EventStoreEntry(eventsEnvelope.userId, eventsEnvelope.timestamp, event)
      eventIdSeq += 1
      eventsRows ::= EventRow(eventIdSeq, aggregateId, AggregateVersion(eventsForAggregate.size + versionsIncreased),
                    AggregateType(event.aggregateRootType.toString), event, eventsEnvelope.userId, eventsEnvelope.timestamp)
      val key = (aggregateId, AggregateVersion(eventsForAggregate.size + versionsIncreased))

      val value = (eventsEnvelope.userId, eventsEnvelope.timestamp, event, eventIdSeq)
      eventsToPublish += key -> value
      versionsIncreased += 1
      (event, key._2)
    })

    eventStore += aggregateId -> eventsForAggregate
    eventsWithVersions
  }


  override def readAndProcessEvents[AGGREGATE_ROOT](eventsVersionsMap: Map[EventTypeVersion, String], aggregateId: AggregateId, upToVersion: Option[AggregateVersion])(eventHandler: (UserId, Instant, Event[AGGREGATE_ROOT], AggregateId, Boolean) => Unit): Unit = {
    var eventsForAggregate: Vector[EventStoreEntry[AGGREGATE_ROOT]] = eventStore.getOrElse(aggregateId, Vector()).asInstanceOf[Vector[EventStoreEntry[AGGREGATE_ROOT]]]

    if(upToVersion.isDefined) {
      eventsForAggregate = eventsForAggregate.take(upToVersion.get.asInt)
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

    eventsWithNoop.foreach(eventWithNoop => eventHandler(eventWithNoop._1.userId, eventWithNoop._1.timestamp, eventWithNoop._1.event, aggregateId, eventWithNoop._2))
  }

  override def readAndProcessAllEvents(eventsVersionsMap: Map[EventTypeVersion, String], batchPerAggregate: Boolean, eventHandler: (Seq[EventInfo[_]], AggregateId, AggregateType) => Unit): Unit = {
    eventsRows.foreach(row => {
      eventHandler(Seq(EventInfo(row.aggregateVersion, row.event, row.userId, row.timestamp)), row.aggregateId, row.aggregateType)
    })
  }

  override def deletePublishedEventsToPublish(eventsIds: Seq[EventIdentifier]): Unit = {

    eventsIds.foreach { eventId =>
      val keyToDelete = eventsToPublish.keys.find(key => key._1 == eventId.aggregateId && key._2 == eventId.version).get
      eventsToPublish -= keyToDelete
    }

  }

  // TODO FIX! this ignores aggregate type name
  override def readAggregatesWithEventsToPublish(aggregateTypeName: String, oldOnly: Boolean)(aggregateHandler: (AggregateId) => Unit): Unit = {
    if(oldOnly) {
      eventsToPublish.filter(_._2._2.isBefore(Instant.now().minusSeconds(60))).keys.groupBy(_._1).keys.foreach(aggregateHandler)
    } else {
      eventsToPublish.filter(_._2._2.isBefore(Instant.now().minusSeconds(10))).keys.groupBy(_._1).keys.foreach(aggregateHandler)
    }
  }

  override def readEventsToPublishForAggregate[AGGREGATE_ROOT](eventsVersionsMap: Map[EventTypeVersion, String], aggregateId: AggregateId): List[IdentifiableEventNoAggregateType[AGGREGATE_ROOT]] = {

    eventsToPublish.filterKeys(_._2 == aggregateId).toList.
      map(e => IdentifiableEventNoAggregateType[AGGREGATE_ROOT](e._1._1, e._1._2, e._2._3.asInstanceOf[Event[AGGREGATE_ROOT]], e._2._1, e._2._2))

  }

  override def countAllEvents(): Int = eventsRows.size

  override def localTx[A](block: (DBSession) => A): A = {
    block(NoSession)
  }

}

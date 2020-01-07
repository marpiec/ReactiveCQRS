package io.reactivecqrs.core.eventstore

import java.sql.Timestamp
import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api._
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.PersistEvents
import io.reactivecqrs.core.types.TypesNamesState
import scalikejdbc._

import scala.util.Try

class PostgresEventStoreState(mpjsons: MPJsons, typesNamesState: TypesNamesState) extends EventStoreState {

  def initSchema(): PostgresEventStoreState = {
    (new PostgresEventStoreSchemaInitializer).initSchema()
    this
  }

  override def persistEvents[AGGREGATE_ROOT](eventsVersionsMapReverse: Map[String, EventTypeVersion],
                                             aggregateId: AggregateId, eventsEnvelope: PersistEvents[AGGREGATE_ROOT])(implicit session: DBSession): Try[Seq[(Event[AGGREGATE_ROOT], AggregateVersion)]] = Try {
    var lastEventVersion: Option[Int] = None

    eventsEnvelope.events.map(event => {

      val eventSerialized = mpjsons.serialize(event, event.getClass.getName)

      val eventType = event.getClass.getName
      val EventTypeVersion(eventBaseType, eventVersion) = eventsVersionsMapReverse.getOrElse(eventType, EventTypeVersion(eventType, 0))
      val eventBaseTypeId = typesNamesState.typeIdByClassName(eventBaseType)

      lastEventVersion = Some(event match {
        case undoEvent: UndoEvent[_] =>
          sql"""SELECT add_undo_event(?, ?, ?, ?, ?, ?, ?, ?, ?)""".bind(
            eventsEnvelope.userId.asLong,
            aggregateId.asLong,
            lastEventVersion.getOrElse(eventsEnvelope.expectedVersion.asInt),
            typesNamesState.typeIdByClassName(event.aggregateRootType.typeSymbol.fullName),
            eventBaseTypeId,
            eventVersion,
            Timestamp.from(Instant.now),
            eventSerialized,
            undoEvent.eventsCount
          ).map(rs => rs.int(1)).single().apply().get
        case duplicationEvent: DuplicationEvent[_] =>
          sql"""SELECT add_duplication_event(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".bind(
            eventsEnvelope.userId.asLong,
            duplicationEvent.spaceId.asLong,
            aggregateId.asLong,
            lastEventVersion.getOrElse(eventsEnvelope.expectedVersion.asInt),
            typesNamesState.typeIdByClassName(event.aggregateRootType.typeSymbol.fullName),
            eventBaseTypeId,
            eventVersion,
            Timestamp.from(Instant.now),
            eventSerialized,
            duplicationEvent.baseAggregateId.asLong,
            duplicationEvent.baseAggregateVersion.asInt
          ).map(rs => rs.int(1)).single().apply().get
        case firstEvent: FirstEvent[_] => {
          sql"""SELECT add_event(?, ?, ?, ?, ?, ?, ?, ?, ?)""".bind(
            eventsEnvelope.userId.asLong,
            firstEvent.spaceId.asLong,
            aggregateId.asLong,
            lastEventVersion.getOrElse(eventsEnvelope.expectedVersion.asInt),
            typesNamesState.typeIdByClassName(event.aggregateRootType.typeSymbol.fullName),
            eventBaseTypeId,
            eventVersion,
            Timestamp.from(Instant.now),
            eventSerialized
          ).map(rs => rs.int(1)).single().apply().get
        }
        case _ =>
          sql"""SELECT add_event(?, ?, ?, ?, ?, ?, ?, ?, ?)""".bind(
            eventsEnvelope.userId.asLong,
            -1L,
            aggregateId.asLong,
            lastEventVersion.getOrElse(eventsEnvelope.expectedVersion.asInt),
            typesNamesState.typeIdByClassName(event.aggregateRootType.typeSymbol.fullName),
            eventBaseTypeId,
            eventVersion,
            Timestamp.from(Instant.now),
            eventSerialized
          ).map(rs => rs.int(1)).single().apply().get
      })

      (event, AggregateVersion(lastEventVersion.get))
    })
  }


  override def overwriteEvents[AGGREGATE_ROOT](aggregateId: AggregateId, events: Iterable[EventWithVersion[AGGREGATE_ROOT]])(implicit session: DBSession): Unit = {
    events.foreach(e => {
      sql"""
           |UPDATE events SET event = ? WHERE aggregate_id = ? AND version = ?
      """.stripMargin.bind(mpjsons.serialize(e.event, e.event.getClass.getName), aggregateId.asLong, e.version.asInt).executeUpdate().apply()
    })
  }


  override def readAndProcessEvents[AGGREGATE_ROOT](eventsVersionsMap: Map[EventTypeVersion, String],
                                                    aggregateId: AggregateId, version: Option[AggregateVersion])
                                                   (eventHandler: (UserId, Instant, Event[AGGREGATE_ROOT], AggregateId, Int, Boolean) => Unit): Unit = {  //event, id, noop

    DB.readOnly { implicit session =>

      val query = version match {
        case Some(v) =>
          sql"""SELECT user_id, event_time, event_type_id, event_type_version, event, events.version, events.aggregate_id,
                noop_events.id IS NOT NULL AND (events.aggregate_id != ? AND noop_events.from_version <= aggregates.base_version OR events.aggregate_id = ? AND noop_events.from_version <= ?) as noop
             FROM events
             JOIN aggregates ON events.aggregate_id = aggregates.base_id AND (events.aggregate_id != ? AND events.version <= aggregates.base_version OR events.aggregate_id = ? AND events.version <= ?)
             LEFT JOIN noop_events ON events.id = noop_events.id AND noop_events.from_version <= aggregates.base_version
             WHERE aggregates.id = ? ORDER BY aggregates.base_order, version""".stripMargin.bind(aggregateId.asLong, aggregateId.asLong, v.asInt, aggregateId.asLong, aggregateId.asLong, v.asInt, aggregateId.asLong)
        case None =>
          sql"""SELECT user_id, event_time, event_type_id, event_type_version, event, events.version, events.aggregate_id, noop_events.id IS NOT NULL AND noop_events.from_version <= aggregates.base_version as noop
             FROM events
             JOIN aggregates ON events.aggregate_id = aggregates.base_id AND events.version <= aggregates.base_version
             LEFT JOIN noop_events ON events.id = noop_events.id AND noop_events.from_version <= aggregates.base_version
             WHERE aggregates.id = ? ORDER BY aggregates.base_order, version""".stripMargin.bind(aggregateId.asLong)
      }

      query.foreach { rs =>

        val eventBaseType = typesNamesState.classNameById(rs.short(3))
        val eventTypeVersion = rs.short(4)
        val eventType = eventsVersionsMap.getOrElse(EventTypeVersion(eventBaseType, eventTypeVersion), eventBaseType)
        val event = mpjsons.deserialize[Event[AGGREGATE_ROOT]](rs.string(5), eventType)
        val id = AggregateId(rs.long(7))
        val eventVersion = rs.int(6)
        if(version.isEmpty || id != aggregateId || eventVersion <= version.get.asInt) {
          eventHandler(UserId(rs.long(1)), rs.timestamp(2).toInstant, event, id, eventVersion, rs.boolean(8))
        } // otherwise it's to new event, TODO optimise as it reads all events from database, also those not needed here
      }
    }
  }


  override def readAndProcessAllEvents(eventsVersionsMap: Map[EventTypeVersion, String], aggregateType: String,
                                       batchPerAggregate: Boolean, eventHandler: (Seq[EventInfo[_]], AggregateId, AggregateType) => Unit): Unit = {

    val aggregateTypeId = typesNamesState.typeIdByClassName(aggregateType)

    var buffer = List[EventInfo[Any]]()
    var lastAggregateId = AggregateId(-1)
    var lastAggregateType = AggregateType("")
    DB.readOnly { implicit session =>
      val query = if(batchPerAggregate) {
        sql"""SELECT event_type_id, event_type_version, event, events.version, events.aggregate_id, aggregates.type_id, user_id, event_time
           FROM events
           JOIN aggregates ON events.aggregate_id = aggregates.id AND events.aggregate_id = aggregates.base_id
           WHERE aggregates.type_id = ?
           ORDER BY aggregates.creation_time, aggregates.id, events.id""".bind(aggregateTypeId)
      } else {
        sql"""SELECT event_type_id, event_type_version, event, events.version, events.aggregate_id, aggregates.type_id, user_id, event_time
           FROM events
           JOIN aggregates ON events.aggregate_id = aggregates.id AND events.aggregate_id = aggregates.base_id
           WHERE aggregates.type_id = ?
           ORDER BY events.id""".bind(aggregateTypeId)
      }

      query.fetchSize(1000).foreach { rs =>
          val eventBaseType = typesNamesState.classNameById(rs.short(1))
          val eventTypeVersion = rs.short(2)
          val eventType = eventsVersionsMap.getOrElse(EventTypeVersion(eventBaseType, eventTypeVersion), eventBaseType)

          val event = mpjsons.deserialize[Event[_]](rs.string(3), eventType)
          val aggregateId = AggregateId(rs.long(5))
          val version = AggregateVersion(rs.int(4))
          val eventInfo: EventInfo[Any] = EventInfo[Any](version, event.asInstanceOf[Event[Any]], UserId(rs.long(7)), rs.timestamp(8).toInstant)
          val aggregateType =  AggregateType(typesNamesState.classNameById(rs.short(6)))

          if(!batchPerAggregate || lastAggregateId != aggregateId) {
            if(buffer.nonEmpty) {
              eventHandler(buffer.reverse, lastAggregateId, lastAggregateType)
            }
            buffer = List(eventInfo)
            lastAggregateId = aggregateId
            lastAggregateType = aggregateType
          } else {
            buffer ::= eventInfo
          }
        }
      if(buffer.nonEmpty) {
        eventHandler(buffer.reverse, lastAggregateId, lastAggregateType)
      }
    }
  }


  override def deletePublishedEventsToPublish(events: Seq[EventWithIdentifier[_]]): Unit = {
    // TODO optimize SQL query so it will be one query
    DB.autoCommit { implicit session =>
      events.foreach {event =>
        if(event.event.isInstanceOf[PermanentDeleteEvent[_]]) {
          sql"""DELETE FROM aggregates WHERE id = ?""".bind(event.aggregateId.asLong).executeUpdate().apply()
          sql"""DELETE FROM events WHERE aggregate_id = ?""".bind(event.aggregateId.asLong).executeUpdate().apply()
          sql"""DELETE FROM noop_events WHERE id IN (SELECT id FROM events WHERE aggregate_id = ?)""".bind(event.aggregateId.asLong).executeUpdate().apply()
          sql"""DELETE FROM events_to_publish WHERE aggregate_id = ?""".bind(event.aggregateId.asLong).executeUpdate().apply()
          sql"""DELETE FROM subscriptions WHERE aggregate_id = ?""".bind(event.aggregateId.asLong).executeUpdate().apply()
        } else {
          sql"""DELETE FROM events_to_publish WHERE aggregate_id = ? AND version = ?"""
            .bind(event.aggregateId.asLong, event.version.asInt)
            .executeUpdate().apply()
        }
      }
    }
  }

  override def readNotYetPublishedEvents(): Map[AggregateId, AggregateVersion] = {
    DB.readOnly { implicit session =>
      sql"""SELECT aggregate_id, MIN(version) FROM events_to_publish GROUP BY aggregate_id"""
        .map(rs => (AggregateId(rs.long(1)), AggregateVersion(rs.int(2)))).list().apply().toMap
    }
  }

  override def readAggregatesWithEventsToPublish(aggregateTypeName: String, oldOnly: Boolean)(aggregateHandler: AggregateId => Unit): Unit = {
    DB.readOnly { implicit session =>
      if(oldOnly) {
        sql"""SELECT DISTINCT aggregate_id
              | FROM events_to_publish
              | JOIN aggregates
              | ON events_to_publish.aggregate_id = aggregates.id
              | WHERE aggregates.type_id = ?
              | AND event_time < NOW() - INTERVAL '1 minute'
           """
      } else {
        sql"""SELECT DISTINCT aggregate_id
              | FROM events_to_publish
              | JOIN aggregates
              | ON events_to_publish.aggregate_id = aggregates.id
              | WHERE aggregates.type_id = ?
              | AND event_time < NOW() - INTERVAL '10 seconds'
           """
      }.bind(typesNamesState.typeIdByClassName(aggregateTypeName)).stripMargin.foreach { rs =>
        aggregateHandler(AggregateId(rs.int(1)))
      }
    }
  }

  override def countAllEvents(): Int =  DB.readOnly { implicit session =>
    sql"""SELECT COUNT(*) FROM events""".map(rs => rs.int(1)).single().apply().get
  }

  override def countEventsForAggregateTypes(aggregateTypes: Seq[String]): Int = DB.readOnly { implicit session =>
    if(aggregateTypes.isEmpty) {
      0
    } else {
      val typesIds = aggregateTypes.map(t => typesNamesState.typeIdByClassName(t)).distinct
      sql"""SELECT COUNT(*) FROM events JOIN aggregates ON events.aggregate_id = aggregates.id WHERE aggregates.type_id in ($typesIds)""".map(rs => rs.int(1)).single().apply().get
    }
  }

  override def readEventsToPublishForAggregate[AGGREGATE_ROOT](eventsVersionsMap: Map[EventTypeVersion, String],
                                                               aggregateId: AggregateId): List[IdentifiableEventNoAggregateType[AGGREGATE_ROOT]] = {
    var result = List[IdentifiableEventNoAggregateType[AGGREGATE_ROOT]]()
    DB.readOnly { implicit session =>
      sql"""SELECT events_to_publish.version, events.event_type_id, event_type_version, events.event, events.user_id, events.event_time
           | FROM events_to_publish
           | JOIN events on events_to_publish.event_id = events.id
           | WHERE events_to_publish.aggregate_id = ?
           | ORDER BY events_to_publish.version""".stripMargin.bind(aggregateId.asLong).foreach { rs =>

        val eventBaseType = typesNamesState.classNameById(rs.short(2))
        val eventTypeVersion = rs.short(3)
        val eventType = eventsVersionsMap.getOrElse(EventTypeVersion(eventBaseType, eventTypeVersion), eventBaseType)

        val event = mpjsons.deserialize[Event[AGGREGATE_ROOT]](rs.string(4), eventType)

        result ::= IdentifiableEventNoAggregateType[AGGREGATE_ROOT](aggregateId, AggregateVersion(rs.int(1)), event, UserId(rs.long(5)), rs.timestamp(6).toInstant)

      }
    }
    result.reverse
  }

  override def localTx[A](block: (DBSession) => A): A = DB.localTx { session =>
    block(session)
  }

}

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

class PostgresEventStoreState(mpjsons: MPJsons, typesNamesState: TypesNamesState, fetchSize: Int = 1000,
                              eventSizeLimit: Int = 100000, aggregateVersionLimit: Int = 10000) extends EventStoreState {

  val doubleNone: (Option[AggregateVersion], Option[Instant]) = (None, None)

  def initSchema(): PostgresEventStoreState = {
    (new PostgresEventStoreSchemaInitializer).initSchema()
    this
  }

  override def persistEvents[AGGREGATE_ROOT](eventsVersionsMapReverse: Map[String, EventTypeVersion],
                                             aggregateId: AggregateId, eventsEnvelope: PersistEvents[AGGREGATE_ROOT])(implicit session: DBSession): Try[Seq[(Event[AGGREGATE_ROOT], AggregateVersion)]] = Try {
    var lastEventVersion: Option[Int] = None

    eventsEnvelope.events.map(event => {

      val eventSerialized = mpjsons.serialize(event, event.getClass.getName)

      val aggregateVersion = lastEventVersion.getOrElse(eventsEnvelope.expectedVersion.asInt)

      if(eventSerialized.length > eventSizeLimit) {
        throw new EventTooLargeException(aggregateId, event.aggregateRootType.typeSymbol.fullName, AggregateVersion(aggregateVersion), event.getClass.getName, eventSerialized.length, eventSizeLimit)
      } else if(aggregateVersion > aggregateVersionLimit) {
        throw new TooManyEventsException(aggregateId, event.aggregateRootType.typeSymbol.fullName, aggregateVersionLimit)
      } else {

        val eventType = event.getClass.getName
        val EventTypeVersion(eventBaseType, eventVersion) = eventsVersionsMapReverse.getOrElse(eventType, EventTypeVersion(eventType, 0))
        val eventBaseTypeId = typesNamesState.typeIdByClassName(eventBaseType)

        lastEventVersion = Some(event match {
          case undoEvent: UndoEvent[_] =>
            sql"""SELECT add_undo_event(?, ?, ?, ?, ?, ?, ?, ?, ?)""".bind(
              eventsEnvelope.userId.asLong,
              aggregateId.asLong,
              eventVersion,
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
              aggregateVersion,
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
              aggregateVersion,
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
              aggregateVersion,
              typesNamesState.typeIdByClassName(event.aggregateRootType.typeSymbol.fullName),
              eventBaseTypeId,
              eventVersion,
              Timestamp.from(Instant.now),
              eventSerialized
            ).map(rs => rs.int(1)).single().apply().get
        })

        (event, AggregateVersion(lastEventVersion.get))
      }
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
                                                    aggregateId: AggregateId, versionOrInstant: Option[Either[AggregateVersion, Instant]])
                                                   (eventHandler: (UserId, Instant, Event[AGGREGATE_ROOT], AggregateId, Int, Boolean) => Unit): Unit = {  //event, id, noop


    val (version: Option[AggregateVersion], instant: Option[Instant]) = versionOrInstant match {
      case Some(vi) => vi match {
        case Left(version) => (Some(version), None)
        case Right(instant) => (None, Some(instant))
      }
      case None => doubleNone
    }

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
          sql"""SELECT user_id, event_time, event_type_id, event_type_version, event, events.version, events.aggregate_id,
                noop_events.id IS NOT NULL AND noop_events.from_version <= aggregates.base_version as noop
             FROM events
             JOIN aggregates ON events.aggregate_id = aggregates.base_id AND events.version <= aggregates.base_version
             LEFT JOIN noop_events ON events.id = noop_events.id AND noop_events.from_version <= aggregates.base_version
             WHERE aggregates.id = ? ORDER BY aggregates.base_order, version""".stripMargin.bind(aggregateId.asLong)
      }


      if(instant.isDefined || version.isDefined) {
        query.foreach { rs =>
          val eventBaseType = typesNamesState.classNameById(rs.short(3))
          val eventTypeVersion = rs.short(4)
          val eventType = eventsVersionsMap.getOrElse(EventTypeVersion(eventBaseType, eventTypeVersion), eventBaseType)
          val event = mpjsons.deserialize[Event[AGGREGATE_ROOT]](rs.string(5), eventType)
          val id = AggregateId(rs.long(7))
          val eventVersion = rs.int(6)
          val eventInstant = rs.timestamp(2).toInstant
          if ((version.isEmpty || id != aggregateId || eventVersion <= version.get.asInt) && (instant.isEmpty || !eventInstant.isAfter(instant.get))) {
            eventHandler(UserId(rs.long(1)), eventInstant, event, id, eventVersion, rs.boolean(8))
          } // otherwise it's to new event, TODO optimise as it reads all events from database, also those not needed here
        }
      } else {

        query.foreach { rs =>
          val eventBaseType = typesNamesState.classNameById(rs.short(3))
          val eventTypeVersion = rs.short(4)
          val eventType = eventsVersionsMap.getOrElse(EventTypeVersion(eventBaseType, eventTypeVersion), eventBaseType)
          val event = mpjsons.deserialize[Event[AGGREGATE_ROOT]](rs.string(5), eventType)
          val id = AggregateId(rs.long(7))
          val eventVersion = rs.int(6)
          eventHandler(UserId(rs.long(1)), rs.timestamp(2).toInstant, event, id, eventVersion, rs.boolean(8))
        }
      }
    }
  }


  override def readAndProcessAllEvents(eventsVersionsMap: Map[EventTypeVersion, String], aggregateTypeName: String,
                                       batchPerAggregate: Boolean, eventHandler: (Seq[EventInfo[_]], AggregateId, AggregateType) => Unit): Unit = {

    val aggregateTypeId = typesNamesState.typeIdByClassName(aggregateTypeName)
    val aggregateType = AggregateType(aggregateTypeName)

    var buffer = List[EventInfo[Any]]()
    var lastAggregateId = AggregateId(-1)
    DB.readOnlyWithConnection { connection =>

      connection.setAutoCommit(false)

      val statement = if(batchPerAggregate) {
        connection.prepareStatement("""SELECT event_type_id, event_type_version, event, events.version, events.aggregate_id, user_id, event_time,
       noop_events.id IS NOT NULL AND noop_events.from_version <= aggregates.base_version as noop
FROM events
         JOIN aggregates ON aggregates.type_id = ? AND events.aggregate_id = aggregates.base_id AND events.aggregate_id = aggregates.id
         LEFT JOIN noop_events ON events.id = noop_events.id AND noop_events.from_version <= aggregates.base_version
ORDER BY events.aggregate_id, events.id""".stripMargin, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)
      } else {
        connection.prepareStatement("""SELECT event_type_id, event_type_version, event, events.version, events.aggregate_id, user_id, event_time,
       noop_events.id IS NOT NULL AND noop_events.from_version <= aggregates.base_version as noop
FROM events
         JOIN aggregates ON aggregates.type_id = ? AND events.aggregate_id = aggregates.base_id AND events.aggregate_id = aggregates.id
         LEFT JOIN noop_events ON events.id = noop_events.id AND noop_events.from_version <= aggregates.base_version
ORDER BY events.id""")
      }

      statement.setFetchSize(fetchSize)
      statement.setInt(1, aggregateTypeId)
      val rs = statement.executeQuery()

      while(rs.next()) {
        if(!rs.getBoolean(8)) { // ignore noop
          val eventBaseType = typesNamesState.classNameById(rs.getShort(1))
          val eventTypeVersion = rs.getShort(2)
          val eventType = eventsVersionsMap.getOrElse(EventTypeVersion(eventBaseType, eventTypeVersion), eventBaseType)

          val event = mpjsons.deserialize[Event[_]](rs.getString(3), eventType)
          val aggregateId = AggregateId(rs.getLong(5))
          val version = AggregateVersion(rs.getInt(4))
          val eventInfo: EventInfo[Any] = EventInfo[Any](version, event.asInstanceOf[Event[Any]], UserId(rs.getLong(6)), rs.getTimestamp(7).toInstant)

          if (!batchPerAggregate || lastAggregateId != aggregateId) {
            if (buffer.nonEmpty) {
              eventHandler(buffer.reverse, lastAggregateId, aggregateType)
            }
            buffer = List(eventInfo)
            lastAggregateId = aggregateId
          } else {
            buffer ::= eventInfo
          }
        }
      }

      if(buffer.nonEmpty) {
        eventHandler(buffer.reverse, lastAggregateId, aggregateType)
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

    // much faster, although might be not exact
    val count = sql"""SELECT reltuples::bigint AS count FROM pg_class WHERE oid = 'public.events'::regclass"""
      .map(rs => rs.int(1)).single().apply().get

    if(count == 0) {
      sql"""SELECT COUNT(*) FROM events""".map(rs => rs.int(1)).single().apply().get
    } else {
      count
    }

  }

  override def countEventsForAggregateTypes(aggregateTypes: Seq[String]): Int = DB.readOnly { implicit session =>
    if(aggregateTypes.isEmpty) {
      0
    } else {
      val typesIds = aggregateTypes.map(t => typesNamesState.typeIdByClassName(t)).distinct
      sql"""SELECT COUNT(*) FROM events JOIN aggregates ON events.aggregate_id = aggregates.id WHERE aggregates.type_id in ($typesIds) and aggregates.base_order = 1""".map(rs => rs.int(1)).single().apply().get
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

package io.reactivecqrs.core.eventstore

import java.sql.Timestamp
import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api._
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.PersistEvents
import io.reactivecqrs.core.aggregaterepository.{EventIdentifier, IdentifiableEventNoAggregateType}
import io.reactivecqrs.core.types.TypesNamesState
import scalikejdbc._

class PostgresEventStoreState(mpjsons: MPJsons, typesNamesState: TypesNamesState) extends EventStoreState {

  def initSchema(): PostgresEventStoreState = {
    (new PostgresEventStoreSchemaInitializer).initSchema()
    this
  }

  override def persistEvents[AGGREGATE_ROOT](aggregateId: AggregateId, eventsEnvelope: PersistEvents[AGGREGATE_ROOT])(implicit session: DBSession): Seq[(Event[AGGREGATE_ROOT], AggregateVersion)] = {
    var lastEventVersion: Option[Int] = None

    eventsEnvelope.events.map(event => {

      val eventSerialized = mpjsons.serialize(event, event.getClass.getName)

      lastEventVersion = Some(event match {
        case undoEvent: UndoEvent[_] =>
          sql"""SELECT add_undo_event(?, ?, ?, ? ,? , ?, ?, ?, ?)""".bind(
            eventsEnvelope.commandId.asLong,
            eventsEnvelope.userId.asLong,
            aggregateId.asLong,
            eventsEnvelope.expectedVersion.map(v => lastEventVersion.getOrElse(v.asInt)).getOrElse(-1),
            typesNamesState.typeIdByClassName(event.aggregateRootType.typeSymbol.fullName),
            typesNamesState.typeIdByClass(event.getClass),
            Timestamp.from(Instant.now),
            eventSerialized,
            undoEvent.eventsCount
          ).map(rs => rs.int(1)).single().apply().get

        case duplicationEvent: DuplicationEvent[_] =>
          sql"""SELECT add_duplication_event(?, ?, ?, ? , ?, ?, ?, ?, ?, ?)""".bind(
            eventsEnvelope.commandId.asLong,
            eventsEnvelope.userId.asLong,
            aggregateId.asLong,
            eventsEnvelope.expectedVersion.map(v => lastEventVersion.getOrElse(v.asInt)).getOrElse(-1),
            typesNamesState.typeIdByClassName(event.aggregateRootType.typeSymbol.fullName),
            typesNamesState.typeIdByClass(event.getClass),
            Timestamp.from(Instant.now),
            eventSerialized,
            duplicationEvent.baseAggregateId.asLong,
            duplicationEvent.baseAggregateVersion.asInt
          ).map(rs => rs.int(1)).single().apply().get
        case _ =>
          sql"""SELECT add_event(?, ?, ?, ? ,? , ? ,?, ?)""".bind(
            eventsEnvelope.commandId.asLong,
            eventsEnvelope.userId.asLong,
            aggregateId.asLong,
            eventsEnvelope.expectedVersion.map(v => lastEventVersion.getOrElse(v.asInt)).getOrElse(-1),
            typesNamesState.typeIdByClassName(event.aggregateRootType.typeSymbol.fullName),
            typesNamesState.typeIdByClass(event.getClass),
            Timestamp.from(Instant.now),
            eventSerialized
          ).map(rs => rs.int(1)).single().apply().get
      })

      (event, AggregateVersion(lastEventVersion.get))
    })
  }


  override def readAndProcessEvents[AGGREGATE_ROOT](aggregateId: AggregateId, version: Option[AggregateVersion])
                                                   (eventHandler: (Event[AGGREGATE_ROOT], AggregateId, Boolean) => Unit): Unit = {  //event, id, noop

    DB.readOnly { implicit session =>

      val query = version match {
        case Some(v) =>
          sql"""SELECT event_type_id, event, events.version, events.aggregate_id,
                noop_events.id IS NOT NULL AND (events.aggregate_id != ? AND noop_events.from_version <= aggregates.base_version OR events.aggregate_id = ? AND noop_events.from_version <= ?) as noop
             FROM events
             JOIN aggregates ON events.aggregate_id = aggregates.base_id AND (events.aggregate_id != ? AND events.version <= aggregates.base_version OR events.aggregate_id = ? AND events.version <= ?)
             LEFT JOIN noop_events ON events.id = noop_events.id AND noop_events.from_version <= aggregates.base_version
             WHERE aggregates.id = ? ORDER BY aggregates.base_order, version""".stripMargin.bind(aggregateId.asLong, aggregateId.asLong, v.asInt, aggregateId.asLong, aggregateId.asLong, v.asInt, aggregateId.asLong)
        case None =>
          sql"""SELECT event_type_id, event, events.version, events.aggregate_id, noop_events.id IS NOT NULL AND noop_events.from_version <= aggregates.base_version as noop
             FROM events
             JOIN aggregates ON events.aggregate_id = aggregates.base_id AND events.version <= aggregates.base_version
             LEFT JOIN noop_events ON events.id = noop_events.id AND noop_events.from_version <= aggregates.base_version
             WHERE aggregates.id = ? ORDER BY aggregates.base_order, version""".stripMargin.bind(aggregateId.asLong)
      }

      query.foreach { rs =>

        val event = mpjsons.deserialize[Event[AGGREGATE_ROOT]](rs.string(2), typesNamesState.classNameById(rs.short(1)))
        val id = AggregateId(rs.long(4))
        val eventVersion = rs.long(3)
        if(version.isEmpty || id != aggregateId || eventVersion <= version.get.asInt) {
          eventHandler(event, id, rs.boolean(5))
        } // otherwise it's to new event, TODO optimise as it reads all events from database, also those not needed here
      }
    }
  }


  override def readAndProcessAllEvents(eventHandler: (Event[_], AggregateId, AggregateVersion, AggregateType, UserId, Instant) => Unit): Unit = {
    DB.readOnly { implicit session =>
      sql"""SELECT event_type_id, event, events.version, events.aggregate_id, aggregates.type_id, user_id, event_time
           FROM events
           JOIN aggregates ON events.aggregate_id = aggregates.id AND events.aggregate_id = aggregates.base_id
           ORDER BY events.id""".fetchSize(1000)
        .foreach { rs =>
          val event = mpjsons.deserialize[Event[_]](rs.string(2), typesNamesState.classNameById(rs.short(1)))
          val aggregateId = AggregateId(rs.long(4))
          val version = AggregateVersion(rs.int(3))
          eventHandler(event, aggregateId, version, AggregateType(typesNamesState.classNameById(rs.short(5))), UserId(rs.long(6)), rs.timestamp(7).toInstant)
        }
    }
  }


  override def deletePublishedEventsToPublish(eventsIds: Seq[EventIdentifier]): Unit = {
    // TODO optimize SQL query so it will be one query
    DB.autoCommit { implicit session =>
      eventsIds.foreach {eventId =>
        sql"""DELETE FROM events_to_publish WHERE aggregate_id = ? AND version = ?"""
          .bind(eventId.aggregateId.asLong, eventId.version.asInt)
          .executeUpdate().apply()
      }
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
    sql"""SELECT COUNT(*) FROM events""".stripMargin.map(rs => rs.int(1)).single().apply().get
  }

  override def readEventsToPublishForAggregate[AGGREGATE_ROOT](aggregateId: AggregateId): List[IdentifiableEventNoAggregateType[AGGREGATE_ROOT]] = {
    var result = List[IdentifiableEventNoAggregateType[AGGREGATE_ROOT]]()
    DB.readOnly { implicit session =>
      sql"""SELECT events_to_publish.version, events.event_type_id, events.event, events.user_id, events.event_time
           | FROM events_to_publish
           | JOIN events on events_to_publish.event_id = events.id
           | WHERE events_to_publish.aggregate_id = ?
           | ORDER BY events_to_publish.version""".stripMargin.bind(aggregateId.asLong).foreach { rs =>

        val event = mpjsons.deserialize[Event[AGGREGATE_ROOT]](rs.string(3), typesNamesState.classNameById(rs.short(2)))

        result ::= IdentifiableEventNoAggregateType[AGGREGATE_ROOT](aggregateId, AggregateVersion(rs.int(1)), event, UserId(rs.long(4)), rs.timestamp(5).toInstant)

      }
    }
    result.reverse
  }

  override def localTx[A](block: (DBSession) => A): A = DB.localTx { session =>
    block(session)
  }
}

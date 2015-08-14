package io.reactivecqrs.core.eventstore

import io.mpjsons.MPJsons
import io.reactivecqrs.api.Event
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.PersistEvents
import io.reactivecqrs.core.aggregaterepository.EventIdentifier
import scalikejdbc._

class PostgresEventStoreState extends EventStoreState {

  val mpjsons = new MPJsons

  def initSchema(): Unit = {
    (new EventStoreSchemaInitializer).initSchema()
  }

  override def persistEvents[AGGREGATE_ROOT](aggregateId: AggregateId, eventsEnvelope: PersistEvents[AGGREGATE_ROOT]): Unit = {
    var versionsIncreased = 0
    DB.autoCommit { implicit session =>
      eventsEnvelope.events.foreach(event => {

        val eventSerialized = mpjsons.serialize(eventsEnvelope.events.head, event.getClass.getName)


          sql"""SELECT add_event(?, ?, ?, ? ,? , ?, ? ,?)""".bind(
            eventsEnvelope.commandId.asLong,
            eventsEnvelope.userId.asLong,
            aggregateId.asLong,
            eventsEnvelope.expectedVersion.asInt + versionsIncreased,
            event.aggregateRootType.typeSymbol.fullName,
            event.getClass.getName,
            0,
            eventSerialized).execute().apply()

          versionsIncreased += 1
      })
    }

  }


  override def readAndProcessAllEvents[AGGREGATE_ROOT](aggregateId: AggregateId)(eventHandler: Event[AGGREGATE_ROOT] => Unit): Unit = {

    DB.readOnly { implicit session =>
      sql"""SELECT event_type, event
            | FROM events
            | WHERE aggregate_id = ?
            | ORDER BY version""".stripMargin.bind(aggregateId.asLong).foreach { rs =>

        val event = mpjsons.deserialize[Event[AGGREGATE_ROOT]](rs.string(2), rs.string(1))
        eventHandler(event)
      }
    }
  }

  override def deletePublishedEventsToPublish(events: Seq[EventIdentifier]): Unit = {
    // TODO optimize SQL query so it will be one query
    DB.autoCommit { implicit session =>
      events.foreach {event =>
        sql"""DELETE FROM events_to_publish WHERE aggregate_id = ? AND version = ?"""
          .bind(event.aggregateId.asLong, event.version.asInt)
          .executeUpdate().apply()
      }
    }
  }



}

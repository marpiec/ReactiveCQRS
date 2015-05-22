package io.reactivecqrs.actor

import io.mpjsons.MPJsons
import io.reactivecqrs.api.guid.AggregateId
import io.reactivecqrs.core.EventsEnvelope
import scalikejdbc._

class EventStore {

  final val eventsTableName = "events"

  val mpjsons = new MPJsons


  val settings = ConnectionPoolSettings(
    initialSize = 5,
    maxSize = 20,
    connectionTimeoutMillis = 3000L)

  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs", settings)

  def initSchema(): Unit = {
    (new EventsSchemaInitializer).initSchema()
  }

  def persistEvent(aggregateId: AggregateId, eventEnvelope: EventsEnvelope[AnyRef]): Unit = {
    println("Persisting event " + aggregateId+ " " +eventEnvelope)
    var versionsIncreased = 0
    DB.autoCommit { implicit session =>
      eventEnvelope.events.foreach(event => {

        val eventSerialized = mpjsons.serialize(eventEnvelope.events.head, event.getClass.getName)


          sql"""SELECT add_event(?, ?, ?, ? ,? , ?, ? ,?)""".bind(
            eventEnvelope.commandId.asLong,
            eventEnvelope.userId.asLong,
            aggregateId.asLong,
            eventEnvelope.expectedVersion.asInt + versionsIncreased,
            event.aggregateRootType.typeSymbol.fullName,
            event.getClass.getName,
            0,
            eventSerialized).execute().apply()

          versionsIncreased += 1
      })
    }

  }


}

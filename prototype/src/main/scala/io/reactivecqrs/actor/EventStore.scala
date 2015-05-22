package io.reactivecqrs.actor

import io.mpjsons.MPJsons
import io.reactivecqrs.api.guid.AggregateId
import io.reactivecqrs.core.EventEnvelope
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

  def persistEvent(aggregateId: AggregateId, eventEnvelope: EventEnvelope[AnyRef]): Unit = {
    println("Persisting event " + aggregateId+ " " +eventEnvelope)
    val eventSerialized = mpjsons.serialize(eventEnvelope.event, eventEnvelope.event.getClass.getName)
    DB.autoCommit { implicit session =>

      sql"""SELECT add_event(?, ?, ?, ? ,? , ?, ? ,?)""".bind(
          eventEnvelope.commandId.asLong,
          eventEnvelope.userId.asLong,
          aggregateId.asLong,
          eventEnvelope.expectedVersion.asInt,
          eventEnvelope.event.aggregateRootType.typeSymbol.fullName,
          eventEnvelope.event.getClass.getName,
          0,
          eventSerialized).execute().apply()
    }

  }


}

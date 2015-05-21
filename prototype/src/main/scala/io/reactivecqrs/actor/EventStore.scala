package io.reactivecqrs.actor

import io.mpjsons.MPJsons
import io.reactivecqrs.api.guid.AggregateId
import io.reactivecqrs.core.{EventEnvelope, Event}
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

  def persistEvent(aggregateId: AggregateId, event: EventEnvelope[AnyRef]): Unit = {

    val eventSerialized = mpjsons.serialize(event, event.getClass.getName)
    DB.autoCommit { implicit session =>
      sql"""SELECT add_event(
         |${event.commandId.id},
         |${event.userId.id},
         |${aggregateId.asLong},
         |${event.expectedVersion},
         |${event.event.aggregateRootType.typeSymbol.fullName},
         |${event.getClass.getName},
         |0,
         |$eventSerialized);""".stripMargin.executeUpdate().apply()
    }

  }


}

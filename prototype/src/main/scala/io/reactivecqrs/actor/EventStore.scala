package io.reactivecqrs.actor

import io.reactivecqrs.actor.EventsSchemaInitializer
import io.reactivecqrs.core.Event
import scalikejdbc.{ConnectionPoolSettings, ConnectionPool}

class EventStore {

  final val eventsTableName = "events"


  val settings = ConnectionPoolSettings(
    initialSize = 5,
    maxSize = 20,
    connectionTimeoutMillis = 3000L)

  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs", settings)

  def initSchema(): Unit = {
    (new EventsSchemaInitializer).initSchema()
  }

  def persistEvent(event: Event[AnyRef]): Unit = {

  }


}

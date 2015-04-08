package io.reactivecqrs.core

import java.sql.Timestamp

import io.reactivecqrs.api.guid.AggregateId
import slick.lifted.Tag

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import slick.driver.PostgresDriver.api._

import scala.concurrent.ExecutionContext.Implicits.global

class Events(tag: Tag) extends Table[(Int, Long, Long,
  Long, Timestamp, Int, String, Int, String)](tag, "EVENTS") {
  def id = column[Int]("ID", O.PrimaryKey)
  def commandId = column[Long]("command_id")
  def userId = column[Long]("user_id")
  def aggregateId = column[Long]("aggregate_id")
  def eventTime = column[Timestamp]("event_time")
  def version = column[Int]("version")
  def eventType = column[String]("event_type")
  def eventTypeVersion = column[Int]("event_type_version")
  def event = column[String]("event")
  override def * = (id, commandId, userId,
    aggregateId, eventTime, version, eventType, eventTypeVersion, event)
}



class PostgresEventStore[AGGREGATE_ROOT] extends EventStore[AGGREGATE_ROOT] {

  val events = TableQuery[Events]

  events.schema.create

  override def putEvent(id: AggregateId, eventRow: EventRow[AGGREGATE_ROOT]): Unit = {

    Database.forURL(
      url = "jdbc:postgresql://localhost:5432/scalacqrs",
      driver = "org.postgresql.Driver",
      user="reactivecqrs",
      password = "reactivecqrs") withSession {
      implicit session =>

        events += ()

    }



    val eventsForAggregate = events.getOrElseUpdate(id, new ListBuffer[EventRow[AGGREGATE_ROOT]])
    eventsForAggregate += eventRow
  }

  override def getEvents(id: AggregateId) = {
    events.get(id).map(_.toStream).getOrElse(Stream())
  }

  override def getEventsToVersion(id: AggregateId, version: Int) = {
    events.get(id).map(_.take(version).toStream).getOrElse(Stream())
  }
}



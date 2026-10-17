package io.reactivecqrs.core.space

import java.sql.Timestamp
import java.time.Instant
import java.util.{Calendar, TimeZone}

import scalikejdbc.ParameterBinder

/**
  * D40: the lines of an organization's export, one JSON object each, written and read with mpjsons: the header, then
  * one line per aggregate, then the events in commit order, then the archived events. Classes are named, never
  * numbered: type ids differ between databases. Times are ISO-8601 instants.
  */
object SpaceFormat {

  val Version = 1

  case class ExportHeader(format: Int, installation: Int, space: Long, exported: String)

  /** A copy's chain row within the space: the base it reads, its order in the chain, the version it reads it up to. */
  case class ExportedChainRow(base: Long, order: Int, version: Int)

  /** `detached`: the aggregate's chain reached outside the space, so its version-1 event in this export is the type's
    * squash event of its state at version 1, and it has no chain. */
  case class ExportedAggregate(id: Long, rootClass: String, version: Int, squashedVersion: Option[Int], created: String,
                               chain: Seq[ExportedChainRow], detached: Boolean)

  /** An event as stored: its JSON, its user and time, its metadata JSON, and the version of the undo that cancelled it. */
  case class ExportedEvent(aggregate: Long, version: Int, eventClass: String, json: String, user: Long, time: String,
                           metadata: Option[String], undoneFrom: Option[Int])

  case class ExportedArchived(aggregate: Long, version: Int, eventClass: String, json: String, user: Long, time: String,
                              metadata: Option[String], undoneFrom: Option[Int], archived: String)

  /** One line of the file: exactly one field is set. */
  case class Line(header: Option[ExportHeader] = None, aggregate: Option[ExportedAggregate] = None, event: Option[ExportedEvent] = None,
                  archived: Option[ExportedArchived] = None)

  // F65: event times are UTC wall times in TIMESTAMP columns.
  private val Utc = TimeZone.getTimeZone("UTC")
  private[space] def utc(instant: Instant): ParameterBinder = {
    val timestamp = Timestamp.from(instant)
    ParameterBinder(timestamp, (statement, index) => statement.setTimestamp(index, timestamp, Calendar.getInstance(Utc)))
  }
  private[space] def utcTime(rs: java.sql.ResultSet, column: Int): Instant = rs.getTimestamp(column, Calendar.getInstance(Utc)).toInstant
}

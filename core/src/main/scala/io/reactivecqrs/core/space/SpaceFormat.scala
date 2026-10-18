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

  /** The format's version, in every export's header. */
  val Version = 1

  /** The first line: the format's version, the exporting installation, the space, and when it was exported. */
  case class ExportHeader(format: Int, installation: Int, space: Long, exported: String)

  /** A copy's chain row within the space: the base it reads, its order in the chain, the version it reads it up to. */
  case class ExportedChainRow(base: Long, order: Int, version: Int)

  /** A base of the space that a detached copy read, at `version`, when it was detached (D76: the copy holds its values). */
  case class ExportedLineage(base: Long, version: Int)

  /** `detached`: the aggregate's chain reached outside the space, so its version-1 event in this export is the type's
    * squash event of its state at version 1, and it has no chain. `lineage`: the bases of the space it was detached
    * from, earlier or by this export. */
  case class ExportedAggregate(id: Long, rootClass: String, version: Int, squashedVersion: Option[Int], created: String,
                               chain: Seq[ExportedChainRow], detached: Boolean, lineage: Seq[ExportedLineage])

  /** An event as stored: its JSON, its user and time, its metadata JSON, and the version of the undo that cancelled it. */
  case class ExportedEvent(aggregate: Long, version: Int, eventClass: String, json: String, user: Long, time: String,
                           metadata: Option[String], undoneFrom: Option[Int])

  /** An archived event, as `ExportedEvent`, with when a squash archived it. */
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

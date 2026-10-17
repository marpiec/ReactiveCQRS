package io.reactivecqrs.core.tools

import io.mpjsons.MPJsons
import io.reactivecqrs.core.commandhandler.PostgresCommandResponseState
import io.reactivecqrs.core.types.TypesNamesState
import scalikejdbc._

import scala.collection.mutable
import scala.reflect.{ClassTag, classTag}
import scala.util.{Failure, Success, Try}

/**
 * The dry run of D78: reads every stored JSON value with the mpjsons version on the classpath and reports, per table
 * and class, how many rows were read and which could not be (F52). It also gives the other pre-checks of a production
 * copy: aggregates without a space (F49) and events by event type version (D29). It only reads.
 *
 * Run it from the application, which has all stored classes on its classpath, against a copy of production:
 * {{{
 * val report = StoredDataCheck.run(mpjsons, typesNamesState,
 *   Seq(StoredDataCheck.ProjectionTable[CaseView]("cases"), ...))
 * println(report.text)
 * }}}
 */
object StoredDataCheck {

  /** A projection table, named as its document store was (without the `projection_` prefix), with its document class. */
  case class ProjectionTable[T <: AnyRef: ClassTag](name: String) {
    private[tools] def read(mpjsons: MPJsons, json: String): Unit = mpjsons.deserialize[T](json, className)
    private[tools] def className: String = classTag[T].runtimeClass.getName // D75: by class, not by Scala 2 reflection
  }

  case class ClassResult(table: String, className: String, rows: Long, unreadable: Long, examples: Seq[String])

  case class Report(results: Seq[ClassResult], aggregatesWithoutSpace: Long, eventsByTypeVersion: Map[Int, Long]) {

    def unreadable: Long = results.map(_.unreadable).sum

    def text: String = {
      val header = s"Stored data check: ${results.map(_.rows).sum} rows read, $unreadable unreadable.\n\n" +
        "| Table | Class | Rows | Unreadable | First unreadable rows |\n|---|---|---|---|---|\n"
      val rows = results.map(r => s"| ${r.table} | ${r.className} | ${r.rows} | ${r.unreadable} | ${r.examples.mkString("; ")} |").mkString("\n")
      header + rows + s"\n\nAggregates without a space (space_id = -1): $aggregatesWithoutSpace\n" +
        "Events by event type version: " + eventsByTypeVersion.toSeq.sorted.map { case (v, n) => s"$v: $n" }.mkString(", ") + "\n"
    }
  }

  def run(mpjsons: MPJsons, typesNamesState: TypesNamesState,
          projections: Seq[ProjectionTable[_]] = Seq.empty, examplesPerClass: Int = 5, fetchSize: Int = 1000): Report = {

    val results = new Results(examplesPerClass)
    val responses = new PostgresCommandResponseState(mpjsons, typesNamesState)

    // An id missing from types_names makes its rows unreadable instead of stopping the check.
    def className(typeId: Short): String = Try(typesNamesState.classNameById(typeId)).getOrElse("unknown type id " + typeId)

    // In a transaction, so PostgreSQL streams the rows instead of loading a whole table.
    def eachRow(query: SQL[Nothing, NoExtractor])(handle: WrappedResultSet => Unit): Unit =
      DB.localTx { implicit session => query.fetchSize(fetchSize).foreach(handle) }

    eachRow(sql"SELECT id, event_type_id, event_type_version, event FROM events") { rs =>
      val eventClass = className(rs.short(2))
      // D29: 1.0 reads only version 0; a row of another version fails to restore.
      val read = if (rs.short(3) != 0) Failure(new IllegalStateException("event_type_version " + rs.short(3) + " (event versioning was removed, D29)"))
        else Try(mpjsons.deserialize[AnyRef](rs.string(4), eventClass))
      results.add("events", eventClass, "id " + rs.long(1), read)
    }

    eachRow(sql"SELECT name, saga_id, order_type_id, saga_order FROM sagas") { rs =>
      val orderClass = className(rs.int(3).toShort)
      results.add("sagas", orderClass, rs.string(1) + " " + rs.long(2), Try(mpjsons.deserialize[AnyRef](rs.string(4), orderClass)))
    }

    eachRow(sql"SELECT id, response, response_type_id, info, info_type_id FROM commands_responses") { rs =>
      results.add("commands_responses", className(rs.short(3)), "id " + rs.long(1),
        Try(responses.readResponse(rs.string(2), rs.short(3), rs.stringOpt(4), rs.shortOpt(5))))
    }

    projections.foreach { projection =>
      val table = "projection_" + projection.name
      eachRow(SQL(s"SELECT id, document FROM $table")) { rs =>
        results.add(table, projection.className, "id " + rs.long(1), Try(projection.read(mpjsons, rs.string(2))))
      }
    }

    DB.readOnly { implicit session =>
      Report(results.all,
        sql"SELECT count(*) FROM aggregates WHERE space_id = -1".map(_.long(1)).single().get,
        sql"SELECT event_type_version, count(*) FROM events GROUP BY event_type_version".map(rs => rs.int(1) -> rs.long(2)).list().toMap)
    }
  }

  private class Results(examplesPerClass: Int) {
    private case class Counts(var rows: Long = 0, var unreadable: Long = 0, examples: mutable.ListBuffer[String] = mutable.ListBuffer())
    private val byClass = mutable.LinkedHashMap[(String, String), Counts]()

    def add(table: String, className: String, row: String, read: Try[Any]): Unit = {
      val counts = byClass.getOrElseUpdate((table, className), Counts())
      counts.rows += 1
      read match {
        case Success(_) => ()
        case Failure(e) =>
          counts.unreadable += 1
          if (counts.examples.size < examplesPerClass) counts.examples += row + ": " + e.getMessage
      }
    }

    def all: Seq[ClassResult] = byClass.toSeq.map { case ((table, className), c) => ClassResult(table, className, c.rows, c.unreadable, c.examples.toList) }
  }
}

package io.reactivecqrs.core.tools

import io.mpjsons.MPJsons
import io.reactivecqrs.api.AggregateContext
import io.reactivecqrs.core.commandhandler.PostgresCommandResponseState
import io.reactivecqrs.core.eventstore.{OrphanEvents, PostgresEventStoreState}
import io.reactivecqrs.core.types.TypesNamesState
import scalikejdbc._

import scala.collection.mutable
import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.util.{Failure, Success, Try}
import io.reactivecqrs.core.util.SqlLogging.withoutValues
import io.reactivecqrs.core.util.Errors

/**
 * The dry run of D78: reads every stored JSON value with the mpjsons version on the classpath and reports, per table
 * and class, how many rows were read and which could not be (F52). It also gives the other pre-checks of a production
 * copy: aggregates without a space (F49), events by event type version (D29) and orphan events (F342). It only reads.
 *
 * Run it from the application, which has all stored classes on its classpath, against a copy of production:
 * {{{
 * val report = StoredDataCheck.run(mpjsons, typesNamesState,
 *   Seq(StoredDataCheck.ProjectionTable[CaseView]("cases"), ...))
 * println(report.text)
 * }}}
 */
object StoredDataCheck {

  /** A projection table, named as its document store was (without the `projection_` prefix), with its document type.
    * G3: read by its full type, as the store reads it, so a document generic in its content (`Wrapper[Item]`) reads. */
  case class ProjectionTable[T <: AnyRef: TypeTag: ClassTag](name: String) {
    private[tools] def read(mpjsons: MPJsons, json: String): Unit = mpjsons.deserialize[T](json)
    private[tools] def className: String = {
      val arguments = typeOf[T].dealias.typeArgs
      classTag[T].runtimeClass.getName + (if (arguments.isEmpty) "" else arguments.mkString("[", ", ", "]"))
    }
  }

  /** One class of one table: rows read, rows that did not read, and the first of those. */
  case class ClassResult(table: String, className: String, rows: Long, unreadable: Long, examples: Seq[String])

  /** `missingTables`: the tables to read that the database does not have (F181), such as the saga table of an
    * application without sagas, or the archive of a database from before 1.0. `orphans` (F342): the events whose
    * aggregate has no row of its own, which no read path reads, with the first 100 of their aggregates. */
  case class Report(results: Seq[ClassResult], aggregatesWithoutSpace: Long, eventsByTypeVersion: Map[Int, Long],
                    missingTables: Seq[String] = Nil, repeatedVersions: Seq[RepeatedVersion] = Nil,
                    typesWithoutContext: Map[String, Long] = Map.empty, orphans: OrphanEvents = OrphanEvents.none) {

    def unreadable: Long = results.map(_.unreadable).sum

    def text: String = {
      val header = s"Stored data check: ${results.map(_.rows).sum} rows read, $unreadable unreadable.\n\n" +
        "| Table | Class | Rows | Unreadable | First unreadable rows |\n|---|---|---|---|---|\n"
      val rows = results.map(r => s"| ${r.table} | ${r.className} | ${r.rows} | ${r.unreadable} | ${r.examples.mkString("; ")} |").mkString("\n")
      val missing = if (missingTables.isEmpty) "" else "\n\nTables not found, so not read: " + missingTables.mkString(", ")
      header + rows + missing + s"\n\nAggregates without a space (space_id = -1): $aggregatesWithoutSpace\n" +
        "Events by event type version: " + eventsByTypeVersion.toSeq.sorted.map { case (v, n) => s"$v: $n" }.mkString(", ") + "\n" +
        (if (repeatedVersions.isEmpty) "" else "Versions stored twice or more (F198; the first 100): " +
          repeatedVersions.map(r => s"aggregate ${r.aggregateId} version ${r.version} (${r.events} events)").mkString(", ") + "\n") +
        (if (typesWithoutContext.isEmpty) "" else "Aggregate types no context given has (F197; their aggregates are not restored): " +
          typesWithoutContext.toSeq.sorted.map { case (t, n) => s"$t: $n" }.mkString(", ") + "\n") +
        (if (orphans.events == 0) "" else s"Events whose aggregate has no row of its own (F342; no read path reads them): ${orphans.events} of " +
          s"${orphans.aggregates} aggregates; the first ${orphans.listed.size}: " + orphans.listed.map(o => s"aggregate ${o.aggregateId.asLong}: " +
          s"${o.events} events (${o.eventClasses.mkString(", ")}), ids ${o.firstEventId} to ${o.lastEventId}, ${o.firstEventTime} to ${o.lastEventTime}")
          .mkString("; ") + "\n")
    }
  }

  /** F198: an aggregate with more than one stored event of one version, which no restore reads right. */
  case class RepeatedVersion(aggregateId: Long, version: Int, events: Long)

  /** Reads every stored row of the library's tables (and of `projections`) with the classes of this code, and reports
    * what does not read. Run it on a copy of production before an upgrade (the runbook). With `contexts`, it also counts
    * the aggregates of types none of them has, which their command buses would not restore (F197). */
  def run(mpjsons: MPJsons, typesNamesState: TypesNamesState,
          projections: Seq[ProjectionTable[_]] = Seq.empty, examplesPerClass: Int = 5, fetchSize: Int = 1000,
          contexts: Seq[AggregateContext[_]] = Nil): Report = {

    val results = new Results(examplesPerClass)
    val responses = new PostgresCommandResponseState(mpjsons, typesNamesState)

    // An id missing from types_names makes its rows unreadable instead of stopping the check.
    def className(typeId: Short): String = Try(typesNamesState.classNameById(typeId)).getOrElse("unknown type id " + typeId)

    // In a transaction, so PostgreSQL streams the rows instead of loading a whole table.
    def eachRow(query: SQL[Nothing, NoExtractor])(handle: WrappedResultSet => Unit): Unit =
      DB.localTx { implicit session => query.fetchSize(fetchSize).foreach(handle) }

    // F181: every table but `events` may be missing: the archive before 1.0, the sagas and the command responses of an
    // application that has none, a listed projection never made. A missing table is reported, not read.
    val missing = Seq.newBuilder[String]
    def ifExists(table: String)(read: => Unit): Unit =
      if (DB.readOnly(implicit session => sql"SELECT to_regclass(${table}::text) IS NOT NULL".map(_.boolean(1)).single().get)) read
      else missing += table

    eachRow(sql"SELECT id, event_type_id, event_type_version, event FROM events") { rs =>
      val eventClass = className(rs.short(2))
      // D29: 1.0 reads only version 0; a row of another version fails to restore.
      val read = if (rs.short(3) != 0) Failure(new IllegalStateException("event_type_version " + rs.short(3) + " (event versioning was removed, D29)"))
        else Try(mpjsons.deserialize[AnyRef](rs.string(4), eventClass))
      results.add("events", eventClass, "id " + rs.long(1), read)
    }

    // R11: archived events are read like events.
    ifExists("events_archive") {
      eachRow(sql"SELECT id, event_type_id, event FROM events_archive") { rs =>
        val eventClass = className(rs.short(2))
        results.add("events_archive", eventClass, "id " + rs.long(1), Try(mpjsons.deserialize[AnyRef](rs.string(3), eventClass)))
      }
    }

    ifExists("sagas") {
      eachRow(sql"SELECT name, saga_id, order_type_id, saga_order FROM sagas") { rs =>
        val orderClass = className(rs.int(3).toShort)
        results.add("sagas", orderClass, rs.string(1) + " " + rs.long(2), Try(mpjsons.deserialize[AnyRef](rs.string(4), orderClass)))
      }
    }

    ifExists("commands_responses") {
      eachRow(sql"SELECT id, response, response_type_id, info, info_type_id FROM commands_responses") { rs =>
        results.add("commands_responses", className(rs.short(3)), "id " + rs.long(1),
          Try(responses.readResponse(rs.string(2), rs.short(3), rs.stringOpt(4), rs.shortOpt(5))))
      }
    }

    projections.foreach { projection =>
      val table = "projection_" + projection.name
      ifExists(table) {
        eachRow(SQL(s"SELECT id, document FROM $table")) { rs =>
          results.add(table, projection.className, "id " + rs.long(1), Try(projection.read(mpjsons, rs.string(2))))
        }
      }
    }

    DB.readOnly { implicit session =>
      Report(results.all,
        sql"SELECT count(*) FROM aggregates WHERE space_id = -1".map(_.long(1)).single().get,
        sql"SELECT event_type_version, count(*) FROM events GROUP BY event_type_version".map(rs => rs.int(1) -> rs.long(2)).list().toMap,
        missing.result(),
        sql"""SELECT aggregate_id, version, count(*) FROM events GROUP BY aggregate_id, version HAVING count(*) > 1
             | ORDER BY aggregate_id, version LIMIT 100""".stripMargin.map(rs => RepeatedVersion(rs.long(1), rs.int(2), rs.long(3))).list(),
        if (contexts.isEmpty) Map.empty else {
          val known = contexts.map(_.aggregateType.typeName).toSet
          sql"SELECT type_id, count(*) FROM aggregates WHERE id = base_id GROUP BY type_id".map(rs => className(rs.short(1)) -> rs.long(2)).list()
            .filterNot { case (name, _) => known.contains(name) }.toMap
        },
        PostgresEventStoreState.orphanEvents(typesNamesState, 100))
    }
  }

  /** R17: what an organization still has: its aggregates, its events (by their keys), its sagas and the documents of
    * every projection table, by table; after a purge, all zero. Projection tables are read whole (no index by space). */
  def spaceRows(space: Long): Map[String, Long] = DB.readOnly { implicit session =>
    def has(table: String, column: String): Boolean =
      sql"""SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $table
           |  AND column_name = $column)""".stripMargin.map(_.boolean(1)).single().getOrElse(false)
    def counted(table: String): (String, Long) =
      table -> sql"SELECT count(*) FROM ${SQLSyntax.createUnsafely(table)} WHERE space_id = $space".map(_.long(1)).single().getOrElse(0L)
    val documents = sql"""SELECT table_name FROM information_schema.columns WHERE table_schema = current_schema()
                         |  AND table_name LIKE 'projection%' AND column_name = 'space_id' ORDER BY table_name""".stripMargin.map(_.string(1)).list()
    (Seq("aggregates", "event_keys", "sagas").filter(has(_, "space_id")) ++ documents).map(counted).toMap
  }

  /** The aggregates that still have events of a class: in `events`, in the archive, and the first `limit` of them with
    * their counts of each. */
  case class ClassUsers(className: String, events: Long, archived: Long, aggregates: Seq[ClassUser])
  /** One aggregate's events of the class, and its archived ones. */
  case class ClassUser(aggregateId: Long, events: Long, archived: Long)

  /** R11: who still uses an event class, in `events` or in the archive. Both counts zero means the class can go from the code (D29:
    * its successor holds the new shape). Reads both tables whole, as there is no index by class: run it on a copy or
    * when the database is quiet. */
  def usersOf(className: String, limit: Int = 100): ClassUsers = DB.readOnly { implicit session =>
    val archive = sql"SELECT to_regclass('events_archive') IS NOT NULL".map(_.boolean(1)).single().get
    def total(table: SQLSyntax): Long =
      sql"SELECT count(*) FROM $table e JOIN types_names t ON t.id = e.event_type_id WHERE t.name = $className".map(_.long(1)).single().get
    val archivedRows = if (archive) sqls"UNION ALL SELECT e.aggregate_id, 0 AS live, 1 AS kept FROM events_archive e JOIN types_names t ON t.id = e.event_type_id WHERE t.name = $className" else sqls""
    val users = sql"""SELECT aggregate_id, sum(live), sum(kept) FROM (
                     |  SELECT e.aggregate_id, 1 AS live, 0 AS kept FROM events e JOIN types_names t ON t.id = e.event_type_id WHERE t.name = $className
                     |  $archivedRows) u
                     | GROUP BY aggregate_id ORDER BY aggregate_id LIMIT $limit""".stripMargin.map(rs => ClassUser(rs.long(1), rs.long(2), rs.long(3))).list()
    ClassUsers(className, total(sqls"events"), if (archive) total(sqls"events_archive") else 0L, users)
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
          if (counts.examples.size < examplesPerClass) counts.examples += row + ": " + Errors.describe(e)
      }
    }

    def all: Seq[ClassResult] = byClass.toSeq.map { case ((table, className), c) => ClassResult(table, className, c.rows, c.unreadable, c.examples.toList) }
  }
}

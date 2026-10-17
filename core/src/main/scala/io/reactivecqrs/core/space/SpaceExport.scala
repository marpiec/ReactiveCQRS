package io.reactivecqrs.core.space

import java.io.Writer
import java.time.Instant

import io.reactivecqrs.api.{AggregateContext, FirstEvent}
import io.reactivecqrs.api.id.{AggregateId, SpaceId}
import io.reactivecqrs.core.aggregate.AggregateRestorer
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.space.SpaceFormat._
import scalikejdbc._

/** What an export wrote. `detached`: the aggregates written with their version-1 event replaced by a squash event, as
  * their chains reached outside the space; `waitingInOutbox`: the space's events not delivered yet when it ran (export
  * a quiet organization); `refused`: why nothing was written. */
case class ExportReport(space: SpaceId, aggregates: Int, events: Long, archived: Long, detached: Seq[AggregateId], waitingInOutbox: Long,
                        refused: Seq[String]) {
  def complete: Boolean = refused.isEmpty
}

/**
  * D40: writes one organization (a space) as JSON lines (`SpaceFormat`), for `SpaceImport` to write into another
  * installation. `contexts` are the aggregate contexts of the space's types: an aggregate whose chain reaches outside the
  * space is written with its type's squash event of its state at version 1, as a detach makes it (D50, D85), so the
  * import needs nothing outside. It only reads, streaming the events; it refuses, writing nothing, an id below 1001 in
  * the space (ids that every installation shares) and an aggregate it cannot detach. Export a quiet organization.
  * Design: docs/design/M6-organizations.md, "Export and import".
  */
class SpaceExport(eventStore: PostgresEventStoreState, contexts: Seq[AggregateContext[_]], installation: Int = 0) {

  private val jsons = eventStore.jsons

  def write(space: SpaceId, out: Writer): ExportReport = {
    val s = space.asLong
    val (chains, outside, inside) = chainsOf(s)
    val constants = DB.readOnly { implicit session =>
      sql"SELECT id FROM aggregates WHERE space_id = $s AND base_id = id AND id < 1001 ORDER BY id LIMIT 10".map(_.long(1)).list()
    }
    val detachedTypes = DB.readOnly { implicit session =>
      if (outside.isEmpty) Map.empty[Long, String]
      else sql"""SELECT a.id, t.name FROM aggregates a JOIN types_names t ON t.id = a.type_id WHERE a.id IN ($outside) AND a.base_id = a.id"""
        .map(rs => rs.long(1) -> rs.string(2)).list().toMap
    }
    val detachEvents = outside.map(id => id -> detachEventOf(AggregateId(id), detachedTypes.getOrElse(id, "?"))).toMap
    val refused = constants.map(id => "aggregate " + id + " has an id below 1001, which every installation shares") ++
      detachEvents.values.collect { case Left(reason) => reason }
    if (refused.nonEmpty) {
      ExportReport(space, 0, 0, 0, Nil, 0, refused)
    } else {
      val detached = detachEvents.collect { case (id, Right(event)) => id -> event }
      def line(l: Line): Unit = {
        out.write(jsons.serialize(l, classOf[Line].getName))
        out.write('\n')
      }
      line(Line(header = Some(ExportHeader(Version, installation, s, Instant.now.toString))))
      val aggregates = writeAggregates(s, chains, detached.keySet, lineagesOf(s, inside, detached.keySet), a => line(Line(aggregate = Some(a))))
      val events = writeEvents(s, detached, e => line(Line(event = Some(e))))
      val archived = writeArchived(s, a => line(Line(archived = Some(a))))
      out.flush()
      val waiting = DB.readOnly { implicit session =>
        sql"""SELECT count(*) FROM events_to_publish p JOIN aggregates a ON a.id = p.aggregate_id AND a.base_id = a.id WHERE a.space_id = $s"""
          .map(_.long(1)).single().getOrElse(0L)
      }
      ExportReport(space, aggregates, events, archived, detached.keys.toSeq.sorted.map(AggregateId(_)), waiting, Nil)
    }
  }

  // The chain rows of the space's copies, the copies whose chain reaches a base of another space, and the chain rows
  // whose base is in the space.
  private def chainsOf(space: Long): (Map[Long, Seq[ExportedChainRow]], Seq[Long], Map[Long, Seq[ExportedChainRow]]) = DB.readOnly { implicit session =>
    val rows = sql"""SELECT a.id, a.base_id, a.base_order, a.base_version, b.space_id FROM aggregates a
                    | JOIN aggregates b ON b.id = a.base_id AND b.base_id = b.id
                    | WHERE a.space_id = $space AND a.base_id <> a.id ORDER BY a.id, a.base_order""".stripMargin
      .map(rs => (rs.long(1), ExportedChainRow(rs.long(2), rs.int(3), rs.int(4)), rs.long(5))).list()
    val outside = rows.collect { case (id, _, baseSpace) if baseSpace != space => id }.distinct
    def byCopy(of: Seq[(Long, ExportedChainRow, Long)]) = of.groupBy(_._1).map { case (id, chain) => id -> chain.map(_._2) }
    (byCopy(rows), outside, byCopy(rows.filter(_._3 == space)))
  }

  // D76: the bases of the space each copy holds values of: those recorded when it was detached, and, for a copy this
  // export detaches, the bases of the space in its chain.
  private def lineagesOf(space: Long, inside: Map[Long, Seq[ExportedChainRow]], detached: Set[Long]): Map[Long, Seq[ExportedLineage]] = {
    val recorded = DB.readOnly { implicit session =>
      sql"""SELECT l.aggregate_id, l.copied_from, l.at_version FROM aggregate_lineage l
           | JOIN aggregates a ON a.id = l.aggregate_id AND a.base_id = a.id JOIN aggregates b ON b.id = l.copied_from AND b.base_id = b.id
           | WHERE a.space_id = $space AND b.space_id = $space""".stripMargin.map(rs => rs.long(1) -> ExportedLineage(rs.long(2), rs.int(3))).list()
    }
    val now = detached.toSeq.flatMap(id => inside.getOrElse(id, Nil).map(row => id -> ExportedLineage(row.base, row.version)))
    (recorded ++ now).groupBy(_._1).map { case (id, rows) => id -> rows.map(_._2).distinct.sortBy(_.base) }
  }

  // D50, D85: the squash event of the copy's state at version 1, built and checked as a detach builds it.
  private def detachEventOf(id: AggregateId, rootClass: String): Either[String, FirstEvent[_]] =
    contexts.find(_.aggregateType.typeName == rootClass) match {
      case None => Left("aggregate " + id.asLong + " reads outside the space, and no context of its type " + rootClass + " was given to detach it")
      case Some(context) => detachEvent(context.asInstanceOf[AggregateContext[Any]], id)
    }

  private def detachEvent[R](context: AggregateContext[R], id: AggregateId): Either[String, FirstEvent[_]] = context.squash match {
    case None => Left("aggregate " + id.asLong + " reads outside the space, and its type " + context.aggregateType.simpleName +
      " has no squash event to detach it")
    case Some(policy) =>
      new AggregateRestorer[R](eventStore, context.eventHandlers, () => context.initialAggregateRoot).detachEvents(Seq(id), policy)
        .map(_.head._2).left.map(reason => "aggregate " + id.asLong + " cannot be detached: " + reason)
  }

  private def writeAggregates(space: Long, chains: Map[Long, Seq[ExportedChainRow]], detached: Set[Long], lineages: Map[Long, Seq[ExportedLineage]],
                              write: ExportedAggregate => Unit): Int =
    DB.localTx { implicit session =>
      var count = 0
      sql"""SELECT a.id, t.name, a.base_version, a.squashed_version, a.creation_time FROM aggregates a JOIN types_names t ON t.id = a.type_id
           | WHERE a.space_id = $space AND a.base_id = a.id ORDER BY a.id""".stripMargin.fetchSize(1000).foreach { rs =>
        val id = rs.long(1)
        val isDetached = detached.contains(id)
        write(ExportedAggregate(id, rs.string(2), rs.int(3), rs.intOpt(4), utcTime(rs.underlying, 5).toString,
          if (isDetached) Nil else chains.getOrElse(id, Nil), isDetached, lineages.getOrElse(id, Nil)))
        count += 1
      }
      count
    }

  // In commit order, the events of the space's aggregates as stored, but a detached copy's first event.
  private def writeEvents(space: Long, detached: Map[Long, FirstEvent[_]], write: ExportedEvent => Unit): Long = DB.localTx { implicit session =>
    var count = 0L
    sql"""SELECT e.aggregate_id, e.version, t.name, e.event, e.user_id, e.event_time, e.metadata::text, n.from_version
         | FROM events e JOIN aggregates a ON a.id = e.aggregate_id AND a.base_id = a.id JOIN types_names t ON t.id = e.event_type_id
         | LEFT JOIN noop_events n ON n.id = e.id
         | WHERE a.space_id = $space ORDER BY e.id""".stripMargin.fetchSize(1000).foreach { rs =>
      val aggregate = rs.long(1)
      val version = rs.int(2)
      val (eventClass, json, undoneFrom) = detached.get(aggregate).filter(_ => version == 1) match {
        case Some(first) => (first.getClass.getName, jsons.serialize(first, first.getClass.getName), None) // its undo mark goes with it
        case None => (rs.string(3), rs.string(4), rs.intOpt(8))
      }
      write(ExportedEvent(aggregate, version, eventClass, json, rs.long(5), utcTime(rs.underlying, 6).toString, rs.stringOpt(7), undoneFrom))
      count += 1
    }
    count
  }

  private def writeArchived(space: Long, write: ExportedArchived => Unit): Long = DB.localTx { implicit session =>
    val archive = sql"SELECT to_regclass('events_archive') IS NOT NULL".map(_.boolean(1)).single().getOrElse(false)
    var count = 0L
    if (archive) {
      sql"""SELECT x.aggregate_id, x.version, t.name, x.event, x.user_id, x.event_time, x.metadata::text, x.undone_from, x.archived
           | FROM events_archive x JOIN aggregates a ON a.id = x.aggregate_id AND a.base_id = a.id JOIN types_names t ON t.id = x.event_type_id
           | WHERE a.space_id = $space ORDER BY x.id""".stripMargin.fetchSize(1000).foreach { rs =>
        write(ExportedArchived(rs.long(1), rs.int(2), rs.string(3), rs.string(4), rs.long(5), utcTime(rs.underlying, 6).toString, rs.stringOpt(7),
          rs.intOpt(8), utcTime(rs.underlying, 9).toString))
        count += 1
      }
    }
    count
  }
}

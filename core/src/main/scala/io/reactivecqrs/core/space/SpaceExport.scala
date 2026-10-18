package io.reactivecqrs.core.space

import java.io.Writer
import java.time.Instant

import io.reactivecqrs.api.{AggregateContext, FirstEvent}
import io.reactivecqrs.api.id.{AggregateId, SpaceId}
import io.reactivecqrs.core.aggregate.AggregateRestorer
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.space.SpaceFormat._
import scalikejdbc._
import io.reactivecqrs.core.util.SqlLogging.withoutValues

/** What an export wrote. `detached`: the aggregates written with their version-1 event replaced by a squash event, as
  * their chains reached outside the space; `waitingInOutbox`: the space's events not delivered yet when it ran (export
  * a quiet organization); `refused`: why the file is not whole (it then has no trailer, and no import takes it). */
case class ExportReport(space: SpaceId, aggregates: Int, events: Long, archived: Long, detached: Seq[AggregateId], waitingInOutbox: Long,
                        refused: Seq[String]) {
  /** Whether the export wrote everything. */
  def complete: Boolean = refused.isEmpty
}

/**
  * D40: writes one organization (a space) as JSON lines (`SpaceFormat`), for `SpaceImport` to write into another
  * installation. `contexts` are the aggregate contexts of the space's types: an aggregate whose chain reaches outside the
  * space is written with its type's squash event of its state at version 1, as a detach makes it (D50, D85), so the
  * import needs nothing outside. It only reads: every row in one snapshot of the database (F198), so a write meanwhile
  * is either in the file whole or not at all; the events are streamed, and each detached copy's first event is made when
  * its row is reached (F225). It refuses, writing nothing, an id below 1001 in the space (ids that every installation
  * shares) and an aggregate whose type cannot detach it; one that fails to detach stops the file without its trailer.
  * Export a quiet organization: the snapshot holds back vacuum while it runs. Design: docs/design/M6-organizations.md,
  * "Export and import".
  */
class SpaceExport(eventStore: PostgresEventStoreState, contexts: Seq[AggregateContext[_]], installation: Int = 0) {

  private val jsons = eventStore.jsons

  /** Writes the organization `space` to `out`. */
  def write(space: SpaceId, out: Writer): ExportReport = DB.localTx { implicit session =>
    // F198: one snapshot for every read: the aggregate rows, their events, their archive and their chains agree.
    sql"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY".execute()
    val s = space.asLong
    val constants = sql"SELECT id FROM aggregates WHERE space_id = $s AND base_id = id AND id < 1001 ORDER BY id LIMIT 10".map(_.long(1)).list()
    val outside = readingOutside(s)
    val refused = constants.map(id => "aggregate " + id + " has an id below 1001, which every installation shares") ++
      outside.toSeq.sortBy(_._1).flatMap { case (id, rootClass) => cannotDetach(id, rootClass) }
    if (refused.nonEmpty) {
      ExportReport(space, 0, 0, 0, Nil, 0, refused)
    } else {
      val digest = new LinesDigest
      def line(l: Line): Unit = {
        val text = jsons.serialize(l, classOf[Line].getName)
        digest.add(text)
        out.write(text)
        out.write('\n')
      }
      line(Line(header = Some(ExportHeader(Version, installation, s, Instant.now.toString))))
      val (chains, inside) = chainsOf(s)
      val aggregates = writeAggregates(s, chains, outside.keySet, lineagesOf(s, inside, outside.keySet), a => line(Line(aggregate = Some(a))))
      writeEvents(s, outside, e => line(Line(event = Some(e)))) match {
        case Left(reason) =>
          out.flush()
          ExportReport(space, aggregates, 0, 0, Nil, 0, Seq(reason + "; the file is not whole and has no trailer"))
        case Right(events) =>
          val archived = writeArchived(s, a => line(Line(archived = Some(a))))
          val trailer = ExportTrailer(aggregates, events, archived, digest.hex)
          out.write(jsons.serialize(Line(trailer = Some(trailer)), classOf[Line].getName))
          out.write('\n')
          out.flush()
          val waiting = sql"""SELECT count(*) FROM events_to_publish p JOIN aggregates a ON a.id = p.aggregate_id AND a.base_id = a.id WHERE a.space_id = $s"""
            .map(_.long(1)).single().getOrElse(0L)
          ExportReport(space, aggregates, events, archived, outside.keys.toSeq.sorted.map(AggregateId(_)), waiting, Nil)
      }
    }
  }

  // The copies of the space whose chain reaches a base of another space, with their root class: they are detached. F225:
  // a join, not their ids bound one by one.
  private def readingOutside(space: Long)(implicit session: DBSession): Map[Long, String] =
    sql"""SELECT DISTINCT a.id, t.name FROM aggregates a
         | JOIN aggregates b ON b.id = a.base_id AND b.base_id = b.id
         | JOIN aggregates o ON o.id = a.id AND o.base_id = o.id JOIN types_names t ON t.id = o.type_id
         | WHERE a.space_id = $space AND a.base_id <> a.id AND b.space_id <> $space""".stripMargin
      .map(rs => rs.long(1) -> rs.string(2)).list().toMap

  // Why a copy that reads outside cannot be detached, checked before anything is written; whether its squash event gives
  // back its state is checked when it is made (D85).
  private def cannotDetach(id: Long, rootClass: String): Option[String] = contexts.find(_.aggregateType.typeName == rootClass) match {
    case None => Some("aggregate " + id + " reads outside the space, and no context of its type " + rootClass + " was given to detach it")
    case Some(context) if context.squash.isEmpty => Some("aggregate " + id + " reads outside the space, and its type " +
      context.aggregateType.simpleName + " has no squash event to detach it")
    case _ => None
  }

  // The chain rows of the space's copies, and those whose base is in the space.
  private def chainsOf(space: Long)(implicit session: DBSession): (Map[Long, Seq[ExportedChainRow]], Map[Long, Seq[ExportedChainRow]]) = {
    val rows = sql"""SELECT a.id, a.base_id, a.base_order, a.base_version, b.space_id FROM aggregates a
                    | JOIN aggregates b ON b.id = a.base_id AND b.base_id = b.id
                    | WHERE a.space_id = $space AND a.base_id <> a.id ORDER BY a.id, a.base_order""".stripMargin
      .map(rs => (rs.long(1), ExportedChainRow(rs.long(2), rs.int(3), rs.int(4)), rs.long(5))).list()
    def byCopy(of: Seq[(Long, ExportedChainRow, Long)]) = of.groupBy(_._1).map { case (id, chain) => id -> chain.map(_._2) }
    (byCopy(rows), byCopy(rows.filter(_._3 == space)))
  }

  // D76: the bases of the space each copy holds values of: those recorded when it was detached, and, for a copy this
  // export detaches, the bases of the space in its chain.
  private def lineagesOf(space: Long, inside: Map[Long, Seq[ExportedChainRow]], detached: Set[Long])(implicit session: DBSession): Map[Long, Seq[ExportedLineage]] = {
    val recorded = sql"""SELECT l.aggregate_id, l.copied_from, l.at_version FROM aggregate_lineage l
         | JOIN aggregates a ON a.id = l.aggregate_id AND a.base_id = a.id JOIN aggregates b ON b.id = l.copied_from AND b.base_id = b.id
         | WHERE a.space_id = $space AND b.space_id = $space""".stripMargin.map(rs => rs.long(1) -> ExportedLineage(rs.long(2), rs.int(3))).list()
    val now = detached.toSeq.flatMap(id => inside.getOrElse(id, Nil).map(row => id -> ExportedLineage(row.base, row.version)))
    (recorded ++ now).groupBy(_._1).map { case (id, rows) => id -> rows.map(_._2).distinct.sortBy(_.base) }
  }

  // D50, D85: the squash event of the copy's state at version 1, built and checked as a detach builds it. It is read from
  // the store as it is now, outside the export's snapshot: a change of the base meanwhile gives the same state (a squash
  // keeps it, D85) or one with values erased.
  private def detachEvent(id: Long, rootClass: String): Either[String, FirstEvent[_]] =
    contexts.find(_.aggregateType.typeName == rootClass).toRight("no context of " + rootClass).flatMap(c => detachEventOf(c.asInstanceOf[AggregateContext[Any]], id))

  private def detachEventOf[R](context: AggregateContext[R], id: Long): Either[String, FirstEvent[_]] = context.squash.toRight("no squash event").flatMap { policy =>
    new AggregateRestorer[R](eventStore, context.eventHandlers, () => context.initialAggregateRoot).detachEvents(Seq(AggregateId(id)), policy)
      .map(_.head._2).left.map(reason => "aggregate " + id + " cannot be detached: " + reason)
  }

  private def writeAggregates(space: Long, chains: Map[Long, Seq[ExportedChainRow]], detached: Set[Long], lineages: Map[Long, Seq[ExportedLineage]],
                              write: ExportedAggregate => Unit)(implicit session: DBSession): Int = {
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

  // In commit order, the events of the space's aggregates as stored, but a detached copy's first event; stops at a copy
  // that cannot be detached.
  private def writeEvents(space: Long, detached: Map[Long, String], write: ExportedEvent => Unit)(implicit session: DBSession): Either[String, Long] = {
    var count = 0L
    var failure = Option.empty[String]
    sql"""SELECT e.aggregate_id, e.version, t.name, e.event, e.user_id, e.event_time, e.metadata::text, n.from_version
         | FROM events e JOIN aggregates a ON a.id = e.aggregate_id AND a.base_id = a.id JOIN types_names t ON t.id = e.event_type_id
         | LEFT JOIN noop_events n ON n.id = e.id
         | WHERE a.space_id = $space ORDER BY e.id""".stripMargin.fetchSize(1000).foreach { rs =>
      if (failure.isEmpty) {
        val aggregate = rs.long(1)
        val version = rs.int(2)
        val first = if (version == 1) detached.get(aggregate).map(detachEvent(aggregate, _)) else None
        first match {
          case Some(Left(reason)) => failure = Some(reason)
          case Some(Right(event)) => // its undo mark goes with it
            write(ExportedEvent(aggregate, version, event.getClass.getName, jsons.serialize(event, event.getClass.getName), rs.long(5),
              utcTime(rs.underlying, 6).toString, rs.stringOpt(7), None))
            count += 1
          case None =>
            write(ExportedEvent(aggregate, version, rs.string(3), rs.string(4), rs.long(5), utcTime(rs.underlying, 6).toString, rs.stringOpt(7), rs.intOpt(8)))
            count += 1
        }
      }
    }
    failure.toLeft(count)
  }

  private def writeArchived(space: Long, write: ExportedArchived => Unit)(implicit session: DBSession): Long = {
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

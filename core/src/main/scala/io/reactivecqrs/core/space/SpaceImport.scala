package io.reactivecqrs.core.space

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api.{AggregateType, Event, EventMetadata}
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.space.SpaceFormat._
import io.reactivecqrs.core.uid.NewAggregatesIdsPool
import io.reactivecqrs.core.uid.UidGeneratorActor.GetNewAggregatesIdsPool
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import org.slf4j.LoggerFactory
import scalikejdbc._

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Try, Using}

/** D40: maps the references an import's codec cannot see, held as numbers or text (a `Long` id, an actor "user:5" in the
  * metadata). Typed ones (`AggregateId`, `UserId`, `SpaceId`) are mapped before it is called. */
trait ImportRewriter {
  /** The event and its metadata with their untyped references mapped by `ids` (an exported aggregate id to its id in
    * the target; other ids unchanged). */
  def rewrite(event: Event[_], metadata: Option[EventMetadata], ids: Long => Long): (Event[_], Option[EventMetadata])
}

/** What an import wrote. `remapped`: the exported ids the target had already, and their new ids; `unknownReferences`:
  * typed references to aggregates neither exported nor in the target (the first 1 000 in `someUnknown`); `refused`: why
  * it did not write, or where it stopped (then purge the target space and import again). */
case class ImportReport(space: SpaceId, aggregates: Int, events: Long, archived: Long, remapped: Map[AggregateId, AggregateId],
                        unknownReferences: Long, someUnknown: Seq[AggregateId], refused: Seq[String]) {
  /** Whether the import wrote everything. */
  def complete: Boolean = refused.isEmpty
}

/**
  * D40: writes an organization exported by `SpaceExport` into this installation's space `into`, which must be empty.
  * Pass 1 reads the file and writes nothing: the classes must all be here, the events within this store's limits, and
  * every exported id this installation has already gets a new one from `uidGenerator`. Pass 2 writes the aggregates, then
  * the events in the file's order under new event ids (the relative order kept), with their undo marks and keys, then the
  * archived events; each event is read by an `MPJsons` from `newMPJsons` (give one set up as your own) whose transforms
  * map `AggregateId` and `UserId` to the new ids and the exported space to `into`, then rewritten by its type's
  * `rewriters`. No outbox rows: rebuild the space's projections after it (`RebuildAggregates(OneSpace(into))`).
  * Design: docs/design/M6-organizations.md, "Export and import".
  */
class SpaceImport(eventStore: PostgresEventStoreState, uidGenerator: ActorRef, rewriters: Map[AggregateType, ImportRewriter] = Map.empty,
                  newMPJsons: () => MPJsons = () => new MPJsons, eventsPerTransaction: Int = 10000) {

  private val log = LoggerFactory.getLogger(classOf[SpaceImport])
  private val lines = new MPJsons
  private val maxReasons = 100

  /** Imports the organization exported to `file` into the space `into`. */
  def read(file: Path, into: SpaceId): ImportReport = {
    val plan = check(file, into)
    if (plan.refused.nonEmpty) {
      ImportReport(into, 0, 0, 0, Map.empty, 0, Nil, plan.refused.toSeq)
    } else {
      val remapped = freshIds(plan.collisions.toSeq)
      write(file, into, plan, remapped)
    }
  }

  // ************** Pass 1: check, write nothing

  private class Plan {
    var source: Option[Long] = None
    val exported = new mutable.LongMap[String]() // id -> root class
    val collisions = mutable.ArrayBuffer[Long]()
    val refused = mutable.ArrayBuffer[String]()
    var reasons = 0
    def refuse(reason: String): Unit = {
      reasons += 1
      if (refused.size < maxReasons) refused += reason
    }
  }

  private def check(file: Path, into: SpaceId): Plan = {
    val plan = new Plan
    if (DB.readOnly(implicit session => sql"SELECT EXISTS (SELECT 1 FROM aggregates WHERE space_id = ${into.asLong})".map(_.boolean(1)).single().get)) {
      plan.refuse("space " + into.asLong + " has aggregates already; an import writes into an empty space")
    }
    val unknownClasses = mutable.SortedSet[String]()
    def known(className: String): Boolean = Try(Class.forName(className)).isSuccess || { unknownClasses += className; false }
    val pending = mutable.ArrayBuffer[Long]()
    def findCollisions(): Unit = if (pending.nonEmpty) {
      plan.collisions ++= DB.readOnly(implicit session => sql"SELECT DISTINCT id FROM aggregates WHERE id IN ($pending)".map(_.long(1)).list())
      pending.clear()
    }
    eachLine(file) { line =>
      line.header.foreach { h =>
        if (h.format != Version) plan.refuse("the file has format " + h.format + "; this version reads format " + Version)
        plan.source = Some(h.space)
      }
      line.aggregate.foreach { a =>
        known(a.rootClass)
        if (a.version > eventStore.versionLimit) plan.refuse("aggregate " + a.id + " has " + a.version + " versions; this store allows " + eventStore.versionLimit)
        plan.exported(a.id) = a.rootClass
        pending += a.id
        if (pending.size >= 1000) findCollisions()
      }
      line.event.foreach { e =>
        known(e.eventClass)
        if (e.json.length > eventStore.sizeLimitOf(e.eventClass)) plan.refuse("event " + e.version + " of aggregate " + e.aggregate + " has " +
          e.json.length + " characters; this store allows " + eventStore.sizeLimitOf(e.eventClass) + " for " + e.eventClass)
        if (!plan.exported.contains(e.aggregate)) plan.refuse("event " + e.version + " of aggregate " + e.aggregate + ", which the file does not have")
      }
      line.archived.foreach(a => known(a.eventClass))
    }
    findCollisions()
    if (plan.source.isEmpty) plan.refuse("the file has no header")
    if (unknownClasses.nonEmpty) plan.refuse("classes missing here: " + unknownClasses.mkString(", "))
    if (plan.reasons > plan.refused.size) plan.refused += ("and " + (plan.reasons - plan.refused.size) + " more")
    plan
  }

  // A new id from this installation's pool for each exported id it has already.
  private def freshIds(collisions: Seq[Long]): mutable.LongMap[Long] = {
    implicit val timeout: Timeout = Timeout(30.seconds)
    val remapped = new mutable.LongMap[Long]()
    var pool = Iterator.empty[Long]
    collisions.foreach { id =>
      if (!pool.hasNext) {
        val NewAggregatesIdsPool(from, size) = Await.result((uidGenerator ? GetNewAggregatesIdsPool).mapTo[NewAggregatesIdsPool], timeout.duration)
        pool = Iterator.range(from, from + size)
      }
      remapped(id) = pool.next()
    }
    remapped
  }

  // ************** Pass 2: write

  private def write(file: Path, into: SpaceId, plan: Plan, remapped: mutable.LongMap[Long]): ImportReport = {
    val source = plan.source.get
    def mapped(id: Long): Long = remapped.getOrElse(id, id)
    val unknown = mutable.LinkedHashSet[Long]()
    var unknownCount = 0L
    val reader = newMPJsons()
    reader.postDeserializationTransform[AggregateId] { id =>
      if (!plan.exported.contains(id.asLong) && !unknown.contains(id.asLong)) {
        unknownCount += 1
        if (unknown.size < 1000) unknown += id.asLong
      }
      AggregateId(mapped(id.asLong))
    }
    reader.postDeserializationTransform[UserId](user => UserId(mapped(user.asLong)))
    reader.postDeserializationTransform[SpaceId](space => if (space.asLong == source) into else space)
    val jsons = eventStore.jsons
    val types = eventStore.types
    val rootTypes = new mutable.LongMap[Short]()
    var (aggregates, events, archived) = (0, 0L, 0L)

    def rewritten(aggregate: Long, eventClass: String, json: String, metadata: Option[String]): (String, String, Option[String]) = {
      val event = reader.deserialize[Event[Any]](json, eventClass)
      val meta = metadata.map(reader.deserialize[EventMetadata](_, classOf[EventMetadata].getName))
      val (e, m) = rewriters.get(AggregateType(plan.exported(aggregate))).fold[(Event[_], Option[EventMetadata])]((event, meta))(_.rewrite(event, meta, mapped))
      val out = jsons.serialize(e, e.getClass.getName)
      if (out.length > eventStore.sizeLimitOf(e.getClass.getName)) throw new IllegalStateException("event of aggregate " + aggregate +
        " has " + out.length + " characters after its ids were mapped; this store allows " + eventStore.sizeLimitOf(e.getClass.getName))
      (e.getClass.getName, out, m.map(jsons.serialize(_, classOf[EventMetadata].getName)))
    }

    val aggregateRows = mutable.ArrayBuffer[Seq[Any]]()
    val lineageRows = mutable.ArrayBuffer[Seq[Any]]()
    val eventRows = mutable.ArrayBuffer[Seq[Any]]()
    val archivedRows = mutable.ArrayBuffer[Seq[Any]]()
    def flushAggregates(): Unit = if (aggregateRows.nonEmpty) {
      DB.localTx { implicit session =>
        sql"""INSERT INTO aggregates (space_id, id, creation_time, type_id, base_order, base_id, base_version, squashed_version)
             | VALUES (?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin.batch(aggregateRows.toSeq: _*).apply[Seq]()
        if (lineageRows.nonEmpty) sql"""INSERT INTO aggregate_lineage (aggregate_id, copied_from, at_version, detached)
             | VALUES (?, ?, ?, current_timestamp) ON CONFLICT (aggregate_id, copied_from) DO NOTHING""".stripMargin.batch(lineageRows.toSeq: _*).apply[Seq]()
      }
      aggregateRows.clear()
      lineageRows.clear()
    }
    def flushEvents(): Unit = if (eventRows.nonEmpty) {
      DB.localTx { implicit session =>
        sql"""WITH e AS (INSERT INTO events (id, user_id, aggregate_id, event_time, version, event_type_id, event_type_version, event, metadata)
             |   VALUES (nextval('events_seq'), ?, ?, ?, ?, ?, 0, ?, ?::jsonb) RETURNING id),
             | k AS (INSERT INTO event_keys (id, type_id, space_id) SELECT id, ?, ? FROM e)
             | INSERT INTO noop_events (id, from_version) SELECT id, ?::int FROM e WHERE ?::int IS NOT NULL""".stripMargin.batch(eventRows.toSeq: _*).apply[Seq]()
      }
      eventRows.clear()
    }
    def flushArchived(): Unit = if (archivedRows.nonEmpty) {
      DB.localTx { implicit session =>
        sql"""INSERT INTO events_archive (id, aggregate_id, version, event_type_id, event_type_version, event, user_id, event_time, metadata, undone_from, archived)
             | VALUES (nextval('events_seq'), ?, ?, ?, 0, ?, ?, ?, ?::jsonb, ?, ?)""".stripMargin.batch(archivedRows.toSeq: _*).apply[Seq]()
      }
      archivedRows.clear()
    }

    val stopped = Try {
      eachLine(file) { line =>
        line.aggregate.foreach { a =>
          val id = mapped(a.id)
          val typeId = types.typeIdByClassName(a.rootClass)
          rootTypes(a.id) = typeId
          val created = utc(Instant.parse(a.created))
          aggregateRows += Seq(into.asLong, id, created, typeId, a.chain.size + 1, id, a.version, a.squashedVersion.map(Integer.valueOf).orNull)
          a.chain.foreach(row => aggregateRows += Seq(into.asLong, id, created, typeId, row.order, mapped(row.base), row.version, null))
          Option(a.lineage).getOrElse(Nil).foreach(l => lineageRows += Seq(id, mapped(l.base), l.version)) // D76
          aggregates += 1
          if (aggregateRows.size >= 1000) flushAggregates()
        }
        line.event.foreach { e =>
          flushAggregates()
          val (eventClass, json, metadata) = rewritten(e.aggregate, e.eventClass, e.json, e.metadata)
          val undone = e.undoneFrom.map(Integer.valueOf).orNull
          eventRows += Seq(mapped(e.user), mapped(e.aggregate), utc(Instant.parse(e.time)), e.version, types.typeIdByClassName(eventClass), json,
            metadata.orNull, rootTypes(e.aggregate), into.asLong, undone, undone)
          events += 1
          if (eventRows.size >= eventsPerTransaction) flushEvents()
        }
        line.archived.foreach { x =>
          flushAggregates()
          flushEvents()
          val (eventClass, json, metadata) = rewritten(x.aggregate, x.eventClass, x.json, x.metadata)
          archivedRows += Seq(mapped(x.aggregate), x.version, types.typeIdByClassName(eventClass), json, mapped(x.user), utc(Instant.parse(x.time)),
            metadata.orNull, x.undoneFrom.map(Integer.valueOf).orNull, utc(Instant.parse(x.archived)))
          archived += 1
          if (archivedRows.size >= eventsPerTransaction) flushArchived()
        }
      }
      flushAggregates()
      flushEvents()
      flushArchived()
    }.failed.toOption.map { e =>
      log.error("Import into space " + into.asLong + " stopped", e)
      "stopped after " + aggregates + " aggregates, " + events + " events and " + archived + " archived events were read (" + e.getMessage +
        "); purge space " + into.asLong + " and import again"
    }

    // References to aggregates this installation does not have either.
    val missing = if (unknown.isEmpty) Seq.empty[Long] else DB.readOnly { implicit session =>
      val here = unknown.grouped(1000).flatMap(ids => sql"SELECT DISTINCT id FROM aggregates WHERE id IN (${ids.toSeq})".map(_.long(1)).list()).toSet
      unknown.toSeq.filterNot(here.contains)
    }
    val report = ImportReport(into, aggregates, events, archived, remapped.map { case (from, to) => AggregateId(from) -> AggregateId(to) }.toMap,
      if (unknown.size < 1000) missing.size.toLong else unknownCount, missing.map(AggregateId(_)), stopped.toSeq)
    if (report.complete) log.info("Imported into space " + into.asLong + ": " + report) else log.warn("Import into space " + into.asLong + " incomplete: " + report)
    report
  }

  private def eachLine(file: Path)(f: Line => Unit): Unit = Using.resource(Files.newBufferedReader(file, StandardCharsets.UTF_8)) { reader =>
    var text = reader.readLine()
    while (text != null) {
      if (text.nonEmpty) f(lines.deserialize[Line](text, classOf[Line].getName))
      text = reader.readLine()
    }
  }
}

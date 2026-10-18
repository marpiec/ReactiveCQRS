package io.reactivecqrs.core.space

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api.{AggregateContext, AggregateType, Event, EventMetadata}
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.space.SpaceFormat._
import io.reactivecqrs.core.uid.{IdRange, NewAggregatesIdsPool}
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
import io.reactivecqrs.core.util.SqlLogging.withoutValues
import io.reactivecqrs.core.util.Errors

/** D40: maps the references an import's codec cannot see, held as numbers or text (a `Long` id, an actor "user:5" in the
  * metadata). Typed ones (`AggregateId`, `UserId`, `SpaceId`) are mapped before it is called. */
trait ImportRewriter {
  /** The event and its metadata with their untyped references mapped by `ids` (an exported aggregate id to its id in
    * the target; other ids unchanged). */
  def rewrite(event: Event[_], metadata: Option[EventMetadata], ids: Long => Long): (Event[_], Option[EventMetadata])
}

/** What an import wrote. `remapped`: the exported ids given new ones (the target had them, or its own generator would
  * hand them out later), and their new ids; `unknownReferences`: typed references to aggregates neither exported nor in
  * the target (the first 1 000 in `someUnknown`); `refused`: why it did not write, or where it stopped (then purge the
  * target space and import again). */
case class ImportReport(space: SpaceId, aggregates: Int, events: Long, archived: Long, remapped: Map[AggregateId, AggregateId],
                        unknownReferences: Long, someUnknown: Seq[AggregateId], refused: Seq[String]) {
  /** Whether the import wrote everything. */
  def complete: Boolean = refused.isEmpty
}

/**
  * D40: writes an organization exported by `SpaceExport` into this installation's space `into`, which must be empty.
  * `contexts`: the aggregate contexts of the space's types, as the export takes them; an aggregate of another type is
  * refused, as it could be neither commanded nor purged here.
  *
  * Pass 1 reads the file and writes nothing (F199): the trailer must be there, last, with the counts and the digest of
  * the lines before it; every class must be here, an event class an `Event` and a root class a context's (classes are
  * looked up, not initialized); each aggregate's events must be its versions from its squash point to its version,
  * each once, in order; its archived events at most its version; its chain's orders 1..n and its chain's and lineage's
  * bases in the file. Every exported id this installation has, or in the range its own generator hands out (the range
  * of `aggregatesSequence`'s value, D39: a pool may hold it, the sequence reach it, or a client choose it: F248), gets a
  * new one from `uidGenerator`; an import from another installation keeps its ids. Pass 2 maps
  * every event and checks it against this store's limits, still writing nothing. Pass 3 writes the aggregates, then
  * the events in the file's order under new event ids (the relative order kept), with their undo marks and keys, then
  * the archived events, in transactions of at most `eventsPerTransaction` events and `bytesPerTransaction` bytes (F226).
  * Each event is read by an `MPJsons` from `newMPJsons` (give one set up as your own) whose transforms map
  * `AggregateId` and `UserId` to the new ids and the exported space to `into`, then rewritten by its type's
  * `rewriters`. No outbox rows: rebuild the space's projections after it (`RebuildAggregates(OneSpace(into))`).
  * `acceptFormat1`: also import a file of format 1, which has no trailer, so a file cut short is not told from a whole
  * one. Design: docs/design/M6-organizations.md, "Export and import".
  */
class SpaceImport(eventStore: PostgresEventStoreState, uidGenerator: ActorRef, contexts: Seq[AggregateContext[_]],
                  rewriters: Map[AggregateType, ImportRewriter] = Map.empty, newMPJsons: () => MPJsons = () => new MPJsons,
                  eventsPerTransaction: Int = 10000, bytesPerTransaction: Long = 16L * 1024 * 1024,
                  aggregatesSequence: String = "aggregates_uids_seq", acceptFormat1: Boolean = false) {

  private val log = LoggerFactory.getLogger(classOf[SpaceImport])
  private val lines = new MPJsons
  private val maxReasons = 100
  private val rootClasses = contexts.map(_.aggregateType.typeName).toSet

  /** Imports the organization exported to `file` into the space `into`. */
  def read(file: Path, into: SpaceId): ImportReport = {
    val plan = check(file, into)
    if (plan.refused.nonEmpty) {
      ImportReport(into, 0, 0, 0, Map.empty, 0, Nil, plan.refused.toSeq)
    } else {
      val remapped = freshIds(plan.collisions.toSeq.distinct.sorted)
      val mapping = new Mapping(plan, into, remapped)
      val tooLarge = dryRun(file, mapping)
      if (tooLarge.nonEmpty) ImportReport(into, 0, 0, 0, Map.empty, 0, Nil, tooLarge)
      else write(file, into, plan, mapping)
    }
  }

  // ************** Pass 1: check, write nothing

  private class Plan {
    var source: Option[Long] = None
    var format = 0
    val exported = new mutable.LongMap[ExportedAggregate]()
    val nextVersion = new mutable.LongMap[Int]() // the version the aggregate's next event must have
    val bases = mutable.ArrayBuffer[(Long, Long, Int)]() // copy, base, version read (chain rows and lineage)
    val collisions = mutable.ArrayBuffer[Long]()
    val refused = mutable.ArrayBuffer[String]()
    var counts = (0, 0L, 0L)
    var trailer: Option[ExportTrailer] = None
    var afterTrailer = false
    var reasons = 0
    def refuse(reason: String): Unit = {
      reasons += 1
      if (refused.size < maxReasons) refused += reason
    }
  }

  private def check(file: Path, into: SpaceId): Plan = {
    val plan = new Plan
    if (contexts.isEmpty) plan.refuse("give the import the aggregate contexts of the space's types, as the export takes them (F199)")
    if (DB.readOnly(implicit session => sql"SELECT EXISTS (SELECT 1 FROM aggregates WHERE space_id = ${into.asLong})".map(_.boolean(1)).single().get)) {
      plan.refuse("space " + into.asLong + " has aggregates already; an import writes into an empty space")
    }
    val unknownClasses = mutable.SortedSet[String]()
    // F199: a class is looked up without initializing it, and must be what the line says it is.
    def known(className: String, kind: Class[_]): Boolean =
      Try(Class.forName(className, false, getClass.getClassLoader)).toOption match {
        case Some(c) if kind.isAssignableFrom(c) => true
        case Some(_) => plan.refuse(className + " is not an " + kind.getSimpleName); false
        case None => unknownClasses += className; false
      }
    val pending = mutable.ArrayBuffer[Long]()
    def findCollisions(): Unit = if (pending.nonEmpty) {
      plan.collisions ++= DB.readOnly(implicit session => sql"SELECT DISTINCT id FROM aggregates WHERE id IN ($pending)".map(_.long(1)).list())
      pending.clear()
    }
    val digest = new LinesDigest
    eachRawLine(file) { (text, line) =>
      if (plan.trailer.isDefined) plan.afterTrailer = true
      if (line.trailer.isEmpty) digest.add(text)
      line.header.foreach { h =>
        plan.format = h.format
        if (h.format != Version && !(h.format == VersionWithoutTrailer && acceptFormat1)) plan.refuse(
          if (h.format == VersionWithoutTrailer) "the file has format 1, which has no trailer to tell a whole file from one cut short; export it " +
            "again, or import it with acceptFormat1 = true" else "the file has format " + h.format + "; this version reads format " + Version)
        plan.source = Some(h.space)
      }
      line.aggregate.foreach { a =>
        if (known(a.rootClass, classOf[Object]) && !rootClasses.contains(a.rootClass)) plan.refuse("aggregate " + a.id + " is a " + a.rootClass +
          ", of which no context was given to the import")
        if (a.version > eventStore.versionLimit) plan.refuse("aggregate " + a.id + " has " + a.version + " versions; this store allows " + eventStore.versionLimit)
        if (a.chain.map(_.order).sorted != (1 to a.chain.size)) plan.refuse("aggregate " + a.id + "'s chain is not in orders 1 to " + a.chain.size)
        a.chain.foreach(row => plan.bases += ((a.id, row.base, row.version)))
        Option(a.lineage).getOrElse(Nil).foreach(l => plan.bases += ((a.id, l.base, l.version)))
        plan.exported(a.id) = a
        plan.nextVersion(a.id) = a.squashedVersion.getOrElse(1)
        plan.counts = plan.counts.copy(_1 = plan.counts._1 + 1)
        pending += a.id
        if (pending.size >= 1000) findCollisions()
      }
      line.event.foreach { e =>
        known(e.eventClass, classOf[Event[_]])
        plan.counts = plan.counts.copy(_2 = plan.counts._2 + 1)
        plan.nextVersion.get(e.aggregate) match {
          case None => plan.refuse("event " + e.version + " of aggregate " + e.aggregate + ", which the file does not have")
          case Some(next) if e.version != next => plan.refuse("aggregate " + e.aggregate + " has event " + e.version + " where " + next + " was due")
          case Some(next) => plan.nextVersion(e.aggregate) = next + 1
        }
      }
      line.archived.foreach { x =>
        known(x.eventClass, classOf[Event[_]])
        plan.counts = plan.counts.copy(_3 = plan.counts._3 + 1)
        plan.exported.get(x.aggregate) match {
          case None => plan.refuse("archived event " + x.version + " of aggregate " + x.aggregate + ", which the file does not have")
          case Some(a) if x.version > a.version => plan.refuse("archived event " + x.version + " of aggregate " + x.aggregate + " is past its version " + a.version)
          case _ => ()
        }
      }
      line.trailer.foreach(t => plan.trailer = Some(t))
    }
    findCollisions()
    if (plan.source.isEmpty) plan.refuse("the file has no header")
    if (plan.format == Version) plan.trailer match {
      case None => plan.refuse("the file has no trailer: it was cut short, or its export stopped")
      case Some(_) if plan.afterTrailer => plan.refuse("the file has lines after its trailer")
      case Some(t) =>
        if ((t.aggregates, t.events, t.archived) != plan.counts) plan.refuse("the trailer counts " + (t.aggregates, t.events, t.archived) +
          " aggregates, events and archived events; the file has " + plan.counts)
        if (t.sha256 != digest.hex) plan.refuse("the file's lines do not match its trailer's digest: it was changed or damaged")
    }
    plan.exported.foreach { case (id, a) =>
      val next = plan.nextVersion(id)
      if (next != a.version + 1) plan.refuse("aggregate " + id + " has version " + a.version + ", but its events end at " + (next - 1))
    }
    plan.bases.foreach { case (copy, base, version) =>
      plan.exported.get(base) match {
        case None => plan.refuse("aggregate " + copy + " reads aggregate " + base + ", which the file does not have")
        case Some(b) if version > b.version => plan.refuse("aggregate " + copy + " reads aggregate " + base + " up to version " + version +
          ", past its version " + b.version)
        case _ => ()
      }
    }
    if (unknownClasses.nonEmpty) plan.refuse("classes missing here: " + unknownClasses.mkString(", "))
    if (plan.refused.isEmpty) plan.collisions ++= inOwnRange(plan.exported.keys.toSeq)
    if (plan.reasons > plan.refused.size) plan.refused += ("and " + (plan.reasons - plan.refused.size) + " more")
    plan
  }

  // F248: the exported ids in the range this installation's generator hands out (D39, its aggregate ids' sequence's):
  // below the sequence a node's pool may hold one, above it the sequence reaches it later, and an id chosen by a client
  // in the range may meet it too. Each gets a new id; the sequence is never moved. An import from another installation
  // has none.
  private def inOwnRange(ids: Seq[Long]): Seq[Long] = DB.readOnly { implicit session =>
    sql"""SELECT last_value FROM pg_sequences WHERE schemaname = current_schema() AND sequencename = $aggregatesSequence"""
      .map(rs => rs.longOpt(1).getOrElse(0L)).single() match {
      case None => Nil
      case Some(last) =>
        val (from, until) = (last / IdRange.Span * IdRange.Span, (last / IdRange.Span + 1) * IdRange.Span)
        ids.filter(id => id >= from && id < until)
    }
  }

  // A new id from this installation's pool for each exported id it has already, or would hand out.
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

  // How the events are read and rewritten into this installation: the ids mapped, the references to aggregates neither
  // exported nor here counted.
  private class Mapping(plan: Plan, into: SpaceId, val remapped: mutable.LongMap[Long]) {
    private val source = plan.source.get
    val unknown = mutable.LinkedHashSet[Long]()
    var unknownCount = 0L
    def mapped(id: Long): Long = remapped.getOrElse(id, id)
    private val reader = newMPJsons()
    reader.postDeserializationTransform[AggregateId] { id =>
      if (!plan.exported.contains(id.asLong) && !unknown.contains(id.asLong)) {
        unknownCount += 1
        if (unknown.size < 1000) unknown += id.asLong
      }
      AggregateId(mapped(id.asLong))
    }
    reader.postDeserializationTransform[UserId](user => UserId(mapped(user.asLong)))
    reader.postDeserializationTransform[SpaceId](space => if (space.asLong == source) into else space)
    private val jsons = eventStore.jsons

    def rootOf(aggregate: Long): String = plan.exported(aggregate).rootClass

    /** The event's class, JSON and metadata JSON as written here, or why it cannot be. */
    def rewritten(aggregate: Long, eventClass: String, json: String, metadata: Option[String]): Either[String, (String, String, Option[String])] = {
      val event = reader.deserialize[Event[Any]](json, eventClass)
      val meta = metadata.map(reader.deserialize[EventMetadata](_, classOf[EventMetadata].getName))
      val (e, m) = rewriters.get(AggregateType(rootOf(aggregate))).fold[(Event[_], Option[EventMetadata])]((event, meta))(_.rewrite(event, meta, mapped))
      val out = jsons.serialize(e, e.getClass.getName)
      if (out.length > eventStore.sizeLimitOf(e.getClass.getName)) Left("an event of aggregate " + aggregate + " has " + out.length +
        " characters after its ids were mapped; this store allows " + eventStore.sizeLimitOf(e.getClass.getName))
      else Right((e.getClass.getName, out, m.map(jsons.serialize(_, classOf[EventMetadata].getName))))
    }
  }

  // ************** Pass 2: map every event, write nothing

  private def dryRun(file: Path, mapping: Mapping): Seq[String] = {
    val refused = mutable.ArrayBuffer[String]()
    def tried(aggregate: Long, eventClass: String, json: String, metadata: Option[String]): Unit = if (refused.size < maxReasons) {
      Try(mapping.rewritten(aggregate, eventClass, json, metadata)) match {
        case scala.util.Success(Left(reason)) => refused += reason
        case scala.util.Failure(e) => refused += "an event of aggregate " + aggregate + " cannot be read: " + Errors.describe(e)
        case _ => ()
      }
    }
    eachRawLine(file) { (_, line) =>
      line.event.foreach(e => tried(e.aggregate, e.eventClass, e.json, e.metadata))
      line.archived.foreach(x => tried(x.aggregate, x.eventClass, x.json, x.metadata))
    }
    mapping.unknown.clear() // counted again as they are written
    mapping.unknownCount = 0
    refused.toSeq
  }

  // ************** Pass 3: write

  private def write(file: Path, into: SpaceId, plan: Plan, mapping: Mapping): ImportReport = {
    import mapping.mapped
    val types = eventStore.types
    val rootTypes = new mutable.LongMap[Short]()
    var (aggregates, events, archived) = (0, 0L, 0L)

    val aggregateRows = mutable.ArrayBuffer[Seq[Any]]()
    val lineageRows = mutable.ArrayBuffer[Seq[Any]]()
    val eventRows = mutable.ArrayBuffer[Seq[Any]]()
    val archivedRows = mutable.ArrayBuffer[Seq[Any]]()
    var bytes = 0L // of the events held, eventRows' or archivedRows'
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
      bytes = 0L
    }
    def flushArchived(): Unit = if (archivedRows.nonEmpty) {
      DB.localTx { implicit session =>
        sql"""INSERT INTO events_archive (id, aggregate_id, version, event_type_id, event_type_version, event, user_id, event_time, metadata, undone_from, archived)
             | VALUES (nextval('events_seq'), ?, ?, ?, 0, ?, ?, ?, ?::jsonb, ?, ?)""".stripMargin.batch(archivedRows.toSeq: _*).apply[Seq]()
      }
      archivedRows.clear()
      bytes = 0L
    }
    def full(rows: Int): Boolean = rows >= eventsPerTransaction || bytes >= bytesPerTransaction // F226
    def rewritten(aggregate: Long, eventClass: String, json: String, metadata: Option[String]): (String, String, Option[String]) =
      mapping.rewritten(aggregate, eventClass, json, metadata).fold(reason => throw new IllegalStateException(reason), identity)

    val stopped = Try {
      eachRawLine(file) { (_, line) =>
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
          bytes += json.length + metadata.map(_.length).getOrElse(0)
          events += 1
          if (full(eventRows.size)) flushEvents()
        }
        line.archived.foreach { x =>
          flushAggregates()
          flushEvents()
          val (eventClass, json, metadata) = rewritten(x.aggregate, x.eventClass, x.json, x.metadata)
          archivedRows += Seq(mapped(x.aggregate), x.version, types.typeIdByClassName(eventClass), json, mapped(x.user), utc(Instant.parse(x.time)),
            metadata.orNull, x.undoneFrom.map(Integer.valueOf).orNull, utc(Instant.parse(x.archived)))
          bytes += json.length + metadata.map(_.length).getOrElse(0)
          archived += 1
          if (full(archivedRows.size)) flushArchived()
        }
      }
      flushAggregates()
      flushEvents()
      flushArchived()
    }.failed.toOption.map { e =>
      log.error("Import into space " + into.asLong + " stopped", Errors.redacted(e))
      "stopped after " + aggregates + " aggregates, " + events + " events and " + archived + " archived events were read (" + Errors.describe(e) +
        "); purge space " + into.asLong + " and import again"
    }

    // References to aggregates this installation does not have either.
    val missing = if (mapping.unknown.isEmpty) Seq.empty[Long] else DB.readOnly { implicit session =>
      val here = mapping.unknown.grouped(1000).flatMap(ids => sql"SELECT DISTINCT id FROM aggregates WHERE id IN (${ids.toSeq})".map(_.long(1)).list()).toSet
      mapping.unknown.toSeq.filterNot(here.contains)
    }
    val report = ImportReport(into, aggregates, events, archived, mapping.remapped.map { case (from, to) => AggregateId(from) -> AggregateId(to) }.toMap,
      if (mapping.unknown.size < 1000) missing.size.toLong else mapping.unknownCount, missing.map(AggregateId(_)), stopped.toSeq)
    if (report.complete) log.info("Imported into space " + into.asLong + ": " + report) else log.warn("Import into space " + into.asLong + " incomplete: " + report)
    report
  }

  private def eachRawLine(file: Path)(f: (String, Line) => Unit): Unit = Using.resource(Files.newBufferedReader(file, StandardCharsets.UTF_8)) { reader =>
    var text = reader.readLine()
    while (text != null) {
      if (text.nonEmpty) f(text, lines.deserialize[Line](text, classOf[Line].getName))
      text = reader.readLine()
    }
  }
}

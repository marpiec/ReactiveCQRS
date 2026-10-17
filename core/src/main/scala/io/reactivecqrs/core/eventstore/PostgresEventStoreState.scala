package io.reactivecqrs.core.eventstore

import java.sql.Timestamp
import java.time.Instant
import java.util.{Calendar, TimeZone}

import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.api._
import io.reactivecqrs.core.types.TypesNamesState
import io.reactivecqrs.core.util.AfterCommit
import scalikejdbc._

import scala.concurrent.duration._
import scala.util.Try

/** What the space backfill did (F49): aggregates given a space, and those neither their first event nor the function
  * gave one. */
case class SpaceBackfill(filled: Int, left: Seq[AggregateId])

/** What the event keys backfill did (F124): keys written, and events still without one (an aggregate row missing). */
case class EventKeysBackfill(written: Long, missing: Long)

/** @param outboxDeleteDelay how long delivered outbox rows wait, at most, to be deleted together with others (F26) */
class PostgresEventStoreState(mpjsons: MPJsons, typesNamesState: TypesNamesState, fetchSize: Int = 1000,
                              eventSizeLimit: Int = 100000, aggregateVersionLimit: Int = 10000,
                              eventSizeLimitPerClassName: Map[String, Int] = Map.empty,
                              outboxDeleteDelay: FiniteDuration = 20.millis) extends EventStoreState {

  def initSchema(): PostgresEventStoreState = {
    (new PostgresEventStoreSchemaInitializer).initSchema()
    this
  }

  // D29: events are stored under their own class name with type version 0. Another version was written through the
  // event versioning that 1.0 removed; its class cannot be known any more, so reading it fails instead of guessing.
  private def eventClass(typeId: Short, typeVersion: Short, aggregateId: Long, version: Int): String = {
    val className = typesNamesState.classNameById(typeId)
    if (typeVersion != 0) throw new UnsupportedEventVersionException(className, typeVersion, aggregateId, version)
    className
  }

  // F65: event times are written and read as UTC whatever the JVM's zone; the column stays TIMESTAMP (D28). Neula runs
  // in UTC, so the values it stored read back unchanged.
  private val Utc = TimeZone.getTimeZone("UTC")
  private def utc(instant: Instant): ParameterBinder = {
    val timestamp = Timestamp.from(instant)
    ParameterBinder(timestamp, (statement, index) => statement.setTimestamp(index, timestamp, Calendar.getInstance(Utc)))
  }
  private def utcTime(rs: java.sql.ResultSet, column: Int): Instant = rs.getTimestamp(column, Calendar.getInstance(Utc)).toInstant

  // D75, F50: the aggregate's type key comes from its root's class, as everywhere else.
  private def aggregateTypeName(event: Event[_]): String = AggregateType.ofClass(event.aggregateRootClass).typeName

  // D32: the metadata envelope of a command's events; a null column means "stored without one".
  private def metadataJson(metadata: Option[EventMetadata]): Option[String] = metadata.map(m => mpjsons.serialize(m, classOf[EventMetadata].getName))
  private def metadataOf(json: String): Option[EventMetadata] = Option(json).map(mpjsons.deserialize[EventMetadata](_, classOf[EventMetadata].getName))

  // Plain and first events of a command go in one `add_events` call (F31); undo and duplication events have procedures
  // of their own, one call per event. Every failure is thrown, so the caller's transaction rolls back whole. Each event's
  // size is its stored JSON's length (F126).
  override def persist[AGGREGATE_ROOT](aggregateId: AggregateId, newEvents: NewEvents[AGGREGATE_ROOT])(implicit session: DBSession): Try[Seq[StoredEvent[AGGREGATE_ROOT]]] = Try {
    val events = newEvents.events
    val metadata = metadataJson(newEvents.metadata)
    if (events.exists(e => e.isInstanceOf[UndoEvent[_]] || e.isInstanceOf[DuplicationEvent[_]])) {
      var version = newEvents.expectedVersion
      events.map { event =>
        val (stored, size) = persistOne(aggregateId, newEvents, event, version, metadata)
        version = stored
        StoredEvent(event, version, size)
      }
    } else {
      val (_, sizes) = persistPlain(aggregateId, newEvents, events, newEvents.expectedVersion, metadata)
      events.zip(sizes).zipWithIndex.map { case ((event, size), i) => StoredEvent(event, newEvents.expectedVersion.incrementBy(i + 1), size) }
    }
  }

  // `version`: the aggregate's version before the event, which takes the next one (F125).
  private def serializedChecked(aggregateId: AggregateId, event: Event[_], version: AggregateVersion): String = {
    val serialized = mpjsons.serialize(event, event.getClass.getName)
    val limit = eventSizeLimitPerClassName.getOrElse(event.getClass.getName, eventSizeLimit)
    if (serialized.length > limit) {
      throw new EventTooLargeException(aggregateId, aggregateTypeName(event), version, event.getClass.getName, serialized.length, limit)
    } else if (version.asInt >= aggregateVersionLimit) {
      throw new TooManyEventsException(aggregateId, aggregateTypeName(event), aggregateVersionLimit)
    }
    serialized
  }

  // Returns the version the procedure gives and the events' sizes.
  private def persistPlain[AGGREGATE_ROOT](aggregateId: AggregateId, newEvents: NewEvents[AGGREGATE_ROOT], events: Seq[Event[AGGREGATE_ROOT]],
                                           expectedVersion: AggregateVersion, metadata: Option[String])(implicit session: DBSession): (AggregateVersion, Seq[Int]) = {
    val serialized = events.zipWithIndex.map { case (event, i) => serializedChecked(aggregateId, event, expectedVersion.incrementBy(i)) }
    val spaceIds = events.map {
      case first: FirstEvent[_] => Long.box(first.spaceId.asLong)
      case _ => Long.box(-1L)
    }
    val eventTypeIds = events.map(e => Short.box(typesNamesState.typeIdByClassName(e.getClass.getName)))
    def array(elementType: String, elements: Seq[AnyRef]): ParameterBinder = {
      val sqlArray = session.connection.createArrayOf(elementType, elements.toArray)
      ParameterBinder(sqlArray, (statement, index) => statement.setArray(index, sqlArray))
    }
    val aggregateTypeId = typesNamesState.typeIdByClassName(aggregateTypeName(events.head))
    val version = sql"""SELECT add_events(${newEvents.userId.asLong}, ${array("bigint", spaceIds)}, ${aggregateId.asLong}, ${expectedVersion.asInt},
         |  $aggregateTypeId, ${array("smallint", eventTypeIds)}, ${utc(newEvents.timestamp)}, ${array("text", serialized)}, $metadata::jsonb)"""
      .stripMargin.map(_.int(1)).single().get
    (AggregateVersion(version), serialized.map(PostgresEventStoreState.utf8Length))
  }

  // Returns the event's version and size.
  private def persistOne[AGGREGATE_ROOT](aggregateId: AggregateId, newEvents: NewEvents[AGGREGATE_ROOT], event: Event[AGGREGATE_ROOT],
                                         expectedVersion: AggregateVersion, metadata: Option[String])(implicit session: DBSession): (AggregateVersion, Int) = event match {
    case undo: UndoEvent[_] =>
      val serialized = serializedChecked(aggregateId, event, expectedVersion)
      (AggregateVersion(sql"""SELECT add_undo_event(${newEvents.userId.asLong}, ${aggregateId.asLong}, ${expectedVersion.asInt},
                             |  ${typesNamesState.typeIdByClassName(aggregateTypeName(event))}, ${typesNamesState.typeIdByClassName(event.getClass.getName)},
                             |  ${utc(newEvents.timestamp)}, $serialized, ${undo.eventsCount}, $metadata::jsonb)"""
        .stripMargin.map(_.int(1)).single().get), PostgresEventStoreState.utf8Length(serialized))
    case duplication: DuplicationEvent[_] =>
      val serialized = serializedChecked(aggregateId, event, expectedVersion)
      (AggregateVersion(sql"""SELECT add_duplication_event(${newEvents.userId.asLong}, ${duplication.spaceId.asLong}, ${aggregateId.asLong}, ${expectedVersion.asInt},
                             |  ${typesNamesState.typeIdByClassName(aggregateTypeName(event))}, ${typesNamesState.typeIdByClassName(event.getClass.getName)},
                             |  ${utc(newEvents.timestamp)}, $serialized, ${duplication.baseAggregateId.asLong}, ${duplication.baseAggregateVersion.asInt}, $metadata::jsonb)"""
        .stripMargin.map(_.int(1)).single().get), PostgresEventStoreState.utf8Length(serialized))
    case _ =>
      val (version, sizes) = persistPlain(aggregateId, newEvents, Seq(event), expectedVersion, metadata)
      (version, sizes.head)
  }


  override def overwriteEvents[AGGREGATE_ROOT](aggregateId: AggregateId, events: Iterable[EventWithVersion[AGGREGATE_ROOT]])(implicit session: DBSession): Unit = {
    // The snapshots of the aggregate and of its duplicates may hold the old values (D30).
    historyChangedFor(sqls"""SELECT id FROM aggregates WHERE base_id = ${aggregateId.asLong}""")
    // D17: a rewritten event keeps its class, so the history means what it meant; only the stored values change.
    events.foreach { e =>
      val className = e.event.getClass.getName
      val updated = sql"""UPDATE events SET event = ${serializedChecked(aggregateId, e.event, AggregateVersion(e.version.asInt - 1))}
                         | WHERE aggregate_id = ${aggregateId.asLong} AND version = ${e.version.asInt} AND event_type_id = ${typesNamesState.typeIdByClassName(className)}"""
        .stripMargin.update()
      if (updated != 1) throw new RewriteChangesClassException(aggregateId, e.version, className)
    }
  }


  // Restores stream the rows (autocommit off and a fetch size, F22), so a long history is not loaded whole first. A read
  // at an instant reads up to the aggregate's version at that instant (F39): its own events up to it, undone as they
  // were then, and its base's events as for any version.
  override def readAndProcessEvents[AGGREGATE_ROOT](aggregateId: AggregateId, versionOrInstant: Option[Either[AggregateVersion, Instant]])
                                                   (eventHandler: (UserId, Instant, Event[AGGREGATE_ROOT], AggregateId, Int, Boolean) => Unit): Unit =
    processEvents[AGGREGATE_ROOT](aggregateId, versionOrInstant, withMetadata = false) { (userId, time, event, eventAggregateId, version, noop, _) =>
      eventHandler(userId, time, event, eventAggregateId, version, noop)
    }

  override def readAndProcessEventsWithMetadata[AGGREGATE_ROOT](aggregateId: AggregateId, versionOrInstant: Option[Either[AggregateVersion, Instant]])
                                                               (eventHandler: (EventInfo[AGGREGATE_ROOT], AggregateId, Boolean) => Unit): Unit =
    processEvents[AGGREGATE_ROOT](aggregateId, versionOrInstant, withMetadata = true) { (userId, time, event, eventAggregateId, version, noop, metadata) =>
      eventHandler(EventInfo(AggregateVersion(version), event, userId, time, metadata), eventAggregateId, noop)
    }

  private def processEvents[AGGREGATE_ROOT](aggregateId: AggregateId, versionOrInstant: Option[Either[AggregateVersion, Instant]], withMetadata: Boolean)
                                           (eventHandler: (UserId, Instant, Event[AGGREGATE_ROOT], AggregateId, Int, Boolean, Option[EventMetadata]) => Unit): Unit = {
    DB.localTx { implicit session =>
      val version: Option[AggregateVersion] = versionOrInstant.map {
        case Left(version) => version
        case Right(instant) =>
          AggregateVersion(sql"""SELECT coalesce(max(version), 0) FROM events WHERE aggregate_id = ${aggregateId.asLong} AND event_time <= ${utc(instant)}"""
            .map(_.int(1)).single().getOrElse(0))
      }
      val query = version match {
        case Some(v) =>
          sql"""SELECT user_id, event_time, event_type_id, event_type_version, event, events.version, events.aggregate_id, events.metadata::text,
                noop_events.id IS NOT NULL AND (events.aggregate_id != ? AND noop_events.from_version <= aggregates.base_version OR events.aggregate_id = ? AND noop_events.from_version <= ?) as noop
             FROM events
             JOIN aggregates ON events.aggregate_id = aggregates.base_id AND (events.aggregate_id != ? AND events.version <= aggregates.base_version OR events.aggregate_id = ? AND events.version <= ?)
             LEFT JOIN noop_events ON events.id = noop_events.id AND noop_events.from_version <= aggregates.base_version
             WHERE aggregates.id = ? ORDER BY aggregates.base_order, version""".stripMargin.bind(aggregateId.asLong, aggregateId.asLong, v.asInt, aggregateId.asLong, aggregateId.asLong, v.asInt, aggregateId.asLong)
        case None =>
          sql"""SELECT user_id, event_time, event_type_id, event_type_version, event, events.version, events.aggregate_id, events.metadata::text,
                noop_events.id IS NOT NULL AND noop_events.from_version <= aggregates.base_version as noop
             FROM events
             JOIN aggregates ON events.aggregate_id = aggregates.base_id AND events.version <= aggregates.base_version
             LEFT JOIN noop_events ON events.id = noop_events.id AND noop_events.from_version <= aggregates.base_version
             WHERE aggregates.id = ? ORDER BY aggregates.base_order, version""".stripMargin.bind(aggregateId.asLong)
      }
      query.fetchSize(fetchSize).foreach { rs =>
        val eventType = eventClass(rs.short(3), rs.short(4), rs.long(7), rs.int(6))
        val event = mpjsons.deserialize[Event[AGGREGATE_ROOT]](rs.string(5), eventType)
        eventHandler(UserId(rs.long(1)), utcTime(rs.underlying, 2), event, AggregateId(rs.long(7)), rs.int(6), rs.boolean(9),
          if (withMetadata) metadataOf(rs.string(8)) else None)
      }
      // D51: the history below the squash point is gone, and a read of it fails instead of giving a wrong state. Checked
      // after the read, so a squash committed while it ran is seen too; an instant before the squash event's time fails
      // as well, as the store cannot tell what the aggregate was then (nor whether it existed).
      for {
        v <- version
        squashed <- squashedPoint(aggregateId.asLong)
        if v.asInt < squashed && (v > AggregateVersion.ZERO || versionOrInstant.exists(_.isRight))
      } throw new AggregateSquashedException(aggregateId, AggregateVersion(squashed), v, versionOrInstant.flatMap(_.toOption))
    }
  }

  override def readAndProcessOwnEvents[AGGREGATE_ROOT](aggregateId: AggregateId, after: AggregateVersion, upTo: Option[AggregateVersion])
                                                      (eventHandler: (UserId, Instant, Event[AGGREGATE_ROOT], Int, Boolean) => Unit): Unit = {
    val last = upTo.map(_.asInt).getOrElse(Int.MaxValue)
    DB.localTx { implicit session =>
      sql"""SELECT user_id, event_time, event_type_id, event_type_version, event, events.version,
           |   noop_events.id IS NOT NULL AND noop_events.from_version <= $last as noop
           | FROM events
           | LEFT JOIN noop_events ON events.id = noop_events.id
           | WHERE events.aggregate_id = ${aggregateId.asLong} AND events.version > ${after.asInt} AND events.version <= $last
           | ORDER BY events.version""".stripMargin.fetchSize(fetchSize).foreach { rs =>
        val eventType = eventClass(rs.short(3), rs.short(4), aggregateId.asLong, rs.int(6))
        eventHandler(UserId(rs.long(1)), utcTime(rs.underlying, 2), mpjsons.deserialize[Event[AGGREGATE_ROOT]](rs.string(5), eventType), rs.int(6), rs.boolean(7))
      }
    }
  }

  override def readSnapshot[AGGREGATE_ROOT](aggregateId: AggregateId, stateClassName: String): Option[Try[Snapshot[AGGREGATE_ROOT]]] = DB.readOnly { implicit session =>
    sql"""SELECT version, context_version, schema_version, state FROM snapshots WHERE aggregate_id = ${aggregateId.asLong}"""
      .map(rs => Try(Snapshot[AGGREGATE_ROOT](AggregateVersion(rs.int(1)), rs.int(2), rs.int(3), mpjsons.deserialize[AGGREGATE_ROOT](rs.string(4), stateClassName))))
      .single()
  }

  override def writeSnapshot[AGGREGATE_ROOT](aggregateId: AggregateId, snapshot: Snapshot[AGGREGATE_ROOT], stateClassName: String, generation: Int): Unit = {
    val state = mpjsons.serialize(snapshot.state, stateClassName)
    DB.autoCommit { implicit session =>
      // Only while the aggregate is stored at that version, with the history the state was made from: an owner that
      // stops after a permanent delete, or after its history changed under it, writes nothing (F119).
      sql"""INSERT INTO snapshots (aggregate_id, version, context_version, schema_version, state, created)
           | SELECT ${aggregateId.asLong}, ${snapshot.version.asInt}, ${snapshot.contextVersion}, ${snapshot.schemaVersion}, $state, current_timestamp
           | WHERE EXISTS (SELECT 1 FROM aggregates WHERE id = ${aggregateId.asLong} AND base_id = ${aggregateId.asLong} AND base_version >= ${snapshot.version.asInt}
           |   AND history_generation = $generation)
           | ON CONFLICT (aggregate_id) DO UPDATE SET version = EXCLUDED.version, context_version = EXCLUDED.context_version,
           |   schema_version = EXCLUDED.schema_version, state = EXCLUDED.state, created = EXCLUDED.created""".stripMargin.update()
    }
  }

  override def historyGeneration(aggregateId: AggregateId): Int = DB.readOnly { implicit session =>
    sql"""SELECT history_generation FROM aggregates WHERE id = ${aggregateId.asLong} AND base_id = ${aggregateId.asLong}""".map(_.int(1)).single().getOrElse(0)
  }

  override def storedVersion(aggregateId: AggregateId): Option[(AggregateVersion, Int)] = DB.readOnly { implicit session =>
    sql"""SELECT base_version, history_generation FROM aggregates WHERE id = ${aggregateId.asLong} AND base_id = ${aggregateId.asLong}"""
      .map(rs => (AggregateVersion(rs.int(1)), rs.int(2))).single()
  }

  // D30, F119: the snapshots of these aggregates may hold states their history no longer gives: they go, and the
  // aggregates' history generation moves on, so an owner still holding such a state cannot write it as a snapshot again.
  private def historyChangedFor(aggregateIds: SQLSyntax)(implicit session: DBSession): Unit = {
    sql"""DELETE FROM snapshots WHERE aggregate_id IN ($aggregateIds)""".update()
    sql"""UPDATE aggregates SET history_generation = history_generation + 1 WHERE id = base_id AND id IN ($aggregateIds)""".update()
  }

  /** Builds the index of F38, `events(aggregate_id, version)`, without blocking writes to `events` (D69): run it once in
    * the background after the upgrade; it does nothing when the index exists. An index an interrupted build left
    * invalid is dropped and built again. */
  def buildIndexesConcurrently(): Unit = DB.autoCommit { implicit session =>
    val invalid = sql"""SELECT NOT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
                       | JOIN pg_namespace n ON n.oid = c.relnamespace
                       | WHERE c.relname = 'events_aggregate_version_idx' AND n.nspname = current_schema()""".stripMargin
      .map(_.boolean(1)).single().getOrElse(false)
    if (invalid) sql"""DROP INDEX CONCURRENTLY IF EXISTS events_aggregate_version_idx""".execute()
    sql"""CREATE INDEX CONCURRENTLY IF NOT EXISTS events_aggregate_version_idx ON events (aggregate_id, version)""".execute()
    // M6: an organization's aggregates in id order, for its purge, export and rebuild (D38). The index of the first M6
    // steps, on the space alone, goes once this one is built.
    val spaceIndexInvalid = sql"""SELECT NOT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
                                 | JOIN pg_namespace n ON n.oid = c.relnamespace
                                 | WHERE c.relname = 'aggregates_space_id_idx' AND n.nspname = current_schema()""".stripMargin
      .map(_.boolean(1)).single().getOrElse(false)
    if (spaceIndexInvalid) sql"""DROP INDEX CONCURRENTLY IF EXISTS aggregates_space_id_idx""".execute()
    sql"""CREATE INDEX CONCURRENTLY IF NOT EXISTS aggregates_space_id_idx ON aggregates (space_id, id)""".execute()
    sql"""DROP INDEX CONCURRENTLY IF EXISTS aggregates_space_idx""".execute()
  }

  /**
    * F124: gives the events written before `event_keys` their keys (their aggregate's type and space), in id order, a
    * batch of `batchSize` ids per transaction, from the first event without one, and once every event has one, records
    * the step, from which builds read by type and space. Online and idempotent (D69): run it in the background once every
    * node runs this version (a node of the previous one, started again, would write events without keys); stopped, it is
    * run again. Returns the keys it wrote and the events still without one (those of a missing aggregate row).
    */
  def backfillEventKeys(batchSize: Int = 10000): EventKeysBackfill = {
    val (first, last) = DB.readOnly { implicit session =>
      (sql"""SELECT min(e.id) FROM events e WHERE NOT EXISTS (SELECT 1 FROM event_keys k WHERE k.id = e.id)""".map(_.longOpt(1)).single().flatten,
        sql"""SELECT coalesce(max(id), 0) FROM events""".map(_.long(1)).single().get)
    }
    var written = 0L
    var after = first.map(_ - 1).getOrElse(last)
    while (after < last) {
      val upTo = math.min(after + batchSize, last)
      written += DB.localTx { implicit session => // each event finds its own aggregate row by key (F172)
        sql"""INSERT INTO event_keys (id, type_id, space_id)
             | SELECT e.id, a.type_id, a.space_id FROM events e CROSS JOIN LATERAL $ownRow a
             | WHERE e.id > $after AND e.id <= $upTo ON CONFLICT (id) DO NOTHING""".stripMargin.update()
      }
      after = upTo
    }
    val missing = DB.readOnly { implicit session =>
      sql"""SELECT count(*) FROM events e WHERE NOT EXISTS (SELECT 1 FROM event_keys k WHERE k.id = e.id)""".map(_.long(1)).single().get
    }
    if (missing == 0) DB.autoCommit { implicit session =>
      sql"""INSERT INTO schema_steps (name, completed) VALUES ('event_keys', current_timestamp) ON CONFLICT (name) DO NOTHING""".update()
    }
    EventKeysBackfill(written, missing)
  }

  // F124: the step is looked for again at most once a minute until it is found; it is never undone.
  @volatile private var eventKeysComplete = false
  @volatile private var eventKeysCheckedAt = Long.MinValue

  private def eventKeysReady()(implicit session: DBSession): Boolean = eventKeysComplete || {
    val now = System.nanoTime()
    if (eventKeysCheckedAt == Long.MinValue || now - eventKeysCheckedAt >= 60L * 1000000000L) {
      eventKeysCheckedAt = now
      eventKeysComplete = Try(exists(sql"""SELECT EXISTS (SELECT 1 FROM schema_steps WHERE name = 'event_keys')""")).getOrElse(false)
    }
    eventKeysComplete
  }

  // The aggregate's own row of its chain gives the type: a duplicate's events are read under it, not its base.
  // F126: the rows are chosen by their stored sizes first (`octet_length`, which PostgreSQL takes from a TOAST pointer
  // without reading the value), so a page never reads more than `maxBytes` of events, but at least one; each row also
  // says how many rows the page had before the cut.
  override def readCommittedPage(types: Seq[AggregateType], after: Long, limit: Int, scope: AggregateScope, maxBytes: Long): CommittedPage =
    if (types.isEmpty) CommittedPage(Seq.empty, cut = false) else DB.readOnly { implicit session =>
      val rows = sql"""WITH page AS (${pageOf(types, after, limit, scope)}),
           |   kept AS (SELECT id, size FROM (SELECT id, size, sum(size) OVER (ORDER BY id) AS up_to, row_number() OVER (ORDER BY id) AS n FROM page) p
           |     WHERE up_to <= $maxBytes OR n = 1)
           |SELECT e.id, a.type_id, e.aggregate_id, e.version, e.event_type_id, e.event_type_version, e.event, e.user_id, e.event_time,
           |   e.metadata::text, a.squashed_version, k.size, (SELECT count(*) FROM page)
           | FROM kept k JOIN events e ON e.id = k.id CROSS JOIN LATERAL $ownRow a
           | ORDER BY e.id""".stripMargin.map { rs =>
        val (aggregateId, version) = (AggregateId(rs.long(3)), AggregateVersion(rs.int(4)))
        val event = Try(EventInfo[Any](version, mpjsons.deserialize[Event[Any]](rs.string(7), eventClass(rs.short(5), rs.short(6), aggregateId.asLong, version.asInt)),
          UserId(rs.long(8)), utcTime(rs.underlying, 9), metadataOf(rs.string(10)))).toEither.left.map(e => e.getClass.getSimpleName + ": " + e.getMessage)
        (CommittedEvent(rs.long(1), AggregateType(typesNamesState.classNameById(rs.short(2))), aggregateId, version, rs.intOpt(11).map(AggregateVersion(_)),
          event, rs.int(12)), rs.long(13))
      }.list()
      CommittedPage(rows.map(_._1), cut = rows.headOption.exists { case (_, pageRows) => pageRows > rows.size })
    }

  // F172: each event's own aggregate row, looked up by its key for that one event. A join on `a.id = e.aggregate_id AND
  // a.base_id = e.aggregate_id` lets the planner estimate `id = base_id` at 0.5 % of the rows (it is nearly all), and once
  // the tables are analyzed it scanned every event of every aggregate for each page: 0.8 s a page over 2 000 000 events.
  // The LIMIT keeps the subquery from being flattened into such a join.
  private val ownRow = sqls"""(SELECT id, type_id, space_id, squashed_version FROM aggregates
                             |  WHERE id = e.aggregate_id AND base_id = e.aggregate_id LIMIT 1)""".stripMargin

  // The ids and sizes of the next `limit` events of `types` in `scope` after `after`, in commit order. F124: named
  // aggregates are read through `events (aggregate_id, id)`; once every event has a key, the others through
  // `event_keys`, one subquery per type (each at most a page, by `(type_id, id)` or `(space_id, type_id, id)`), merged
  // by id. Before that, `events` is walked up to the page's last row.
  private def pageOf(types: Seq[AggregateType], after: Long, limit: Int, scope: AggregateScope)(implicit session: DBSession): SQLSyntax = scope match {
    case AggregateScope.Only(ids) if ids.nonEmpty =>
      sqls"""SELECT e.id, octet_length(e.event) AS size FROM events e CROSS JOIN LATERAL $ownRow a
            | WHERE e.aggregate_id IN (${ids.toSeq.map(_.asLong)}) AND e.id > $after AND a.type_id IN (${typeIds(types)})
            | ORDER BY e.id LIMIT $limit""".stripMargin
    case AggregateScope.All | AggregateScope.InSpace(_) if eventKeysReady() =>
      val inSpace = scope match {
        case AggregateScope.InSpace(spaceId) => sqls"AND space_id = ${spaceId.asLong}"
        case _ => sqls""
      }
      val perType = typeIds(types).map(typeId => sqls"(SELECT id FROM event_keys WHERE type_id = $typeId $inSpace AND id > $after ORDER BY id LIMIT $limit)")
      sqls"""SELECT k.id, octet_length(e.event) AS size FROM (SELECT id FROM (${sqls.join(perType, sqls"UNION ALL")}) u ORDER BY id LIMIT $limit) k
            | JOIN events e ON e.id = k.id""".stripMargin
    case _ =>
      sqls"""SELECT e.id, octet_length(e.event) AS size FROM events e CROSS JOIN LATERAL $ownRow a
            | WHERE e.id > $after AND a.type_id IN (${typeIds(types)}) ${scoped(scope)}
            | ORDER BY e.id LIMIT $limit""".stripMargin
  }

  override def aggregatesOf(types: Seq[AggregateType], after: AggregateId, limit: Int, scope: AggregateScope = AggregateScope.All): Seq[StoredAggregate] =
    if (types.isEmpty) Seq.empty else DB.readOnly { implicit session =>
      sql"""SELECT a.type_id, a.id, a.base_version, a.squashed_version FROM aggregates a
           | WHERE a.base_id = a.id AND a.id > ${after.asLong} AND a.type_id IN (${typeIds(types)}) ${scoped(scope)}
           | ORDER BY a.id LIMIT $limit""".stripMargin
        .map(rs => StoredAggregate(AggregateType(typesNamesState.classNameById(rs.short(1))), AggregateId(rs.long(2)), AggregateVersion(rs.int(3)),
          rs.intOpt(4).map(AggregateVersion(_)))).list()
    }

  private def typeIds(types: Seq[AggregateType]): Seq[Short] = types.map(t => typesNamesState.typeIdByClassName(t.typeName)).distinct

  // On the aggregate's own row, `a`.
  private def scoped(scope: AggregateScope): SQLSyntax = scope match {
    case AggregateScope.All => sqls""
    case AggregateScope.Only(ids) if ids.isEmpty => sqls"AND false"
    case AggregateScope.Only(ids) => sqls"AND a.id IN (${ids.toSeq.map(_.asLong)})"
    case AggregateScope.InSpace(spaceId) => sqls"AND a.space_id = ${spaceId.asLong}"
  }

  /**
    * F49, D38: gives the aggregates stored without a space (`space_id = -1`: rows from before the column) the space of
    * their first event (the squash event of a squashed one), or, when that event does not know it (a deprecated event
    * with `SpaceId.unknown`), the one `spaceOf` gives. Online and in batches of `batchSize` aggregates, one transaction
    * each (D69). Returns how many it gave a space, and those it could not.
    */
  def backfillSpaces(spaceOf: (AggregateType, AggregateId, Event[_]) => Option[SpaceId] = (_, _, _) => None, batchSize: Int = 1000): SpaceBackfill = {
    var after = Long.MinValue
    var filled = 0
    var left = Vector[AggregateId]()
    var more = true
    while (more) {
      val batch = DB.readOnly { implicit session =>
        sql"""SELECT id, type_id, coalesce(squashed_version, 1) FROM aggregates WHERE base_id = id AND space_id = -1 AND id > $after ORDER BY id LIMIT $batchSize"""
          .map(rs => (AggregateId(rs.long(1)), AggregateType(typesNamesState.classNameById(rs.short(2))), AggregateVersion(rs.int(3)))).list()
      }
      val spaces = batch.map { case (id, aggregateType, first) =>
        id -> Try(readEventsRange[Any](id, first, first).headOption.map(_.event)).toOption.flatten.flatMap {
          case event: FirstEvent[_] if event.spaceId.asLong >= 0 => Some(event.spaceId)
          case event => spaceOf(aggregateType, id, event)
        }
      }
      DB.localTx { implicit session =>
        spaces.foreach {
          case (id, Some(space)) =>
            // F124: the keys of its events carry its space too.
            if (sql"""UPDATE aggregates SET space_id = ${space.asLong} WHERE id = ${id.asLong} AND space_id = -1""".update() > 0) {
              sql"""UPDATE event_keys SET space_id = ${space.asLong} WHERE id IN (SELECT id FROM events WHERE aggregate_id = ${id.asLong})""".update()
            }
          case _ => ()
        }
      }
      filled += spaces.count(_._2.isDefined)
      left ++= spaces.collect { case (id, None) => id }
      after = batch.lastOption.map(_._1.asLong).getOrElse(after)
      more = batch.size == batchSize
    }
    SpaceBackfill(filled, left)
  }

  // F26: one statement deletes the delivered rows of many acknowledgements.
  // The rows travel as two arrays, so one statement deletes thousands of them.
  private val outboxDeletes = new OutboxDeletes(outboxDeleteDelay, maxRows = 5000)(rows => DB.autoCommit { implicit session =>
    val aggregateIds = session.connection.createArrayOf("bigint", rows.map { case (aggregateId, _) => java.lang.Long.valueOf(aggregateId): AnyRef }.toArray)
    val versions = session.connection.createArrayOf("integer", rows.map { case (_, version) => java.lang.Integer.valueOf(version): AnyRef }.toArray)
    sql"""DELETE FROM events_to_publish USING unnest($aggregateIds::bigint[], $versions::integer[]) AS delivered(aggregate_id, version)
         | WHERE events_to_publish.aggregate_id = delivered.aggregate_id AND events_to_publish.version = delivered.version""".stripMargin.update()
  })

  override def deletePublished(aggregateId: AggregateId, versions: Seq[AggregateVersion]): Unit =
    outboxDeletes.add(aggregateId.asLong, versions.map(_.asInt))

  // A system without projections has no subscriptions tables.
  private def tableExists(table: String)(implicit session: DBSession): Boolean =
    SQL(s"SELECT to_regclass('$table') IS NOT NULL").map(_.boolean(1)).single().getOrElse(false)

  private def exists(query: SQL[Nothing, NoExtractor])(implicit session: DBSession): Boolean = query.map(_.boolean(1)).single().getOrElse(false)

  private def failedSubscription(aggregateId: Long)(implicit session: DBSession): Boolean = tableExists("subscription_failures") &&
    exists(sql"""SELECT EXISTS (SELECT 1 FROM subscription_failures WHERE aggregate_id = $aggregateId)""")

  // Locks every chain row of the aggregate and of each duplicate that reads it, at any depth, in key order, as
  // `add_duplication_event` locks the chain it copies: a duplication of the aggregate or of one of its duplicates waits
  // for this transaction, so the duplicates read after this are all there are (D50, F76, F96). All the rows a detach
  // later changes are locked here, in that one order, so this cannot deadlock with a duplication of a duplicate.
  private def lockChain(aggregateId: Long)(implicit session: DBSession): Unit =
    sql"""SELECT id FROM aggregates WHERE id IN (SELECT id FROM aggregates WHERE base_id = $aggregateId)
         | ORDER BY id, base_id FOR UPDATE""".stripMargin.map(_.long(1)).list()

  private def duplicates(baseId: Long, below: Option[Int])(implicit session: DBSession): Seq[AggregateId] = (below match {
    case Some(version) => sql"""SELECT id FROM aggregates WHERE base_id = $baseId AND id <> $baseId AND base_version < $version ORDER BY id"""
    case None => sql"""SELECT id FROM aggregates WHERE base_id = $baseId AND id <> $baseId ORDER BY id"""
  }).map(rs => AggregateId(rs.long(1))).list()

  private def detachingAll(found: Seq[AggregateId], detach: Seq[(AggregateId, FirstEvent[_])]): Either[String, Unit] =
    Either.cond(found.toSet == detach.map(_._1).toSet, (), "the duplicates of the aggregate changed meanwhile")

  override def deleteAggregatePermanently(aggregateId: AggregateId, detach: Seq[(AggregateId, FirstEvent[_])], archive: Set[String]): Either[String, Unit] = {
    outboxDeletes.flushNow() // acknowledged rows go first, so the outbox checks see only what still waits
    localTx { implicit session => deletingPermanently(aggregateId, detach, archive) }
  }

  private def deletingPermanently(aggregateId: AggregateId, detach: Seq[(AggregateId, FirstEvent[_])], archive: Set[String])
                                 (implicit session: DBSession): Either[String, Unit] = {
    val id = aggregateId.asLong
    lockChain(id)
    for {
      _ <- Either.cond(!failedSubscription(id), (), "a subscription of it is failed")
      _ <- detachBlocker(id, None, "the delete").toLeft(())
      _ <- detachingAll(duplicates(id, None), detach)
    } yield {
      detach.foreach(detachDuplicate(_, archive)) // F76: they read this aggregate's events, which go now
      sql"""DELETE FROM aggregates WHERE id = $id""".update()
      sql"""DELETE FROM events_archive WHERE aggregate_id = $id""".update()
      // noop_events must be deleted BEFORE events: it is resolved via a subquery over events,
      // so deleting events first would make the subquery match nothing and leak noop_events rows.
      sql"""DELETE FROM noop_events WHERE id IN (SELECT id FROM events WHERE aggregate_id = $id)""".update()
      sql"""DELETE FROM events WHERE aggregate_id = $id""".update()
      sql"""DELETE FROM events_to_publish WHERE aggregate_id = $id""".update()
      sql"""DELETE FROM snapshots WHERE aggregate_id = $id""".update()
      // D76: what was copied from it, or what it copied, has nothing left to follow; the audit of its rewrites stays.
      sql"""DELETE FROM aggregate_lineage WHERE aggregate_id = $id OR copied_from = $id""".update()
      if (tableExists("subscriptions")) {
        sql"""DELETE FROM subscriptions WHERE aggregate_id = $id""".update()
      }
      if (columnExists("commands_responses", "aggregate_id")) { // F167: as a purge does
        sql"""DELETE FROM commands_responses WHERE aggregate_id = $id""".update()
      }
    }
  }

  override def detachDuplicates(baseId: AggregateId, detach: Seq[(AggregateId, FirstEvent[_])], archive: Set[String]): Either[String, Unit] =
    if (detach.isEmpty) Right(()) // none to check: `detachBlocker` reads no names as all of them
    else {
      outboxDeletes.flushNow() // acknowledged rows go first, so the outbox check sees only what still waits
      localTx { implicit session =>
        val id = baseId.asLong
        lockChain(id)
        val detached = detach.map(_._1)
        for {
          _ <- Either.cond(detached.forall(duplicates(id, None).contains), (), "the duplicates of the aggregate changed meanwhile")
          _ <- detachBlocker(id, None, "the purge", detached.map(_.asLong)).toLeft(())
        } yield detach.foreach(detachDuplicate(_, archive))
      }
    }

  // ************** Moving an organization (D40): what SpaceExport and SpaceImport take from the store.

  private[core] def jsons: MPJsons = mpjsons
  private[core] def types: TypesNamesState = typesNamesState
  /** The most characters of JSON an event of the class may have in this store. */
  private[core] def sizeLimitOf(className: String): Int = eventSizeLimitPerClassName.getOrElse(className, eventSizeLimit)
  /** The most events an aggregate may have in this store. */
  private[core] def versionLimit: Int = aggregateVersionLimit

  // ************** Purge of an organization (R17)

  /** R17: up to `limit` of the space's aggregates with ids below `before`, newest first, from `aggregates (space_id, id)`. */
  def spaceAggregates(space: SpaceId, before: AggregateId, limit: Int): Seq[StoredAggregate] = DB.readOnly { implicit session =>
    sql"""SELECT type_id, id, base_version, squashed_version FROM aggregates
         | WHERE space_id = ${space.asLong} AND base_id = id AND id < ${before.asLong} ORDER BY space_id DESC, id DESC LIMIT $limit""".stripMargin
      .map(rs => StoredAggregate(AggregateType(typesNamesState.classNameById(rs.short(1))), AggregateId(rs.long(2)), AggregateVersion(rs.int(3)),
        rs.intOpt(4).map(AggregateVersion(_)))).list()
  }

  /** R17: the types of the space's aggregates. */
  def spaceTypes(space: SpaceId): Set[AggregateType] = DB.readOnly { implicit session =>
    sql"""SELECT DISTINCT type_id FROM aggregates WHERE space_id = ${space.asLong} AND base_id = id"""
      .map(rs => AggregateType(typesNamesState.classNameById(rs.short(1)))).list().toSet
  }

  /** R17: how many aggregates the space has. */
  def spaceSize(space: SpaceId): Int = DB.readOnly { implicit session =>
    sql"""SELECT count(*) FROM aggregates WHERE space_id = ${space.asLong} AND base_id = id""".map(_.int(1)).single().getOrElse(0)
  }

  /** R17: the aggregates of other spaces that read one of the space's aggregates, each with the aggregate it reads (its
    * base) and their type. A copy's chain rows carry the copy's own space. */
  def duplicatesInOtherSpaces(space: SpaceId): Seq[(AggregateType, AggregateId, AggregateId)] = DB.readOnly { implicit session =>
    sql"""SELECT b.type_id, b.id, d.id FROM aggregates b JOIN aggregates d ON d.base_id = b.id AND d.id <> b.id
         | WHERE b.space_id = ${space.asLong} AND b.base_id = b.id AND d.space_id <> ${space.asLong} ORDER BY b.id, d.id""".stripMargin
      .map(rs => (AggregateType(typesNamesState.classNameById(rs.short(1))), AggregateId(rs.long(2)), AggregateId(rs.long(3)))).list()
  }

  /** R17, in the purge's transaction: locks the chains of the given aggregates (as `lockChain` does, in one order) and
    * returns those that may go: still of the space, and read by no aggregate of another space (one duplicated meanwhile
    * stays for the next pass, which detaches it first). */
  def lockForPurge(space: SpaceId, ids: Seq[AggregateId])(implicit session: DBSession): Seq[AggregateId] = if (ids.isEmpty) Nil else {
    val longs = ids.map(_.asLong)
    sql"""SELECT id FROM aggregates WHERE id IN (SELECT id FROM aggregates WHERE base_id IN ($longs))
         | ORDER BY id, base_id FOR UPDATE""".stripMargin.map(_.long(1)).list()
    val read = sql"""SELECT DISTINCT base_id FROM aggregates WHERE base_id IN ($longs) AND id <> base_id AND space_id <> ${space.asLong}"""
      .map(_.long(1)).list().toSet
    sql"""SELECT id FROM aggregates WHERE id IN ($longs) AND base_id = id AND space_id = ${space.asLong} ORDER BY id DESC"""
      .map(_.long(1)).list().filterNot(read.contains).map(AggregateId(_))
  }

  /** R17, in the purge's transaction, after `lockForPurge`: deletes everything the library stores for these aggregates,
    * their keys going with their events (the cascade), and answers the rows deleted per table. */
  def purgeAggregates(ids: Seq[AggregateId])(implicit session: DBSession): Map[String, Long] = if (ids.isEmpty) Map.empty else {
    val longs = ids.map(_.asLong)
    def deleted(table: String, where: SQLSyntax): Option[(String, Long)] = Some(table -> sql"DELETE FROM ${SQLSyntax.createUnsafely(table)} WHERE $where".update().toLong)
    Seq(
      // noop rows first: they are found through the events they mark (F76's lesson).
      deleted("noop_events", sqls"id IN (SELECT id FROM events WHERE aggregate_id IN ($longs))"),
      deleted("events", sqls"aggregate_id IN ($longs)"),
      deleted("events_archive", sqls"aggregate_id IN ($longs)"),
      deleted("events_to_publish", sqls"aggregate_id IN ($longs)"),
      deleted("snapshots", sqls"aggregate_id IN ($longs)"),
      if (tableExists("subscriptions")) deleted("subscriptions", sqls"aggregate_id IN ($longs)") else None,
      if (tableExists("subscription_failures")) deleted("subscription_failures", sqls"aggregate_id IN ($longs)") else None,
      if (columnExists("commands_responses", "aggregate_id")) deleted("commands_responses", sqls"aggregate_id IN ($longs)") else None,
      deleted("aggregate_lineage", sqls"aggregate_id IN ($longs) OR copied_from IN ($longs)"), // D76
      deleted("history_rewrites", sqls"aggregate_id IN ($longs)"),
      deleted("aggregates", sqls"id IN ($longs)")
    ).flatten.toMap
  }

  private def columnExists(table: String, column: String)(implicit session: DBSession): Boolean =
    sql"""SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $table AND column_name = $column)"""
      .map(_.boolean(1)).single().getOrElse(false)

  // ************** Squash (D47–D52)

  override def squashedVersion(aggregateId: AggregateId): Option[AggregateVersion] =
    DB.readOnly(implicit session => squashedPoint(aggregateId.asLong).map(AggregateVersion(_)))

  private def squashedPoint(id: Long)(implicit session: DBSession): Option[Int] =
    sql"""SELECT squashed_version FROM aggregates WHERE id = $id AND base_id = $id""".map(_.intOpt(1)).single().flatten

  override def squashBlocker(aggregateId: AggregateId, upTo: AggregateVersion): Option[String] = {
    outboxDeletes.flushNow()
    DB.readOnly(implicit session => blocker(aggregateId.asLong, upTo.asInt))
  }

  // D51. The duplicates the squash detaches count too: their first event changes.
  private def blocker(id: Long, n: Int)(implicit session: DBSession): Option[String] = {
    val squashed = squashedPoint(id)
    if (squashed.exists(_ >= n)) {
      Some("already squashed up to version " + squashed.get)
    } else if (exists(sql"""SELECT EXISTS (SELECT 1 FROM events_to_publish WHERE aggregate_id = $id AND version <= $n)""")) {
      Some("events at or below version " + n + " still wait in the outbox")
    } else if (failedSubscription(id)) {
      Some("a subscription of the aggregate is failed")
    } else if (exists(sql"""SELECT EXISTS (SELECT 1 FROM noop_events JOIN events ON events.id = noop_events.id
                          |  WHERE events.aggregate_id = $id AND events.version <= $n AND noop_events.from_version > $n)""".stripMargin)) {
      Some("an undo after version " + n + " undid events at or below it")
    } else detachBlocker(id, Some(n), "the squash")
  }

  // D51, F143: a detach replaces a duplicate's first event, so it waits while that event was undone, still waits in the
  // outbox, or a subscription of the duplicate is failed: subscribers must have the original first event before it
  // changes. For the duplicates that read the aggregate below `below`, or all of them.
  private def detachBlocker(id: Long, below: Option[Int], by: String, only: Seq[Long] = Nil)(implicit session: DBSession): Option[String] = {
    def detachedOne(joined: SQLSyntax): Option[Long] = duplicateWith(id, below, joined, only)
    def failedDuplicate = if (tableExists("subscription_failures")) detachedOne(sqls"JOIN subscription_failures f ON f.aggregate_id = a.id") else None
    detachedOne(undoneFirstEvent).map(d => "the first event of duplicate " + d + ", which " + by + " detaches, was undone")
      .orElse(detachedOne(sqls"JOIN events_to_publish p ON p.aggregate_id = a.id AND p.version = 1")
        .map(d => "the first event of duplicate " + d + ", which " + by + " detaches, still waits in the outbox"))
      .orElse(failedDuplicate.map(d => "a subscription of duplicate " + d + ", which " + by + " detaches, is failed"))
  }

  // R11: copies the events `which` selects, of the classes in `archive`, into the archive with their undo marks.
  private def archived(which: SQLSyntax, archive: Set[String])(implicit session: DBSession): Unit = if (archive.nonEmpty) {
    val typeIds = archive.toSeq.map(typesNamesState.typeIdByClassName)
    sql"""INSERT INTO events_archive (id, aggregate_id, version, event_type_id, event_type_version, event, user_id, event_time, metadata, undone_from, archived)
         | SELECT e.id, e.aggregate_id, e.version, e.event_type_id, e.event_type_version, e.event, e.user_id, e.event_time, e.metadata, n.from_version, current_timestamp
         | FROM events e LEFT JOIN noop_events n ON n.id = e.id
         | WHERE $which AND e.event_type_id IN ($typeIds) ON CONFLICT (id) DO NOTHING""".stripMargin.update()
  }

  override def readArchive[AGGREGATE_ROOT](aggregateId: AggregateId): Seq[ArchivedEvent[AGGREGATE_ROOT]] = DB.readOnly { implicit session =>
    sql"""SELECT version, event_type_id, event_type_version, event, user_id, event_time, metadata::text, undone_from, archived
         | FROM events_archive WHERE aggregate_id = ${aggregateId.asLong} ORDER BY version""".stripMargin.map { rs =>
      val version = AggregateVersion(rs.int(1))
      val event = mpjsons.deserialize[Event[AGGREGATE_ROOT]](rs.string(4), eventClass(rs.short(2), rs.short(3), aggregateId.asLong, version.asInt))
      ArchivedEvent(EventInfo(version, event, UserId(rs.long(5)), utcTime(rs.underlying, 6), metadataOf(rs.string(7))),
        rs.intOpt(8).map(AggregateVersion(_)), utcTime(rs.underlying, 9))
    }.list()
  }

  // ************** Complete rewrite (D76)

  // Every row is read and checked before any is written, so a refusal writes nothing. The rows are streamed, and only
  // the changed ones are held, so an aggregate of 50 000 events that the change leaves alone costs one read.
  override def rewriteEverywhere[AGGREGATE_ROOT](aggregateId: AggregateId, userId: UserId, change: HistoryChange,
                                                alter: (Event[AGGREGATE_ROOT], Option[EventMetadata]) => (Event[AGGREGATE_ROOT], Option[EventMetadata]))
                                               (implicit session: DBSession): Either[String, RewriteCount] = {
    val id = aggregateId.asLong
    lockChain(id)
    case class Changed(archived: Boolean, rowId: Long, version: Int, event: Event[AGGREGATE_ROOT], metadata: Option[EventMetadata])
    val changed = scala.collection.mutable.ListBuffer[Changed]()
    var (refusal, classes, fields) = (Option.empty[String], Set.empty[String], Set.empty[String])
    def check(archived: Boolean)(rs: WrappedResultSet): Unit = if (refusal.isEmpty) {
      val version = rs.int(2)
      val event = mpjsons.deserialize[Event[AGGREGATE_ROOT]](rs.string(5), eventClass(rs.short(3), rs.short(4), id, version))
      val metadata = metadataOf(rs.string(6))
      val (newEvent, newMetadata) = alter(event, metadata)
      if (newEvent != event || newMetadata != metadata) {
        val where = (if (archived) "archived event " else "event ") + version + " of aggregate " + id
        if (newEvent.getClass != event.getClass) refusal = Some("the change makes " + where + " a " + newEvent.getClass.getName + " (D17)")
        else if (newEvent != event && (event.isInstanceOf[UndoEvent[_]] || event.isInstanceOf[DuplicationEvent[_]]))
          refusal = Some("the change alters " + where + ", an undo or a duplication, whose meaning the store keeps elsewhere")
        else {
          changed += Changed(archived, rs.long(1), version, newEvent, newMetadata)
          classes += event.getClass.getName
          fields ++= RewriteCount.fieldsChanged(event, newEvent) ++ RewriteCount.fieldsChanged(metadata.orNull, newMetadata.orNull, "metadata.")
        }
      }
    }
    sql"""SELECT id, version, event_type_id, event_type_version, event, metadata::text FROM events WHERE aggregate_id = $id ORDER BY version"""
      .fetchSize(fetchSize).foreach(check(archived = false))
    sql"""SELECT id, version, event_type_id, event_type_version, event, metadata::text FROM events_archive WHERE aggregate_id = $id ORDER BY version"""
      .fetchSize(fetchSize).foreach(check(archived = true))
    refusal.toLeft(()).map { _ =>
      Seq(false -> sqls"events", true -> sqls"events_archive").foreach { case (archived, table) =>
        val rows = changed.filter(_.archived == archived).map(c => Seq[Any](serializedChecked(aggregateId, c.event, AggregateVersion(c.version - 1)),
          metadataJson(c.metadata).orNull, c.rowId))
        if (rows.nonEmpty) sql"""UPDATE $table SET event = ?, metadata = ?::jsonb WHERE id = ?""".batch(rows.toSeq: _*).apply()
      }
      // The snapshots of the aggregate and of its duplicates were made from the values of before.
      if (changed.nonEmpty) historyChangedFor(sqls"""SELECT id FROM aggregates WHERE base_id = $id""")
      val count = RewriteCount(changed.count(!_.archived), changed.count(_.archived), classes, fields)
      sql"""INSERT INTO history_rewrites (aggregate_id, user_id, at, change_class, classes, fields, events, archived)
           | VALUES ($id, ${userId.asLong}, current_timestamp, ${change.getClass.getName}, ${classes.toSeq.sorted.mkString(",")},
           |  ${fields.toSeq.sorted.mkString(",")}, ${count.events}, ${count.archived})""".stripMargin.update()
      count
    }
  }

  override def copiesOf(aggregateId: AggregateId): Seq[AggregateId] = DB.readOnly { implicit session =>
    val id = aggregateId.asLong
    sql"""SELECT id FROM aggregates WHERE base_id = $id AND id <> $id UNION SELECT aggregate_id FROM aggregate_lineage WHERE copied_from = $id
         | ORDER BY 1""".stripMargin.map(rs => AggregateId(rs.long(1))).list()
  }

  override def basesOf(aggregateId: AggregateId): Seq[AggregateId] = DB.readOnly { implicit session =>
    val id = aggregateId.asLong
    sql"""SELECT base_id FROM aggregates WHERE id = $id AND base_id <> $id ORDER BY base_order DESC""".map(rs => AggregateId(rs.long(1))).list()
  }

  // A duplicate of the aggregate, reading it below `below` if given, that `joined` finds.
  private def duplicateWith(id: Long, below: Option[Int], joined: SQLSyntax, only: Seq[Long])(implicit session: DBSession): Option[Long] = {
    val reading = below.fold(sqls"")(n => sqls"AND a.base_version < $n")
    val some = if (only.isEmpty) sqls"" else sqls"AND a.id IN ($only)"
    sql"""SELECT a.id FROM aggregates a $joined WHERE a.base_id = $id AND a.id <> $id $reading $some LIMIT 1""".map(_.long(1)).single()
  }

  // A detach replaces the first event; one that was undone cannot be, as its undo would then take the base's state too.
  private val undoneFirstEvent = sqls"JOIN events e ON e.aggregate_id = a.id AND e.version = 1 JOIN noop_events n ON n.id = e.id"

  override def duplicatesReading(baseId: AggregateId, below: Option[AggregateVersion]): Seq[AggregateId] =
    DB.readOnly(implicit session => duplicates(baseId.asLong, below.map(_.asInt)))

  // D50: the duplicate's first event becomes one that holds its state at version 1, so it no longer reads its bases.
  private def detachDuplicate(duplicate: (AggregateId, FirstEvent[_]), archive: Set[String])(implicit session: DBSession): Unit = {
    val (duplicateId, firstEvent) = duplicate
    val id = duplicateId.asLong
    val className = firstEvent.getClass.getName
    archived(sqls"e.aggregate_id = $id AND e.version = 1", archive)
    sql"""DELETE FROM noop_events WHERE id IN (SELECT id FROM events WHERE aggregate_id = $id AND version = 1)""".update()
    val updated = sql"""UPDATE events SET event = ${serializedChecked(duplicateId, firstEvent, AggregateVersion.ZERO)}, event_type_id = ${typesNamesState.typeIdByClassName(className)}
                       | WHERE aggregate_id = $id AND version = 1""".stripMargin.update()
    if (updated != 1) throw new IllegalStateException("Duplicate " + id + " has no event at version 1 to detach")
    if (firstEvent.isInstanceOf[SquashMayChangeState]) historyChangedFor(sqls"""$id""") // D85
    // D76: the copy now holds what it read of its bases; a complete rewrite of one of them must find it.
    sql"""INSERT INTO aggregate_lineage (aggregate_id, copied_from, at_version, detached)
         | SELECT id, base_id, base_version, current_timestamp FROM aggregates WHERE id = $id AND base_id <> $id
         | ON CONFLICT (aggregate_id, copied_from) DO NOTHING""".stripMargin.update()
    sql"""DELETE FROM aggregates WHERE id = $id AND base_id <> $id""".update()
    sql"""UPDATE aggregates SET base_order = 1 WHERE id = $id AND base_id = $id""".update()
  }

  override def squash[AGGREGATE_ROOT](aggregateId: AggregateId, upTo: AggregateVersion, squashEvent: FirstEvent[AGGREGATE_ROOT], userId: UserId,
                                      metadata: Option[EventMetadata], detach: Seq[(AggregateId, FirstEvent[_])], archive: Set[String]): Either[String, Unit] = localTx { implicit session =>
    val id = aggregateId.asLong
    val n = upTo.asInt
    lockChain(id)
    for {
      _ <- blocker(id, n).toLeft(())
      _ <- detachingAll(duplicates(id, Some(n)), detach)
    } yield {
      detach.foreach(detachDuplicate(_, archive))
      val className = squashEvent.getClass.getName
      archived(sqls"e.aggregate_id = $id AND e.version <= $n", archive) // R11: before the rows and their undo marks go
      // noop rows first: they are found through the events they mark (F76's lesson).
      sql"""DELETE FROM noop_events WHERE id IN (SELECT id FROM events WHERE aggregate_id = $id AND version <= $n)""".update()
      sql"""DELETE FROM events WHERE aggregate_id = $id AND version < $n""".update()
      val updated = sql"""UPDATE events SET event = ${serializedChecked(aggregateId, squashEvent, AggregateVersion(n - 1))}, event_type_id = ${typesNamesState.typeIdByClassName(className)},
                         |   user_id = ${userId.asLong}, metadata = ${metadataJson(metadata)}::jsonb
                         | WHERE aggregate_id = $id AND version = $n""".stripMargin.update()
      if (updated != 1) throw new IllegalStateException("Aggregate " + id + " has no event at version " + n + " to squash into")
      sql"""DELETE FROM events_to_publish WHERE aggregate_id = $id AND version <= $n""".update()
      // A squash event that may change the state makes every snapshot of the aggregate wrong, and those of the duplicates
      // that still read it (D85); one that gives the state back, only those it replaces.
      if (squashEvent.isInstanceOf[SquashMayChangeState]) historyChangedFor(sqls"""SELECT id FROM aggregates WHERE base_id = $id""")
      else sql"""DELETE FROM snapshots WHERE aggregate_id = $id AND version <= $n""".update()
      sql"""DELETE FROM aggregates WHERE id = $id AND base_id <> $id""".update()
      sql"""UPDATE aggregates SET base_order = 1, squashed_version = $n WHERE id = $id AND base_id = $id""".update()
    }
  }

  // R11: with `keepNewerThan`, the point is the aggregate's newest version below the first event not older than that: one
  // probe of `events (aggregate_id, id)` per aggregate, not a walk back through every newer event of it.
  override def squashCandidates(aggregateType: AggregateType, policy: AutomaticSquash, after: AggregateId, limit: Int,
                                now: Instant): Seq[(AggregateId, AggregateVersion)] =
    DB.readOnly { implicit session =>
      val keep = policy.keepLast
      val upTo = policy.keepNewerThan match {
        case None => sqls"a.base_version - $keep"
        case Some(age) =>
          val boundary = firstEventNotOlderThan(now.minusMillis(age.toMillis))
          sqls"""LEAST(a.base_version - $keep, coalesce((SELECT e.version FROM events e WHERE e.aggregate_id = a.id AND e.id < $boundary
                |  ORDER BY e.id DESC LIMIT 1), 0))""".stripMargin
      }
      sql"""SELECT id, up_to FROM (SELECT a.id, $upTo AS up_to, a.squashed_version FROM aggregates a
           |   WHERE a.type_id = ${typesNamesState.typeIdByClassName(aggregateType.typeName)} AND a.base_id = a.id AND a.id > ${after.asLong}
           |     AND a.base_version - $keep - coalesce(a.squashed_version, 0) >= ${policy.afterEvents}) c
           | WHERE up_to - coalesce(squashed_version, 0) >= ${policy.afterEvents}
           | ORDER BY id LIMIT $limit""".stripMargin.map(rs => (AggregateId(rs.long(1)), AggregateVersion(rs.int(2)))).list()
    }

  // R11: the id of the first event not older than `at`, by a binary search of the primary key (about 40 reads). Ids follow
  // the commits and times the commands' starts, so it is exact up to the longest command's duration and the nodes' clock
  // skew; events written later with old times (an import) count as new, so they are kept longer, never squashed early.
  private def firstEventNotOlderThan(at: Instant)(implicit session: DBSession): Long = {
    val time = utc(at) // stored as UTC wall time (F65)
    var low = 0L // the events below are older
    var high = sql"SELECT coalesce(max(id), 0) + 1 FROM events".map(_.long(1)).single().get // the first event from here is not
    while (low < high) {
      val mid = low + (high - low) / 2
      sql"SELECT id, event_time < $time FROM events WHERE id >= $mid ORDER BY id LIMIT 1".map(rs => (rs.long(1), rs.boolean(2))).single() match {
        case Some((id, true)) => low = math.min(id + 1, high)
        case _ => high = mid
      }
    }
    low
  }

  // One query for all aggregate types (F37); the aggregate's own row of its chain gives the type. The limit bounds what
  // the poller holds, not the grouping, which still reads every row (F122: cheap while the outbox is near empty).
  override def readWaitingInOutbox(limit: Int = Int.MaxValue): Seq[WaitingInOutbox] = DB.readOnly { implicit session =>
    sql"""SELECT aggregates.type_id, events_to_publish.aggregate_id, MIN(events_to_publish.version), MIN(events_to_publish.event_time) AS since
         | FROM events_to_publish
         | JOIN aggregates ON aggregates.id = events_to_publish.aggregate_id AND aggregates.base_id = aggregates.id
         | GROUP BY aggregates.type_id, events_to_publish.aggregate_id
         | ORDER BY since, events_to_publish.aggregate_id LIMIT $limit""".stripMargin
      .map(rs => WaitingInOutbox(AggregateType(typesNamesState.classNameById(rs.short(1))), AggregateId(rs.long(2)),
        AggregateVersion(rs.int(3)), utcTime(rs.underlying, 4)))
      .list()
  }

  // F126: as for `readCommitted`, the rows are chosen by their stored sizes first.
  override def readPart[AGGREGATE_ROOT](aggregateId: AggregateId, from: AggregateVersion, to: AggregateVersion,
                                        maxBytes: Long): Seq[(EventInfo[AGGREGATE_ROOT], Int)] = DB.readOnly { implicit session =>
    sql"""WITH part AS (SELECT version, octet_length(event) AS size, sum(octet_length(event)) OVER (ORDER BY version) AS up_to,
         |     row_number() OVER (ORDER BY version) AS n
         |     FROM events WHERE aggregate_id = ${aggregateId.asLong} AND version BETWEEN ${from.asInt} AND ${to.asInt})
         |SELECT e.version, e.event_type_id, e.event_type_version, e.event, e.user_id, e.event_time, e.metadata::text, p.size
         | FROM part p JOIN events e ON e.aggregate_id = ${aggregateId.asLong} AND e.version = p.version
         | WHERE p.up_to <= $maxBytes OR p.n = 1
         | ORDER BY e.version""".stripMargin.map { rs =>
      val eventType = eventClass(rs.short(2), rs.short(3), aggregateId.asLong, rs.int(1))
      (EventInfo[AGGREGATE_ROOT](AggregateVersion(rs.int(1)), mpjsons.deserialize[Event[AGGREGATE_ROOT]](rs.string(4), eventType),
        UserId(rs.long(5)), utcTime(rs.underlying, 6), metadataOf(rs.string(7))), rs.int(8))
    }.list()
  }

  override def oldestVersionToPublish(aggregateId: AggregateId): Option[AggregateVersion] = DB.readOnly { implicit session =>
    sql"""SELECT min(version) FROM events_to_publish WHERE aggregate_id = ?""".bind(aggregateId.asLong)
      .map(_.intOpt(1).map(AggregateVersion(_))).single().flatten
  }

  // Cache updates made in the transaction through `AfterCommit`, an outbox writer's too (D33), apply once it committed.
  override def localTx[A](block: (DBSession) => A): A = AfterCommit.localTx(block)

}

object PostgresEventStoreState {

  /** The bytes of `s` in UTF-8, as PostgreSQL keeps it (`octet_length`), counted without encoding it: a size the store
    * reports when it writes is the one it reports when it reads (F142). */
  private[eventstore] def utf8Length(s: String): Int = {
    var bytes = 0
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      if (c < 0x80) bytes += 1
      else if (c < 0x800) bytes += 2
      else if (Character.isHighSurrogate(c) && i + 1 < s.length && Character.isLowSurrogate(s.charAt(i + 1))) { bytes += 4; i += 1 }
      else bytes += 3
      i += 1
    }
    bytes
  }
}

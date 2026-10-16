package io.reactivecqrs.core.eventstore

import java.sql.Timestamp
import java.time.Instant
import java.util.{Calendar, TimeZone}

import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api._
import io.reactivecqrs.core.types.TypesNamesState
import io.reactivecqrs.core.util.AfterCommit
import scalikejdbc._

import scala.concurrent.duration._
import scala.util.Try

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

  // D32: the metadata envelope of a command's events; a null column means "stored without one".
  private def metadataJson(metadata: Option[EventMetadata]): Option[String] = metadata.map(m => mpjsons.serialize(m, classOf[EventMetadata].getName))
  private def metadataOf(json: String): Option[EventMetadata] = Option(json).map(mpjsons.deserialize[EventMetadata](_, classOf[EventMetadata].getName))

  // Plain and first events of a command go in one `add_events` call (F31); undo and duplication events have procedures
  // of their own, one call per event. Every failure is thrown, so the caller's transaction rolls back whole.
  override def persistEvents[AGGREGATE_ROOT](aggregateId: AggregateId, newEvents: NewEvents[AGGREGATE_ROOT])(implicit session: DBSession): Try[Seq[(Event[AGGREGATE_ROOT], AggregateVersion)]] = Try {
    val events = newEvents.events
    val metadata = metadataJson(newEvents.metadata)
    if (events.exists(e => e.isInstanceOf[UndoEvent[_]] || e.isInstanceOf[DuplicationEvent[_]])) {
      var version = newEvents.expectedVersion
      events.map { event =>
        version = persistOne(aggregateId, newEvents, event, version, metadata)
        (event, version)
      }
    } else {
      persistPlain(aggregateId, newEvents, events, newEvents.expectedVersion, metadata)
      events.zipWithIndex.map { case (event, i) => (event, newEvents.expectedVersion.incrementBy(i + 1)) }
    }
  }

  private def serializedChecked(aggregateId: AggregateId, event: Event[_], version: AggregateVersion): String = {
    val serialized = mpjsons.serialize(event, event.getClass.getName)
    val limit = eventSizeLimitPerClassName.getOrElse(event.getClass.getName, eventSizeLimit)
    if (serialized.length > limit) {
      throw new EventTooLargeException(aggregateId, event.aggregateRootType.typeSymbol.fullName, version, event.getClass.getName, serialized.length, limit)
    } else if (version.asInt > aggregateVersionLimit) {
      throw new TooManyEventsException(aggregateId, event.aggregateRootType.typeSymbol.fullName, aggregateVersionLimit)
    }
    serialized
  }

  private def persistPlain[AGGREGATE_ROOT](aggregateId: AggregateId, newEvents: NewEvents[AGGREGATE_ROOT], events: Seq[Event[AGGREGATE_ROOT]],
                                           expectedVersion: AggregateVersion, metadata: Option[String])(implicit session: DBSession): AggregateVersion = {
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
    val aggregateTypeId = typesNamesState.typeIdByClassName(events.head.aggregateRootType.typeSymbol.fullName)
    AggregateVersion(sql"""SELECT add_events(${newEvents.userId.asLong}, ${array("bigint", spaceIds)}, ${aggregateId.asLong}, ${expectedVersion.asInt},
                          |  $aggregateTypeId, ${array("smallint", eventTypeIds)}, ${utc(newEvents.timestamp)}, ${array("text", serialized)}, $metadata::jsonb)"""
      .stripMargin.map(_.int(1)).single().get)
  }

  private def persistOne[AGGREGATE_ROOT](aggregateId: AggregateId, newEvents: NewEvents[AGGREGATE_ROOT], event: Event[AGGREGATE_ROOT],
                                         expectedVersion: AggregateVersion, metadata: Option[String])(implicit session: DBSession): AggregateVersion = event match {
    case undo: UndoEvent[_] =>
      val serialized = serializedChecked(aggregateId, event, expectedVersion)
      AggregateVersion(sql"""SELECT add_undo_event(${newEvents.userId.asLong}, ${aggregateId.asLong}, ${expectedVersion.asInt},
                            |  ${typesNamesState.typeIdByClassName(event.aggregateRootType.typeSymbol.fullName)}, ${typesNamesState.typeIdByClassName(event.getClass.getName)},
                            |  ${utc(newEvents.timestamp)}, $serialized, ${undo.eventsCount}, $metadata::jsonb)"""
        .stripMargin.map(_.int(1)).single().get)
    case duplication: DuplicationEvent[_] =>
      val serialized = serializedChecked(aggregateId, event, expectedVersion)
      AggregateVersion(sql"""SELECT add_duplication_event(${newEvents.userId.asLong}, ${duplication.spaceId.asLong}, ${aggregateId.asLong}, ${expectedVersion.asInt},
                            |  ${typesNamesState.typeIdByClassName(event.aggregateRootType.typeSymbol.fullName)}, ${typesNamesState.typeIdByClassName(event.getClass.getName)},
                            |  ${utc(newEvents.timestamp)}, $serialized, ${duplication.baseAggregateId.asLong}, ${duplication.baseAggregateVersion.asInt}, $metadata::jsonb)"""
        .stripMargin.map(_.int(1)).single().get)
    case _ =>
      persistPlain(aggregateId, newEvents, Seq(event), expectedVersion, metadata)
  }


  override def overwriteEvents[AGGREGATE_ROOT](aggregateId: AggregateId, events: Iterable[EventWithVersion[AGGREGATE_ROOT]])(implicit session: DBSession): Unit = {
    // The snapshots of the aggregate and of its duplicates may hold the old values (D30).
    sql"""DELETE FROM snapshots WHERE aggregate_id IN (SELECT id FROM aggregates WHERE base_id = ${aggregateId.asLong})""".update()
    events.foreach(e => {
      sql"""
           |UPDATE events SET event = ? WHERE aggregate_id = ? AND version = ?
      """.stripMargin.bind(mpjsons.serialize(e.event, e.event.getClass.getName), aggregateId.asLong, e.version.asInt).update()
    })
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

  override def writeSnapshot[AGGREGATE_ROOT](aggregateId: AggregateId, snapshot: Snapshot[AGGREGATE_ROOT], stateClassName: String): Unit = {
    val state = mpjsons.serialize(snapshot.state, stateClassName)
    DB.autoCommit { implicit session =>
      sql"""INSERT INTO snapshots (aggregate_id, version, context_version, schema_version, state, created)
           | VALUES (${aggregateId.asLong}, ${snapshot.version.asInt}, ${snapshot.contextVersion}, ${snapshot.schemaVersion}, $state, current_timestamp)
           | ON CONFLICT (aggregate_id) DO UPDATE SET version = EXCLUDED.version, context_version = EXCLUDED.context_version,
           |   schema_version = EXCLUDED.schema_version, state = EXCLUDED.state, created = EXCLUDED.created""".stripMargin.update()
    }
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
  }

  override def readAndProcessAllEvents(aggregateTypeName: String,
                                       batchPerAggregate: Boolean, eventHandler: (Seq[EventInfo[_]], AggregateId, AggregateType) => Unit): Unit = {

    val aggregateTypeId = typesNamesState.typeIdByClassName(aggregateTypeName)
    val aggregateType = AggregateType(aggregateTypeName)

    var buffer = List[EventInfo[Any]]()
    var lastAggregateId = AggregateId(-1)
    DB.readOnlyWithConnection { connection =>

      connection.setAutoCommit(false)

      val statement = if(batchPerAggregate) {
        connection.prepareStatement("""SELECT event_type_id, event_type_version, event, events.version, events.aggregate_id, user_id, event_time, events.metadata::text,
       noop_events.id IS NOT NULL AND noop_events.from_version <= aggregates.base_version as noop
FROM events
         JOIN aggregates ON aggregates.type_id = ? AND events.aggregate_id = aggregates.base_id AND events.aggregate_id = aggregates.id
         LEFT JOIN noop_events ON events.id = noop_events.id AND noop_events.from_version <= aggregates.base_version
ORDER BY events.aggregate_id, events.id""".stripMargin, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)
      } else {
        connection.prepareStatement("""SELECT event_type_id, event_type_version, event, events.version, events.aggregate_id, user_id, event_time, events.metadata::text,
       noop_events.id IS NOT NULL AND noop_events.from_version <= aggregates.base_version as noop
FROM events
         JOIN aggregates ON aggregates.type_id = ? AND events.aggregate_id = aggregates.base_id AND events.aggregate_id = aggregates.id
         LEFT JOIN noop_events ON events.id = noop_events.id AND noop_events.from_version <= aggregates.base_version
ORDER BY events.id""")
      }

      statement.setFetchSize(fetchSize)
      statement.setInt(1, aggregateTypeId)
      val rs = statement.executeQuery()

      while(rs.next()) {
        if(!rs.getBoolean(9)) { // ignore noop
          val eventType = eventClass(rs.getShort(1), rs.getShort(2), rs.getLong(5), rs.getInt(4))

          val event = mpjsons.deserialize[Event[_]](rs.getString(3), eventType)
          val aggregateId = AggregateId(rs.getLong(5))
          val version = AggregateVersion(rs.getInt(4))
          val eventInfo: EventInfo[Any] = EventInfo[Any](version, event.asInstanceOf[Event[Any]], UserId(rs.getLong(6)), utcTime(rs, 7), metadataOf(rs.getString(8)))

          if (!batchPerAggregate || lastAggregateId != aggregateId) {
            if (buffer.nonEmpty) {
              eventHandler(buffer.reverse, lastAggregateId, aggregateType)
            }
            buffer = List(eventInfo)
            lastAggregateId = aggregateId
          } else {
            buffer ::= eventInfo
          }
        }
      }

      if(buffer.nonEmpty) {
        eventHandler(buffer.reverse, lastAggregateId, aggregateType)
      }
    }
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

  override def deleteAggregatePermanently(aggregateId: AggregateId): Boolean = DB.localTx { implicit session =>
    val id = aggregateId.asLong
    // A system without projections has no subscriptions tables.
    def exists(table: String): Boolean = SQL(s"SELECT to_regclass('$table') IS NOT NULL").map(_.boolean(1)).single().getOrElse(false)
    val failed = exists("subscription_failures") &&
      sql"""SELECT EXISTS (SELECT 1 FROM subscription_failures WHERE aggregate_id = $id)""".map(_.boolean(1)).single().getOrElse(false)
    if (!failed) {
      sql"""DELETE FROM aggregates WHERE id = $id""".update()
      // noop_events must be deleted BEFORE events: it is resolved via a subquery over events,
      // so deleting events first would make the subquery match nothing and leak noop_events rows.
      sql"""DELETE FROM noop_events WHERE id IN (SELECT id FROM events WHERE aggregate_id = $id)""".update()
      sql"""DELETE FROM events WHERE aggregate_id = $id""".update()
      sql"""DELETE FROM events_to_publish WHERE aggregate_id = $id""".update()
      sql"""DELETE FROM snapshots WHERE aggregate_id = $id""".update()
      if (exists("subscriptions")) {
        sql"""DELETE FROM subscriptions WHERE aggregate_id = $id""".update()
      }
    }
    !failed
  }

  override def readNotYetPublishedEvents(): Map[AggregateId, AggregateVersion] = {
    DB.readOnly { implicit session =>
      sql"""SELECT aggregate_id, MIN(version) FROM events_to_publish GROUP BY aggregate_id"""
        .map(rs => (AggregateId(rs.long(1)), AggregateVersion(rs.int(2)))).list().toMap
    }
  }

  // One query for all aggregate types (F37); the aggregate's own row of its chain gives the type.
  override def readWaitingInOutbox(): Seq[WaitingInOutbox] = DB.readOnly { implicit session =>
    sql"""SELECT aggregates.type_id, events_to_publish.aggregate_id, MIN(events_to_publish.version), MIN(events_to_publish.event_time)
         | FROM events_to_publish
         | JOIN aggregates ON aggregates.id = events_to_publish.aggregate_id AND aggregates.base_id = aggregates.id
         | GROUP BY aggregates.type_id, events_to_publish.aggregate_id""".stripMargin
      .map(rs => WaitingInOutbox(AggregateType(typesNamesState.classNameById(rs.short(1))), AggregateId(rs.long(2)),
        AggregateVersion(rs.int(3)), utcTime(rs.underlying, 4)))
      .list()
  }

  override def countAllEvents(): Int =  DB.readOnly { implicit session =>

    // much faster, although might be not exact; the events table of the current schema (F71)
    val count = sql"""SELECT reltuples::bigint AS count FROM pg_class WHERE oid = 'events'::regclass"""
      .map(rs => rs.int(1)).single().get

    if(count <= 0) { // -1 until the table was first vacuumed or analyzed (PostgreSQL 14+)
      sql"""SELECT COUNT(*) FROM events""".map(rs => rs.int(1)).single().get
    } else {
      count
    }

  }

  override def countEventsForAggregateTypes(aggregateTypes: Seq[String]): Int = DB.readOnly { implicit session =>
    if(aggregateTypes.isEmpty) {
      0
    } else {
      val typesIds = aggregateTypes.map(t => typesNamesState.typeIdByClassName(t)).distinct
      sql"""SELECT COUNT(*) FROM events JOIN aggregates ON events.aggregate_id = aggregates.id WHERE aggregates.type_id in ($typesIds) and aggregates.base_order = 1""".map(rs => rs.int(1)).single().get
    }
  }

  override def readEventsRange[AGGREGATE_ROOT](aggregateId: AggregateId,
                                               from: AggregateVersion, to: AggregateVersion): Seq[EventInfo[AGGREGATE_ROOT]] = {
    DB.readOnly { implicit session =>
      sql"""SELECT version, event_type_id, event_type_version, event, user_id, event_time, metadata::text
           | FROM events
           | WHERE aggregate_id = ? AND version BETWEEN ? AND ?
           | ORDER BY version""".stripMargin.bind(aggregateId.asLong, from.asInt, to.asInt).map { rs =>
        val eventType = eventClass(rs.short(2), rs.short(3), aggregateId.asLong, rs.int(1))
        EventInfo[AGGREGATE_ROOT](AggregateVersion(rs.int(1)), mpjsons.deserialize[Event[AGGREGATE_ROOT]](rs.string(4), eventType),
          UserId(rs.long(5)), utcTime(rs.underlying, 6), metadataOf(rs.string(7)))
      }.list()
    }
  }

  override def oldestVersionToPublish(aggregateId: AggregateId): Option[AggregateVersion] = DB.readOnly { implicit session =>
    sql"""SELECT min(version) FROM events_to_publish WHERE aggregate_id = ?""".bind(aggregateId.asLong)
      .map(_.intOpt(1).map(AggregateVersion(_))).single().flatten
  }

  // Cache updates made in the transaction through `AfterCommit`, an outbox writer's too (D33), apply once it committed.
  override def localTx[A](block: (DBSession) => A): A = AfterCommit.localTx(block)

}

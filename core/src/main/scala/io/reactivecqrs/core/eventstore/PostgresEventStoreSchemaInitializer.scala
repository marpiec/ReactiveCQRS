package io.reactivecqrs.core.eventstore

import io.reactivecqrs.core.util.{Ddl, SchemaLock}

import scalikejdbc._
import io.reactivecqrs.core.util.SqlLogging.withoutValues


/** Creates the event store's tables, indexes and procedures, when missing (`PostgresEventStoreState.initSchema`). */
class PostgresEventStoreSchemaInitializer  {

  /** Creates what is missing, under the schema lock. */
  def initSchema(): Unit = SchemaLock {
    createEventsTable()
    dropColumnCommandId()
    createEventsAggregateIdIndex()
    createEventsIdAndAggregateIndex()
    createNoopEventTable()
    createEventsBroadcastTable()
    createEventsBroadcastAggregateIndex()
    createAggregatesTable()
    addAggregateSpaceColumn()
    createAggregatesTypeIndex()
    createAggregatesBaseIdIndex()
    createEventsSequence()
    createSnapshotsTable()
    addEventsMetadataColumn()
    addAggregatesSquashedVersionColumn()
    addAggregatesHistoryGenerationColumn()
    createEventsAggregateVersionIndexWhenSmall()
    createAggregatesSpaceIndexWhenSmall()
    createAggregatesOwnTypeIndexWhenSmall()
    createEventKeysTable()
    createEventsArchiveTable()
    createLineageAndRewritesTables()
    dropOldFunctions()
    createAddEventsFunction()
    createAddEventFunction()
    createAddUndoEventFunction()
    createAddDuplicationEventFunction()
    completeEventKeysWhenNoEvents()
  }


  private def createEventsTable(): Unit = DB.autoCommit { implicit session =>
    sql"""
        CREATE TABLE IF NOT EXISTS events (
          id BIGINT NOT NULL PRIMARY KEY,
          user_id BIGINT NOT NULL,
          aggregate_id BIGINT NOT NULL,
          event_time TIMESTAMP NOT NULL,
          version INT NOT NULL,
          event_type_id SMALLINT NOT NULL,
          event_type_version SMALLINT NOT NULL,
          event TEXT NOT NULL)
      """.execute()
  }

  // F185: each step that changes a table first looks whether there is anything to change (Ddl).
  private def dropColumnCommandId(): Unit = Ddl.dropColumn("events", "command_id")

  private def createEventsIdAndAggregateIndex(): Unit =
    Ddl.createIndex("events_id_aggregates_idx", "CREATE INDEX IF NOT EXISTS events_id_aggregates_idx ON events (aggregate_id, id)")

  private def createEventsAggregateIdIndex(): Unit =
    Ddl.createIndex("events_aggregates_idx", "CREATE INDEX IF NOT EXISTS events_aggregates_idx ON events (aggregate_id)")

  private def createAggregatesTypeIndex(): Unit =
    Ddl.createIndex("aggregates_type_idx", "CREATE INDEX IF NOT EXISTS aggregates_type_idx ON aggregates (type_id)")

  private def createAggregatesBaseIdIndex(): Unit =
    Ddl.createIndex("aggregates_base_id_idx", "CREATE INDEX IF NOT EXISTS aggregates_base_id_idx ON aggregates (base_id)")



  private def createNoopEventTable(): Unit = DB.autoCommit { implicit session =>
    sql"""
        CREATE TABLE IF NOT EXISTS noop_events (
          id BIGINT NOT NULL PRIMARY KEY,
          from_version INT NOT NULL)
      """.execute()
  }

  private def createEventsBroadcastTable(): Unit = DB.autoCommit { implicit session =>
    sql"""
        CREATE TABLE IF NOT EXISTS events_to_publish (
          event_id BIGINT NOT NULL PRIMARY KEY,
          aggregate_id BIGINT NOT NULL,
          version INT NOT NULL,
          user_id BIGINT NOT NULL,
          event_time TIMESTAMP NOT NULL)
      """.execute()

  }

  // events_to_publish is the outbox: queried/joined/deleted by aggregate_id (often ordered by version)
  // on every AggregateRepositoryActor init and outbox resend. Without this index those are full table scans.
  private def createEventsBroadcastAggregateIndex(): Unit =
    Ddl.createIndex("events_to_publish_aggregate_idx", "CREATE INDEX IF NOT EXISTS events_to_publish_aggregate_idx ON events_to_publish (aggregate_id, version)")

  private def createAggregatesTable(): Unit = DB.autoCommit { implicit session =>
    sql"""
      CREATE TABLE IF NOT EXISTS aggregates (
        space_id BIGINT NOT NULL,
        id BIGINT NOT NULL,
        creation_time TIMESTAMP NOT NULL,
        type_id SMALLINT NOT NULL,
        base_order INT NOT NULL,
        base_id BIGINT NOT NULL,
        base_version INT NOT NULL,
        CONSTRAINT aggregates_pk PRIMARY KEY (id, base_id))
      """.execute()
  }

  private def addAggregateSpaceColumn(): Unit = Ddl.addColumn("aggregates", "space_id", "BIGINT NOT NULL DEFAULT -1")

  private def createEventsSequence(): Unit = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE IF NOT EXISTS events_seq""".execute()
  }

  // D30: the latest performance snapshot of an aggregate, as its root's JSON; disposable, never part of the history.
  private def createSnapshotsTable(): Unit = DB.autoCommit { implicit session =>
    sql"""
      CREATE TABLE IF NOT EXISTS snapshots (
        aggregate_id BIGINT NOT NULL PRIMARY KEY,
        version INT NOT NULL,
        context_version INT NOT NULL,
        schema_version INT NOT NULL,
        state TEXT NOT NULL,
        created TIMESTAMP NOT NULL)
      """.execute()
  }

  // D32: the metadata envelope of an event; null for events stored without one.
  private def addEventsMetadataColumn(): Unit = Ddl.addColumn("events", "metadata", "JSONB")

  // D49, D51: the version an aggregate's history is squashed up to; null when it never was.
  private def addAggregatesSquashedVersionColumn(): Unit = Ddl.addColumn("aggregates", "squashed_version", "INT")

  // F119: raised by every change that may make the aggregate's snapshots wrong; a snapshot is written only at the
  // generation its state was read at. Adding a column with a constant default rewrites nothing.
  private def addAggregatesHistoryGenerationColumn(): Unit = Ddl.addColumn("aggregates", "history_generation", "INT NOT NULL DEFAULT 0")

  // F38: a new or small installation gets the restore index at once; a large one builds it online
  // (`PostgresEventStoreState.buildIndexesConcurrently`), since a plain build would block writes for its duration (D69).
  private def createEventsAggregateVersionIndexWhenSmall(): Unit = if (small("events")) {
    Ddl.createIndex("events_aggregate_version_idx", "CREATE INDEX IF NOT EXISTS events_aggregate_version_idx ON events (aggregate_id, version)")
  }

  // M6: an organization is found through its aggregates' space (D38); built online on a large installation (D69).
  private def createAggregatesSpaceIndexWhenSmall(): Unit = if (small("aggregates")) {
    Ddl.createIndex("aggregates_space_id_idx", "CREATE INDEX IF NOT EXISTS aggregates_space_id_idx ON aggregates (space_id, id)")
    Ddl.dropIndex("aggregates_space_idx") // the first M6 steps' index, on the space alone
  }

  // F173: a type's aggregates, their own rows, in id order: the walks of builds and targeted rebuilds read them a page at a
  // time. Built online on a large installation (D69).
  private def createAggregatesOwnTypeIndexWhenSmall(): Unit = if (small("aggregates")) {
    Ddl.createIndex("aggregates_own_type_idx", "CREATE INDEX IF NOT EXISTS aggregates_own_type_idx ON aggregates (type_id, id) WHERE id = base_id")
  }

  private def small(table: String): Boolean = DB.readOnly { implicit session =>
    SQL(s"SELECT reltuples::bigint FROM pg_class WHERE oid = '$table'::regclass").map(_.long(1)).single.apply().getOrElse(0L) < 100000L
  }

  // F124: each event's aggregate type and space, so a build of some types, or of one space, reads only their events in
  // commit order. The procedures write a key with each event, and the cascade deletes it with the event. The rows from
  // before get theirs from `PostgresEventStoreState.backfillEventKeys`, an online job (D69), which records the step in
  // `schema_steps` once every event has a key; until then builds read as before.
  private def createEventKeysTable(): Unit = {
    DB.autoCommit { implicit session =>
      sql"""CREATE TABLE IF NOT EXISTS schema_steps (name VARCHAR(128) NOT NULL PRIMARY KEY, completed TIMESTAMP NOT NULL)""".execute()
      sql"""CREATE TABLE IF NOT EXISTS event_keys (id BIGINT NOT NULL PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
           |  type_id SMALLINT NOT NULL, space_id BIGINT NOT NULL)""".stripMargin.execute()
    }
    Ddl.createIndex("event_keys_type_idx", "CREATE INDEX IF NOT EXISTS event_keys_type_idx ON event_keys (type_id, id)")
    Ddl.createIndex("event_keys_space_idx", "CREATE INDEX IF NOT EXISTS event_keys_space_idx ON event_keys (space_id, type_id, id)")
  }

  // R11: the audit events a squash keeps, as they were stored, with their undo marks; never read by restores or builds.
  private def createEventsArchiveTable(): Unit = {
    DB.autoCommit { implicit session =>
      sql"""CREATE TABLE IF NOT EXISTS events_archive (id BIGINT NOT NULL PRIMARY KEY, aggregate_id BIGINT NOT NULL, version INT NOT NULL,
           |  event_type_id SMALLINT NOT NULL, event_type_version SMALLINT NOT NULL, event TEXT NOT NULL, user_id BIGINT NOT NULL, event_time TIMESTAMP NOT NULL, metadata JSONB,
           |  undone_from INT, archived TIMESTAMP NOT NULL)""".stripMargin.execute()
    }
    Ddl.createIndex("events_archive_aggregate_idx", "CREATE INDEX IF NOT EXISTS events_archive_aggregate_idx ON events_archive (aggregate_id, version)")
  }

  // D76: a detached copy holds a copy of its bases' state, so a complete rewrite of a base must find it: every detach
  // records the bases the copy read, at which version. And each complete rewrite leaves an audit row, never a value.
  private def createLineageAndRewritesTables(): Unit = {
    DB.autoCommit { implicit session =>
      sql"""CREATE TABLE IF NOT EXISTS aggregate_lineage (aggregate_id BIGINT NOT NULL, copied_from BIGINT NOT NULL, at_version INT NOT NULL,
           |  detached TIMESTAMP NOT NULL, PRIMARY KEY (aggregate_id, copied_from))""".stripMargin.execute()
      sql"""CREATE TABLE IF NOT EXISTS history_rewrites (id BIGSERIAL PRIMARY KEY, aggregate_id BIGINT NOT NULL, user_id BIGINT NOT NULL,
           |  at TIMESTAMP NOT NULL, change_class VARCHAR(512) NOT NULL, classes TEXT NOT NULL, fields TEXT NOT NULL, events INT NOT NULL,
           |  archived INT NOT NULL)""".stripMargin.execute()
    }
    Ddl.createIndex("aggregate_lineage_copied_from_idx", "CREATE INDEX IF NOT EXISTS aggregate_lineage_copied_from_idx ON aggregate_lineage (copied_from)")
    Ddl.createIndex("history_rewrites_aggregate_idx", "CREATE INDEX IF NOT EXISTS history_rewrites_aggregate_idx ON history_rewrites (aggregate_id)")
  }

  // An installation without events has none to backfill. Checked once the procedures write keys, so an event written
  // before them by a node of the previous version is never taken for keyed.
  private def completeEventKeysWhenNoEvents(): Unit = DB.autoCommit { implicit session =>
    sql"""INSERT INTO schema_steps (name, completed) SELECT 'event_keys', current_timestamp WHERE NOT EXISTS (SELECT 1 FROM events)
         | ON CONFLICT (name) DO NOTHING""".stripMargin.update()
  }

  // Signatures of earlier versions only: the current functions are replaced in place, so a node starting never drops a
  // function that other nodes are using (the migration runner of M5 takes this over).
  private def dropOldFunctions(): Unit = DB.autoCommit { implicit session =>
    sql"""DROP FUNCTION IF EXISTS add_event(BIGINT, BIGINT, INT, SMALLINT, SMALLINT, SMALLINT, TIMESTAMP, VARCHAR)""".execute()
    sql"""DROP FUNCTION IF EXISTS add_event(BIGINT, BIGINT, BIGINT, INT, SMALLINT, SMALLINT, SMALLINT, TIMESTAMP, VARCHAR)""".execute()
    sql"""DROP FUNCTION IF EXISTS add_event(BIGINT, BIGINT, BIGINT, BIGINT, INT, SMALLINT, SMALLINT, SMALLINT, TIMESTAMP, VARCHAR)""".execute()
    sql"""DROP FUNCTION IF EXISTS add_events(BIGINT, BIGINT[], BIGINT, INT, SMALLINT, SMALLINT[], SMALLINT[], TIMESTAMP, VARCHAR[])""".execute()
    sql"""DROP FUNCTION IF EXISTS add_undo_event(BIGINT, BIGINT, INT, SMALLINT, SMALLINT, SMALLINT, TIMESTAMP, VARCHAR, INT)""".execute()
    sql"""DROP FUNCTION IF EXISTS add_undo_event(BIGINT, BIGINT, BIGINT, INT, SMALLINT, SMALLINT, SMALLINT, TIMESTAMP, VARCHAR, INT)""".execute()
    sql"""DROP FUNCTION IF EXISTS add_duplication_event(BIGINT, BIGINT, INT, SMALLINT, SMALLINT, SMALLINT, TIMESTAMP, VARCHAR, BIGINT, INT)""".execute()
    sql"""DROP FUNCTION IF EXISTS add_duplication_event(BIGINT, BIGINT, BIGINT, INT, SMALLINT, SMALLINT, SMALLINT, TIMESTAMP, VARCHAR, BIGINT, INT)""".execute()
    sql"""DROP FUNCTION IF EXISTS add_duplication_event(BIGINT, BIGINT, BIGINT, BIGINT, INT, SMALLINT, SMALLINT, SMALLINT, TIMESTAMP, VARCHAR, BIGINT, INT)""".execute()
  }

  // One event: `add_events` with a single element.
  private def createAddEventFunction(): Unit = DB.autoCommit { implicit session =>
    SQL("""
          |CREATE OR REPLACE FUNCTION add_event(user_id BIGINT, space_id BIGINT, aggregate_id BIGINT, expected_version INT, aggregate_type_id SMALLINT, event_type_id SMALLINT, event_time TIMESTAMP, event TEXT, metadata JSONB)
          |RETURNS INT AS
          |$$
          |BEGIN
          |    RETURN add_events(user_id, ARRAY[space_id], aggregate_id, expected_version, aggregate_type_id, ARRAY[event_type_id], event_time, ARRAY[event], metadata);
          |END;
          |$$
          |LANGUAGE 'plpgsql' VOLATILE
        """.stripMargin).execute()
  }

  // The aggregate's own row of its chain holds its version: updating it locks the aggregate for the transaction, so the
  // writers of one aggregate queue up there, and a version other than the expected one raises (the optimistic lock).
  // F31: one update of that row for the whole command, then the events and their outbox rows. Events are written with
  // type version 0 (D29) and the command's metadata envelope (D32). A first event (space id >= 0) creates the row.
  // Returns the aggregate's new version.
  private def createAddEventsFunction(): Unit = DB.autoCommit { implicit session =>
    SQL("""
          |CREATE OR REPLACE FUNCTION add_events(_user_id BIGINT, _space_ids BIGINT[], _aggregate_id BIGINT, _expected_version INT, _aggregate_type_id SMALLINT, _event_type_ids SMALLINT[], _event_time TIMESTAMP, _events TEXT[], _metadata JSONB)
          |RETURNS INT AS
          |$$
          |DECLARE
          |    n INT := array_length(_events, 1);
          |    current_version INT;
          |    event_id BIGINT;
          |    _space BIGINT;
          |BEGIN
          |    UPDATE aggregates SET base_version = base_version + n WHERE id = _aggregate_id AND base_id = _aggregate_id RETURNING base_version - n, space_id INTO current_version, _space;
          |    IF NOT FOUND THEN
          |        IF _expected_version = 0 AND _space_ids[1] >= 0 THEN
          |            INSERT INTO aggregates (space_id, id, creation_time, type_id, base_order, base_id, base_version) VALUES (_space_ids[1], _aggregate_id, current_timestamp, _aggregate_type_id, 1, _aggregate_id, n);
          |            current_version := 0;
          |            _space := _space_ids[1];
          |        ELSIF _space_ids[1] = -1 THEN
          |            RAISE EXCEPTION 'Space incorrect defined for aggregate not found, id %, aggregate_type_id %. Maybe FirstEvent was not used.', _aggregate_id, _aggregate_type_id;
          |        ELSE
          |            RAISE EXCEPTION 'aggregate not found, id %, aggregate_type_id %', _aggregate_id, _aggregate_type_id;
          |        END IF;
          |    END IF;
          |    IF _expected_version >= 0 AND current_version != _expected_version THEN
          |        RAISE EXCEPTION 'Concurrent aggregate modification exception, user id %, aggregate id %, expected version %, current_version %', _user_id, _aggregate_id, _expected_version, current_version;
          |    END IF;
          |    FOR i IN 1 .. n LOOP
          |        event_id := nextval('events_seq');
          |        INSERT INTO events (id, user_id, aggregate_id, event_time, version, event_type_id, event_type_version, event, metadata) VALUES (event_id, _user_id, _aggregate_id, _event_time, current_version + i, _event_type_ids[i], 0, _events[i], _metadata);
          |        INSERT INTO events_to_publish (event_id, aggregate_id, version, user_id, event_time) VALUES (event_id, _aggregate_id, current_version + i, _user_id, _event_time);
          |        INSERT INTO event_keys (id, type_id, space_id) VALUES (event_id, _aggregate_type_id, _space);
          |    END LOOP;
          |    RETURN current_version + n;
          |END;
          |$$
          |LANGUAGE 'plpgsql' VOLATILE
        """.stripMargin).execute()
  }

  // Marks the last `undo_count` events not undone yet, and itself, as noop from the new version. F31: the newest events
  // are found walking back the index on (aggregate_id, version), not by sorting the history. D49: an undo that would reach
  // the squashed history is refused. D30: the aggregate's snapshot no longer matches its history and goes.
  private def createAddUndoEventFunction(): Unit = DB.autoCommit { implicit session =>
    SQL("""
          |CREATE OR REPLACE FUNCTION add_undo_event(user_id BIGINT, _aggregate_id BIGINT, expected_version INT, aggregate_type_id SMALLINT, event_type_id SMALLINT, event_time TIMESTAMP, event TEXT, undo_count INT, metadata JSONB)
          |RETURNS INT AS
          |$$
          |DECLARE
          |    current_version INT;
          |    squashed INT;
          |    first_undone INT;
          |    event_id BIGINT;
          |    _space BIGINT;
          |BEGIN
          |    UPDATE aggregates SET base_version = base_version + 1 WHERE id = _aggregate_id AND base_id = _aggregate_id RETURNING base_version - 1, squashed_version, space_id INTO current_version, squashed, _space;
          |    IF NOT FOUND THEN
          |        IF expected_version = 0 THEN
          |            RAISE EXCEPTION 'Cannot undo event for non existing aggregate';
          |        ELSE
          |            RAISE EXCEPTION 'aggregate not found, id %, aggregate_type_id %', _aggregate_id, aggregate_type_id;
          |        END IF;
          |    END IF;
          |    IF expected_version >= 0 AND current_version != expected_version THEN
          |        RAISE EXCEPTION 'Concurrent aggregate modification exception, user id %, aggregate id %, expected version %, current_version %', user_id, _aggregate_id, expected_version, current_version;
          |    END IF;
          |    SELECT min(last.version) INTO first_undone FROM (
          |        SELECT events.version FROM events LEFT JOIN noop_events ON events.id = noop_events.id
          |        WHERE events.aggregate_id = _aggregate_id AND noop_events.id IS NULL ORDER BY events.version DESC LIMIT undo_count) AS last;
          |    IF squashed IS NOT NULL AND first_undone <= squashed THEN
          |        RAISE EXCEPTION 'Undo cannot reach the squashed history: aggregate %, squashed up to version %, the undo would reach version %', _aggregate_id, squashed, first_undone;
          |    END IF;
          |    event_id := nextval('events_seq');
          |    INSERT INTO noop_events (id, from_version)
          |        SELECT events.id, current_version + 1 FROM events LEFT JOIN noop_events ON events.id = noop_events.id
          |        WHERE events.aggregate_id = _aggregate_id AND noop_events.id IS NULL ORDER BY events.version DESC LIMIT undo_count;
          |    INSERT INTO noop_events (id, from_version) VALUES (event_id, current_version + 1);
          |    INSERT INTO events (id, user_id, aggregate_id, event_time, version, event_type_id, event_type_version, event, metadata) VALUES (event_id, user_id, _aggregate_id, event_time, current_version + 1, event_type_id, 0, event, metadata);
          |    INSERT INTO events_to_publish (event_id, aggregate_id, version, user_id, event_time) VALUES (event_id, _aggregate_id, current_version + 1, user_id, event_time);
          |    INSERT INTO event_keys (id, type_id, space_id) VALUES (event_id, aggregate_type_id, _space);
          |    DELETE FROM snapshots WHERE aggregate_id = _aggregate_id;
          |    UPDATE aggregates SET history_generation = history_generation + 1 WHERE id = _aggregate_id AND base_id = _aggregate_id;
          |    RETURN current_version + 1;
          |END;
          |$$
          |LANGUAGE 'plpgsql' VOLATILE
        """.stripMargin).execute()
  }

  // A duplicate starts with its base's chain up to the base version, then a row of its own. The base's chain rows stay
  // locked until the transaction ends, in key order as a squash or a permanent delete locks them (D50, F76): either
  // that one waits and then sees this duplicate, or this one waits and then sees the squash point or the delete.
  private def createAddDuplicationEventFunction(): Unit = DB.autoCommit { implicit session =>
    SQL("""
          |CREATE OR REPLACE FUNCTION add_duplication_event(user_id BIGINT, new_space_id BIGINT, aggregate_id BIGINT, expected_version INT, aggregate_type_id SMALLINT, event_type_id SMALLINT, event_time TIMESTAMP, event TEXT, _base_id BIGINT, _base_version INT, metadata JSONB)
          |RETURNS INT AS
          |$$
          |DECLARE
          |    current_version INT;
          |    base_count INT;
          |    base_squashed INT;
          |    base_current INT;
          |    event_id BIGINT;
          |BEGIN
          |    UPDATE aggregates SET base_version = base_version + 1 WHERE id = aggregate_id AND base_id = aggregate_id RETURNING base_version - 1 INTO current_version;
          |    IF NOT FOUND THEN
          |      IF expected_version >= 0 AND expected_version != 0 THEN
          |        RAISE EXCEPTION 'Duplication event might occur only for non existing aggregate, so expected version need to be 0';
          |      ELSE
          |        PERFORM 1 FROM aggregates WHERE id = _base_id ORDER BY id, base_id FOR KEY SHARE;
          |        SELECT squashed_version, base_version INTO base_squashed, base_current FROM aggregates WHERE id = _base_id AND base_id = _base_id;
          |        IF NOT FOUND THEN
          |          RAISE EXCEPTION 'Duplication of aggregate %, which does not exist', _base_id;
          |        END IF;
          |        IF _base_version > base_current THEN
          |          RAISE EXCEPTION 'Duplication of aggregate % at version %, which it has not reached (it is at version %)', _base_id, _base_version, base_current;
          |        END IF;
          |        IF _base_version < base_squashed THEN
          |          RAISE EXCEPTION 'Duplication cannot read the squashed history: aggregate %, squashed up to version %, duplicated at version %', _base_id, base_squashed, _base_version;
          |        END IF;
          |        current_version := 0;
          |        INSERT INTO aggregates (space_id, id, creation_time, type_id, base_order, base_id, base_version) (select new_space_id, aggregate_id, current_timestamp, aggregate_type_id, base_order, base_id, base_version FROM aggregates WHERE id = _base_id);
          |        SELECT base_order INTO base_count FROM aggregates WHERE id = aggregate_id AND base_id = _base_id;
          |        INSERT INTO aggregates (space_id, id, creation_time, type_id, base_order, base_id, base_version) VALUES (new_space_id, aggregate_id, current_timestamp, aggregate_type_id, base_count + 1, aggregate_id, 1);
          |        UPDATE aggregates SET base_version = _base_version WHERE id = aggregate_id AND base_id = _base_id;
          |      END IF;
          |    ELSE
          |      RAISE EXCEPTION 'Duplication event might occur only for non existing aggregate, but such was found';
          |    END IF;
          |    event_id := nextval('events_seq');
          |    INSERT INTO events (id, user_id, aggregate_id, event_time, version, event_type_id, event_type_version, event, metadata) VALUES (event_id, user_id, aggregate_id, event_time, current_version + 1, event_type_id, 0, event, metadata);
          |    INSERT INTO events_to_publish (event_id, aggregate_id, version, user_id, event_time) VALUES (event_id, aggregate_id, current_version + 1, user_id, event_time);
          |    INSERT INTO event_keys (id, type_id, space_id) VALUES (event_id, aggregate_type_id, new_space_id);
          |    RETURN current_version + 1;
          |END;
          |$$
          |LANGUAGE 'plpgsql' VOLATILE
        """.stripMargin).execute()
  }

}

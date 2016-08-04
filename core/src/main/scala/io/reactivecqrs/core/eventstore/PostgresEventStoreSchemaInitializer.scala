package io.reactivecqrs.core.eventstore

import org.postgresql.util.PSQLException
import scalikejdbc._


class PostgresEventStoreSchemaInitializer  {

  def initSchema(): Unit = {
    createEventsTable()
    createNoopEventTable()
    createEventsBroadcastTable()
    createAggregatesTable()
    try {
      createEventsSequence()
    } catch {
      case e: PSQLException => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
    }
    dropExistingFunctions()
    createAddEventFunction()
    createAddUndoEventFunction()
    createAddDuplicationEventFunction()
  }


  private def createEventsTable(): Unit = DB.autoCommit { implicit session =>
    sql"""
        CREATE TABLE IF NOT EXISTS events (
          id BIGINT NOT NULL PRIMARY KEY,
          command_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          aggregate_id BIGINT NOT NULL,
          event_time TIMESTAMP NOT NULL,
          version INT NOT NULL,
          event_type_id SMALLINT NOT NULL,
          event TEXT NOT NULL)
      """.execute().apply()
  }

  private def createNoopEventTable(): Unit = DB.autoCommit { implicit session =>
    sql"""
        CREATE TABLE IF NOT EXISTS noop_events (
          id BIGINT NOT NULL PRIMARY KEY,
          from_version INT NOT NULL)
      """.execute().apply()
  }

  private def createEventsBroadcastTable(): Unit = DB.autoCommit { implicit session =>
    sql"""
        CREATE TABLE IF NOT EXISTS events_to_publish (
          event_id BIGINT NOT NULL PRIMARY KEY,
          aggregate_id BIGINT NOT NULL,
          version INT NOT NULL,
          user_id BIGINT NOT NULL,
          event_time TIMESTAMP NOT NULL)
      """.execute().apply()

  }

  private def createAggregatesTable(): Unit = DB.autoCommit { implicit session =>
    sql"""
        CREATE TABLE IF NOT EXISTS aggregates (
          id BIGINT NOT NULL,
          creation_time TIMESTAMP NOT NULL,
          type_id SMALLINT NOT NULL,
          base_order INT NOT NULL,
          base_id BIGINT NOT NULL,
          base_version INT NOT NULL,
          CONSTRAINT aggregates_pk PRIMARY KEY (id, base_id))

      """.execute().apply()
  }

  private def createEventsSequence(): Unit = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE events_seq""".execute().apply()
  }

  private def dropExistingFunctions(): Unit = DB.autoCommit { implicit session =>
    sql"""DROP FUNCTION IF EXISTS add_event(BIGINT, BIGINT, BIGINT, INT, INT, INT, TIMESTAMP, VARCHAR(10240))""".execute().apply()
    sql"""DROP FUNCTION IF EXISTS add_undo_event(BIGINT, BIGINT, BIGINT, INT, INT, INT, TIMESTAMP, VARCHAR(10240), INT)""".execute().apply()
    sql"""DROP FUNCTION IF EXISTS add_duplication_event(BIGINT, BIGINT, BIGINT, INT, INT, INT, TIMESTAMP, VARCHAR(10240), BIGINT, INT)""".execute().apply()
  }

  private def createAddEventFunction(): Unit = DB.autoCommit { implicit session =>
    SQL("""
          |CREATE OR REPLACE FUNCTION add_event(command_id BIGINT, user_id BIGINT, aggregate_id BIGINT, expected_version INT, aggregate_type_id SMALLINT, event_type_id SMALLINT, event_time TIMESTAMP, event VARCHAR(10240))
          |RETURNS BIGINT AS
          |$$
          |DECLARE
          |    current_version INT;
          |    event_id BIGINT;
          |BEGIN
          |    UPDATE aggregates SET base_version = base_version + 1 WHERE id = aggregate_id AND base_id = aggregate_id RETURNING base_version - 1 INTO current_version;
          |    IF NOT FOUND THEN
          |        IF expected_version = 0 THEN
          |            INSERT INTO AGGREGATES (id, creation_time, type_id, base_order, base_id, base_version) VALUES (aggregate_id, current_timestamp, aggregate_type_id, 1, aggregate_id, 1);
          |            current_version := 0;
          |        ELSE
          |	           RAISE EXCEPTION 'aggregate not found, id %, aggregate_type_id %', aggregate_id, aggregate_type_id;
          |        END IF;
          |    END IF;
          |    IF expected_version >= 0 AND current_version != expected_version THEN
          |  	     RAISE EXCEPTION 'Concurrent aggregate modification exception, command id %, user id %, aggregate id %, expected version %, current_version %, event_type_id %, event %', command_id, user_id, aggregate_id, expected_version, current_version, event_type_id, event;
          |    END IF;
          |    SELECT NEXTVAL('events_seq') INTO event_id;
          |    INSERT INTO events (id, command_id, user_id, aggregate_id, event_time, version, event_type_id, event) VALUES (event_id, command_id, user_id, aggregate_id, event_time, current_version + 1, event_type_id, event);
          |    INSERT INTO events_to_publish (event_id, aggregate_id, version, user_id, event_time) VALUES(event_id, aggregate_id, current_version + 1, user_id, event_time);
          |    RETURN current_version + 1;
          |END;
          |$$
          |LANGUAGE 'plpgsql' VOLATILE
        """.stripMargin).execute().apply()
  }

  private def createAddUndoEventFunction(): Unit = DB.autoCommit { implicit session =>
    SQL("""
          |CREATE OR REPLACE FUNCTION add_undo_event(command_id BIGINT, user_id BIGINT, _aggregate_id BIGINT, expected_version INT, aggregate_type_id SMALLINT, event_type_id SMALLINT, event_time TIMESTAMP, event VARCHAR(10240), undo_count INT)
          |RETURNS BIGINT AS
          |$$
          |DECLARE
          |    current_version INT;
          |    event_id BIGINT;
          |BEGIN
          |    UPDATE aggregates SET base_version = base_version + 1 WHERE id = _aggregate_id AND base_id = _aggregate_id RETURNING base_version - 1 INTO current_version;
          |    IF NOT FOUND THEN
          |        IF expected_version = 0 THEN
          |            RAISE EXCEPTION 'Cannot undo event for non existing aggregate';
          |        ELSE
          |	           RAISE EXCEPTION 'aggregate not found, id %, aggregate_type_id %', _aggregate_id, aggregate_type_id;
          |        END IF;
          |    END IF;
          |    IF expected_version >= 0 AND current_version != expected_version THEN
          |	       RAISE EXCEPTION 'Concurrent aggregate modification exception, command id %, user id %, aggregate id %, expected version %, current_version %, event_type_id %, event %', command_id, user_id, aggregate_id, expected_version, current_version, event_type_id, event;
          |    END IF;
          |    SELECT NEXTVAL('events_seq') INTO event_id;
          |    INSERT INTO noop_events (id, from_version) (select events.id, current_version + 1
          |     from events
          |     left join noop_events on events.id = noop_events.id
          |     where events.aggregate_id = _aggregate_id AND noop_events.id is null order by events.version desc limit undo_count);
          |    INSERT INTO noop_events(id, from_version) VALUES (event_id, current_version + 1);
          |    INSERT INTO events (id, command_id, user_id, aggregate_id, event_time, version, event_type_id, event) VALUES (event_id, command_id, user_id, _aggregate_id, event_time, current_version + 1, event_type_id, event);
          |    INSERT INTO events_to_publish (event_id, aggregate_id, version, user_id, event_time) VALUES(event_id, _aggregate_id, current_version + 1, user_id, event_time);
          |    RETURN current_version + 1;
          |END;
          |$$
          |LANGUAGE 'plpgsql' VOLATILE
        """.stripMargin).execute().apply()
  }

  private def createAddDuplicationEventFunction(): Unit = DB.autoCommit { implicit session =>
    SQL("""
          |CREATE OR REPLACE FUNCTION add_duplication_event(command_id BIGINT, user_id BIGINT, aggregate_id BIGINT, expected_version INT, aggregate_type_id SMALLINT, event_type_id SMALLINT, event_time TIMESTAMP, event VARCHAR(10240), _base_id BIGINT, _base_version INT)
          |RETURNS BIGINT AS
          |$$
          |DECLARE
          |    current_version INT;
          |    base_count INT;
          |    event_id BIGINT;
          |BEGIN
          |    UPDATE aggregates SET base_version = base_version + 1 WHERE id = aggregate_id AND base_id = aggregate_id RETURNING base_version - 1 INTO current_version;
          |    IF NOT FOUND THEN
          |        IF expected_version >= 0 AND expected_version != 0 THEN
          |          RAISE EXCEPTION 'Duplication event might occur only for non existing aggregate, so expected version need to be 0';
          |        ELSE
          |          INSERT INTO aggregates (id, creation_time, type_id, base_order, base_id, base_version) (select aggregate_id, current_timestamp, aggregate_type_id, base_order, base_id, base_version
          |            from aggregates
          |            where id = _base_id);
          |          current_version := 0;
          |          SELECT base_order INTO base_count FROM aggregates WHERE id = aggregate_id AND base_id = _base_id;
          |          INSERT INTO aggregates (id, creation_time, type_id, base_order, base_id, base_version) VALUES (aggregate_id, current_timestamp, aggregate_type_id, base_count + 1, aggregate_id, 1);
          |          UPDATE aggregates SET base_version = _base_version WHERE id = aggregate_id AND base_id = _base_id;
          |        END IF;
          |    ELSE
          |      RAISE EXCEPTION 'Duplication event might occur only for non existing aggregate, but such was found';
          |    END IF;
          |    SELECT NEXTVAL('events_seq') INTO event_id;
          |    INSERT INTO events (id, command_id, user_id, aggregate_id, event_time, version, event_type_id, event) VALUES (event_id, command_id, user_id, aggregate_id, event_time, current_version + 1, event_type_id, event);
          |    INSERT INTO events_to_publish (event_id, aggregate_id, version, user_id, event_time) VALUES(event_id, aggregate_id, current_version + 1, user_id, event_time);
          |    RETURN current_version + 1;
          |END;
          |$$
          |LANGUAGE 'plpgsql' VOLATILE
        """.stripMargin).execute().apply()
  }
  
}

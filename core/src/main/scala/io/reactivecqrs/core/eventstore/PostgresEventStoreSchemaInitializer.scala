package io.reactivecqrs.core.eventstore

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
      case e: Exception => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
    }
    createAddEventFunction()
    createAddUndoEventFunction()
    createAddDuplicationEventFunction()
  }


  private def createEventsTable() = DB.autoCommit { implicit session =>
    sql"""
        CREATE TABLE IF NOT EXISTS events (
          id BIGINT NOT NULL PRIMARY KEY,
          command_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          aggregate_id BIGINT NOT NULL,
          event_time TIMESTAMP NOT NULL,
          version INT NOT NULL,
          event_type VARCHAR(1024) NOT NULL,
          event TEXT NOT NULL)
      """.execute().apply()
  }

  private def createNoopEventTable(): Unit = DB.autoCommit { implicit session =>
    sql"""
        CREATE TABLE IF NOT EXISTS noop_events (
          id INT NOT NULL PRIMARY KEY,
          from_version INT NOT NULL)
      """.execute().apply()
  }

  private def createEventsBroadcastTable() = DB.autoCommit { implicit session =>
    sql"""
        CREATE TABLE IF NOT EXISTS events_to_publish (
          event_id BIGINT NOT NULL PRIMARY KEY,
          aggregate_id BIGINT NOT NULL,
          version INT NOT NULL,
          user_id BIGINT NOT NULL,
          event_time TIMESTAMP NOT NULL)
      """.execute().apply()

  }

  private def createAggregatesTable() = DB.autoCommit { implicit session =>
    sql"""
        CREATE TABLE IF NOT EXISTS aggregates (
          id BIGINT NOT NULL,
          type VARCHAR(1024) NOT NULL,
          base_order INT NOT NULL,
          base_id BIGINT NOT NULL,
          base_version INT NOT NULL)
      """.execute().apply()
  }

  private def createEventsSequence() = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE events_seq""".execute().apply()
  }

  private def createAddEventFunction(): Unit = DB.autoCommit { implicit session =>
    SQL("""
          |CREATE OR REPLACE FUNCTION add_event(command_id BIGINT, user_id BIGINT, aggregate_id BIGINT, expected_version INT, aggregate_type VARCHAR(128), event_type VARCHAR(128), event_time TIMESTAMP, event VARCHAR(10240))
          |RETURNS BIGINT AS
          |$$
          |DECLARE
          |    current_version INT;
          |    event_id BIGINT;
          |BEGIN
          | SELECT aggregates.base_version INTO current_version FROM aggregates WHERE id = aggregate_id AND base_id = aggregate_id;
          |    IF NOT FOUND THEN
          |        IF expected_version = 0 THEN
          |            INSERT INTO AGGREGATES (id, type, base_order, base_id, base_version) VALUES (aggregate_id, aggregate_type, 1, aggregate_id, 0);
          |            current_version := 0;
          |        ELSE
          |	           RAISE EXCEPTION 'aggregate not found, id %, aggregate_type %', aggregate_id, aggregate_type;
          |        END IF;
          |    END IF;
          |    IF current_version != expected_version THEN
          |  	     RAISE EXCEPTION 'Concurrent aggregate modification exception, command id %, user id %, aggregate id %, expected version %, current_version %, event_type %, event %', command_id, user_id, aggregate_id, expected_version, current_version, event_type, event;
          |    END IF;
          |    SELECT NEXTVAL('events_seq') INTO event_id;
          |    INSERT INTO events (id, command_id, user_id, aggregate_id, event_time, version, event_type, event) VALUES (event_id, command_id, user_id, aggregate_id, event_time, current_version + 1, event_type, event);
          |    INSERT INTO events_to_publish (event_id, aggregate_id, version, user_id, event_time) VALUES(event_id, aggregate_id, current_version + 1, user_id, event_time);
          |    UPDATE aggregates SET base_version = current_version + 1 WHERE id = aggregate_id AND base_id = aggregate_id;
          |    RETURN event_id;
          |END;
          |$$
          |LANGUAGE 'plpgsql' VOLATILE
        """.stripMargin).execute().apply()
  }

  private def createAddUndoEventFunction(): Unit = DB.autoCommit { implicit session =>
    SQL("""
          |CREATE OR REPLACE FUNCTION add_undo_event(command_id BIGINT, user_id BIGINT, _aggregate_id BIGINT, expected_version INT, aggregate_type VARCHAR(128), event_type VARCHAR(128), event_time TIMESTAMP, event VARCHAR(10240), undo_count INT)
          |RETURNS BIGINT AS
          |$$
          |DECLARE
          |    current_version INT;
          |    event_id BIGINT;
          |BEGIN
          | SELECT aggregates.base_version INTO current_version FROM aggregates WHERE id = _aggregate_id AND base_id = _aggregate_id;
          |    IF NOT FOUND THEN
          |        IF expected_version = 0 THEN
          |            RAISE EXCEPTION 'Cannot undo event for non existing aggregate';
          |        ELSE
          |	    RAISE EXCEPTION 'aggregate not found, id %, aggregate_type %', _aggregate_id, aggregate_type;
          |        END IF;
          |    END IF;
          |    IF current_version != expected_version THEN
          |	RAISE EXCEPTION 'Concurrent aggregate modification exception, command id %, user id %, aggregate id %, expected version %, current_version %, event_type %, event %', command_id, user_id, aggregate_id, expected_version, current_version, event_type, event;
          |    END IF;
          |    SELECT NEXTVAL('events_seq') INTO event_id;
          |    INSERT INTO noop_events (id, from_version) (select events.id, current_version + 1
          |     from events
          |     left join noop_events on events.id = noop_events.id
          |     where events.aggregate_id = _aggregate_id AND noop_events.id is null order by events.version desc limit undo_count);
          |    INSERT INTO noop_events(id, from_version) VALUES (event_id, current_version + 1);
          |    INSERT INTO events (id, command_id, user_id, aggregate_id, event_time, version, event_type, event) VALUES (event_id, command_id, user_id, _aggregate_id, event_time, current_version + 1, event_type, event);
          |    INSERT INTO events_to_publish (event_id, aggregate_id, version, user_id, event_time) VALUES(event_id, _aggregate_id, current_version + 1, user_id, event_time);
          |    UPDATE aggregates SET base_version = current_version + 1 WHERE id = _aggregate_id AND base_id = _aggregate_id;
          |    RETURN event_id;
          |END;
          |$$
          |LANGUAGE 'plpgsql' VOLATILE
        """.stripMargin).execute().apply()
  }

  private def createAddDuplicationEventFunction(): Unit = DB.autoCommit { implicit session =>
    SQL("""
          |CREATE OR REPLACE FUNCTION add_duplication_event(command_id BIGINT, user_id BIGINT, aggregate_id BIGINT, expected_version INT, aggregate_type VARCHAR(128), event_type VARCHAR(128), event_time TIMESTAMP, event VARCHAR(10240), _base_id BIGINT, _base_version INT)
          |RETURNS BIGINT AS
          |$$
          |DECLARE
          |    current_version INT;
          |    base_count INT;
          |    event_id BIGINT;
          |BEGIN
          |    SELECT aggregates.base_version INTO current_version FROM aggregates WHERE id = aggregate_id AND base_id = aggregate_id;
          |    IF NOT FOUND THEN
          |        IF expected_version != 0 THEN
          |          RAISE EXCEPTION 'Duplication event might occur only for non existing aggregate, so expected version need to be 0';
          |        ELSE
          |          INSERT INTO aggregates (id, type, base_order, base_id, base_version) (select aggregate_id, aggregate_type, base_order, base_id, base_version
          |            from aggregates
          |            where id = _base_id);
          |            current_version := 0;
          |          SELECT base_order INTO base_count FROM aggregates WHERE id = aggregate_id AND base_id = _base_id;
          |          INSERT INTO aggregates (id, type, base_order, base_id, base_version) VALUES (aggregate_id, aggregate_type, base_count + 1, aggregate_id, 0);
          |          UPDATE aggregates SET base_version = _base_version WHERE id = aggregate_id AND base_id = _base_id;
          |        END IF;
          |    ELSE
          |      RAISE EXCEPTION 'Duplication event might occur only for non existing aggregate, but such was found';
          |    END IF;
          |    SELECT NEXTVAL('events_seq') INTO event_id;
          |    INSERT INTO events (id, command_id, user_id, aggregate_id, event_time, version, event_type, event) VALUES (event_id, command_id, user_id, aggregate_id, event_time, current_version + 1, event_type, event);
          |    INSERT INTO events_to_publish (event_id, aggregate_id, version, user_id, event_time) VALUES(event_id, aggregate_id, current_version + 1, user_id, event_time);
          |    UPDATE aggregates SET base_version = current_version + 1 WHERE id = aggregate_id AND base_id = aggregate_id;
          |    RETURN event_id;
          |END;
          |$$
          |LANGUAGE 'plpgsql' VOLATILE
        """.stripMargin).execute().apply()
  }
  
}

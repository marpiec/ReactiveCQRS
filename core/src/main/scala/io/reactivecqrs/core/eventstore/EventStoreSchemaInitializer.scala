package io.reactivecqrs.core.eventstore

import scalikejdbc._


class EventStoreSchemaInitializer  {

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
          event_type_version INT NOT NULL,
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
          version INT NOT NULL)
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
          |CREATE OR REPLACE FUNCTION add_event(command_id BIGINT, user_id BIGINT, aggregate_id BIGINT, expected_version INT, aggregate_type VARCHAR(128), event_type VARCHAR(128), event_type_version INT, event VARCHAR(10240))
          |RETURNS void AS
          |$$
          |DECLARE
          |    current_version int;
          |BEGIN
          | SELECT aggregates.base_version INTO current_version FROM aggregates WHERE id = aggregate_id AND base_id = aggregate_id;
          |    IF NOT FOUND THEN
          |        IF expected_version = 0 THEN
          |            INSERT INTO AGGREGATES (id, type, base_order, base_id, base_version) VALUES (aggregate_id, aggregate_type, 1, aggregate_id, 0);
          |            current_version := 0;
          |        ELSE
          |	    RAISE EXCEPTION 'aggregate not found, id %, aggregate_type %', aggregate_id, aggregate_type;
          |        END IF;
          |    END IF;
          |    IF current_version != expected_version THEN
          |	RAISE EXCEPTION 'Concurrent aggregate modification exception, command id %, user id %, aggregate id %, expected version %, current_version %, event_type %, event_type_version %, event %', command_id, user_id, aggregate_id, expected_version, current_version, event_type, event_type_version, event;
          |    END IF;
          |    INSERT INTO events (id, command_id, user_id, aggregate_id, event_time, version, event_type, event_type_version, event) VALUES (NEXTVAL('events_seq'), command_id, user_id, aggregate_id, current_timestamp, current_version + 1, event_type, event_type_version, event);
          |    INSERT INTO events_to_publish (event_id, aggregate_id, version) VALUES(CURRVAL('events_seq'), aggregate_id, current_version + 1);
          |    UPDATE aggregates SET base_version = current_version + 1 WHERE id = aggregate_id AND base_id = aggregate_id;
          |END;
          |$$
          |LANGUAGE 'plpgsql' VOLATILE
        """.stripMargin).execute().apply()
  }

  private def createAddUndoEventFunction(): Unit = DB.autoCommit { implicit session =>
    SQL("""
          |CREATE OR REPLACE FUNCTION add_undo_event(command_id BIGINT, user_id BIGINT, _aggregate_id BIGINT, expected_version INT, aggregate_type VARCHAR(128), event_type VARCHAR(128), event_type_version INT, event VARCHAR(10240), undo_count INT)
          |RETURNS void AS
          |$$
          |DECLARE
          |    current_version int;
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
          |	RAISE EXCEPTION 'Concurrent aggregate modification exception, command id %, user id %, aggregate id %, expected version %, current_version %, event_type %, event_type_version %, event %', command_id, user_id, aggregate_id, expected_version, current_version, event_type, event_type_version, event;
          |    END IF;
          |    INSERT INTO noop_events (id, from_version) (select events.id, current_version + 1
          |     from events
          |     left join noop_events on events.id = noop_events.id
          |     where events.aggregate_id = _aggregate_id AND noop_events.id is null order by events.version desc limit undo_count);
          |    INSERT INTO noop_events(id, from_version) VALUES (NEXTVAL('events_seq'), current_version + 1);
          |    INSERT INTO events (id, command_id, user_id, aggregate_id, event_time, version, event_type, event_type_version, event) VALUES (CURRVAL('events_seq'), command_id, user_id, _aggregate_id, current_timestamp, current_version + 1, event_type, event_type_version, event);
          |    INSERT INTO events_to_publish (event_id, aggregate_id, version) VALUES(CURRVAL('events_seq'), _aggregate_id, current_version + 1);
          |    UPDATE aggregates SET base_version = current_version + 1 WHERE id = _aggregate_id AND base_id = _aggregate_id;
          |END;
          |$$
          |LANGUAGE 'plpgsql' VOLATILE
        """.stripMargin).execute().apply()
  }

  private def createAddDuplicationEventFunction(): Unit = DB.autoCommit { implicit session =>
    SQL("""
          |CREATE OR REPLACE FUNCTION add_duplication_event(command_id BIGINT, user_id BIGINT, aggregate_id BIGINT, expected_version INT, aggregate_type VARCHAR(128), event_type VARCHAR(128), event_type_version INT, event VARCHAR(10240), _base_id BIGINT, _base_version INT)
          |RETURNS void AS
          |$$
          |DECLARE
          |    current_version int;
          |    base_count int;
          |BEGIN
          |    SELECT aggregates.base_version INTO current_version FROM aggregates WHERE id = aggregate_id AND base_id = aggregate_id;
          |    IF NOT FOUND THEN
          |        IF expected_version != 0 THEN
          |          RAISE EXCEPTION 'Duplication event might occur only for non existing aggregate, so expected version need to be 0';
          |        ELSE
          |          INSERT INTO AGGREGATES (id, type, base_order, base_id, base_version) (select aggregate_id, aggregate_type, base_order, base_id, base_version
          |            from AGGREGATES
          |            where id = _base_id);
          |            current_version := 0;
          |          SELECT base_order INTO base_count FROM aggregates WHERE id = aggregate_id AND base_id = _base_id;
          |          INSERT INTO AGGREGATES (id, type, base_order, base_id, base_version) VALUES (aggregate_id, aggregate_type, base_count + 1, aggregate_id, 0);
          |          UPDATE aggregates SET base_version = _base_version WHERE id = aggregate_id AND base_id = _base_id;
          |        END IF;
          |    ELSE
          |      RAISE EXCEPTION 'Duplication event might occur only for non existing aggregate, but such was found';
          |    END IF;
          |    INSERT INTO events (id, command_id, user_id, aggregate_id, event_time, version, event_type, event_type_version, event) VALUES (NEXTVAL('events_seq'), command_id, user_id, aggregate_id, current_timestamp, current_version + 1, event_type, event_type_version, event);
          |    INSERT INTO events_to_publish (event_id, aggregate_id, version) VALUES(CURRVAL('events_seq'), aggregate_id, current_version + 1);
          |    UPDATE aggregates SET base_version = current_version + 1 WHERE id = aggregate_id AND base_id = aggregate_id;
          |END;
          |$$
          |LANGUAGE 'plpgsql' VOLATILE
        """.stripMargin).execute().apply()
  }
  
}

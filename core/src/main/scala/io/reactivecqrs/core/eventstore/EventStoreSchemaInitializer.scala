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
          event_type VARCHAR(128) NOT NULL,
          event_type_version INT NOT NULL,
          event VARCHAR(10240) NOT NULL)
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
          id BIGINT NOT NULL PRIMARY KEY,
          type VARCHAR(128) NOT NULL,
          base_id BIGINT NOT NULL,
          base_version INT NOT NULL)
      """.execute().apply()
  }

  private def createEventsSequence() = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE events_seq""".execute().apply()
  }

  private def createAddEventFunction(): Unit = DB.autoCommit { implicit session =>
    SQL("""
          |CREATE OR REPLACE FUNCTION add_event(command_id bigint, user_id bigint, aggregate_id bigint, expected_version INT, aggregate_type VARCHAR(128), event_type VARCHAR(128), event_type_version INT, event VARCHAR(10240))
          |RETURNS void AS
          |$$
          |DECLARE
          |    current_version int;
          |BEGIN
          | SELECT aggregates.base_version INTO current_version FROM aggregates WHERE id = aggregate_id AND base_id = aggregate_id;
          |    IF NOT FOUND THEN
          |        IF expected_version = 0 THEN
          |            INSERT INTO AGGREGATES (id, type, base_id, base_version) VALUES (aggregate_id, aggregate_type, aggregate_id, 0);
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
          |CREATE OR REPLACE FUNCTION add_undo_event(command_id bigint, user_id bigint, _aggregate_id bigint, expected_version INT, aggregate_type VARCHAR(128), event_type VARCHAR(128), event_type_version INT, event VARCHAR(10240), undo_count INT)
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

}

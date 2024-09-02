package io.reactivecqrs.core.eventbus

import scalikejdbc._


class EventBusSchemaInitializer {

   def initSchema(): Unit = {
     createEventsToRouteTable()
     createEventsToRouteSequence()
      createEventsToRouteIndex()
   }


   private def createEventsToRouteTable() = DB.autoCommit { implicit session =>
     sql"""
         CREATE TABLE IF NOT EXISTS events_to_route (
           id BIGINT NOT NULL PRIMARY KEY,
           aggregate_id BIGINT NOT NULL,
           version INT NOT NULL,
           message_time TIMESTAMP NOT NULL,
           subscriber VARCHAR(256) NOT NULL,
           message_type VARCHAR(256) NOT NULL,
           message bytea NOT NULL)
       """.execute().apply()

   }


  private def createEventsToRouteIndex() = DB.autoCommit { implicit session =>
    sql"""CREATE UNIQUE INDEX IF NOT EXISTS events_to_route_idx ON events_to_route (aggregate_id, version, subscriber)""".execute().apply()
  }


  private def createEventsToRouteSequence() = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE IF NOT EXISTS events_to_route_seq""".execute().apply()
  }

}

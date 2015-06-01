package io.reactivecqrs.core.db.eventbus

import scalikejdbc._


class EventBusSchemaInitializer {

   def initSchema(): Unit = {
     createMessagesToSendTable()
     try {
      createMessagesToSendSequence()
     } catch {
       case e: Exception => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
     }
   }


   private def createMessagesToSendTable() = DB.autoCommit { implicit session =>
     sql"""
         CREATE TABLE IF NOT EXISTS messages_to_send (
           id BIGINT NOT NULL PRIMARY KEY,
           aggregate_id BIGINT NOT NULL,
           version INT NOT NULL,
           message_time TIMESTAMP NOT NULL,
           subscriber VARCHAR(256) NOT NULL,
           message bytea NOT NULL)
       """.execute().apply()

   }

  private def createMessagesToSendSequence() = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE messages_to_send_seq""".execute().apply()
  }

}

package io.reactivecqrs.core.db.eventbus

import scalikejdbc._


class EventBusSchemaInitializer {

   def initSchema(): Unit = {
     createMessagesToSendTable()
     createMessagesToSendSequence()
   }


   private def createMessagesToSendTable() = DB.autoCommit { implicit session =>
     sql"""
         CREATE TABLE IF NOT EXISTS messages_to_send (
           id BIGINT NOT NULL PRIMARY KEY,
           message_time TIMESTAMP NOT NULL,
           subscriber VARCHAR(256) NOT NULL,
           message bytea NOT NULL)
       """.execute().apply()

   }

  private def createMessagesToSendSequence() = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE messages_to_send_seq""".execute().apply()
  }

}

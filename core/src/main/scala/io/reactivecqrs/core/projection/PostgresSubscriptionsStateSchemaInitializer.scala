package io.reactivecqrs.core.projection

import scalikejdbc._

class PostgresSubscriptionsStateSchemaInitializer {
  def initSchema(): Unit = {
    createSubscriptionsTable()
    try {
      createSubscriptionsSequence()
    } catch {
      case e: Exception => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
    }
  }


  private def createSubscriptionsTable() = DB.autoCommit { implicit session =>
    sql"""
         CREATE TABLE IF NOT EXISTS subscriptions (
           id BIGINT NOT NULL PRIMARY KEY,
           name VARCHAR(256) NOT NULL,
           aggregate_type VARCHAR(256) NOT NULL,
           subscription_type VARCHAR(32) NOT NULL,
           last_event_id BIGINT NOT NULL)
       """.execute().apply()

  }

  private def createSubscriptionsSequence() = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE subscriptions_seq""".execute().apply()
  }
}

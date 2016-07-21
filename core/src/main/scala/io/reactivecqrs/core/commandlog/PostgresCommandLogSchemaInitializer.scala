package io.reactivecqrs.core.commandlog

import org.postgresql.util.PSQLException
import scalikejdbc._

class PostgresCommandLogSchemaInitializer {

  def initSchema(): Unit = {
    createCommandLogTable()
    try {
      createCommandLogSequence()
    } catch {
      case e: PSQLException => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
    }
  }

  private def createCommandLogTable() = DB.autoCommit { implicit session =>
    sql"""
        CREATE TABLE IF NOT EXISTS commands (
          id BIGINT NOT NULL PRIMARY KEY,
          command_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          aggregate_id BIGINT NOT NULL,
          command_time TIMESTAMP NOT NULL,
          expected_version INT NOT NULL,
          command_type VARCHAR(1024) NOT NULL,
          command TEXT NOT NULL)
      """.execute().apply()
  }

  private def createCommandLogSequence() = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE commands_seq""".execute().apply()
  }


}

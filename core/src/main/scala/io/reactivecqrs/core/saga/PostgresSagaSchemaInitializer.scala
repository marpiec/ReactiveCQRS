package io.reactivecqrs.core.saga

import org.postgresql.util.PSQLException
import scalikejdbc._

class PostgresSagaSchemaInitializer {
  def initSchema(): Unit = {
    createSagasTable()
    createSagaKeyIndex()
  }

  private def createSagasTable(): Unit = DB.autoCommit { implicit session =>
    sql"""
        CREATE TABLE IF NOT EXISTS sagas (
          name VARCHAR NOT NULL,
          saga_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          respond_to VARCHAR(256) NOT NULL,
          creation_time TIMESTAMP NOT NULL,
          phase VARCHAR(16) NOT NULL,
          step INT NOT NULL,
          update_time TIMESTAMP NOT NULL,
          saga_order TEXT NOT NULL,
          order_type_id INT NOT NULL)
      """.execute()
  }

  // One row per (name, saga_id) (F34); the index also serves loading the sagas of one name. It is built
  // CONCURRENTLY, so a large table stays writable, and an invalid index left by an interrupted build is rebuilt.
  private def createSagaKeyIndex(): Unit = DB.autoCommit { implicit session =>
    val valid = sql"""SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'sagas_name_saga_id_idx'"""
      .map(_.boolean(1)).single()
    if (valid.contains(false)) {
      sql"""DROP INDEX CONCURRENTLY IF EXISTS sagas_name_saga_id_idx""".execute()
    }
    if (!valid.contains(true)) {
      try {
        sql"""CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS sagas_name_saga_id_idx ON sagas (name, saga_id)""".execute()
      } catch {
        case e: PSQLException => throw new IllegalStateException("Cannot create the unique index on sagas (name, saga_id); " +
          "if the table holds duplicate rows, remove them and start again", e)
      }
    }
  }
}

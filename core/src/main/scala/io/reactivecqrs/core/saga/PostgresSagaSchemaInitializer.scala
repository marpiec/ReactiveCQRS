package io.reactivecqrs.core.saga

import scalikejdbc._

class PostgresSagaSchemaInitializer {
  def initSchema(): Unit = {
    createSagasTable()
  }

  private def createSagasTable(): Unit = DB.autoCommit { implicit session =>
    sql"""
        CREATE TABLE IF NOT EXISTS sagas (
          name VARCHAR NOT NULL,
          saga_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          respond_to VARCHAR(256) NOT NULL,
          creation_time TIMESTAMP,
          phase VARCHAR(16) NOT NULL,
          update_time TIMESTAMP,
          saga_order bytea NOT NULL)
      """.execute().apply()
  }
}

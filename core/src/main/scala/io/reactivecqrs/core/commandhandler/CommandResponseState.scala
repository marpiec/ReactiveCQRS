package io.reactivecqrs.core.commandhandler

import io.mpjsons.MPJsons
import io.reactivecqrs.api.CustomCommandResponse
import io.reactivecqrs.core.types.TypesNamesState
import scalikejdbc._


trait CommandResponseState {
  def storeResponse(key: String, response: CustomCommandResponse[_])(implicit session: DBSession): Unit
  def responseByKey(key: String): Option[CustomCommandResponse[_]]
}

class MemoryCommandResponseState extends CommandResponseState {

  private var cache = Map[String, CustomCommandResponse[_]]()

  override def storeResponse(key: String, response: CustomCommandResponse[_])(implicit session: DBSession): Unit = {
    cache += key -> response
  }

  override def responseByKey(key: String): Option[CustomCommandResponse[_]] = {
    cache.get(key)
  }
}

class PostgresCommandResponseState(mpjsons: MPJsons, typesNamesState: TypesNamesState) extends CommandResponseState {

  def initSchema(): PostgresCommandResponseState = {
    createCommandResponseTable()
    createCommandResponseSequence()
    createKeyIndex()
    this
  }

  private def createCommandResponseTable(): Unit = DB.autoCommit { implicit session =>
    sql"""
         CREATE TABLE IF NOT EXISTS commands_responses (
           id BIGINT NOT NULL PRIMARY KEY,
           key VARCHAR(256) NOT NULL,
           handling_timestamp TIMESTAMP,
           response TEXT NOT NULL,
           response_type_id SMALLINT NOT NULL)
       """.execute()
  }

  private def createCommandResponseSequence() = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE IF NOT EXISTS commands_responses_seq""".execute()
  }

  // Idempotency keys are global (sagaId|step), so the key alone is unique. The previous index was
  // declared on (aggregate_id, key), a column this table never had, so it could not be created.
  private def createKeyIndex() = DB.autoCommit { implicit session =>
    sql"""DROP INDEX IF EXISTS commands_responses_key_idx""".execute()
    sql"""CREATE UNIQUE INDEX IF NOT EXISTS commands_responses_unique_key_idx ON commands_responses (key)""".execute()
  }

  override def storeResponse(key: String, response: CustomCommandResponse[_])(implicit session: DBSession): Unit = {
    sql"""INSERT INTO commands_responses (id, key, handling_timestamp, response, response_type_id) VALUES (nextval('commands_responses_seq'), ?, current_timestamp, ?, ?)"""
      .bind(key, mpjsons.serialize(response, response.getClass.getName), typesNamesState.typeIdByClass(response.getClass)).update()
  }

  override def responseByKey(key: String): Option[CustomCommandResponse[_]] = {
    DB.readOnly { implicit session =>
      sql"""SELECT response, response_type_id FROM commands_responses WHERE key = ?"""
        .bind(key).map(rs => mpjsons.deserialize[CustomCommandResponse[_]](rs.string(1), typesNamesState.classNameById(rs.short(2)))).single()
    }
  }
}
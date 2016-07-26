package io.reactivecqrs.core.commandhandler

import io.mpjsons.MPJsons
import io.reactivecqrs.api.CustomCommandResponse
import io.reactivecqrs.core.types.TypesNamesState
import org.postgresql.util.PSQLException
import scalikejdbc._


trait CommandResponseState {
  def storeResponse(key: String, response: CustomCommandResponse[_])(implicit session: DBSession): Unit
  def responseByKey(key: String): Option[CustomCommandResponse[_]]
}

class MemoryCommandResponseState extends CommandResponseState {

  var cache = Map[String, CustomCommandResponse[_]]()

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
    try {
      createCommandResponseSequence()
    } catch {
      case e: PSQLException => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
    }
    try {
      createKeyIndex()
    } catch {
      case e: PSQLException => () //ignore until CREATE UNIQUE INDEX IF NOT EXISTS is available in PostgreSQL
    }
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
       """.execute().apply()
  }

  private def createCommandResponseSequence() = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE commands_responses_seq""".execute().apply()
  }

  private def createKeyIndex() = DB.autoCommit { implicit session =>
    sql"""CREATE UNIQUE INDEX commands_responses_key_idx ON commands_responses (aggregate_id, key)""".execute().apply()
  }

  override def storeResponse(key: String, response: CustomCommandResponse[_])(implicit session: DBSession): Unit = {
    sql"""INSERT INTO commands_responses (id, key, handling_timestamp, response, response_type_id) VALUES (nextval('commands_responses_seq'), ?, current_timestamp, ?, ?)"""
      .bind(key, mpjsons.serialize(response, response.getClass.getName), typesNamesState.typeIdByClass(response.getClass)).executeUpdate().apply()
  }

  override def responseByKey(key: String): Option[CustomCommandResponse[_]] = {
    DB.readOnly { implicit session =>
      sql"""SELECT response, response_type_id FROM commands_responses WHERE key = ?"""
        .bind(key).map(rs => mpjsons.deserialize[CustomCommandResponse[_]](rs.string(1), typesNamesState.classNameById(rs.short(2)))).single.apply()
    }
  }
}
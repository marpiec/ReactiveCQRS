package io.reactivecqrs.core.commandlog

import io.mpjsons.MPJsons
import scalikejdbc._
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.types.TypesNamesState
import org.postgresql.util.PSQLException

class PostgresCommandLogState(mpjsons: MPJsons, typesNamesState: TypesNamesState) extends CommandLogState {


  def initSchema(): PostgresCommandLogState = {
    createCommandLogTable()
    try {
      createCommandLogSequence()
    } catch {
      case e: PSQLException => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
    }
    this
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
          command_type_id SMALLINT NOT NULL,
          command TEXT NOT NULL)
      """.execute().apply()
  }

  private def createCommandLogSequence() = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE commands_seq""".execute().apply()
  }


  def storeFirstCommand(commandId: CommandId, aggregateId: AggregateId, command: FirstCommand[_, _ <: CustomCommandResponse[_]]): Unit = {

    val c = command match {
      case c: FirstCommandLogTransform[_, _] => c.transform()
      case cc => cc
    }

    storeAnyCommand(commandId, c.userId, aggregateId, 0, c.getClass.getName, mpjsons.serialize(c, c.getClass.getName))
  }

  def storeCommand(commandId: CommandId, aggregateId: AggregateId, command: Command[_, _ <: CustomCommandResponse[_]]): Unit = {
    val c = command match {
      case c: CommandLogTransform[_, _] => c.transform()
      case cc => cc
    }

    storeAnyCommand(commandId, c.userId, aggregateId, c.expectedVersion.asInt, c.getClass.getName, mpjsons.serialize(c, c.getClass.getName))
  }

  def storeConcurrentCommand(commandId: CommandId, aggregateId: AggregateId, command: ConcurrentCommand[_, _ <: CustomCommandResponse[_]]): Unit = {

    val c = command match {
      case c: ConcurrentCommandLogTransform[_, _] => c.transform()
      case cc => cc
    }

    storeAnyCommand(commandId, c.userId, aggregateId, -1, c.getClass.getName, mpjsons.serialize(c, c.getClass.getName))
  }


  private def storeAnyCommand(commandId: CommandId, userId: UserId, aggregateId: AggregateId, expectedVersion: Int, commandType: String, commandJson: String): Unit = {
    DB.autoCommit { implicit session =>
      sql"""INSERT INTO commands (id, command_id, user_id, aggregate_id, command_time, expected_version, command_type_id, command)
          |VALUES (NEXTVAL('commands_seq'), ?, ?, ?, current_timestamp, ?, ?, ?)""".stripMargin
        .bind(commandId.asLong, userId.asLong, aggregateId.asLong, expectedVersion, typesNamesState.typeIdByClassName(commandType), commandJson).executeUpdate().apply()
      }
  }

}

package io.reactivecqrs.core.commandlog

import io.mpjsons.MPJsons
import scalikejdbc._
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}

class PostgresCommandLogState(mpjsons: MPJsons) extends CommandLogState {

  def initSchema(): Unit = {
    new PostgresCommandLogSchemaInitializer().initSchema()
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
      sql"""INSERT INTO commands (id ,command_id ,user_id ,aggregate_id ,command_time ,expected_version ,command_type ,command)
          |VALUES (NEXTVAL('commands_seq'), ?, ?, ?, current_timestamp, ?, ?, ?)""".stripMargin
        .bind(commandId.asLong, userId.asLong, aggregateId.asLong, expectedVersion, commandType, commandJson).executeUpdate().apply()
      }
  }

}

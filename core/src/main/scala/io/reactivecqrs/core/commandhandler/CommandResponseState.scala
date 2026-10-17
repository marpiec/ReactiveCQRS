package io.reactivecqrs.core.commandhandler

import io.reactivecqrs.core.util.SchemaLock

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api.{CustomCommandResponse, CustomSuccessResponse, SuccessResponse}
import io.reactivecqrs.core.types.TypesNamesState
import scalikejdbc._

import scala.concurrent.duration._

/** Another copy of an idempotent command already stored its response under this key. */
class IdempotencyKeyTakenException(val key: String) extends Exception("Idempotency key already used: " + key) {
  override def fillInStackTrace(): Throwable = this
}

/** Responses of idempotent commands, by idempotency key. Use it through `Idempotency`. */
trait CommandResponseState {
  /** Stores a response in the transaction that persists the command's events.
    * Throws `IdempotencyKeyTakenException` when the key already has a response. */
  def storeResponse(key: String, response: CustomCommandResponse[_])(implicit session: DBSession): Unit
  def responseByKey(key: String): Option[CustomCommandResponse[_]]
  /** Removes the responses older than the retention period and returns how many were removed. */
  def removeExpired(): Int
}

/** In-memory responses for tests. Not transactional: a response stays stored when the persist fails later. */
class MemoryCommandResponseState(retention: FiniteDuration = 30.days, clock: () => Instant = () => Instant.now) extends CommandResponseState {

  private var cache = Map[String, (CustomCommandResponse[_], Instant)]()

  override def storeResponse(key: String, response: CustomCommandResponse[_])(implicit session: DBSession): Unit = synchronized {
    if (cache.contains(key)) {
      throw new IdempotencyKeyTakenException(key)
    }
    cache += key -> (response, clock())
  }

  override def responseByKey(key: String): Option[CustomCommandResponse[_]] = synchronized {
    cache.get(key).map(_._1)
  }

  override def removeExpired(): Int = synchronized {
    val oldest = clock().minusMillis(retention.toMillis)
    val (expired, kept) = cache.partition(_._2._2.isBefore(oldest))
    cache = kept
    expired.size
  }
}

/**
  * Responses in `commands_responses`, unique by key. A `CustomSuccessResponse` is stored as its id and version plus
  * its info in the `info` columns, because its info type cannot be recovered from the response class (F64); the
  * info must therefore be of a class without type parameters, such as a case class.
  */
class PostgresCommandResponseState(mpjsons: MPJsons, typesNamesState: TypesNamesState, retention: FiniteDuration = 30.days) extends CommandResponseState {

  private val customSuccessClassName = classOf[CustomSuccessResponse[_]].getName

  def initSchema(): PostgresCommandResponseState = SchemaLock {
    createCommandResponseTable()
    createCommandResponseSequence()
    addInfoColumns()
    createKeyIndex()
    createHandlingTimestampIndex()
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

  private def addInfoColumns() = DB.autoCommit { implicit session =>
    sql"""ALTER TABLE commands_responses ADD COLUMN IF NOT EXISTS info TEXT, ADD COLUMN IF NOT EXISTS info_type_id SMALLINT""".execute()
  }

  // Idempotency keys are global (sagaId|step), so the key alone is unique. The previous index was
  // declared on (aggregate_id, key), a column this table never had, so it could not be created.
  private def createKeyIndex() = DB.autoCommit { implicit session =>
    sql"""DROP INDEX IF EXISTS commands_responses_key_idx""".execute()
    sql"""CREATE UNIQUE INDEX IF NOT EXISTS commands_responses_unique_key_idx ON commands_responses (key)""".execute()
  }

  private def createHandlingTimestampIndex() = DB.autoCommit { implicit session =>
    sql"""CREATE INDEX IF NOT EXISTS commands_responses_handling_timestamp_idx ON commands_responses (handling_timestamp)""".execute()
  }

  /** A JSON value with the id of its class. */
  private case class Stored(json: String, typeId: Short)

  override def storeResponse(key: String, response: CustomCommandResponse[_])(implicit session: DBSession): Unit = {
    val (stored, info) = response match {
      case CustomSuccessResponse(aggregateId, aggregateVersion, info) =>
        (Stored(mpjsons.serialize(SuccessResponse(aggregateId, aggregateVersion), classOf[SuccessResponse].getName),
          typesNamesState.typeIdByClassName(customSuccessClassName)), Option(info).map(i => storedInfo(i.asInstanceOf[AnyRef])))
      case _ =>
        (Stored(mpjsons.serialize(response, response.getClass.getName), typesNamesState.typeIdByClass(response.getClass)), None)
    }
    // ON CONFLICT waits for a concurrent copy's transaction, so a taken key is seen once that copy committed.
    val inserted = sql"""INSERT INTO commands_responses (id, key, handling_timestamp, response, response_type_id, info, info_type_id)
                         VALUES (nextval('commands_responses_seq'), ?, current_timestamp, ?, ?, ?, ?) ON CONFLICT (key) DO NOTHING"""
      .bind(key, stored.json, stored.typeId, info.map(_.json), info.map(_.typeId)).update()
    if (inserted == 0) {
      throw new IdempotencyKeyTakenException(key)
    }
  }

  private def storedInfo(info: AnyRef): Stored = {
    try {
      Stored(mpjsons.serialize(info, info.getClass.getName), typesNamesState.typeIdByClass(info.getClass))
    } catch {
      case e: Exception => throw new IllegalArgumentException("The response info of an idempotent command must be of a class " +
        "without type parameters, such as a case class; " + info.getClass.getName + " cannot be stored", e)
    }
  }

  override def responseByKey(key: String): Option[CustomCommandResponse[_]] = {
    DB.readOnly { implicit session =>
      sql"""SELECT response, response_type_id, info, info_type_id FROM commands_responses WHERE key = ?"""
        .bind(key).map(rs => readResponse(rs.string(1), rs.short(2), rs.stringOpt(3), rs.shortOpt(4))).single()
    }
  }

  /** Reads one stored row; also used by the stored-data check. */
  private[core] def readResponse(json: String, typeId: Short, info: Option[String], infoTypeId: Option[Short]): CustomCommandResponse[_] = {
    val className = typesNamesState.classNameById(typeId)
    if (className == customSuccessClassName) {
      val success = mpjsons.deserialize[SuccessResponse](json, classOf[SuccessResponse].getName)
      val storedInfo: Any = info.zip(infoTypeId).map { case (i, t) => mpjsons.deserialize[AnyRef](i, typesNamesState.classNameById(t)) }.orNull
      CustomSuccessResponse[Any](success.aggregateId, success.aggregateVersion, storedInfo)
    } else {
      mpjsons.deserialize[CustomCommandResponse[_]](json, className)
    }
  }

  override def removeExpired(): Int = DB.autoCommit { implicit session =>
    sql"""DELETE FROM commands_responses WHERE handling_timestamp < current_timestamp - (? * INTERVAL '1 second')"""
      .bind(retention.toSeconds).update()
  }
}

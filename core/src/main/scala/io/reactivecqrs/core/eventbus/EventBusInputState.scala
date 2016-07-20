package io.reactivecqrs.core.eventbus

import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.projection.OptimisticLockingFailed
import scalikejdbc._

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

abstract class EventBusInputState {
  def lastPublishedEventForAggregate(aggregateId: AggregateId): AggregateVersion
  def eventPublished(aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion): Try[Unit]
}

class MemoryEventBusInputState extends EventBusInputState {
  
  var state = new mutable.HashMap[AggregateId, AggregateVersion]()

  override def lastPublishedEventForAggregate(aggregateId: AggregateId): AggregateVersion = {
    state.getOrElse(aggregateId, {
      state += aggregateId -> AggregateVersion.ZERO
      AggregateVersion.ZERO
    })
  }

  override def eventPublished(aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion): Try[Unit] = {
    if (state.get(aggregateId).contains(lastAggregateVersion)) {
      state += aggregateId -> aggregateVersion
      Success(())
    } else {
      Failure(new OptimisticLockingFailed)
    }
  }
}

class PostgresEventBusInputState extends EventBusInputState {

  def initSchema(): Unit = {
    createEventBusInputTable()
    try {
      createEventBusInputSequence()
    } catch {
      case e: Exception => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
    }
  }

  private def createEventBusInputTable() = DB.autoCommit { implicit session =>
    sql"""
       CREATE TABLE IF NOT EXISTS event_bus_input (
         id BIGINT NOT NULL PRIMARY KEY,
         aggregate_id BIGINT NOT NULL,
         aggregate_version INT NOT NULL)
     """.execute().apply()
  }

  private def createEventBusInputSequence() = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE event_bus_input_seq""".execute().apply()
  }

  override def lastPublishedEventForAggregate(aggregateId: AggregateId): AggregateVersion = {
    DB.localTx { session =>
      val versionOption = sql"""SELECT aggregate_version FROM event_bus_input WHERE aggregate_id = ?"""
        .bind(aggregateId.asLong).map(rs => AggregateVersion(rs.int(1))).single().apply()

      versionOption match {
        case Some(version) => version
        case None => addAggregateEntry(aggregateId)
      }
    }
  }

  private def addAggregateEntry(aggregateId: AggregateId)(implicit session: DBSession): AggregateVersion = {
    sql"""INSERT INTO aggregate_version (id, aggregate_id, aggregate_version) VALUES (nextval('event_bus_input_seq'), ?, 0)"""
      .bind(aggregateId.asLong).executeUpdate().apply()
    AggregateVersion.ZERO
  }

  override def eventPublished(aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion): Try[Unit] = {
    DB.localTx { session =>
      val rowsUpdated = sql"""UPDATE event_bus_input SET aggregate_version = ? WHERE aggregate_id = ? AND aggregate_version = ?"""
        .bind(aggregateVersion.asInt, aggregateId.asLong, lastAggregateVersion.asInt).map(rs => rs.int(1)).single().executeUpdate().apply()
      if (rowsUpdated == 1) {
        Success(())
      } else {
        Failure(new OptimisticLockingFailed) // TODO handle this
      }
    }
  }



}
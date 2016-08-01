package io.reactivecqrs.core.eventbus

import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.eventbus.PostgresEventBusState.CacheValue
import io.reactivecqrs.core.projection.OptimisticLockingFailed
import org.postgresql.util.PSQLException
import scalikejdbc._

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

abstract class EventBusState {
  def lastPublishedEventForAggregate(aggregateId: AggregateId): AggregateVersion
  def eventPublished(aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion): Try[Unit]
  def flushUpdates(): Try[Unit]
}

class MemoryEventBusState extends EventBusState {

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

  override def flushUpdates(): Try[Unit] = {
    Success(())
  }
}

object PostgresEventBusState {
  case class CacheValue(last: AggregateVersion, current: AggregateVersion)
}

class PostgresEventBusState extends EventBusState {

  def initSchema(): PostgresEventBusState = {
    createEventBusTable()

    try {
      createEventBusSequence()
    } catch {
      case e: PSQLException => () //ignore until CREATE SEQUENCE IF NOT EXISTS is available in PostgreSQL
    }
    try {
      createAggregateIdIndex()
    } catch {
      case e: PSQLException => () //ignore until CREATE UNIQUE INDEX IF NOT EXISTS is available in PostgreSQL
    }
    try {
      createAggregateIdVersionIndex()
    } catch {
      case e: PSQLException => () //ignore until CREATE UNIQUE INDEX IF NOT EXISTS is available in PostgreSQL
    }
    this
  }

  private def createEventBusTable() = DB.autoCommit { implicit session =>
    sql"""
       CREATE TABLE IF NOT EXISTS event_bus (
         id BIGINT NOT NULL PRIMARY KEY,
         aggregate_id BIGINT NOT NULL,
         aggregate_version INT NOT NULL)
     """.execute().apply()
  }

  private def createEventBusSequence() = DB.autoCommit { implicit session =>
    sql"""CREATE SEQUENCE event_bus_seq""".execute().apply()
  }

  private def createAggregateIdIndex() = DB.autoCommit { implicit session =>
    sql"""CREATE UNIQUE INDEX event_bus_agg_id_idx ON event_bus (aggregate_id)""".execute().apply()
  }

  private def createAggregateIdVersionIndex() = DB.autoCommit { implicit session =>
    sql"""CREATE UNIQUE INDEX event_bus_agg_id_version_idx ON event_bus (aggregate_id, aggregate_version)""".execute().apply()
  }

  override def lastPublishedEventForAggregate(aggregateId: AggregateId): AggregateVersion = {
    DB.localTx { implicit session =>
      val versionOption = sql"""SELECT aggregate_version FROM event_bus WHERE aggregate_id = ?"""
        .bind(aggregateId.asLong).map(rs => AggregateVersion(rs.int(1))).single().apply()

      versionOption match {
        case Some(version) => version
        case None => addAggregateEntry(aggregateId)
      }
    }
  }

  private def addAggregateEntry(aggregateId: AggregateId)(implicit session: DBSession): AggregateVersion = {
    sql"""INSERT INTO event_bus (id, aggregate_id, aggregate_version) VALUES (nextval('event_bus_seq'), ?, 0)"""
      .bind(aggregateId.asLong).executeUpdate().apply()
    AggregateVersion.ZERO
  }

  // aggregate id -> (base version, current version)
  private var aggregatesToUpdate = Map[AggregateId, CacheValue]()


  override def eventPublished(aggregateId: AggregateId, lastAggregateVersion: AggregateVersion, aggregateVersion: AggregateVersion): Try[Unit] = synchronized {

    aggregatesToUpdate.get(aggregateId) match {
      case None => aggregatesToUpdate += aggregateId -> CacheValue(lastAggregateVersion, aggregateVersion)
      case Some(CacheValue(last, current)) => aggregatesToUpdate += aggregateId -> CacheValue(last, aggregateVersion)
    }

    if(aggregatesToUpdate.size > 20) {
      flushUpdates()
    } else {
      Success(())
    }


//    DB.localTx { implicit session =>
//      val rowsUpdated = sql"""UPDATE event_bus SET aggregate_version = ? WHERE aggregate_id = ? AND aggregate_version = ?"""
//        .bind(aggregateVersion.asInt, aggregateId.asLong, lastAggregateVersion.asInt).map(rs => rs.int(1)).single().executeUpdate().apply()
//      if (rowsUpdated == 1) {
//        Success(())
//      } else {
//        Failure(new OptimisticLockingFailed) // TODO handle this
//      }
//    }
  }

//TODO handle optimistic locking!!!!
  override def flushUpdates(): Try[Unit] = synchronized {
    if(aggregatesToUpdate.nonEmpty) {
      try {
        DB.localTx { implicit session =>
          val params: Seq[Seq[Any]] = aggregatesToUpdate.toSeq.map {
            case (key, CacheValue(last, current))=> Seq(current.asInt, key.asLong, last.asInt)
          }

          aggregatesToUpdate = Map[AggregateId, CacheValue]()

          sql"""UPDATE event_bus SET aggregate_version = ? WHERE aggregate_id = ? AND aggregate_version = ?"""
            .batch(params: _*).apply()

          // TODO check if all updates occured

        }
      } catch {
        case e: Exception => e.printStackTrace(); throw e;
      }

    }
    Success(())
  }


}
package io.reactivecqrs.core.saga

import io.reactivecqrs.core.util.SchemaLock

import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.{SagaId, SpaceId, UserId}
import io.reactivecqrs.core.types.TypesNamesState
import org.slf4j.LoggerFactory
import scalikejdbc._

import scala.util.{Failure, Success, Try}

/** Sagas' progress in the table `sagas`. */
class PostgresSagaState(mpjsons: MPJsons, typesNamesState: TypesNamesState) extends SagaState {

  private val log = LoggerFactory.getLogger(classOf[PostgresSagaState])

  /** Creates the table and its index, when missing. */
  def initSchema(): Unit = SchemaLock {
    new PostgresSagaSchemaInitializer().initSchema()
  }

  override def createSaga(name: String, sagaId: SagaId, respondTo: String, order: SagaInternalOrder, spaceId: Option[SpaceId]): Unit = {
    DB.autoCommit { implicit session =>
      sql"""INSERT INTO sagas (name, saga_id, user_id, respond_to, creation_time, update_time, phase, step, saga_order, order_type_id, space_id)
            |VALUES (?, ?, ?, ?, current_timestamp, current_timestamp, ?, ?, ?, ?, ?)""".stripMargin
        .bind(name, sagaId.asLong, order.userId.asLong, respondTo, CONTINUES.name, 0,
          mpjsons.serialize(order, order.getClass.getName), typesNamesState.typeIdByClass(order.getClass), spaceId.map(_.asLong)).update()
    }
  }

  override def updateSaga(name: String, sagaId: SagaId, order: SagaInternalOrder, phase: SagaPhase, step: Int): Boolean = {
    DB.autoCommit { implicit session =>
      sql"""UPDATE sagas SET update_time = current_timestamp, saga_order = ?, order_type_id = ?, phase = ?, step = ? WHERE name = ? AND saga_id = ?"""
        .bind(mpjsons.serialize(order, order.getClass.getName), typesNamesState.typeIdByClass(order.getClass), phase.name, step, name, sagaId.asLong).update() == 1
    }
  }

  override def deleteSaga(name: String, sagaId: SagaId): Boolean = {
    DB.autoCommit { implicit session =>
      sql"""DELETE FROM sagas WHERE name = ? AND saga_id = ?"""
        .bind(name, sagaId.asLong).update() == 1
    }
  }

  // F151: a saga whose order no longer reads is logged and left out (StoredDataCheck lists such rows); the others load.
  def loadAllSagas(name: String, handler: (SagaId, UserId, String, SagaPhase, Int, SagaInternalOrder) => Unit): Unit = {
    DB.readOnly { implicit session =>
      sql"""SELECT saga_id, user_id, respond_to, phase, step, saga_order, order_type_id FROM sagas WHERE name = ?"""
        .bind(name)
        .foreach { rs =>
          val sagaId = SagaId(rs.long(1))
          Try(mpjsons.deserialize[SagaInternalOrder](rs.string(6), typesNamesState.classNameById(rs.short(7)))) match {
            case Success(order) => handler(sagaId, UserId(rs.long(2)), rs.string(3), SagaPhase.byName(rs.string(4)), rs.int(5), order)
            case Failure(e) => log.error("Saga " + name + " " + sagaId.asLong + " is not resumed: its stored order cannot be read", e)
          }
        }
    }
  }
}

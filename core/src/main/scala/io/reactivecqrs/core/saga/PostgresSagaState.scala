package io.reactivecqrs.core.saga

import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.{SagaId, UserId}
import io.reactivecqrs.core.types.TypesNamesState
import scalikejdbc._

class PostgresSagaState(mpjsons: MPJsons, typesNamesState: TypesNamesState) extends SagaState {

  def initSchema(): Unit = {
    new PostgresSagaSchemaInitializer().initSchema()
  }

  override def createSaga(name: String, sagaId: SagaId, respondTo: String, order: SagaInternalOrder): Unit = {
    DB.autoCommit { implicit session =>
      sql"""INSERT INTO sagas (name, saga_id, user_id, respond_to, creation_time, update_time, phase, step, saga_order, order_type_id)
            |VALUES (?, ?, ?, ?, current_timestamp, current_timestamp, ?, ?, ?, ?)""".stripMargin
        .bind(name, sagaId.asLong, order.userId.asLong, respondTo, CONTINUES.name, 0,
          mpjsons.serialize(order, order.getClass.getName), typesNamesState.typeIdByClass(order.getClass)).executeUpdate().apply()
    }
  }

  override def updateSaga(name: String, sagaId: SagaId, order: SagaInternalOrder, phase: SagaPhase, step: Int): Unit = {
    DB.autoCommit { implicit session =>
      sql"""UPDATE sagas SET update_time = current_timestamp, saga_order = ?, order_type_id = ?, phase = ?, step = ? WHERE name = ? AND saga_id = ?"""
        .bind(mpjsons.serialize(order, order.getClass.getName), typesNamesState.typeIdByClass(order.getClass), phase.name, step, name, sagaId.asLong).executeUpdate().apply()
    }
  }

  override def deleteSaga(name: String, sagaId: SagaId): Unit = {
    DB.autoCommit { implicit session =>
      sql"""DELETE FROM sagas WHERE name = ? AND saga_id = ?"""
        .bind(name, sagaId.asLong).executeUpdate().apply()
    }
  }

  def loadAllSagas(handler: (String, SagaId, UserId, String, SagaPhase, Int, SagaInternalOrder) => Unit): Unit = {
    DB.readOnly { implicit session =>
      sql"""SELECT name, saga_id, user_id, respond_to, phase, step, saga_order, order_type_id FROM sagas"""
        .foreach { rs =>
          handler(rs.string(1), SagaId(rs.long(2)), UserId(rs.long(3)), rs.string(4), SagaPhase.byName(rs.string(5)),
            rs.int(6), mpjsons.deserialize[SagaInternalOrder](rs.string(7), typesNamesState.classNameById(rs.short(8))))
        }
    }
  }
}

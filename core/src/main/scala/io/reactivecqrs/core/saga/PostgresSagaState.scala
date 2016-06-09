package io.reactivecqrs.core.saga

import akka.actor.ActorRef
import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.UserId
import scalikejdbc._

class PostgresSagaState(mpjsons: MPJsons) extends SagaState {

  def initSchema(): Unit = {
    new PostgresSagaSchemaInitializer().initSchema()
  }

  override def createSaga(name: String, sagaId: Long, respondTo: String, order: SagaInternalOrder): Unit = {
    DB.autoCommit { implicit session =>
      sql"""INSERT INTO sagas (name, saga_id, user_id, respond_to, creation_time, update_time, phase, saga_order, order_type)
            |VALUES (?, ?, ?, ?, current_timestamp, current_timestamp, ?, ?, ?)""".stripMargin
        .bind(name, sagaId, order.userId.asLong, respondTo, CONTINUES.name,
          mpjsons.serialize(order, order.getClass.getName), order.getClass.getName).executeUpdate().apply()
    }
  }

  override def updateSaga(name: String, sagaId: Long, order: SagaInternalOrder, phase: SagaPhase): Unit = {
    DB.autoCommit { implicit session =>
      sql"""UPDATE sagas SET update_time = current_timestamp, saga_order = ?, order_type = ?, phase = ? WHERE name = ? AND saga_id = ?"""
        .bind(mpjsons.serialize(order, order.getClass.getName), order.getClass.getName, phase.name, name, sagaId).executeUpdate().apply()
    }
  }

  override def deleteSaga(name: String, sagaId: Long): Unit = {
    DB.autoCommit { implicit session =>
      sql"""DELETE FROM sagas WHERE name = ? AND saga_id = ?"""
        .bind(name, sagaId).executeUpdate().apply()
    }
  }

  def loadAllSagas(handler: (String, Long, UserId, String, SagaPhase, SagaInternalOrder) => Unit): Unit = {
    DB.readOnly { implicit session =>
      sql"""SELECT name, saga_id, user_id, respond_to, phase, saga_order, order_type FROM sagas"""
        .foreach { rs =>
          handler(rs.string(1), rs.long(2), UserId(rs.long(3)), rs.string(4), SagaPhase.byName(rs.string(5)), mpjsons.deserialize[SagaInternalOrder](rs.string(6), rs.string(7)))
        }
    }
  }
}

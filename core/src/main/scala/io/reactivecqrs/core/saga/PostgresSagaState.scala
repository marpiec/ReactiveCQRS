package io.reactivecqrs.core.saga

import akka.actor.ActorRef
import akka.serialization.Serialization
import scalikejdbc._

class PostgresSagaState(serialization: Serialization) extends SagaState {

  def initSchema(): Unit = {
    new PostgresSagaSchemaInitializer().initSchema()
  }

  override def createSaga(name: String, sagaId: Long, respondTo: ActorRef, order: SagaInternalOrder): Unit = {
    DB.autoCommit { implicit session =>
      sql"""INSERT INTO sagas (name, saga_id, user_id, respond_to, creation_time, update_time, phase, saga_order)
            |VALUES (?, ?, ?, ?, current_timestamp, current_timestamp, ?, ?)""".stripMargin
        .bind(name, sagaId, order.userId.asLong, respondTo.path.toString, CONTINUES.name, serialization.serialize(order).get).executeUpdate().apply()
    }
  }

  override def updateSaga(name: String, sagaId: Long, order: SagaInternalOrder, phase: SagaPhase): Unit = {
    DB.autoCommit { implicit session =>
      sql"""UPDATE sagas SET update_time = current_timestamp, saga_order = ?, phase = ? WHERE name = ? AND saga_id = ?"""
        .bind(serialization.serialize(order).get, phase.name, name, sagaId).executeUpdate().apply()
    }
  }

  override def deleteSaga(name: String, sagaId: Long): Unit = {
    DB.autoCommit { implicit session =>
      sql"""DELETE FROM sagas WHERE name = ? AND saga_id = ?"""
        .bind(name, sagaId).executeUpdate().apply()
    }
  }
}

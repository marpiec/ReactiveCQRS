package io.reactivecqrs.core.saga

import io.reactivecqrs.core.util.SchemaLock

import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.{SagaId, SpaceId}
import io.reactivecqrs.core.types.TypesNamesState
import org.slf4j.LoggerFactory
import scalikejdbc._

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}
import io.reactivecqrs.core.util.SqlLogging.withoutValues
import io.reactivecqrs.core.util.Errors

/** Sagas' progress in the table `sagas`. */
class PostgresSagaState(mpjsons: MPJsons, typesNamesState: TypesNamesState) extends SagaState {

  private val log = LoggerFactory.getLogger(classOf[PostgresSagaState])

  /** Creates the table and its index, when missing. */
  def initSchema(): Unit = SchemaLock {
    new PostgresSagaSchemaInitializer().initSchema()
  }

  // F206: a create whose commit was not acknowledged is tried again into the existing row, which counts as stored.
  // F207: a key another saga has gives that saga, and its answer if it has one.
  override def createSaga(name: String, sagaId: SagaId, respondTo: String, order: SagaInternalOrder, spaceId: Option[SpaceId],
                          orderKey: Option[String]): SagaCreation = DB.autoCommit { implicit session =>
    val inserted = sql"""INSERT INTO sagas (name, saga_id, user_id, respond_to, creation_time, update_time, phase, step, saga_order, order_type_id,
                        |  space_id, order_key)
                        |VALUES (?, ?, ?, ?, current_timestamp, current_timestamp, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING""".stripMargin
      .bind(name, sagaId.asLong, order.userId.asLong, respondTo, CONTINUES.name, 0,
        mpjsons.serialize(order, order.getClass.getName), typesNamesState.typeIdByClass(order.getClass), spaceId.map(_.asLong), orderKey.orNull).update()
    if (inserted == 1) NewSaga else {
      sql"""SELECT saga_id, answer, answer_type_id FROM sagas WHERE name = $name AND (saga_id = ${sagaId.asLong} OR order_key = $orderKey)
           | ORDER BY saga_id = ${sagaId.asLong} DESC LIMIT 1""".stripMargin
        .map(rs => (SagaId(rs.long(1)), rs.stringOpt(2).zip(rs.intOpt(3)))).single() match {
        case Some((`sagaId`, _)) => NewSaga
        case Some((other, answer)) => SagaOfKey(other, answer.map { case (json, typeId) => readAnswer(json, typeId) })
        case None => throw new IllegalStateException("Saga " + name + " " + sagaId.asLong + " was neither stored nor found")
      }
    }
  }

  private def writtenAnswer(answer: Option[SagaResponse]): (Option[String], Option[Int]) =
    (answer.map(a => mpjsons.serialize(a, a.getClass.getName)), answer.map(a => typesNamesState.typeIdByClass(a.getClass).toInt))

  private def readAnswer(json: String, typeId: Int): SagaResponse = mpjsons.deserialize[SagaResponse](json, typesNamesState.classNameById(typeId.toShort))

  // F232: only a saga still at the phase and step `from` changes.
  private def at(from: (SagaPhase, Int)): SQLSyntax = sqls"AND phase = ${from._1.name} AND step = ${from._2}"

  override def updateSaga(name: String, sagaId: SagaId, order: SagaInternalOrder, phase: SagaPhase, step: Int,
                          from: (SagaPhase, Int), answer: Option[SagaResponse]): Boolean = DB.autoCommit { implicit session =>
    val (answerJson, answerType) = writtenAnswer(answer)
    sql"""UPDATE sagas SET update_time = current_timestamp, saga_order = ${mpjsons.serialize(order, order.getClass.getName)},
         |  order_type_id = ${typesNamesState.typeIdByClass(order.getClass)}, phase = ${phase.name}, step = $step,
         |  answer = coalesce($answerJson, answer), answer_type_id = coalesce($answerType, answer_type_id)
         | WHERE name = $name AND saga_id = ${sagaId.asLong} ${at(from)}""".stripMargin.update() == 1
  }

  override def deleteSaga(name: String, sagaId: SagaId, from: (SagaPhase, Int)): Boolean = DB.autoCommit { implicit session =>
    sql"""DELETE FROM sagas WHERE name = $name AND saga_id = ${sagaId.asLong} ${at(from)}""".update() == 1
  }

  override def finishSaga(name: String, sagaId: SagaId, answer: Option[SagaResponse], from: (SagaPhase, Int)): Boolean =
    DB.autoCommit { implicit session =>
      val (answerJson, answerType) = writtenAnswer(answer)
      sql"""UPDATE sagas SET update_time = current_timestamp, phase = ${FINISHED.name},
           |  answer = coalesce($answerJson, answer), answer_type_id = coalesce($answerType, answer_type_id)
           | WHERE name = $name AND saga_id = ${sagaId.asLong} ${at(from)}""".stripMargin.update() == 1
    }

  override def removeFinished(name: String, olderThan: FiniteDuration): Int = DB.autoCommit { implicit session =>
    sql"""DELETE FROM sagas WHERE name = $name AND phase = ${FINISHED.name} AND update_time < current_timestamp - (${olderThan.toSeconds} * INTERVAL '1 second')"""
      .update()
  }

  // F151: a saga whose order no longer reads is logged and left out (StoredDataCheck lists such rows); the others load.
  // F252: a row of a phase this version does not know is left out the same way, instead of failing the whole load.
  def loadAllSagas(name: String, handler: StoredSaga => Unit): Unit = {
    DB.readOnly { implicit session =>
      sql"""SELECT saga_id, user_id, respond_to, phase, step, saga_order, order_type_id, order_key FROM sagas WHERE name = ? AND phase <> ?"""
        .bind(name, FINISHED.name)
        .foreach { rs =>
          val sagaId = SagaId(rs.long(1))
          Try((mpjsons.deserialize[SagaInternalOrder](rs.string(6), typesNamesState.classNameById(rs.short(7))), SagaPhase.byName(rs.string(4)))) match {
            case Success((order, phase)) => handler(StoredSaga(sagaId, rs.string(3), phase, rs.int(5), order, rs.stringOpt(8)))
            case Failure(e) => log.error("Saga " + name + " " + sagaId.asLong + " is not resumed: its stored order or phase cannot be read", Errors.redacted(e))
          }
        }
    }
  }
}

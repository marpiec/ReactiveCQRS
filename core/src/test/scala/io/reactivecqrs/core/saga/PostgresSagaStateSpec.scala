package io.reactivecqrs.core.saga

import java.util.UUID

import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.{SagaId, UserId}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testkit.TestDatabase
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import scalikejdbc._

case class TestSagaOrder(userId: UserId, text: String) extends SagaOrder
case class TestSagaAnswer(text: String) extends SagaResponse

// F34, F36: the sagas table is keyed by (name, saga_id), and writes report whether the saga was there.
class PostgresSagaStateSpec extends AnyFeatureSpec with Matchers {

  TestDatabase.init()

  private val state = new PostgresSagaState(new MPJsons, new PostgresTypesNamesState().initSchema())
  state.initSchema()

  private def sagaName(): String = "PostgresSagaStateSpec-" + UUID.randomUUID()
  private val order = TestSagaOrder(UserId(1), "order")

  Feature("Writing saga progress") {

    Scenario("updating or deleting a saga that is not stored reports that it was not found (F36)") {
      val name = sagaName()

      state.updateSaga(name, SagaId(1), order, CONTINUES, 1) mustBe false
      state.deleteSaga(name, SagaId(1)) mustBe false
    }

    Scenario("updating and deleting a stored saga reports success") {
      val name = sagaName()
      state.createSaga(name, SagaId(1), "respondTo", order)

      state.updateSaga(name, SagaId(1), order, REVERTING, 2) mustBe true
      state.deleteSaga(name, SagaId(1)) mustBe true
    }
  }

  Feature("Loading the sagas of a name") {

    Scenario("F151 a saga whose order no longer reads is left out, and the others load") {
      val name = sagaName()
      state.createSaga(name, SagaId(1), "respondTo", order)
      state.createSaga(name, SagaId(2), "respondTo", TestSagaOrder(UserId(1), "soon unreadable"))
      DB.autoCommit { implicit session => sql"""UPDATE sagas SET saga_order = '{"broken' WHERE name = $name AND saga_id = 2""".update() }

      var loaded = Vector[Long]()
      state.loadAllSagas(name, saga => loaded :+= saga.sagaId.asLong)
      loaded mustBe Vector(1L)
    }

    Scenario("F252 a saga of a phase this version does not know is left out too, and the others load") {
      val name = sagaName()
      state.createSaga(name, SagaId(1), "respondTo", order)
      state.createSaga(name, SagaId(2), "respondTo", order)
      DB.autoCommit { implicit session => sql"""UPDATE sagas SET phase = 'paused' WHERE name = $name AND saga_id = 2""".update() }

      var loaded = Vector[Long]()
      state.loadAllSagas(name, saga => loaded :+= saga.sagaId.asLong)
      loaded mustBe Vector(1L)
    }
  }

  Feature("The sagas table") {

    Scenario("holds one row per saga name and id (F34); a create stored before counts as stored (F206)") {
      val name = sagaName()
      state.createSaga(name, SagaId(1), "respondTo", order) mustBe NewSaga

      state.createSaga(name, SagaId(1), "respondTo", order) mustBe NewSaga
      DB.readOnly { implicit session =>
        sql"SELECT count(*) FROM sagas WHERE name = $name".map(_.int(1)).single().get
      } mustBe 1
    }

    Scenario("F232 a write from a phase and step the saga is no longer at changes nothing") {
      val name = sagaName()
      state.createSaga(name, SagaId(1), "respondTo", order)
      state.updateSaga(name, SagaId(1), order, CONTINUES, 1, Some((CONTINUES, 0))) mustBe true

      state.updateSaga(name, SagaId(1), order, REVERTING, 1, Some((CONTINUES, 0))) mustBe false
      state.deleteSaga(name, SagaId(1), Some((CONTINUES, 0))) mustBe false
      state.finishSaga(name, SagaId(1), None, Some((CONTINUES, 0))) mustBe false
      state.deleteSaga(name, SagaId(1), Some((CONTINUES, 1))) mustBe true
    }

    Scenario("F207 a second saga of an order key gives the first, and its answer once it has one; finished sagas go after a while") {
      val name = sagaName()
      state.createSaga(name, SagaId(1), "respondTo", order, orderKey = Some("key-1")) mustBe NewSaga
      state.createSaga(name, SagaId(2), "respondTo", order, orderKey = Some("key-1")) mustBe SagaOfKey(SagaId(1), None)

      state.finishSaga(name, SagaId(1), Some(TestSagaAnswer("done")), Some((CONTINUES, 0))) mustBe true
      state.createSaga(name, SagaId(3), "respondTo", order, orderKey = Some("key-1")) mustBe SagaOfKey(SagaId(1), Some(TestSagaAnswer("done")))
      var loaded = 0
      state.loadAllSagas(name, _ => loaded += 1)
      loaded mustBe 0 // a finished saga is not run

      state.removeFinished(name, scala.concurrent.duration.Duration.Zero) mustBe 1
    }
  }
}

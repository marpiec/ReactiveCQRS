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
      state.loadAllSagas(name, (sagaId, _, _, _, _, _) => loaded :+= sagaId.asLong)
      loaded mustBe Vector(1L)
    }
  }

  Feature("The sagas table") {

    Scenario("holds one row per saga name and id (F34)") {
      val name = sagaName()
      state.createSaga(name, SagaId(1), "respondTo", order)

      an[Exception] must be thrownBy state.createSaga(name, SagaId(1), "respondTo", order)
      DB.readOnly { implicit session =>
        sql"SELECT count(*) FROM sagas WHERE name = $name".map(_.int(1)).single().get
      } mustBe 1
    }
  }
}

package io.reactivecqrs.testdomain.spec

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}

import io.mpjsons.MPJsons
import io.reactivecqrs.api.SagaStep
import io.reactivecqrs.api.id.{SagaId, SpaceId, UserId}
import io.reactivecqrs.core.saga.SagaRunner.{GetSagaStatus, LoadPendingSagas, SagaStatus}
import io.reactivecqrs.core.saga._
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.core.util.Backoff
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe
import scalikejdbc._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

case class Pay(userId: UserId, orderKey: String, amount: Int) extends IdempotentSagaOrder
case class Paid(amount: Int, runs: Int) extends SagaResponse
case class PayRefused(reason: String) extends SagaResponse

/** One step that pays; `failFirst` attempts fail as a command that timed out would, `refuse` refuses; `hold` waits, so
  * a repeat of the order can come meanwhile. Counts each start of a step and of a revert. */
class Payments(val name: String, failFirst: Int = 0, refuse: Boolean = false, hold: Option[CountDownLatch] = None,
               writeRetry: FiniteDuration = 20.millis)(implicit ec: ExecutionContext) extends SagaDefinition {
  val steps = new AtomicInteger()
  val reverts = new AtomicInteger()
  override val stepRetries: Backoff = Backoff(10.millis, 20.millis, maxAttempts = 4)
  override val persistRetries: Backoff = Backoff(writeRetry, writeRetry, maxAttempts = 3)
  override def handleOrder(step: SagaStep): ReceiveOrder = { case Pay(_, _, amount) =>
    val run = steps.incrementAndGet()
    Future {
      hold.foreach(_.await(10, TimeUnit.SECONDS))
      if (run <= failFirst) throw new IllegalStateException("no answer in time")
      if (refuse) SagaFailed(PayRefused("insufficient funds")) else SagaSucceded(Paid(amount, run))
    }
  }
  override def handleRevert(step: SagaStep): ReceiveRevert = { case _ => reverts.incrementAndGet(); Future.successful(SagaRevertSucceded) }
}

/** A saga state whose first loads fail, and whose progress writes fail a given number of times. */
class FaultySagaState(underlying: SagaState, loadFailures: Int, updateFailures: Int) extends SagaState {
  private val (loads, updates) = (new AtomicInteger, new AtomicInteger)
  override def createSaga(name: String, sagaId: SagaId, respondTo: String, order: SagaInternalOrder, spaceId: Option[SpaceId], orderKey: Option[String]) =
    underlying.createSaga(name, sagaId, respondTo, order, spaceId, orderKey)
  override def updateSaga(name: String, sagaId: SagaId, order: SagaInternalOrder, phase: SagaPhase, step: Int,
                          from: Option[(SagaPhase, Int)], answer: Option[SagaResponse]) =
    if (updates.incrementAndGet() <= updateFailures) throw new IllegalStateException("database unavailable")
    else underlying.updateSaga(name, sagaId, order, phase, step, from, answer)
  override def deleteSaga(name: String, sagaId: SagaId, from: Option[(SagaPhase, Int)]) = underlying.deleteSaga(name, sagaId, from)
  override def finishSaga(name: String, sagaId: SagaId, answer: Option[SagaResponse], from: Option[(SagaPhase, Int)]) =
    underlying.finishSaga(name, sagaId, answer, from)
  override def removeFinished(name: String, olderThan: FiniteDuration) = underlying.removeFinished(name, olderThan)
  override def loadAllSagas(name: String, handler: StoredSaga => Unit): Unit =
    if (loads.incrementAndGet() <= loadFailures) throw new IllegalStateException("database unavailable") else underlying.loadAllSagas(name, handler)
}

// Step 3 of the risk assessment of 1.0.0: sagas under faults (F205, F206, F207, F232).
class SagaFaultsSpec extends CommonSpec {

  TestDatabase.init()

  private implicit val ec: ExecutionContext = system.dispatcher
  private val sagaState = new PostgresSagaState(new MPJsons, new PostgresTypesNamesState().initSchema())
  sagaState.initSchema()
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val user = UserId(1)

  private def newName(): String = "Payments-" + System.nanoTime()
  private def newKey(): String = "order-" + System.nanoTime()
  private def runner(definition: SagaDefinition, state: SagaState = sagaState): ActorRef =
    system.actorOf(SagaRunner.props(definition, uidGenerator, state))
  private def rows(name: String): Seq[(String, Int)] = DB.readOnly(implicit session =>
    sql"SELECT phase, step FROM sagas WHERE name = $name ORDER BY saga_id".map(rs => (rs.string(1), rs.int(2))).list())

  Feature("A saga under faults") {

    Scenario("F205 a step that failed runs again with its key before the saga reverts, and goes on forward") {
      val payments = new Payments(newName(), failFirst = 2)

      runner(payments).askFor[SagaResponse](Pay(user, newKey(), 10)) mustBe Paid(10, 3)

      (payments.steps.get, payments.reverts.get) mustBe ((3, 0))
    }

    Scenario("F206 a failure is answered only once the reverting saga is stored, and a saga whose writes fail is never dropped") {
      val name = newName()
      val payments = new Payments(name, refuse = true, writeRetry = 200.millis)
      val caller = TestProbe()

      runner(payments, new FaultySagaState(sagaState, loadFailures = 0, updateFailures = 5)).tell(Pay(user, newKey(), 10), caller.ref)

      caller.expectNoMessage(600.millis) // five failed writes take a second: no answer before the move to REVERTING is stored
      caller.expectMsg(10.seconds, PayRefused("insufficient funds"))
      awaitAssert(payments.reverts.get mustBe 1, 5.seconds, 20.millis)
      awaitAssert(rows(name).map(_._1) mustBe Seq("finished"), 5.seconds, 20.millis) // keyed: kept with its answer
    }

    Scenario("F207 an order repeated with its key joins the saga under way, and after it gets the stored answer") {
      val (hold, name, k) = (new CountDownLatch(1), newName(), newKey())
      val payments = new Payments(name, hold = Some(hold))
      val sagas = runner(payments)
      val (first, second) = (TestProbe(), TestProbe())
      sagas.tell(Pay(user, k, 10), first.ref)
      awaitAssert(payments.steps.get mustBe 1, 5.seconds, 20.millis)
      sagas.tell(Pay(user, k, 10), second.ref)
      sagas.askFor[SagaStatus](GetSagaStatus).running mustBe 1
      hold.countDown()

      first.expectMsg(10.seconds, Paid(10, 1))
      second.expectMsg(10.seconds, Paid(10, 1))
      sagas.askFor[SagaResponse](Pay(user, k, 10)) mustBe Paid(10, 1)
      payments.steps.get mustBe 1
      rows(name).size mustBe 1
    }

    Scenario("F232 orders wait for the first load, and a load while a saga runs does not start it twice") {
      val (hold, name) = (new CountDownLatch(1), newName())
      val payments = new Payments(name, hold = Some(hold))
      val sagas = runner(payments, new FaultySagaState(sagaState, loadFailures = 2, updateFailures = 0))
      val caller = TestProbe()
      sagas.tell(Pay(user, newKey(), 10), caller.ref)
      awaitAssert(payments.steps.get mustBe 1, 10.seconds, 20.millis)

      sagas ! LoadPendingSagas
      Thread.sleep(300)
      hold.countDown()

      caller.expectMsg(10.seconds, Paid(10, 1))
      payments.steps.get mustBe 1
    }
  }
}

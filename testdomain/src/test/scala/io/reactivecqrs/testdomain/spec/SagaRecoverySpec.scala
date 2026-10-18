package io.reactivecqrs.testdomain.spec

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.{SagaId, SpaceId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.saga._
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{IdsPool, PostgresUidGenerator, UidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart.MultipleCartCreatorSaga.{CartsCreated, CreateMultipleCarts}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.core.util.Backoff
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.concurrent.duration._

/** A saga state whose writes fail a given number of times, as during a database outage. */
class FlakySagaState(underlying: SagaState, createFailures: Int, updateFailures: Int) extends SagaState {
  private val creates = new AtomicInteger(0)
  private val updates = new AtomicInteger(0)
  private val loads = new AtomicInteger(0)

  def updateAttempts: Int = updates.get
  def loadsOfPendingSagas: Int = loads.get

  override def createSaga(name: String, sagaId: SagaId, respondTo: String, order: SagaInternalOrder, spaceId: Option[SpaceId]) = {
    if (creates.incrementAndGet() <= createFailures) throw new IllegalStateException("database unavailable")
    underlying.createSaga(name, sagaId, respondTo, order, spaceId)
  }

  override def updateSaga(name: String, sagaId: SagaId, order: SagaInternalOrder, phase: SagaPhase, step: Int) = {
    if (updates.incrementAndGet() <= updateFailures) throw new IllegalStateException("database unavailable")
    underlying.updateSaga(name, sagaId, order, phase, step)
  }

  override def deleteSaga(name: String, sagaId: SagaId) = underlying.deleteSaga(name, sagaId)

  override def loadAllSagas(name: String, handler: (SagaId, UserId, String, SagaPhase, Int, SagaInternalOrder) => Unit): Unit = {
    loads.incrementAndGet()
    underlying.loadAllSagas(name, handler)
  }
}

// F35, F17, F75: a saga survives failing writes of its progress: they are retried, and a saga whose progress cannot be
// stored resumes from its last stored step after a restart (its steps are idempotent commands, H3).
class SagaRecoverySpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val sagaState = new PostgresSagaState(mpjsons, typesNamesState)
  sagaState.initSchema()

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  eventBus ! RegisterSubscribers(Nil)
  private val commandBus = system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore,
    new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema(), eventBus))

  private val userId = UserId(1)

  /** A cart-creating saga under its own name, so it never resumes sagas left by other specs. */
  private def newSaga(sagaName: String, state: SagaState, uids: ActorRef = uidGenerator): ActorRef =
    system.actorOf(SagaRunner.props(new MultipleCartCreatorSaga(commandBus)(system.dispatcher, Timeout(60.seconds)) {
      override val name: String = sagaName
      override val persistRetries: Backoff = Backoff(20.millis, 20.millis, maxAttempts = 3)
    }, uids, state))

  private def unique(text: String): String = text + " " + UUID.randomUUID()

  private def eventsNamed(name: String): Int = DB.readOnly { implicit session =>
    sql"SELECT count(*) FROM events WHERE event LIKE ${"%\"" + name + "\"%"}".map(_.int(1)).single().get
  }

  private def storedSagas(sagaName: String): Int = DB.readOnly { implicit session =>
    sql"SELECT count(*) FROM sagas WHERE name = $sagaName".map(_.int(1)).single().get
  }

  Feature("A saga whose progress fails to be stored") {

    Scenario("completes when the writes succeed again after a few failures") {
      val sagaName = unique("Saga")
      val cartName = unique("Cart")
      val saga = newSaga(sagaName, new FlakySagaState(sagaState, createFailures = 1, updateFailures = 2))

      val response = saga.askFor[SagaResponse](CreateMultipleCarts(userId, cartName, 3))

      response mustBe a[CartsCreated]
      response.asInstanceOf[CartsCreated].carts.size mustBe 3
      (1 to 3).map(i => eventsNamed(cartName + " " + i)) mustBe Seq(1, 1, 1)
      awaitAssert(storedSagas(sagaName) mustBe 0, 3.seconds, 100.millis)
    }

    Scenario("that cannot be stored at all is answered with a failure, and nothing is done") {
      val sagaName = unique("Saga")
      val cartName = unique("Cart")
      val saga = newSaga(sagaName, new FlakySagaState(sagaState, createFailures = Int.MaxValue, updateFailures = 0))

      val response = saga.askFor[SagaResponse](CreateMultipleCarts(userId, cartName, 3))

      response mustBe a[SagaFailureResponse]
      eventsNamed(cartName + " 1") mustBe 0
    }

    Scenario("resumes from its last stored step after a restart, without repeating the effects of that step") {
      Given("a saga that created its first cart but could not store that it did")
      val sagaName = unique("Saga")
      val cartName = unique("Cart")
      val failingState = new FlakySagaState(sagaState, createFailures = 0, updateFailures = Int.MaxValue)
      val caller = TestProbe()
      val firstRun = newSaga(sagaName, failingState)
      firstRun.tell(CreateMultipleCarts(userId, cartName, 3), caller.ref)
      awaitAssert(failingState.updateAttempts mustBe 3, 5.seconds, 50.millis)
      eventsNamed(cartName + " 1") mustBe 1

      When("the saga actor restarts with a working database")
      system.stop(firstRun)
      newSaga(sagaName, sagaState)

      Then("the saga finishes, the caller gets the result, and each cart exists once")
      caller.expectMsgType[CartsCreated](15.seconds).carts.size mustBe 3
      (1 to 3).map(i => eventsNamed(cartName + " " + i)) mustBe Seq(1, 1, 1)
      awaitAssert(storedSagas(sagaName) mustBe 0, 3.seconds, 100.millis)
    }
  }

  Feature("A saga order when no saga id can be had") {

    Scenario("is answered with a failure, and the saga actor keeps running without reloading its sagas (F75)") {
      val sagaName = unique("Saga")
      val state = new FlakySagaState(sagaState, createFailures = 0, updateFailures = 0)
      val failingSagaIds = system.actorOf(Props(new UidGeneratorActor(new PostgresUidGenerator("aggregates_uids_seq"),
        new PostgresUidGenerator("commands_uids_seq"), new UidGenerator {
        override def nextIdsPool: IdsPool = throw new IllegalStateException("sequence unavailable")
      })))
      val saga = newSaga(sagaName, state, failingSagaIds)
      awaitAssert(state.loadsOfPendingSagas mustBe 1, 3.seconds, 50.millis)

      val response = saga.askFor[SagaResponse](CreateMultipleCarts(userId, unique("Cart"), 1))

      response mustBe a[SagaFailureResponse]
      state.loadsOfPendingSagas mustBe 1
    }
  }
}

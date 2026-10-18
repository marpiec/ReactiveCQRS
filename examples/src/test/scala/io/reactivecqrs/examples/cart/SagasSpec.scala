package io.reactivecqrs.examples.cart

// Chapter 12's code, run against the test domain's carts and its saga: each block of the chapter is a region here,
// followed by the lines that check what the chapter says it does.

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.saga.SagaResponse
import io.reactivecqrs.core.util.Backoff
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testdomain.shoppingcart.MultipleCartCreatorSaga.{CartsCreated, CartsCreationFailure, CreateMultipleCarts}
import io.reactivecqrs.testkit.TestSystems
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

class SagasSpec extends CartSpec {

  private val alice = UserId(1)
  private val carts = commandBus

  // #sagaState
  import io.reactivecqrs.core.saga.PostgresSagaState

  val sagaState = new PostgresSagaState(mpjsons, typesNames)
  sagaState.initSchema()
  // #sagaState

  // #runner
  import io.reactivecqrs.core.saga.SagaRunner

  val cartsSaga: ActorRef = system.actorOf(SagaRunner.props(
    new MultipleCartCreatorSaga(carts)(system.dispatcher, Timeout(60.seconds)), uidGenerator, sagaState), "cartsSaga")
  // #runner

  private def nameOf(cartId: AggregateId): Option[String] =
    carts.askFor[Try[Aggregate[ShoppingCart]]](GetAggregate(cartId)).get.aggregateRoot.map(_.name)

  /** The carts created under `name`, read from their creation events. */
  private def createdAs(name: String): Seq[AggregateId] = DB.readOnly { implicit session =>
    sql"""SELECT e.aggregate_id FROM events e JOIN types_names t ON t.id = e.event_type_id
         | WHERE t.name = ${classOf[ShoppingCartCreated].getName} AND e.event::jsonb->>'name' = $name""".stripMargin
      .map(rs => AggregateId(rs.long(1))).list()
  }

  private def sagasInFlight: Long = DB.readOnly { implicit session =>
    sql"SELECT count(*) FROM sagas WHERE name = 'MultipleCartCreatorSaga'".map(_.long(1)).single().get
  }

  Feature("The test domain's saga") {

    Scenario("three carts made in three steps") {
      // #order
      import io.reactivecqrs.core.saga.SagaResponse
      import io.reactivecqrs.testdomain.shoppingcart.MultipleCartCreatorSaga.CreateMultipleCarts

      implicit val timeout: Timeout = Timeout(2.minutes) // long enough for the whole saga, not one step

      val answer: Future[SagaResponse] = (cartsSaga ? CreateMultipleCarts(alice, "Groceries", 3)).mapTo[SagaResponse]
      // #order
      Await.result(answer, timeout.duration) match {
        case CartsCreated(created) => created.map(c => nameOf(c.id)) mustBe List(Some("Groceries 3"), Some("Groceries 2"), Some("Groceries 1"))
        case refused => fail("Not created: " + refused)
      }
      awaitAssert(sagasInFlight mustBe 0L, 30.seconds, 200.millis)
    }

    Scenario("a cart refused: the caller hears of it at once, and the carts made before are deleted") {
      implicit val timeout: Timeout = Timeout(2.minutes)
      Await.result((cartsSaga ? CreateMultipleCarts(alice, "Groceries M", 5)).mapTo[SagaResponse], timeout.duration) mustBe
        CartsCreationFailure(List("Cannot add cart *M 4"))
      awaitAssert(Seq("Groceries M 1", "Groceries M 2", "Groceries M 3").flatMap(createdAs).map(nameOf) mustBe Seq(None, None, None),
        30.seconds, 200.millis)
      awaitAssert(sagasInFlight mustBe 0L, 30.seconds, 200.millis)
    }

    Scenario("progress writes are retried as the definition says; the chapter's rule is the default") {
      implicit val timeout: Timeout = Timeout(60.seconds)
      new PatientCartsSaga(carts)(system.dispatcher, timeout).persistRetries mustBe
        new MultipleCartCreatorSaga(carts)(system.dispatcher, timeout).persistRetries
    }

    Scenario("started for a cluster, the runner runs once and answers like any other") {
      val node = TestSystems.newSystem("sagaNode")
      try {
        val system = node
        // #start
        val cartsSaga: ActorRef = SagaRunner.start(system,
          new MultipleCartCreatorSaga(carts)(system.dispatcher, Timeout(60.seconds)), uidGenerator, sagaState)
        // #start
        implicit val timeout: Timeout = Timeout(2.minutes)
        Await.result((cartsSaga ? CreateMultipleCarts(alice, "Gifts", 1)).mapTo[SagaResponse], timeout.duration) mustBe
          a[CartsCreated]
      } finally Await.ready(node.terminate(), 30.seconds)
    }
  }
}

class PatientCartsSaga(bus: ActorRef)(implicit executionContext: ExecutionContext, timeout: Timeout)
  extends MultipleCartCreatorSaga(bus) {
  // #persistRetries
  override val persistRetries: Backoff = Backoff(100.millis, 30.seconds, maxAttempts = 10)
  // #persistRetries
}

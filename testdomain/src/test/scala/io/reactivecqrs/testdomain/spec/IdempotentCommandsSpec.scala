package io.reactivecqrs.testdomain.spec

import java.util.UUID

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SagaId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.{EventBusSubscriptionsManager, EventBusSubscriptionsManagerApi, EventsBusActor, MemoryEventBusState}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

// F9, F64, R7 (D12): an idempotent command takes effect once, however often and however concurrently it is sent.
class IdempotentCommandsSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor(new MemoryEventBusState,
    new EventBusSubscriptionsManagerApi(system.actorOf(Props(new EventBusSubscriptionsManager(0)))))))

  /** A command bus with its own actors and response state, as after a restart. */
  private def newCommandBus(): ActorRef = system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore,
    new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema(), eventBus, eventsReplayMode = false))

  private val commandBus = newCommandBus()

  private val userId = UserId(1)

  private def unique(text: String): String = text + " " + UUID.randomUUID()
  private def clientKey(): Option[ClientKey] = Some(ClientKey(UUID.randomUUID().toString))
  private def sagaStep(): Option[SagaStep] = Some(SagaStep(SagaId(System.nanoTime()), 1))

  /** Stored events whose JSON holds this (unique) name: how many times a command took effect. */
  private def eventsNamed(name: String): Int = DB.readOnly { implicit session =>
    sql"SELECT count(*) FROM events WHERE event LIKE ${"%\"" + name + "\"%"}".map(_.int(1)).single().get
  }

  private def sendConcurrently(times: Int, command: AnyRef): Seq[CustomCommandResponse[_]] = {
    import system.dispatcher
    implicit val timeout: Timeout = Timeout(15.seconds)
    Await.result(Future.sequence((1 to times).map(_ => (commandBus ? command).mapTo[CustomCommandResponse[_]])), 20.seconds)
  }

  private def newCart(): AggregateId = {
    val created: CustomCommandResponse[_] = commandBus ?? CreateShoppingCart(None, userId, unique("Cart"))
    created.asInstanceOf[SuccessResponse].aggregateId
  }

  Feature("An idempotent command sent more than once") {

    Scenario("sent again after it completed, it returns the stored response and changes nothing") {
      val name = unique("Cart")
      val command = CreateShoppingCart(sagaStep(), userId, name)

      val first: CustomCommandResponse[_] = commandBus ?? command
      val second: CustomCommandResponse[_] = commandBus ?? command

      first mustBe a[SuccessResponse]
      second mustBe first
      eventsNamed(name) mustBe 1
    }

    Scenario("a first command sent concurrently creates one aggregate, and every caller gets the same response") {
      val name = unique("Cart")

      val responses = sendConcurrently(5, CreateShoppingCart(sagaStep(), userId, name))

      responses.head mustBe a[SuccessResponse]
      responses.distinct mustBe Seq(responses.head)
      eventsNamed(name) mustBe 1
    }

    Scenario("a command with an expected version, sent concurrently, is applied once") {
      val cartId = newCart()
      val item = unique("apple")

      val responses = sendConcurrently(5, AddItemWithReceipt(clientKey(), userId, cartId, AggregateVersion(1), item))

      responses.distinct mustBe Seq(CustomSuccessResponse(cartId, AggregateVersion(2), ItemReceipt(item)))
      eventsNamed(item) mustBe 1
    }

    Scenario("a concurrent command sent concurrently is applied once") {
      val cartId = newCart()
      val item = unique("pear")

      val responses = sendConcurrently(5, AddItemConcurrently(userId, cartId, item, clientKey()))

      responses.distinct mustBe Seq(SuccessResponse(cartId, AggregateVersion(2)))
      eventsNamed(item) mustBe 1
    }

    Scenario("a response with custom info is stored and returned again (F64)") {
      val cartId = newCart()
      val item = unique("plum")
      val command = AddItemWithReceipt(clientKey(), userId, cartId, AggregateVersion(1), item)

      val first: CustomCommandResponse[_] = commandBus ?? command
      val second: CustomCommandResponse[_] = commandBus ?? command

      first mustBe CustomSuccessResponse(cartId, AggregateVersion(2), ItemReceipt(item))
      second mustBe first
      eventsNamed(item) mustBe 1
    }

    Scenario("after a restart, the stored response is returned") {
      val cartId = newCart()
      val item = unique("fig")
      val command = AddItemWithReceipt(clientKey(), userId, cartId, AggregateVersion(1), item)
      val first: CustomCommandResponse[_] = commandBus ?? command

      val afterRestart: CustomCommandResponse[_] = newCommandBus() ?? command

      afterRestart mustBe first
      eventsNamed(item) mustBe 1
    }
  }

  Feature("A first command with an aggregate id chosen by the caller") {

    Scenario("creates the aggregate under that id") {
      val cartId: AggregateId = commandBus ?? ReserveAggregateId
      val name = unique("Cart")

      val response: CustomCommandResponse[_] = commandBus ?? CreateShoppingCartWithId(None, userId, cartId, name)
      val cart: Try[Aggregate[ShoppingCart]] = commandBus ?? GetAggregate(cartId)

      response mustBe SuccessResponse(cartId, AggregateVersion(1))
      cart.get mustBe Aggregate(cartId, AggregateVersion(1), Some(ShoppingCart(name, Vector())))
    }

    Scenario("sent again without a key, it is rejected because the aggregate exists") {
      val cartId: AggregateId = commandBus ?? ReserveAggregateId
      val name = unique("Cart")
      val first: CustomCommandResponse[_] = commandBus ?? CreateShoppingCartWithId(None, userId, cartId, name)

      val second: CustomCommandResponse[_] = commandBus ?? CreateShoppingCartWithId(None, userId, cartId, name)

      first mustBe SuccessResponse(cartId, AggregateVersion(1))
      second mustBe an[AggregateConcurrentModificationError]
      eventsNamed(name) mustBe 1
    }

    Scenario("sent concurrently with one key, it creates the aggregate once and returns the same response") {
      val cartId: AggregateId = commandBus ?? ReserveAggregateId
      val name = unique("Cart")

      val responses = sendConcurrently(5, CreateShoppingCartWithId(clientKey(), userId, cartId, name))

      responses.distinct mustBe Seq(SuccessResponse(cartId, AggregateVersion(1)))
      eventsNamed(name) mustBe 1
    }
  }
}

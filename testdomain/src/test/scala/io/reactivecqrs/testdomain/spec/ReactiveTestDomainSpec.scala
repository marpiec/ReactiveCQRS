package io.reactivecqrs.testdomain.spec

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.serialization.SerializationExtension
import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.commandlog.PostgresCommandLogState
import io.reactivecqrs.core.documentstore.{MemoryDocumentStore, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus._
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.saga.PostgresSagaState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart.MultipleCartCreatorSaga.{CreateMultipleCarts, MultipleCartCreatorSagaResponse}
import io.reactivecqrs.testdomain.shoppingcart.{ShoppingCartAggregateContext, _}
import io.reactivecqrs.testutils.CommonSpec
import org.apache.commons.dbcp.BasicDataSource
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

import scala.util.Try


class ReactiveTestDomainSpec extends CommonSpec {

  val settings = ConnectionPoolSettings(
    initialSize = 5,
    maxSize = 20,
    connectionTimeoutMillis = 3000L)

  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs", settings)



  def Fixture = new {

    val system = ActorSystem("main-actor-system")

    val mpjsons = new MPJsons
    val eventStoreState = new PostgresEventStoreState(mpjsons).initSchema() // or MemoryEventStore

    val commandLogState = new PostgresCommandLogState(mpjsons).initSchema()

    val commandResponseState = new PostgresCommandResponseState(mpjsons).initSchema()

    val userId = UserId(1L)
    val serialization = SerializationExtension(system)

    val aggregatesUidGenerator = new PostgresUidGenerator("aggregates_uids_seq") // or MemoryUidGenerator
    val commandsUidGenerator = new PostgresUidGenerator("commands_uids_seq") // or MemoryUidGenerator
    val sagasUidGenerator = new PostgresUidGenerator("sagas_uids_seq") // or MemoryUidGenerator
    val uidGenerator = system.actorOf(Props(new UidGeneratorActor(aggregatesUidGenerator, commandsUidGenerator, sagasUidGenerator)), "uidGenerator")
    val eventBusSubscriptionsManager = new EventBusSubscriptionsManagerApi(system.actorOf(Props(new EventBusSubscriptionsManager(2))))
    private val eventBusState = new PostgresEventBusState().initSchema()

    val eventBusActor = system.actorOf(Props(new EventsBusActor(eventBusState, eventBusSubscriptionsManager)), "eventBus")

    val shoppingCartContext = new ShoppingCartAggregateContext
    val shoppingCartCommandBus: ActorRef = system.actorOf(
      AggregateCommandBusActor(shoppingCartContext, uidGenerator, eventStoreState, commandLogState, commandResponseState, eventBusActor), "ShoppingCartCommandBus")

    val sagaState = new PostgresSagaState(mpjsons)
    sagaState.initSchema()

    val multipleCartCreatorSaga: ActorRef = system.actorOf(
      Props(new MultipleCartCreatorSaga(sagaState, uidGenerator, shoppingCartCommandBus))
    )
    val subscriptionsState = new PostgresSubscriptionsState
    subscriptionsState.initSchema()


    val inMemory = false

    private val storeA = if(inMemory) {
      new MemoryDocumentStore[String, AggregateVersion]
    } else {
      new PostgresDocumentStore[String, AggregateVersion]("storeA", mpjsons)
    }
    private val storeB = if(inMemory) {
      new MemoryDocumentStore[String, AggregateVersion]
    } else {
      new PostgresDocumentStore[String, AggregateVersion]("storeB", mpjsons)
    }


    val shoppingCartsListProjectionEventsBased = system.actorOf(Props(new ShoppingCartsListProjectionEventsBased(eventBusSubscriptionsManager, subscriptionsState, shoppingCartCommandBus, storeA)), "ShoppingCartsListProjectionEventsBased")
    val shoppingCartsListProjectionAggregatesBased = system.actorOf(Props(new ShoppingCartsListProjectionAggregatesBased(eventBusSubscriptionsManager, subscriptionsState, storeB)), "ShoppingCartsListProjectionAggregatesBased")

    Thread.sleep(100) // Wait until all subscriptions in place


    def shoppingCartCommand(command: AnyRef): Aggregate[ShoppingCart]= {
      val result: CustomCommandResponse[_] = shoppingCartCommandBus ?? command
      val success = result.asInstanceOf[SuccessResponse]

      val shoppingCartTry:Try[Aggregate[ShoppingCart]] = shoppingCartCommandBus ?? GetAggregate(success.aggregateId)
      shoppingCartTry.get
    }

    def getShoppingCartForVersion(aggregateId: AggregateId, aggregateVersion: AggregateVersion): Aggregate[ShoppingCart] = {
      val shoppingCartTry:Try[Aggregate[ShoppingCart]] = shoppingCartCommandBus ?? GetAggregateForVersion(aggregateId, aggregateVersion)
      shoppingCartTry.get
    }
  }



  feature("Aggregate storing and getting with event sourcing") {

    scenario("Creation and modification of shopping cart aggregate") {

      val fixture = Fixture
      import fixture._

      step("Create shopping cart")

      var shoppingCartA = shoppingCartCommand(CreateShoppingCart(None, userId,"Groceries"))
      shoppingCartA mustBe Aggregate(shoppingCartA.id, AggregateVersion(1), Some(ShoppingCart("Groceries", Vector())))

      step("Add items to cart")

      shoppingCartA = shoppingCartCommand(AddItem(userId, shoppingCartA.id, AggregateVersion(1), "apples"))
      shoppingCartA = shoppingCartCommand(AddItem(userId, shoppingCartA.id, AggregateVersion(2), "oranges"))
      shoppingCartA mustBe Aggregate(shoppingCartA.id, AggregateVersion(3), Some(ShoppingCart("Groceries", Vector(Item(1, "apples"), Item(2, "oranges")))))

      step("Remove items from cart")

      shoppingCartA = shoppingCartCommand(RemoveItem(userId, shoppingCartA.id, AggregateVersion(3), 1))
      shoppingCartA mustBe Aggregate(shoppingCartA.id, AggregateVersion(4), Some(ShoppingCart("Groceries", Vector(Item(2, "oranges")))))


      Thread.sleep(500) // Projections are eventually consistent, so let's wait until they are consistent

      step("Undo removing items from cart")

      shoppingCartA = shoppingCartCommand(UndoShoppingCartChange(userId, shoppingCartA.id, AggregateVersion(4), 1))
      shoppingCartA mustBe Aggregate(shoppingCartA.id, AggregateVersion(5), Some(ShoppingCart("Groceries", Vector(Item(1, "apples"), Item(2, "oranges")))))

      step("Remove different items from cart")

      shoppingCartA = shoppingCartCommand(RemoveItem(userId, shoppingCartA.id, AggregateVersion(5), 2))
      shoppingCartA mustBe Aggregate(shoppingCartA.id, AggregateVersion(6), Some(ShoppingCart("Groceries", Vector(Item(1, "apples")))))

      step("Duplicate some previous cart")

      var shoppingCartB = shoppingCartCommand(DuplicateShoppingCart(userId, shoppingCartA.id, AggregateVersion(3)))
      shoppingCartB.id mustNot be(shoppingCartA.id)
      shoppingCartB mustBe Aggregate(shoppingCartB.id, AggregateVersion(1), Some(ShoppingCart("Groceries", Vector(Item(1, "apples"), Item(2, "oranges")))))

      shoppingCartB = shoppingCartCommand(AddItem(userId, shoppingCartB.id, AggregateVersion(1), "tomatoes"))
      shoppingCartB mustBe Aggregate(shoppingCartB.id, AggregateVersion(2), Some(ShoppingCart("Groceries", Vector(Item(1, "apples"), Item(2, "oranges"), Item(3, "tomatoes")))))

      Then("We can get old version of shopping cart A")
      shoppingCartA = getShoppingCartForVersion(shoppingCartA.id, AggregateVersion(4))
      shoppingCartA mustBe Aggregate(shoppingCartA.id, AggregateVersion(4), Some(ShoppingCart("Groceries", Vector(Item(2, "oranges")))))

      Thread.sleep(500) // time to cleanup
    }


    scenario("Create multiple carts at once") {

      val fixture = Fixture
      import fixture._

      multipleCartCreatorSaga ! CreateMultipleCarts(userId, "My special cart", 5000)

      Thread.sleep(10000) // time to cleanup
    }

    scenario("Fail to create multiple carts at once") {

      val fixture = Fixture
      import fixture._

      // Will not be able to create cart ending with M 4
      val result: MultipleCartCreatorSagaResponse = multipleCartCreatorSaga ?? CreateMultipleCarts(userId, "My special cart M", 5)

      Thread.sleep(500) // time to cleanup
    }


    scenario("Reinitialization") {

      val fixture = Fixture
      import fixture._

      Thread.sleep(10000)

      }
    }

}

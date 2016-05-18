package io.reactivecqrs.testdomain.spec

import akka.actor.{ActorRef, Props}
import akka.serialization.SerializationExtension
import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor
import io.reactivecqrs.core.commandlog.PostgresCommandLogState
import io.reactivecqrs.core.documentstore.MemoryDocumentStore
import io.reactivecqrs.core.eventbus.{EventsBusActor, PostgresEventBusState}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.saga.PostgresSagaState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart.MultipleCartCreatorSaga.{CreateMultipleCarts, MultipleCartCreatorSagaResponse}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.CommonSpec
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
    val mpjsons = new MPJsons
    val eventStoreState = new PostgresEventStoreState(mpjsons) // or MemoryEventStore
    eventStoreState.initSchema()

    val commandLogState = new PostgresCommandLogState(mpjsons)
    commandLogState.initSchema()

    val userId = UserId(1L)
    val serialization = SerializationExtension(system)
    val eventBusState = new PostgresEventBusState(serialization) // or MemoryEventBusState
    eventBusState.initSchema()

    val aggregatesUidGenerator = new PostgresUidGenerator("aggregates_uids_seq") // or MemoryUidGenerator
    val commandsUidGenerator = new PostgresUidGenerator("commands_uids_seq") // or MemoryUidGenerator
    val sagasUidGenerator = new PostgresUidGenerator("sagas_uids_seq") // or MemoryUidGenerator
    val uidGenerator = system.actorOf(Props(new UidGeneratorActor(aggregatesUidGenerator, commandsUidGenerator, sagasUidGenerator)), "uidGenerator")
    val eventBusActor = system.actorOf(Props(new EventsBusActor(eventBusState)), "eventBus")
    val shoppingCartCommandBus: ActorRef = system.actorOf(
      AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStoreState, commandLogState, eventBusActor), "ShoppingCartCommandBus")

    val sagaState = new PostgresSagaState(serialization)
    sagaState.initSchema()

    val multipleCartCreatorSaga: ActorRef = system.actorOf(
      Props(new MultipleCartCreatorSaga(sagaState, uidGenerator, shoppingCartCommandBus))
    )
    val subscriptionsState = new PostgresSubscriptionsState
    subscriptionsState.initSchema()
    val shoppingCartsListProjectionEventsBased = system.actorOf(Props(new ShoppingCartsListProjectionEventsBased(eventBusActor, subscriptionsState, shoppingCartCommandBus, new MemoryDocumentStore[String, AggregateVersion])), "ShoppingCartsListProjectionEventsBased")
    val shoppingCartsListProjectionAggregatesBased = system.actorOf(Props(new ShoppingCartsListProjectionAggregatesBased(eventBusActor, subscriptionsState, new MemoryDocumentStore[String, AggregateVersion])), "ShoppingCartsListProjectionAggregatesBased")

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

      var shoppingCartA = shoppingCartCommand(CreateShoppingCart(userId,"Groceries"))
      shoppingCartA mustBe Aggregate(shoppingCartA.id, AggregateVersion(1), Some(ShoppingCart("Groceries", Vector())))

      step("Add items to cart")

      shoppingCartA = shoppingCartCommand(AddItem(userId, shoppingCartA.id, AggregateVersion(1), "apples"))
      shoppingCartA = shoppingCartCommand(AddItem(userId, shoppingCartA.id, AggregateVersion(2), "oranges"))
      shoppingCartA mustBe Aggregate(shoppingCartA.id, AggregateVersion(3), Some(ShoppingCart("Groceries", Vector(Item(1, "apples"), Item(2, "oranges")))))

      step("Remove items from cart")

      shoppingCartA = shoppingCartCommand(RemoveItem(userId, shoppingCartA.id, AggregateVersion(3), 1))
      shoppingCartA mustBe Aggregate(shoppingCartA.id, AggregateVersion(4), Some(ShoppingCart("Groceries", Vector(Item(2, "oranges")))))


      Thread.sleep(300) // Projections are eventually consistent, so let's wait until they are consistent

      var cartsNames: Vector[String] = shoppingCartsListProjectionEventsBased ?? ShoppingCartsListProjection.GetAllCartsNames()
      cartsNames must have size 1

      cartsNames = shoppingCartsListProjectionAggregatesBased ?? ShoppingCartsListProjection.GetAllCartsNames()
      cartsNames must have size 1


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

    }


    scenario("Crate multiple carts at once") {

      val fixture = Fixture
      import fixture._

      val result: MultipleCartCreatorSagaResponse = multipleCartCreatorSaga ?? CreateMultipleCarts(userId, "My special cart", 5)

      Thread.sleep(500) // time to cleanup
    }

    scenario("Fail to create multiple carts at once") {

      val fixture = Fixture
      import fixture._

      // Will not be able to create cart ending with M 4
      val result: MultipleCartCreatorSagaResponse = multipleCartCreatorSaga ?? CreateMultipleCarts(userId, "My special cart M", 5)

      Thread.sleep(500) // time to cleanup
    }
  }

}

package io.reactivecqrs.testdomain.spec

import akka.actor.{ActorRef, Props}
import akka.serialization.SerializationExtension
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor
import io.reactivecqrs.core.documentstore.MemoryDocumentStore
import io.reactivecqrs.core.eventbus.{EventsBusActor, PostgresEventBusState}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testdomain.spec.utils.CommonSpec

import scala.util.Try


class ReactiveTestDomainSpec extends CommonSpec {

  feature("Aggregate storing and getting with event sourcing") {

    scenario("Creation and modification of user aggregate") {

      val eventStoreState = new PostgresEventStoreState // or MemoryEventStore
      eventStoreState.initSchema()
      val userId = UserId(1L)
      val serialization = SerializationExtension(system)
      val eventBusState = new PostgresEventBusState(serialization) // or MemoryEventBusState
      eventBusState.initSchema()
      val uidGenerator = system.actorOf(Props(new UidGeneratorActor), "uidGenerator")
      val eventBusActor = system.actorOf(Props(new EventsBusActor(eventBusState)), "eventBus")
      val shoppingCartCommandBus: ActorRef = system.actorOf(
        AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStoreState, eventBusActor), "ShoppingCartCommandBus")

      val shoppingCartsListProjectionEventsBased = system.actorOf(Props(new ShoppingCartsListProjectionEventsBased(eventBusActor, new MemoryDocumentStore[String, AggregateVersion])), "ShoppingCartsListProjectionEventsBased")
      val shoppingCartsListProjectionAggregatesBased = system.actorOf(Props(new ShoppingCartsListProjectionAggregatesBased(eventBusActor, new MemoryDocumentStore[String, AggregateVersion])), "ShoppingCartsListProjectionAggregatesBased")

      Thread.sleep(50) // Wait until all subscriptions in place


      step("Create shopping cart")

      var result: CommandResponse = shoppingCartCommandBus ?? CreateShoppingCart(userId,"Groceries")
      val shoppingCartId: AggregateId = result match {
        case SuccessResponse(aggregateId, aggregateVersion) => aggregateId
        case FailureResponse(reason) => fail()
      }
      var shoppingCartTry:Try[Aggregate[ShoppingCart]] = shoppingCartCommandBus ?? GetAggregate(shoppingCartId)
      var shoppingCart = shoppingCartTry.get
      shoppingCart mustBe Aggregate(shoppingCartId, AggregateVersion(1), Some(ShoppingCart("Groceries", Vector())))

      step("Add items to cart")

      result = shoppingCartCommandBus ?? AddItem(userId, shoppingCart.id, AggregateVersion(1), "apples")
      result mustBe SuccessResponse(shoppingCart.id, AggregateVersion(2))
      var success = result.asInstanceOf[SuccessResponse]

      result = shoppingCartCommandBus ?? AddItem(userId, shoppingCart.id, AggregateVersion(2), "oranges")
      result mustBe SuccessResponse(shoppingCart.id, AggregateVersion(3))
      success = result.asInstanceOf[SuccessResponse]

      shoppingCartTry = shoppingCartCommandBus ?? GetAggregate(shoppingCartId)
      shoppingCart = shoppingCartTry.get
      shoppingCart mustBe Aggregate(success.aggregateId, success.aggregateVersion, Some(ShoppingCart("Groceries", Vector(Item(1, "apples"), Item(2, "oranges")))))

      step("Remove items from cart")

      result = shoppingCartCommandBus ?? RemoveItem(userId, shoppingCart.id, AggregateVersion(3), 1)
      result mustBe SuccessResponse(shoppingCart.id, AggregateVersion(4))
      success = result.asInstanceOf[SuccessResponse]

      shoppingCartTry = shoppingCartCommandBus ?? GetAggregate(shoppingCartId)
      shoppingCart = shoppingCartTry.get
      shoppingCart mustBe Aggregate(success.aggregateId, success.aggregateVersion, Some(ShoppingCart("Groceries", Vector(Item(2, "oranges")))))


      Thread.sleep(50) // Projections are eventually consistent, so let's wait until they are consistent

      var cartsNames: Vector[String] = shoppingCartsListProjectionEventsBased ?? ShoppingCartsListProjection.GetAllCartsNames()
      cartsNames must have size 1

      cartsNames = shoppingCartsListProjectionAggregatesBased ?? ShoppingCartsListProjection.GetAllCartsNames()

      cartsNames must have size 1


    }


  }

}

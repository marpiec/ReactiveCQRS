package io.reactivecqrs.examples.e11

// E11, testing (chapter 19): E1's carts and read model, wired as in E1 but over the in-memory doubles: no database.

// #memory
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, MemoryCommandResponseState}
import io.reactivecqrs.core.documentstore.MemoryDocumentStore
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.MemoryEventStoreState
import io.reactivecqrs.core.projection.MemorySubscriptionsState
import io.reactivecqrs.core.uid.{MemoryUidGenerator, UidGeneratorActor}
import io.reactivecqrs.examples.e1._
import org.apache.pekko.actor.{ActorRef, Props}

import scala.util.Try

class CartsInMemorySpec extends ShopSpec {

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new MemoryUidGenerator, new MemoryUidGenerator, new MemoryUidGenerator)))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  private val carts: ActorRef = system.actorOf(AggregateCommandBusActor(new ShoppingCartContext,
    uidGenerator, new MemoryEventStoreState, new MemoryCommandResponseState, eventBus))
  private val cartsList: ActorRef = system.actorOf(Props(new CartsListProjection(
    new MemorySubscriptionsState, new MemoryDocumentStore[CartSummary])))
  eventBus ! RegisterSubscribers(List(cartsList)) // RegisterSubscribers(Nil) when there are none

  private val user = UserId(1)

  private def send(command: AnyRef): CustomCommandResponse[_] = carts.askFor[CustomCommandResponse[_]](command)

  private def newCart(): AggregateId = send(CreateShoppingCart(user, "Groceries")) match {
    case SuccessResponse(cartId, _) => cartId
    case refused => fail("Not created: " + refused)
  }

  Feature("A cart in memory") {

    Scenario("an item added reaches the read model") {
      val cartId = newCart()
      send(AddItem(user, cartId, AggregateVersion(1), "apples")) mustBe SuccessResponse(cartId, AggregateVersion(2))
      val summary = cartsList.askFor[Option[CartSummary]](GetCartSummaryAtLeast(cartId, AggregateVersion(2)))
      summary mustBe Some(CartSummary("Groceries", 1, 2))
    }

    Scenario("the command that expects an old version is refused") {
      Given("a cart with apples in it")
      val cartId = newCart()
      send(AddItem(user, cartId, AggregateVersion(1), "apples")) mustBe SuccessResponse(cartId, AggregateVersion(2))

      When("someone still looking at version 1 adds pears")
      val late = send(AddItem(user, cartId, AggregateVersion(1), "pears"))

      Then("the command is refused, and the cart keeps only the apples")
      late mustBe AggregateConcurrentModificationError(cartId, AggregateType.of[ShoppingCart], AggregateVersion(1), AggregateVersion(2))
      val cart = carts.askFor[Try[Aggregate[ShoppingCart]]](GetAggregate(cartId))
      cart.get.aggregateRoot.get.items mustBe Vector("apples")
    }
  }
}
// #memory

package io.reactivecqrs.testdomain.spec

import akka.actor.{ActorRef, Props}
import io.reactivecqrs.api.guid.UserId
import io.reactivecqrs.api._
import io.reactivecqrs.core.AggregateCommandBusActor.CommandEnvelope
import io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.core.{AggregateCommandBusActor, EventStore}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testdomain.spec.utils.CommonSpec



class ReactiveTestDomainSpec extends CommonSpec {

  feature("Aggregate storing and getting with event sourcing") {

    scenario("Creation and modification of user aggregate") {

      (new EventStore).initSchema()
      val userId = UserId(1L)
      val uidGenerator = system.actorOf(Props(new UidGeneratorActor), "uidGenerator")
      val shoppingCartCommandBus: ActorRef = system.actorOf(AggregateCommandBusActor(new ShoppingCartCommandBus, uidGenerator), "ShoppingCartCommandBus")


      step("Create shopping cart")

      var result: CommandResult = shoppingCartCommandBus ?? CommandEnvelope(userId, CreateShoppingCart("Groceries"))
      val shoppingCartId = result.aggregateId
      var shoppingCart:Aggregate[ShoppingCart] = shoppingCartCommandBus ?? GetAggregate(shoppingCartId)

      shoppingCart mustBe Aggregate(shoppingCartId, AggregateVersion(1), Some(ShoppingCart("Groceries", Vector())))

      step("Add items to cart")

      result = shoppingCartCommandBus ?? CommandEnvelope(userId, shoppingCart.id, AggregateVersion(1), AddItem("apples"))
      result mustBe CommandResult(shoppingCart.id, AggregateVersion(2))

      result = shoppingCartCommandBus ?? CommandEnvelope(userId, shoppingCart.id, AggregateVersion(2), AddItem("oranges"))
      result mustBe CommandResult(shoppingCart.id, AggregateVersion(3))

      shoppingCart = shoppingCartCommandBus ?? GetAggregate(shoppingCartId)
      shoppingCart mustBe Aggregate(result.aggregateId, result.aggregateVersion, Some(ShoppingCart("Groceries", Vector(Item(1, "apples"), Item(2, "oranges")))))

      step("Remove items from cart")

      result = shoppingCartCommandBus ?? CommandEnvelope(userId, shoppingCart.id, AggregateVersion(3), RemoveItem(1))
      result mustBe CommandResult(shoppingCart.id, AggregateVersion(4))

      shoppingCart = shoppingCartCommandBus ?? GetAggregate(shoppingCartId)
      shoppingCart mustBe Aggregate(result.aggregateId, result.aggregateVersion, Some(ShoppingCart("Groceries", Vector(Item(2, "oranges")))))

    }


  }

}

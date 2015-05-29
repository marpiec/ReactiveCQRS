package io.reactivecqrs.testdomain.spec

import akka.actor.{ActorRef, Props}
import io.reactivecqrs.actor.AggregateCommandBusActor.CommandEnvelope
import io.reactivecqrs.actor.{AggregateCommandBusActor, EventStore}
import io.reactivecqrs.api.Aggregate
import io.reactivecqrs.api.guid.UserId
import io.reactivecqrs.core._
import io.reactivecqrs.testdomain.ShoppingCartCommandBus
import io.reactivecqrs.testdomain.api._
import io.reactivecqrs.testdomain.spec.utils.CommonSpec
import io.reactivecqrs.uid.UidGeneratorActor





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

      result = shoppingCartCommandBus ?? CommandEnvelope(userId, shoppingCart.id, shoppingCart.version, AddItem("apples"))

      result mustBe CommandResult(shoppingCart.id, AggregateVersion(2))

      shoppingCart = shoppingCartCommandBus ?? GetAggregate(shoppingCartId)
      shoppingCart mustBe Aggregate(result.aggregateId, result.aggregateVersion, Some(ShoppingCart("Groceries", Vector(Item(1, "apples")))))

    }


  }

}

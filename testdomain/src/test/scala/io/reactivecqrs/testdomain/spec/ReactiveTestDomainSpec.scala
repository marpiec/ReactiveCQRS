package io.reactivecqrs.testdomain.spec

import akka.actor.{Actor, ActorRef, Props}
import akka.serialization.SerializationExtension
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.AggregateCommandBusActor.CommandEnvelope
import io.reactivecqrs.core.EventsBusActor.{EventReceived, SubscribeForEvents}
import io.reactivecqrs.core.api.IdentifiableEvent
import io.reactivecqrs.core.db.eventbus.EventBus
import io.reactivecqrs.core.db.eventstore.EventStore
import io.reactivecqrs.core.uid.UidGeneratorActor
import io.reactivecqrs.core.{AggregateCommandBusActor, EventsBusActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testdomain.spec.utils.CommonSpec

class SampleProjection extends Actor {
  override def receive: Receive = {
    case e: IdentifiableEvent[_] =>
      println("SampleProjection received event " + e)
      sender() ! EventReceived(self, e.aggregateId, e.version)
    case e => println("SampleProjection received " + e);
  }
}


class ReactiveTestDomainSpec extends CommonSpec {

  feature("Aggregate storing and getting with event sourcing") {

    scenario("Creation and modification of user aggregate") {

      val eventStore = new EventStore
      eventStore.initSchema()
      val userId = UserId(1L)
      val serialization = SerializationExtension(system)
      val eventBus = new EventBus(serialization)
      eventBus.initSchema()
      val uidGenerator = system.actorOf(Props(new UidGeneratorActor), "uidGenerator")
      val eventBusActor = system.actorOf(Props(new EventsBusActor(eventBus)), "eventBus")
      val shoppingCartCommandBus: ActorRef = system.actorOf(
        AggregateCommandBusActor(new ShoppingCartCommandBus, uidGenerator, eventStore, eventBusActor), "ShoppingCartCommandBus")

      val sampleProjection = system.actorOf(Props(new SampleProjection), "SampleProjection")

      eventBusActor ! SubscribeForEvents(sampleProjection)

      val shoppingCartsListProjection = system.actorOf(Props(new ShoppingCartsListProjection(eventBusActor)), "ShoppingCartsListProjection")


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


      val cartsNames: Vector[String] = shoppingCartsListProjection ?? ShoppingCartsListProjection.GetAllCartsNames()

      cartsNames must have size 1


    }


  }

}

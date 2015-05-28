package io.reactivecqrs.testdomain.spec

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import io.reactivecqrs.actor.{Aggregate, AkkaAggregate, EventStore}
import io.reactivecqrs.api.guid.UserId
import io.reactivecqrs.core._
import io.reactivecqrs.testdomain.ShoppingCartCommandBus
import io.reactivecqrs.testdomain.api._
import io.reactivecqrs.testdomain.spec.utils.CommonSpec
import io.reactivecqrs.uid.UidGeneratorActor





class ReactiveTestDomainSpec  extends TestKit(ActorSystem("testsystem", ConfigFactory.parseString("""
                          akka.persistence.journal.plugin = "postgres-journal"
                              akka.persistence.snapshot-store.plugin = "postgres-snapshot-store"
        postgres-journal.class = "io.reactivecqrs.persistance.postgres.PostgresSyncJournal"
        postgres-snapshot-store.class = "io.reactivecqrs.persistance.postgres.PostgresSyncSnapshotStore"
          akka.loglevel = "DEBUG"
          akka.actor.debug.receive = on
          akka.actor.debug.receive = on
          akka.actor.debug.fsm = on
          akka.actor.debug.lifecycle = on""")))
    with CommonSpec {

  feature("Aggregate storing and getting with event sourcing") {

    scenario("Creation and modification of user aggregate") {

      (new EventStore).initSchema()
      val userId = UserId(1L)
      val uidGenerator = system.actorOf(Props(new UidGeneratorActor), "uidGenerator")
      val shoppingCartCommandBus: ActorRef = AkkaAggregate.create(new ShoppingCartCommandBus, uidGenerator)(system).commandBus


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

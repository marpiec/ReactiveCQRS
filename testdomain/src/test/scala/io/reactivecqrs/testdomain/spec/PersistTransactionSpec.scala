package io.reactivecqrs.testdomain.spec

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import scalikejdbc._

import scala.util.Try

// F68: the events of one command are stored together or not at all.
class PersistTransactionSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  // Items of more than about 30 characters are too large here, so the second event of a command can fail its size
  // check after the first one was written.
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState,
    eventSizeLimitPerClassName = Map(classOf[ItemAdded].getName -> 50)).initSchema()

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  eventBus ! RegisterSubscribers(Nil)
  private val commandBus: ActorRef = system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore,
    new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema(), eventBus))

  private val userId = UserId(1)

  private def storedEvents(cartId: AggregateId): Int = DB.readOnly { implicit session =>
    sql"SELECT count(*) FROM events WHERE aggregate_id = ${cartId.asLong}".map(_.int(1)).single().get
  }

  Feature("A command whose events cannot all be stored") {

    Scenario("stores none of them, and the aggregate carries on from its previous version") {
      val created: CustomCommandResponse[_] = commandBus ?? CreateShoppingCart(None, userId, "Groceries")
      val cartId = created.asInstanceOf[SuccessResponse].aggregateId

      When("the second of two events fails")
      val failed: CustomCommandResponse[_] = commandBus ?? AddItems(userId, cartId, AggregateVersion(1), Seq("apple", "x" * 100))

      Then("the command fails and the first event is not stored either")
      failed mustBe an[EventHandlingError]
      storedEvents(cartId) mustBe 1

      And("the next command, expecting version 1, is stored, on a state without the failed command's items")
      val next: CustomCommandResponse[_] = commandBus ?? AddItem(userId, cartId, AggregateVersion(1), "pear")
      next mustBe SuccessResponse(cartId, AggregateVersion(2))
      storedEvents(cartId) mustBe 2
      val cart: Try[Aggregate[ShoppingCart]] = commandBus ?? GetAggregate(cartId)
      cart.get.aggregateRoot.get.items.map(_.name) mustBe Vector("pear")
    }
  }
}

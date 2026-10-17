package io.reactivecqrs.testdomain.spec

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.eventstore.NewEvents
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase, TestIds}
import org.apache.pekko.actor.{ActorRef, Props}
import scalikejdbc._

import scala.concurrent.duration._
import scala.util.Try

// F10: an aggregate whose restore fails answers at once, and a later command retries the restore (Neula R4).
class FailedRestoreSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  eventBus ! RegisterSubscribers(Nil)
  private val commandBus: ActorRef = system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore,
    new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema(), eventBus,
    failedRestoreRetryAfter = 300.millis))

  private val userId = UserId(1)

  /** A cart written straight to the store, so no actor has loaded it yet. */
  private def storedCart(): AggregateId = {
    val cartId = AggregateId(TestIds.aggregate())
    eventStore.localTx { session =>
      eventStore.persistEvents(cartId,
        NewEvents[ShoppingCart](userId, AggregateVersion.ZERO, Instant.now, Seq(ShoppingCartCreated("Groceries"))))(session)
    }.get
    cartId
  }

  private val validCreation = mpjsons.serialize(ShoppingCartCreated("Groceries"), classOf[ShoppingCartCreated].getName)

  private def setStoredEvent(cartId: AggregateId, json: String): Unit = DB.autoCommit { implicit session =>
    sql"UPDATE events SET event = $json WHERE aggregate_id = ${cartId.asLong}".update()
  }

  Feature("An aggregate whose stored events cannot be read") {

    Scenario("commands and queries get an error at once instead of waiting for the timeout") {
      val cartId = storedCart()
      setStoredEvent(cartId, "not json")

      try {
        val commandResponse: CustomCommandResponse[_] = commandBus ?? AddItem(userId, cartId, AggregateVersion(1), "apple")
        commandResponse mustBe a[CommandHandlingError]

        val query: Try[Aggregate[ShoppingCart]] = commandBus ?? GetAggregate(cartId)
        query.failed.get mustBe an[AggregateRestoreFailedException]
      } finally {
        setStoredEvent(cartId, validCreation) // later specs in this schema read every stored event
      }
    }

    Scenario("after the data is repaired, a later command restores the aggregate and succeeds") {
      Given("a cart whose restore failed")
      val cartId = storedCart()

      setStoredEvent(cartId, "not json")
      val failed: CustomCommandResponse[_] = commandBus ?? AddItem(userId, cartId, AggregateVersion(1), "apple")
      failed mustBe a[CommandHandlingError]

      When("the event is repaired and the retry window has passed")
      setStoredEvent(cartId, validCreation)

      Then("a command succeeds")
      awaitAssert({
        val response: CustomCommandResponse[_] = commandBus ?? AddItem(userId, cartId, AggregateVersion(1), "apple")
        response mustBe SuccessResponse(cartId, AggregateVersion(2))
      }, 5.seconds, 200.millis)
    }
  }
}

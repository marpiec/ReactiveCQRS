package io.reactivecqrs.testdomain.spec

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.PersistEvents
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.{EventBusSubscriptionsManager, EventBusSubscriptionsManagerApi, EventsBusActor, MemoryEventBusState}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import scalikejdbc.{DB, DBSession}

import scala.util.Try

/** An event store that lets a test write events "from another node" right before the actor persists its own. */
class InterferingEventStore(mpjsons: MPJsons, typesNamesState: PostgresTypesNamesState) extends PostgresEventStoreState(mpjsons, typesNamesState) {

  @volatile var interfereWith: Option[AggregateId] = None

  override def persistEvents[AGGREGATE_ROOT](eventsVersionsMapReverse: Map[String, EventTypeVersion], aggregateId: AggregateId,
                                             eventsEnvelope: PersistEvents[AGGREGATE_ROOT])(implicit session: DBSession): Try[Seq[(Event[AGGREGATE_ROOT], AggregateVersion)]] = {
    if (interfereWith.contains(aggregateId)) {
      writeFromElsewhere(aggregateId, eventsEnvelope.expectedVersion, ItemAdded("from elsewhere"))
    }
    super.persistEvents(eventsVersionsMapReverse, aggregateId, eventsEnvelope)
  }

  /** Commits an event in its own transaction, as another writer would. */
  def writeFromElsewhere(aggregateId: AggregateId, expectedVersion: AggregateVersion, event: Event[ShoppingCart]): Unit = {
    DB.localTx { session =>
      super.persistEvents(Map.empty, aggregateId,
        PersistEvents[ShoppingCart](ActorRef.noSender, CommandId(0), UserId(2), expectedVersion, Instant.now, Seq(event), None))(session).get
    }
  }
}

// F19, F24: an aggregate written by someone else must reload instead of failing, and conflicts must end in bounded time.
class ConcurrentModificationSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new InterferingEventStore(mpjsons, typesNamesState)
  eventStore.initSchema()

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor(new MemoryEventBusState,
    new EventBusSubscriptionsManagerApi(system.actorOf(Props(new EventBusSubscriptionsManager(0)))))))
  private val commandBus: ActorRef = system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore,
    new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema(), eventBus, eventsReplayMode = false))

  private val userId = UserId(1)

  /** Creates a cart and loads it into its repository actor. */
  private def loadedCart(): AggregateId = {
    val created: SuccessResponse = commandBus ?? CreateShoppingCart(None, userId, "Groceries")
    val loaded: Try[Aggregate[ShoppingCart]] = commandBus ?? GetAggregate(created.aggregateId)
    loaded.get.version mustBe AggregateVersion(1)
    created.aggregateId
  }

  Feature("An aggregate changed by another writer") {

    Scenario("a concurrent command succeeds on top of the external change") {
      Given("a loaded cart that another writer then changes to version 2")
      val cartId = loadedCart()
      eventStore.writeFromElsewhere(cartId, AggregateVersion(1), ItemAdded("from elsewhere"))

      When("a concurrent command is sent")
      val response: CustomCommandResponse[_] = commandBus ?? AddItemConcurrently(userId, cartId, "apple")

      Then("it is applied as version 3")
      response mustBe SuccessResponse(cartId, AggregateVersion(3))
    }

    Scenario("a command expecting the version the other writer produced succeeds") {
      val cartId = loadedCart()
      eventStore.writeFromElsewhere(cartId, AggregateVersion(1), ItemAdded("from elsewhere"))

      val response: CustomCommandResponse[_] = commandBus ?? AddItem(userId, cartId, AggregateVersion(2), "apple")

      response mustBe SuccessResponse(cartId, AggregateVersion(3))
    }
  }

  Feature("A concurrent command that keeps conflicting") {

    Scenario("gives up with a concurrent-modification error instead of retrying until the timeout") {
      Given("a cart that another writer changes right before every attempt to persist")
      val cartId = loadedCart()
      eventStore.interfereWith = Some(cartId)

      try {
        When("a concurrent command is sent")
        val response: CustomCommandResponse[_] = commandBus ?? AddItemConcurrently(userId, cartId, "apple")

        Then("the caller gets a concurrent-modification error well within the ask timeout")
        response mustBe an[AggregateConcurrentModificationError]
      } finally {
        eventStore.interfereWith = None
      }
    }
  }
}

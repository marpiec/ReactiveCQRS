package io.reactivecqrs.testdomain.spec

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.PersistEvents
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

/** An event store that lets a test write events "from another node" right before the actor persists its own. */
class InterferingEventStore(mpjsons: MPJsons, typesNamesState: PostgresTypesNamesState) extends PostgresEventStoreState(mpjsons, typesNamesState) {

  @volatile var interfereWith: Option[AggregateId] = None
  /** How many more persists of `interfereWith` meet a write from elsewhere. */
  val interferencesLeft = new java.util.concurrent.atomic.AtomicInteger(Int.MaxValue)

  override def persistEvents[AGGREGATE_ROOT](eventsVersionsMapReverse: Map[String, EventTypeVersion], aggregateId: AggregateId,
                                             eventsEnvelope: PersistEvents[AGGREGATE_ROOT])(implicit session: DBSession): Try[Seq[(Event[AGGREGATE_ROOT], AggregateVersion)]] = {
    if (interfereWith.contains(aggregateId) && interferencesLeft.getAndDecrement() > 0) {
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

// F19, F24, F69: an aggregate written by someone else must reload instead of failing, conflicts must end in bounded time,
// a reload must never make the actor apply events twice, and a retried rewrite must rewrite (F73).
class ConcurrentModificationSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new InterferingEventStore(mpjsons, typesNamesState)
  eventStore.initSchema()

  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val eventBus = system.actorOf(Props(new EventsBusActor()))
  eventBus ! RegisterSubscribers(Nil)
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

  Feature("Commands racing on one aggregate") {

    Scenario("after a version conflict the actor's state still matches the store (F69)") {
      Given("a cart and five commands that all expect version 1, sent at once")
      val cartId = loadedCart()
      import system.dispatcher
      implicit val timeout: Timeout = Timeout(15.seconds)
      val racing = Future.sequence((1 to 5).map(i => (commandBus ? AddItem(userId, cartId, AggregateVersion(1), "racer " + i)).mapTo[CustomCommandResponse[_]]))
      val responses = Await.result(racing, 20.seconds)

      Then("one of them is applied")
      responses.count(_ == SuccessResponse(cartId, AggregateVersion(2))) mustBe 1

      And("the aggregate is at version 2 with one item, as stored")
      val cart: Try[Aggregate[ShoppingCart]] = commandBus ?? GetAggregate(cartId)
      cart.get.version mustBe AggregateVersion(2)
      cart.get.aggregateRoot.get.items.size mustBe 1

      And("a command expecting version 2 succeeds")
      val next: CustomCommandResponse[_] = commandBus ?? AddItem(userId, cartId, AggregateVersion(2), "after the race")
      next mustBe SuccessResponse(cartId, AggregateVersion(3))
    }
  }

  Feature("A history rewrite on the current version") {

    Scenario("re-run after a conflict, it still rewrites the history (F73)") {
      Given("a cart that another writer changes right before the first attempt to persist")
      val cartId = loadedCart()
      eventStore.interferencesLeft.set(1)
      eventStore.interfereWith = Some(cartId)

      try {
        When("its name is rewritten throughout its history")
        val response: CustomCommandResponse[_] = commandBus ?? RewriteCartNameConcurrently(userId, cartId, "Renamed")

        Then("the retry rewrote the creation event too")
        response mustBe SuccessResponse(cartId, AggregateVersion(3))
        val created = DB.readOnly { implicit session =>
          sql"SELECT event FROM events WHERE aggregate_id = ${cartId.asLong} AND version = 1".map(_.string(1)).single().get
        }
        created must include("Renamed")
      } finally {
        eventStore.interfereWith = None
        eventStore.interferencesLeft.set(Int.MaxValue)
      }
    }
  }
}

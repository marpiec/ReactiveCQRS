package io.reactivecqrs.testdomain.spec

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.{NewEvents, PostgresEventStoreState}
import io.reactivecqrs.core.projection.{MemorySubscriptionsState, ProjectionActor, SubscriptionsState}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.core.util.BlockingDispatcher
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}

import scala.concurrent.duration._
import scala.util.Try

case object GetCartsMetadata

/** Records the metadata envelope of every cart event it applies, by aggregate and version. */
class CartsMetadataProjection(val subscriptionsState: SubscriptionsState) extends ProjectionActor {
  private var seen = Map[(AggregateId, Int), EventMetadata]()

  protected val listeners = List(EventsListener.noDBSession[ShoppingCart] { (id, events) =>
    seen ++= events.map(e => (id, e.version.asInt) -> e.metadata)
  })

  override protected def receiveQuery: Receive = {
    case GetCartsMetadata => sender() ! seen
  }

  override protected val projectionName: String = "CartsMetadataProjection"
}

// D32, R13: the metadata envelope travels from the command to the store, the live delivery, the re-publication that
// fills a gap, event queries, and through a history rewrite.
class MetadataSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val userId = UserId(5)
  private val onBehalf = EventMetadata("agent:planner", correlationId = Some("case 12"), causationId = Some("event 7"),
    traceContext = Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"), spaceId = Some(SpaceId(3)), custom = Map("channel" -> "api"))

  /** A system with one metadata-recording projection. */
  private class Fixture {
    val eventBus: ActorRef = system.actorOf(Props(new EventsBusActor()))
    val projection: ActorRef = system.actorOf(Props(new CartsMetadataProjection(new MemorySubscriptionsState)).withDispatcher(BlockingDispatcher.id))
    eventBus ! RegisterSubscribers(Seq(projection))
    val commandBus: ActorRef = system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore, responses, eventBus))

    def seen: Map[(AggregateId, Int), EventMetadata] = {
      val recorded: Map[(AggregateId, Int), EventMetadata] = projection ?? GetCartsMetadata
      recorded
    }
  }

  private def storedCart(metadata: Option[EventMetadata] = None): AggregateId = {
    val cartId = AggregateId(System.nanoTime())
    eventStore.localTx(session => eventStore.persistEvents(cartId,
      NewEvents[ShoppingCart](userId, AggregateVersion.ZERO, Instant.now, Seq(ShoppingCartCreated("Groceries")), metadata))(session)).get
    cartId
  }

  Feature("The metadata envelope (R13)") {

    Scenario("reaches a projection with each event, live and when a gap is filled from the store") {
      val f = new Fixture
      Given("a cart whose creation, written before the projection saw anything, carries an envelope")
      val cartId = storedCart(Some(onBehalf))

      When("an item is added on someone's behalf, and one without an envelope")
      val first: CustomCommandResponse[_] = f.commandBus ?? AddItemOnBehalf(userId, cartId, AggregateVersion(1), "apple", onBehalf)
      first mustBe SuccessResponse(cartId, AggregateVersion(2))
      val second: CustomCommandResponse[_] = f.commandBus ?? AddItem(userId, cartId, AggregateVersion(2), "pear")
      second mustBe SuccessResponse(cartId, AggregateVersion(3))

      Then("the projection gets the creation from the store with its envelope, and the live events with theirs")
      awaitAssert(f.seen.filter(_._1._1 == cartId) mustBe Map(
        (cartId, 1) -> onBehalf,
        (cartId, 2) -> onBehalf,
        (cartId, 3) -> EventMetadata.ofUser(userId)), 10.seconds, 50.millis)
    }

    Scenario("is returned with the events of an aggregate, also after a history rewrite of those events") {
      val f = new Fixture
      val cartId = storedCart(Some(onBehalf))

      val rewritten: CustomCommandResponse[_] = f.commandBus ?? RewriteCartName(userId, cartId, AggregateVersion(1), "Renamed")
      rewritten mustBe SuccessResponse(cartId, AggregateVersion(2))

      val events: Try[List[EventInfo[ShoppingCart]]] = f.commandBus ?? GetEventsForAggregate(cartId)
      events.get.map(e => (e.version.asInt, e.event, e.metadata)) mustBe List(
        (1, ShoppingCartCreated("Renamed"), onBehalf),
        (2, CartNameRewritten("Renamed"), EventMetadata.ofUser(userId)))
    }
  }
}

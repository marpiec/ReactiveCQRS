package io.reactivecqrs.testdomain.spec

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.DeliveryOptions
import io.reactivecqrs.core.eventbus.EventsBusActor.{DeliveryStatus, GetDeliveryStatus, RegisterSubscribers}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.{MemorySubscriptionsState, ProjectionActor, SubscriptionsState}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.core.util.BlockingDispatcher
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase, TestIds}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe

import scala.concurrent.duration._

case object GetDeliveredCarts

/** Records the versions and timestamps of every cart event it applies. */
class DeliveredCartsProjection(val subscriptionsState: SubscriptionsState) extends ProjectionActor {
  private var delivered = Map[AggregateId, Vector[(Int, Instant)]]()

  protected val listeners = List(EventsListener.noDBSession[ShoppingCart] { (id, events) =>
    delivered += id -> (delivered.getOrElse(id, Vector.empty) ++ events.map(e => (e.version.asInt, e.timestamp)))
  })

  override protected def receiveQuery: Receive = {
    case GetDeliveredCarts => sender() ! delivered
  }

  override protected val projectionName: String = "DeliveredCartsProjection"
}

// D3, D20c, R6: through the real bus and owner, a projection that starts behind fills its gap from the store and sees
// every version once, in order, with the timestamps the store keeps.
class DeliveryEndToEndSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val userId = UserId(1)

  private def commandBusOn(bus: ActorRef): ActorRef =
    system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore, responses, bus))

  Feature("Delivery through the bus and the owner") {

    Scenario("a projection that starts behind gets the missing versions from the owner, then every version once and in order") {
      Given("a cart with three events, delivered while the projection did not exist")
      val firstBus = system.actorOf(Props(new EventsBusActor()))
      firstBus ! RegisterSubscribers(Nil)
      val cartId = AggregateId(TestIds.aggregate())
      val firstCommands = commandBusOn(firstBus)
      Seq[AnyRef](CreateShoppingCartWithId(None, userId, cartId, "Groceries"), AddItem(userId, cartId, AggregateVersion(1), "apple"),
        AddItem(userId, cartId, AggregateVersion(2), "pear")).foreach { command =>
        val response: CustomCommandResponse[_] = firstCommands ?? command
        response mustBe a[SuccessResponse]
      }
      awaitAssert(eventStore.oldestVersionToPublish(cartId) mustBe None, 5.seconds, 50.millis)

      When("a new system with the projection adds a fourth event")
      val bus = system.actorOf(Props(new EventsBusActor()))
      val projection = system.actorOf(Props(new DeliveredCartsProjection(new MemorySubscriptionsState)).withDispatcher(BlockingDispatcher.id))
      bus ! RegisterSubscribers(Seq(projection))
      val commands = commandBusOn(bus)
      val added: CustomCommandResponse[_] = commands ?? AddItem(userId, cartId, AggregateVersion(3), "plum")
      added mustBe SuccessResponse(cartId, AggregateVersion(4))

      Then("the projection asks for versions 1 to 3, and applies 1 to 4 once each, in order")
      awaitAssert(projection.askActor[Map[AggregateId, Vector[(Int, Instant)]]](GetDeliveredCarts)(3.seconds)
        .getOrElse(cartId, Vector.empty).map(_._1) mustBe Vector(1, 2, 3, 4), 10.seconds, 100.millis)

      And("the live event's delivery is acknowledged, so its outbox row is gone")
      awaitAssert(eventStore.oldestVersionToPublish(cartId) mustBe None, 5.seconds, 50.millis)

      And("every timestamp the projection saw, the live one included, is the stored one (R6)")
      val delivered = projection.askActor[Map[AggregateId, Vector[(Int, Instant)]]](GetDeliveredCarts)(3.seconds)(cartId)
      delivered.map(_._2) mustBe eventStore.readEventsRange[ShoppingCart](cartId, AggregateVersion(1), AggregateVersion(4)).map(_.timestamp)
    }

    // F93: through the real owners, what a slow projection's window cannot take waits in the store and is pulled for it.
    Scenario("a projection slower than the commands gets at most its window; the rest is pulled from the owners, every event once") {
      Given("a projection that takes 20 ms per delivery, and a bus with a window of 5 events")
      val bus = system.actorOf(Props(new EventsBusActor(DeliveryOptions(window = 5))))
      val projection = system.actorOf(Props(new SlowCartsProjection(new MemorySubscriptionsState, 20.millis)).withDispatcher(BlockingDispatcher.id))
      bus ! RegisterSubscribers(Seq(projection))
      val commands = commandBusOn(bus)

      When("30 carts get two items each, the commands sent at once")
      val ids = (1 to 30).map(_ => AggregateId(TestIds.aggregate()))
      ids.foreach { id =>
        val created: CustomCommandResponse[_] = commands ?? CreateShoppingCartWithId(None, userId, id, "cart")
        created mustBe a[SuccessResponse]
      }
      val replies = TestProbe()
      ids.foreach(id => (1 to 2).foreach(n => commands.tell(AddItemConcurrently(userId, id, "item " + n), replies.ref)))
      replies.receiveN(60, 30.seconds).foreach(_ mustBe a[SuccessResponse])

      Then("the bus held events for the projection")
      val held = TestProbe()
      bus.tell(GetDeliveryStatus, held.ref)
      held.expectMsgType[DeliveryStatus](3.seconds).behind.values.sum must be > 0

      And("every event reaches the projection once, in order, and every outbox row goes")
      awaitAssert(projection.askActor[Map[AggregateId, Vector[Int]]](GetDeliveredCarts)(3.seconds) mustBe ids.map(_ -> Vector(1, 2, 3)).toMap,
        60.seconds, 200.millis)
      awaitAssert(ids.foreach(id => eventStore.oldestVersionToPublish(id) mustBe None), 10.seconds, 100.millis)
    }
  }
}

/** Records the versions of every cart event it applies, taking `delay` for each batch: slower than the commands. */
class SlowCartsProjection(val subscriptionsState: SubscriptionsState, delay: FiniteDuration) extends ProjectionActor {
  private var delivered = Map[AggregateId, Vector[Int]]()

  protected val listeners = List(EventsListener.noDBSession[ShoppingCart] { (id, events) =>
    Thread.sleep(delay.toMillis)
    delivered += id -> (delivered.getOrElse(id, Vector.empty) ++ events.map(_.version.asInt))
  })

  override protected def receiveQuery: Receive = {
    case GetDeliveredCarts => sender() ! delivered
  }

  override protected val projectionName: String = "SlowCartsProjection"
}

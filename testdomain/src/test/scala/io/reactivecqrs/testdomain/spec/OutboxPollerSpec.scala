package io.reactivecqrs.testdomain.spec

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.eventstore.NewEvents
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate
import io.reactivecqrs.core.delivery.OutboxPollerActor.{GetOutboxStatus, OutboxStatus, Poll}
import io.reactivecqrs.core.delivery.{OutboxPollerActor, OutboxPollerOptions}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.{CommonSpec, TestClock, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe

import scala.concurrent.Await
import scala.concurrent.duration._

// D8, D20b, F2: the outbox poller alone re-delivers. Rows from before the start go out at the first poll; a delivery
// that stays unacknowledged while the aggregate is active is re-published at 3 and 6 minutes and given up at 10.
class OutboxPollerSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))

  private val userId = UserId(1)
  private val cartType = AggregateType.of[ShoppingCart]

  /** A system whose only subscriber is a probe the test answers for, and whose poller polls when the test says so. */
  private class Fixture {
    val clock = new TestClock()
    val subscriber = TestProbe()
    val bus: ActorRef = system.actorOf(Props(new EventsBusActor()))
    bus ! RegisterSubscribers(Seq(subscriber.ref))
    subscriber.expectMsg(GetSubscriptionRequests)
    subscriber.reply(SubscriptionRequests(Seq(SubscribeForEvents(cartType, subscriber.ref))))
    val commandBus: ActorRef = system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore,
      responses, bus, eventsReplayMode = false))
    val poller: ActorRef = system.actorOf(Props(new OutboxPollerActor(eventStore, bus,
      OutboxPollerOptions(firstPollAfter = 1.hour, pollInterval = 1.hour), clock)))

    /** The next delivery of this cart's events; deliveries of other aggregates (left by other specs) are skipped. */
    def deliveredVersions(cartId: AggregateId): Seq[Int] =
      subscriber.fishForSpecificMessage(10.seconds) { case e: IdentifiableEvents[_] if e.aggregateId == cartId => e.events.map(_.version.asInt) }

    def noDeliveryOf(cartId: AggregateId, within: FiniteDuration): Unit =
      subscriber.receiveWhile(within) { case e: IdentifiableEvents[_] => e.aggregateId } must not contain cartId

    def ack(cartId: AggregateId, versions: Int*): Unit = bus ! MessageAck(subscriber.ref, cartId, versions.map(AggregateVersion(_)), EventsDelivery)

    def pollAfter(time: FiniteDuration): Unit = {
      clock.advance(time)
      poller ! Poll
    }

    def gaveUp(): Seq[AggregateId] = poller.askActor[OutboxStatus](GetOutboxStatus)(5.seconds).gaveUp.map(_.aggregateId)
  }

  private def newCart(f: Fixture): AggregateId = {
    val cartId = AggregateId(System.nanoTime())
    val created: CustomCommandResponse[_] = f.commandBus ?? CreateShoppingCartWithId(None, userId, cartId, "Groceries")
    created mustBe SuccessResponse(cartId, AggregateVersion(1))
    cartId
  }

  private def outboxEmptyFor(cartId: AggregateId): Unit =
    awaitAssert(eventStore.oldestVersionToPublish(cartId) mustBe None, 5.seconds, 50.millis)

  Feature("Re-delivery by the outbox poller") {

    Scenario("events committed before a restart reach the subscribers at the first poll") {
      Given("a cart committed but never published, as when the process stops right after the commit")
      val cartId = AggregateId(System.nanoTime())
      eventStore.localTx { session =>
        eventStore.persistEvents(cartId, NewEvents[ShoppingCart](userId, AggregateVersion.ZERO,
          Instant.now, Seq(ShoppingCartCreated("Groceries"), ItemAdded("apple"))))(session)
      }.get
      val f = new Fixture

      When("the poller polls for the first time")
      f.poller ! Poll

      Then("the subscriber receives the events, and once it acknowledges them their outbox rows are gone")
      f.deliveredVersions(cartId) mustBe Seq(1, 2)
      f.ack(cartId, 1, 2)
      outboxEmptyFor(cartId)
    }

    Scenario("a delivery lost while the aggregate stays active is delivered again after 3 minutes (F2)") {
      val f = new Fixture
      val cartId = newCart(f)
      f.deliveredVersions(cartId) mustBe Seq(1) // and lost: never acknowledged

      f.pollAfter(2.minutes)
      f.noDeliveryOf(cartId, 500.millis)

      f.pollAfter(1.minute + 1.second)
      f.deliveredVersions(cartId) mustBe Seq(1)
      f.ack(cartId, 1)
      outboxEmptyFor(cartId)
    }

    Scenario("a subscriber that stays silent gets the events again at 3 and 6 minutes; at 10 minutes they are reported undelivered") {
      val f = new Fixture
      val cartId = newCart(f)
      f.deliveredVersions(cartId) mustBe Seq(1)

      f.pollAfter(3.minutes + 1.second)
      f.deliveredVersions(cartId) mustBe Seq(1)
      f.pollAfter(3.minutes)
      f.deliveredVersions(cartId) mustBe Seq(1)

      f.pollAfter(4.minutes)
      awaitAssert(f.gaveUp() must contain(cartId), 5.seconds, 100.millis)

      f.pollAfter(5.minutes)
      f.noDeliveryOf(cartId, 1.second)
    }
  }

  Feature("Deleting delivered rows") {

    Scenario("happens when the delivery is acknowledged after the aggregate's owner has stopped (reaped, passivated)") {
      val f = new Fixture
      val cartId = newCart(f)
      f.deliveredVersions(cartId) mustBe Seq(1)

      val owner = Await.result(system.actorSelection(f.commandBus.path / s"ShoppingCart_${cartId.asLong}_*").resolveOne(3.seconds), 3.seconds)
      val watcher = TestProbe()
      watcher.watch(owner)
      f.commandBus ! PassivateAggregate(cartId)
      watcher.expectTerminated(owner, 3.seconds)

      f.ack(cartId, 1)
      outboxEmptyFor(cartId)
    }
  }
}

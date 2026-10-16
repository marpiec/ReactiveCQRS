package io.reactivecqrs.testdomain.spec

import java.time.Instant

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.aggregate.{AggregateOwner, OwnerSettings}
import io.reactivecqrs.core.eventstore.NewEvents
import io.reactivecqrs.core.commandhandler.{Idempotency, MemoryCommandResponseState}
import io.reactivecqrs.core.delivery.{RepublishOutbox, RepublishRange}
import io.reactivecqrs.core.eventbus.EventsBusActor.{PublishEvents, PublishEventsAck}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testutils.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.testkit.TestProbe
import scalikejdbc._

import scala.concurrent.duration._

// D8, D20c: the owner re-publishes from a version up to its current one, with its current state.
class RepublishSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val context = new ShoppingCartAggregateContext

  private val userId = UserId(1)

  /** A cart with three events in the store and in the outbox; no actor has published them. */
  private def storedCart(): AggregateId = {
    val cartId = AggregateId(System.nanoTime())
    eventStore.localTx { session =>
      eventStore.persistEvents(cartId, NewEvents[ShoppingCart](userId, AggregateVersion.ZERO,
        Instant.now, Seq(ShoppingCartCreated("Groceries"), ItemAdded("apple"), ItemAdded("pear"))))(session)
    }.get
    cartId
  }

  private def ownerOf(cartId: AggregateId, eventBus: TestProbe, maxRepublishEvents: Int = 1000): ActorRef = system.actorOf(Props(new AggregateOwner[ShoppingCart](cartId,
    new OwnerSettings[ShoppingCart](context.aggregateType, eventStore, new Idempotency(new MemoryCommandResponseState), eventBus.ref, ActorRef.noSender,
      context.commandHandlers, context.rewriteHistoryCommandHandlers, context.eventHandlers, () => context.initialAggregateRoot,
      context.commandTimeout, 5.seconds, maxRepublishEvents))))

  private def published(eventBus: TestProbe): PublishEvents[ShoppingCart] =
    eventBus.expectMsgType[PublishEvents[ShoppingCart]](5.seconds)

  Feature("Re-publishing what waits in the outbox") {

    Scenario("sends the events from the oldest waiting version to the current one, with the current state") {
      val cartId = storedCart()
      DB.autoCommit { implicit session => sql"DELETE FROM events_to_publish WHERE aggregate_id = ${cartId.asLong} AND version = 1".update() }
      val eventBus = TestProbe()

      ownerOf(cartId, eventBus) ! RepublishOutbox(cartId)

      val publication = published(eventBus)
      publication.events.map(_.version.asInt) mustBe Seq(2, 3)
      publication.events.map(_.event) mustBe Seq(ItemAdded("apple"), ItemAdded("pear"))
      publication.aggregate.get.items.map(_.name) mustBe Vector("apple", "pear")
      publication.onlyTo mustBe None
    }

    Scenario("sends a long range in parts, the next one when the previous one is acknowledged") {
      val cartId = storedCart()
      val eventBus = TestProbe()
      val owner = ownerOf(cartId, eventBus, maxRepublishEvents = 2)

      owner ! RepublishOutbox(cartId)
      published(eventBus).events.map(_.version.asInt) mustBe Seq(1, 2)
      eventBus.expectNoMessage(300.millis)

      owner ! PublishEventsAck(cartId, Seq(AggregateVersion(1), AggregateVersion(2)))
      published(eventBus).events.map(_.version.asInt) mustBe Seq(3)
    }

    Scenario("sends nothing when nothing waits") {
      val cartId = storedCart()
      DB.autoCommit { implicit session => sql"DELETE FROM events_to_publish WHERE aggregate_id = ${cartId.asLong}".update() }
      val eventBus = TestProbe()

      ownerOf(cartId, eventBus) ! RepublishOutbox(cartId)

      eventBus.expectNoMessage(500.millis)
    }
  }

  Feature("Re-publishing a range to one subscriber") {

    Scenario("sends the versions from the requested one to the current one, to that subscriber only") {
      val cartId = storedCart()
      val eventBus = TestProbe()
      val subscriber = TestProbe()

      ownerOf(cartId, eventBus) ! RepublishRange(cartId, AggregateVersion(2), subscriber.ref)

      val publication = published(eventBus)
      publication.events.map(_.version.asInt) mustBe Seq(2, 3)
      publication.onlyTo mustBe Some(subscriber.ref)
    }

    Scenario("sends a long range in parts: the subscriber asks for the next one") {
      val cartId = storedCart()
      val eventBus = TestProbe()
      val subscriber = TestProbe()

      ownerOf(cartId, eventBus, maxRepublishEvents = 2) ! RepublishRange(cartId, AggregateVersion(1), subscriber.ref)

      published(eventBus).events.map(_.version.asInt) mustBe Seq(1, 2)
      eventBus.expectNoMessage(300.millis)
    }

    Scenario("sends nothing for a version the aggregate has not reached") {
      val cartId = storedCart()
      val eventBus = TestProbe()

      ownerOf(cartId, eventBus) ! RepublishRange(cartId, AggregateVersion(9), TestProbe().ref)

      eventBus.expectNoMessage(500.millis)
    }
  }
}

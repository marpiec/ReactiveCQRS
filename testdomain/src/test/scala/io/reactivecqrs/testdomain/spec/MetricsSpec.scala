package io.reactivecqrs.testdomain.spec

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.PassivateAggregate
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventbus.{DeliveryOptions, EventsBusActor}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.metrics.{ReactiveMetrics, RecordingMetrics}
import io.reactivecqrs.core.projection.MemorySubscriptionsState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.core.util.BlockingDispatcher
import io.reactivecqrs.testdomain.shoppingcart._
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase, TestIds}
import org.apache.pekko.actor.Props

import scala.concurrent.duration._
import scala.util.Try

// R19: what the library measures reaches the metrics the host set.
class MetricsSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))
  private val userId = UserId(1)

  private val recording = new RecordingMetrics
  ReactiveMetrics(system).use(recording)

  Feature("Metrics (R19)") {

    Scenario("commands, stored events, restores, a projection's work and the bus's levels are measured") {
      Given("a system with a projection, whose host set its metrics")
      val bus = system.actorOf(Props(new EventsBusActor(DeliveryOptions(tickInterval = 100.millis))))
      val projection = system.actorOf(Props(new DeliveredCartsProjection(new MemorySubscriptionsState)).withDispatcher(BlockingDispatcher.id))
      bus ! RegisterSubscribers(Seq(projection))
      val commands = system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore, responses, bus))
      val cart = AggregateId(TestIds.aggregate())

      When("a cart is created, gets an item, and a command expects an older version")
      val created = commands.askFor[CustomCommandResponse[_]](CreateShoppingCartWithId(None, userId, cart, "cart"))
      created mustBe a[SuccessResponse]
      val added = commands.askFor[CustomCommandResponse[_]](AddItem(userId, cart, AggregateVersion(1), "apple"))
      added mustBe a[SuccessResponse]
      val late = commands.askFor[CustomCommandResponse[_]](AddItem(userId, cart, AggregateVersion(1), "pear"))
      late mustBe a[AggregateConcurrentModificationError]

      Then("each command's time is measured with its type, its class and its answer's")
      recording.timed("reactivecqrs.command", Map("type" -> "ShoppingCart", "outcome" -> "SuccessResponse")).size mustBe 2
      recording.timed("reactivecqrs.command", Map("command" -> "AddItem", "outcome" -> "AggregateConcurrentModificationError")).size mustBe 1

      And("the stored events and their bytes are counted")
      recording.counted("reactivecqrs.events.stored", Map("type" -> "ShoppingCart")) mustBe 2
      recording.counted("reactivecqrs.events.stored.bytes", Map("type" -> "ShoppingCart")) must be > 0L

      And("the projection's applied events, and the bus's levels")
      awaitAssert(recording.counted("reactivecqrs.projection.applied", Map("projection" -> "DeliveredCartsProjection")) mustBe 2, 10.seconds, 100.millis)
      awaitAssert(recording.gauged("reactivecqrs.bus.waiting") mustBe Seq(0L), 10.seconds, 100.millis)
      awaitAssert(recording.gauged("reactivecqrs.bus.behind", Map("subscriber" -> projection.path.name)) mustBe Seq(0L), 10.seconds, 100.millis)

      And("a restore, by where it read the aggregate from")
      val restoresBefore = recording.timed("reactivecqrs.restore", Map("type" -> "ShoppingCart", "from" -> "events")).size
      commands ! PassivateAggregate(cart)
      awaitAssert {
        val read = commands.askFor[Try[Aggregate[ShoppingCart]]](GetAggregate(cart))
        read.get.version mustBe AggregateVersion(2)
        recording.timed("reactivecqrs.restore", Map("type" -> "ShoppingCart", "from" -> "events")).size must be > restoresBefore
      }
    }
  }
}

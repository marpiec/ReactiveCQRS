package io.reactivecqrs.examples.cart

// Chapter 18's code, run against the test domain's carts: each block of the chapter is a region here, followed by the
// lines that check it.

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.commandhandler.PostgresCommandResponseState
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventstore.{EventKeysBackfill, PostgresEventStoreState, SpaceBackfill}
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.rebuild.PostgresProjectionRegistry
import io.reactivecqrs.core.saga.PostgresSagaState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.actor.Props
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._

class ProductionSpec extends CartSpec {

  implicit val timeout: Timeout = Timeout(70.seconds)

  Feature("What a production host starts") {

    Scenario("every store's tables, made or brought up to date at every start") {
      // #tables
      val mpjsons = new MPJsons
      val typesNames = new PostgresTypesNamesState().initSchema()
      val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
      val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
      val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
      val registry = new PostgresProjectionRegistry().initSchema()
      val sagaState = new PostgresSagaState(mpjsons, typesNames)
      sagaState.initSchema() // this one returns Unit, so it gets a line of its own
      // #tables
      Seq[AnyRef](eventStore, responses, subscriptions, registry, sagaState) must not contain (null)
    }

    Scenario("what runs once per installation, started as singletons; the scheduler squashes by its rule") {
      val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
      eventBus ! EventsBusActor.RegisterSubscribers(Nil)
      val cartContext = new AutomaticallySquashedCarts // chapter 14's: after 1 000 events, keeping the last 100
      val carts = cartsOf(cartContext, "productionCarts")
      val cartId = Await.result((carts ? CreateShoppingCart(None, UserId(1), "Groceries")).mapTo[CustomCommandResponse[_]],
        timeout.duration).asInstanceOf[SuccessResponse].aggregateId
      Await.result(carts ? AddItems(UserId(1), cartId, AggregateVersion(1), (1 to 1100).map("item " + _)), timeout.duration)

      // #singletons
      import io.reactivecqrs.core.aggregate.SquashScheduler
      import io.reactivecqrs.core.aggregate.SquashScheduler.SquashTarget
      import io.reactivecqrs.core.cluster.Singletons
      import io.reactivecqrs.core.delivery.OutboxPollerActor
      import org.apache.pekko.actor.Props

      // eventStore and eventBus as wired before; cartContext has an automatic squash rule, carts is its command bus
      val outboxPoller = Singletons.start(system, "outboxPoller", Props(new OutboxPollerActor(eventStore, eventBus)))
      val squashScheduler = Singletons.start(system, "squashScheduler",
        Props(new SquashScheduler(eventStore, Seq(SquashTarget.of(cartContext, carts)))))
      // #singletons
      outboxPoller must not be null
      awaitAssert({
        squashScheduler ! SquashScheduler.Run // a run now; the first comes a minute after the start
        Await.result((squashScheduler ? SquashScheduler.GetSquashStatus).mapTo[SquashScheduler.SquashStatus], 5.seconds).squashed mustBe 1L
      }, 30.seconds, 500.millis)
    }

    Scenario("the maintenance of an upgrade, and the stored data check") {
      // #maintenance
      eventStore.buildIndexesConcurrently()          // indexes built without blocking writes
      val spaces = eventStore.backfillSpaces()       // SpaceBackfill(filled, left)
      val keys = eventStore.backfillEventKeys()      // EventKeysBackfill(written, missing, orphans)
      // #maintenance
      (spaces, keys) mustBe ((SpaceBackfill(0, Nil), EventKeysBackfill(0, 0))) // a 1.0 store has every space and key

      // #check
      import io.reactivecqrs.core.tools.StoredDataCheck

      val report = StoredDataCheck.run(mpjsons, typesNames,
        Seq(StoredDataCheck.ProjectionTable[CartSummary]("carts_list")))
      println(report.text)
      // #check
      report.unreadable mustBe 0L

      locally {
        // #upgradeCheck
        import io.reactivecqrs.core.tools.StoredDataCheck

        val report = StoredDataCheck.run(mpjsons, typesNames,
          Seq(StoredDataCheck.ProjectionTable[String]("storeA"))) // one per document store, with its document class
        println(report.text)
        if (report.unreadable > 0) sys.error("Fix the unreadable rows before the upgrade")
        // #upgradeCheck
        report.missingTables mustBe Seq("projection_storeA") // this test schema has no such store
      }
    }

    Scenario("the library's measures forwarded to a metrics system of one's own") {
      // #useMetrics
      import io.reactivecqrs.core.metrics.ReactiveMetrics

      ReactiveMetrics(system).use(new ForwardingMetrics)
      // #useMetrics
      ReactiveMetrics(system).metrics mustBe a[ForwardingMetrics]
    }

    Scenario("an event store with a larger event for one class, read in smaller pieces") {
      // #largeEvents
      val eventStore = new PostgresEventStoreState(mpjsons, typesNames,
        fetchSize = 10,
        eventSizeLimitPerClassName = Map(classOf[ReceiptAttached].getName -> 6000000) // an event of your own
      ).initSchema()
      // #largeEvents
      eventStore must not be null
    }
  }
}

// #forwardingMetrics
import io.reactivecqrs.core.metrics.Metrics

class ForwardingMetrics extends Metrics {
  override def count(name: String, tags: Map[String, String], amount: Long): Unit =
    () // add `amount` to your metrics system's counter `name`, with `tags`
  override def time(name: String, tags: Map[String, String], nanos: Long): Unit =
    () // record `nanos` in your timer `name`
  override def gauge(name: String, tags: Map[String, String], value: Long): Unit =
    () // set your gauge `name` to `value`
}
// #forwardingMetrics

/** A cart's receipt, a scan of a few megabytes: an event larger than the store's usual limit. */
case class ReceiptAttached(scan: String) extends Event[ShoppingCart]

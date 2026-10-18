package io.reactivecqrs.examples.cart

// Chapter 11's code, run against the test domain's carts: each block of the chapter is a region here, followed by the
// lines that check what the chapter says it does.

import java.util.concurrent.ConcurrentLinkedQueue

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor
import io.reactivecqrs.core.documentstore.{MultipleTextIndex, NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.projection.SubscribableProjectionActor._
import io.reactivecqrs.core.util.BlockingDispatcher
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.testkit.TestProbe
import org.apache.pekko.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class LiveUpdatesSpec extends CartSpec {

  implicit val timeout: Timeout = Timeout(70.seconds)
  private val alice = UserId(1)

  // Chapter 4's wiring, with chapter 8's notebook and cart list.
  private val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
  private val carts = system.actorOf(
    AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore, responseState, eventBus), "pagedCarts")
  private val subscriptionsState = new PostgresSubscriptionsState(typesNames).initSchema()
  private val cartsList = system.actorOf(Props(new CartsListProjection(subscriptionsState, new PostgresDocumentStore[CartSummary](
    "carts_list", mpjsons, new NoopDocumentStoreCache, Seq(MultipleTextIndex(1, Seq("name")))))).withDispatcher(BlockingDispatcher.id), "CartsList")

  // #start
  val liveCarts = system.actorOf(
    Props(new LiveCartsProjection(subscriptionsState,
      new PostgresDocumentStore[CartSummary]("live_carts", mpjsons, new NoopDocumentStoreCache)))
      .withDispatcher(BlockingDispatcher.id),
    "LiveCarts")

  eventBus ! EventsBusActor.RegisterSubscribers(List(cartsList, liveCarts))
  // #start

  private def succeeds(bus: ActorRef, command: AnyRef): SuccessResponse =
    Await.result((bus ? command).mapTo[CustomCommandResponse[_]], timeout.duration) match {
      case done: SuccessResponse => done
      case refused => fail(s"$command was answered $refused")
    }

  private def pageOf(projection: ActorRef, cartId: AggregateId): CartPageUpdate = projection.askFor[CartPageUpdate](GetCartPage(cartId))

  Feature("A live cart page") {

    Scenario("an open page shows the cart, then the milk Alice adds") {
      val cartId = succeeds(carts, CreateShoppingCart(None, alice, "Groceries")).aggregateId
      succeeds(carts, AddItem(alice, cartId, AggregateVersion(1), "bread"))
      succeeds(carts, AddItem(alice, cartId, AggregateVersion(2), "butter"))
      awaitAssert(pageOf(liveCarts, cartId).summary.map(_.version) mustBe Some(3), 10.seconds, 100.millis)
      val pushed = new ConcurrentLinkedQueue[CartPageUpdate]()
      object browser { def send(update: CartPageUpdate): Unit = pushed.add(update) } // the web layer, here a list

      // #openSession
      val session = system.actorOf(Props(new CartPageSession(liveCarts, cartId, update => browser.send(update))))
      // #openSession
      awaitAssert(pushed.asScala.toSeq mustBe Seq(CartPageUpdate(cartId, Some(CartSummary("Groceries", 2, 3)))), 10.seconds, 50.millis)

      When("Alice adds milk")
      succeeds(carts, AddItem(alice, cartId, AggregateVersion(3), "milk"))
      awaitAssert(pushed.asScala.toSeq.last mustBe CartPageUpdate(cartId, Some(CartSummary("Groceries", 3, 4))), 10.seconds, 50.millis)

      // #closeSession
      system.stop(session)
      // #closeSession
    }

    Scenario("an unknown subscription is answered as cancelled, renewed or cancelled") {
      val probe = TestProbe()
      probe.send(liveCarts, RenewSubscription("forgotten"))
      probe.expectMsg(ProjectionSubscriptionsCancelled(List("forgotten")))
      probe.send(liveCarts, CancelProjectionSubscriptions(List("unknown")))
      probe.expectMsg(ProjectionSubscriptionsCancelled(List("unknown")))
    }

    Scenario("a time to live of one's own; a cache that a late subscriber catches up from") {
      // #ttl
      val patientLiveCarts = Props(new LiveCartsProjection(subscriptionsState,
        new PostgresDocumentStore[CartSummary]("live_carts", mpjsons, new NoopDocumentStoreCache),
        subscriptionTTL = 5 * 60 * 1000)) // 5 minutes instead of 10
      // #ttl
      patientLiveCarts must not be null

      // #cache
      val cachingLiveCarts = system.actorOf(
        Props(new LiveCartsProjection(subscriptionsState,
          new PostgresDocumentStore[CartSummary]("cached_live_carts", mpjsons, new NoopDocumentStoreCache),
          updatesCacheTTL = java.time.Duration.ofSeconds(30)))
          .withDispatcher(BlockingDispatcher.id),
        "CachingLiveCarts")
      // #cache
      val cachedCarts = cartsOf(new ShoppingCartAggregateContext, "cachedCarts", subscribers = List(cachingLiveCarts))
      val cartId = succeeds(cachedCarts, CreateShoppingCart(None, alice, "Groceries")).aggregateId
      succeeds(cachedCarts, AddItem(alice, cartId, AggregateVersion(1), "milk"))
      awaitAssert(pageOf(cachingLiveCarts, cartId).summary.map(_.version) mustBe Some(2), 10.seconds, 100.millis)

      Then("a subscriber that comes late gets the updates it missed, oldest first")
      val probe = TestProbe()
      probe.send(cachingLiveCarts, WatchCart("late", cartId))
      probe.expectMsg(SubscribedForProjectionUpdates("late"))
      probe.expectMsg(SubscriptionUpdated("late", CartPageUpdate(cartId, Some(CartSummary("Groceries", 0, 1)))))
      probe.expectMsg(SubscriptionUpdated("late", CartPageUpdate(cartId, Some(CartSummary("Groceries", 1, 2)))))
    }
  }
}

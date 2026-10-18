package io.reactivecqrs.examples.cart

// Chapter 9's code, run against the test domain's carts: each block of the chapter is a region here, followed by the
// lines that check what the chapter says it does.

import java.util.concurrent.ConcurrentHashMap

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor
import io.reactivecqrs.core.documentstore.{MultipleTextIndex, NoopDocumentStoreCache, PostgresDocumentStore, UniqueTextIndex}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.projection.{GetProjectionStatus, PostgresSubscriptionsState, ProjectionActorOptions, ProjectionStatus}
import io.reactivecqrs.core.util.{Backoff, BlockingDispatcher}
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._

class FailuresSpec extends CartSpec {

  implicit val timeout: Timeout = Timeout(70.seconds)
  private val alice = UserId(1)
  private val bob = UserId(2)
  private val subscriptionsState = new PostgresSubscriptionsState(typesNames).initSchema()

  // Chapter 4's wiring, for the bus's and the poller's status.
  private val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
  system.actorOf(AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore, responseState, eventBus), "carts4")
  eventBus ! EventsBusActor.RegisterSubscribers(Nil)

  private def succeeds(bus: ActorRef, command: AnyRef): SuccessResponse =
    Await.result((bus ? command).mapTo[CustomCommandResponse[_]], timeout.duration) match {
      case done: SuccessResponse => done
      case refused => fail(s"$command was answered $refused")
    }

  private def statusOf(projection: ActorRef): ProjectionStatus = projection.askFor[ProjectionStatus](GetProjectionStatus)

  Feature("A listener with a side effect") {

    Scenario("a deleted cart gets one mail") {
      val sent = new ConcurrentHashMap[String, String]()
      val mailer = new Mailer { def sendOnce(key: String, text: String): Unit = sent.putIfAbsent(key, text) }
      val mails = system.actorOf(Props(new CartDeletedMailer(subscriptionsState, mailer)).withDispatcher(BlockingDispatcher.id), "mailer")
      val mailedCarts = cartsOf(new ShoppingCartAggregateContext, "mailedCarts", subscribers = List(mails))
      val cartId = succeeds(mailedCarts, CreateShoppingCart(None, alice, "Groceries")).aggregateId
      succeeds(mailedCarts, DeleteShoppingCart(None, alice, cartId, AggregateVersion(1)))
      awaitAssert(sent.get("cart-deleted-" + cartId.asLong) mustBe "Your cart " + cartId.asLong + " was deleted.", 10.seconds, 100.millis)
      sent.size mustBe 1
    }
  }

  Feature("When a listener throws") {

    Scenario("a cart whose document breaks a unique index is recorded as failed, shown, and retried once fixed") {
      Given("the cart list on a store with a unique index on the name, retried three times, quickly")
      // #uniqueStore
      val cartsStore = new PostgresDocumentStore[CartSummary]("unique_carts", mpjsons, new NoopDocumentStoreCache,
        Seq(UniqueTextIndex(1, Seq("name"))))
      // #uniqueStore
      val cartsList = system.actorOf(Props(new CartsListProjection(subscriptionsState, cartsStore,
        ProjectionActorOptions.DEFAULT.withRetries(Backoff(10.millis, 50.millis, maxAttempts = 3)))).withDispatcher(BlockingDispatcher.id),
        "UniqueCartsList")
      val namedCarts = cartsOf(new ShoppingCartAggregateContext, "namedCarts", subscribers = List(cartsList))
      val groceries = succeeds(namedCarts, CreateShoppingCart(None, alice, "Groceries")).aggregateId
      awaitAssert(cartsList.askFor[Option[CartSummary]](GetCartSummary(groceries)) mustBe Some(CartSummary("Groceries", 0, 1)),
        10.seconds, 100.millis)

      When("Bob creates a cart of the same name")
      val cartId = succeeds(namedCarts, CreateShoppingCart(None, bob, "Groceries")).aggregateId
      awaitAssert(statusOf(cartsList).failures.map(_.aggregateId) mustBe Seq(cartId), 20.seconds, 100.millis)

      Then("the projection's status names the cart, its version and the error")
      // #status
      import io.reactivecqrs.core.projection.{GetProjectionStatus, ProjectionStatus}

      val status = Await.result((cartsList ? GetProjectionStatus).mapTo[ProjectionStatus], 5.seconds)
      status.failures.foreach { f =>
        println(f.aggregateType.simpleName + " " + f.aggregateId.asLong + " stuck at version " + f.version.asInt + ": " + f.error)
      }
      // ShoppingCart 43 stuck at version 1: PSQLException: ERROR: duplicate key value violates unique constraint ...
      // #status
      status.failures.map(f => (f.aggregateType.simpleName, f.aggregateId, f.version)) mustBe
        Seq(("ShoppingCart", cartId, AggregateVersion(1)))
      status.failures.head.error must include("PSQLException") // the rest is in the database's language

      When("the unique index gives way to an ordinary one, and the failure is retried")
      val fixedStore = new PostgresDocumentStore[CartSummary]("unique_carts", mpjsons, new NoopDocumentStoreCache,
        // #indexSwap
        Seq(MultipleTextIndex(2, Seq("name")))
        // #indexSwap
      )
      // #retry
      import io.reactivecqrs.core.projection.{FailedSubscriptionsHandled, RetryFailedSubscription}

      val handled = Await.result((cartsList ? RetryFailedSubscription(cartId)).mapTo[FailedSubscriptionsHandled], 5.seconds)
      // FailedSubscriptionsHandled(Id(43),1)
      // #retry
      handled mustBe FailedSubscriptionsHandled(cartId, 1)

      Then("Bob's cart is in the list, and nothing is failed any more")
      awaitAssert(fixedStore.getDocument(cartId.asLong).map(_.document) mustBe Some(CartSummary("Groceries", 0, 1)),
        10.seconds, 100.millis)
      statusOf(cartsList).failures mustBe empty
    }
  }

  Feature("Options, and the health of delivery") {

    Scenario("retries of one's own; the bus's delivery status; the outbox poller's options and status") {
      // #retries
      import io.reactivecqrs.core.projection.ProjectionActorOptions
      import io.reactivecqrs.core.util.Backoff

      val options = ProjectionActorOptions.DEFAULT
        .withRetries(Backoff(1.second, 10.seconds, maxAttempts = 4)) // four attempts, about 7 seconds of waiting
        .copy(gapGiveUpAfter = 30.minutes)
      // #retries
      (options.retries, options.gapGiveUpAfter) mustBe ((Backoff(1.second, 10.seconds, maxAttempts = 4), 30.minutes))

      // #delivery
      import io.reactivecqrs.core.eventbus.EventsBusActor.{DeliveryStatus, GetDeliveryStatus}

      val delivery = Await.result((eventBus ? GetDeliveryStatus).mapTo[DeliveryStatus], 5.seconds)
      // #delivery
      delivery.undelivered mustBe empty

      // #pollerOptions
      import io.reactivecqrs.core.delivery.{OutboxPollerActor, OutboxPollerOptions, RedeliveryPolicy}

      val outboxPoller = system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus,
        OutboxPollerOptions(redelivery = RedeliveryPolicy.Options(maxRepublishPerPoll = 5000)))), "outboxPoller")
      // #pollerOptions
      // #outboxStatus
      import io.reactivecqrs.core.delivery.OutboxPollerActor.{GetOutboxStatus, OutboxStatus}

      val outbox = Await.result((outboxPoller ? GetOutboxStatus).mapTo[OutboxStatus], 5.seconds)
      // outbox.waiting: aggregates with rows waiting at the last poll; outbox.gaveUp: those reported undelivered
      // #outboxStatus
      outbox.gaveUp mustBe empty
    }
  }
}

package io.reactivecqrs.examples.cart

// Chapter 8's code, run against the test domain's carts: each block of the chapter is a region here, followed by the
// lines that check what the chapter says it answers.

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.actor.Props
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.concurrent.Await
import scala.concurrent.duration._

class ReadModelsSpec extends CartSpec {

  implicit val timeout: Timeout = Timeout(70.seconds)
  private val alice = UserId(1)

  // Chapter 4's wiring of the test domain's carts; the chapter's projection joins their event bus.
  private val eventBus = system.actorOf(Props(new EventsBusActor()), "eventBus")
  private val carts = system.actorOf(
    AggregateCommandBusActor(new ShoppingCartAggregateContext, uidGenerator, eventStore, responseState, eventBus), "listedCarts")

  // #notebook
  import io.reactivecqrs.core.projection.PostgresSubscriptionsState

  val subscriptionsState = new PostgresSubscriptionsState(typesNames).initSchema()
  // #notebook

  // #store
  import io.reactivecqrs.core.documentstore.{MultipleTextIndex, NoopDocumentStoreCache, PostgresDocumentStore}

  val cartsStore = new PostgresDocumentStore[CartSummary](
    "carts_list",                           // the table: projection_carts_list
    mpjsons,                                // the MPJsons you made in chapter 4
    new NoopDocumentStoreCache,             // a cache that keeps nothing
    Seq(MultipleTextIndex(1, Seq("name")))) // an index on the JSON field "name"
  // #store

  // #start
  import org.apache.pekko.actor.Props
  import io.reactivecqrs.core.util.BlockingDispatcher

  val cartsList = system.actorOf(
    Props(new CartsListProjection(subscriptionsState, cartsStore)).withDispatcher(BlockingDispatcher.id),
    "CartsList")
  // #start

  // #register
  import io.reactivecqrs.core.eventbus.EventsBusActor

  eventBus ! EventsBusActor.RegisterSubscribers(List(cartsList))
  // #register

  private def send(command: AnyRef): CustomCommandResponse[_] =
    Await.result((carts ? command).mapTo[CustomCommandResponse[_]], 5.seconds)

  /** A Groceries cart with `items`; its id. */
  private def cartWith(items: String*): AggregateId = {
    val cartId = send(CreateShoppingCart(None, alice, "Groceries")).asInstanceOf[SuccessResponse].aggregateId
    items.zipWithIndex.foreach { case (item, i) => send(AddItem(alice, cartId, AggregateVersion(i + 1), item)) }
    cartId
  }

  private def summaryOf(cartId: AggregateId): Option[CartSummary] = cartsList.askFor[Option[CartSummary]](GetCartSummary(cartId))

  Feature("The cart list") {

    Scenario("a new cart with milk shows up in the list, and in its table") {
      // #tryIt
      import org.apache.pekko.pattern.ask
      import org.apache.pekko.util.Timeout
      import io.reactivecqrs.api._
      import io.reactivecqrs.testdomain.shoppingcart._
      import scala.concurrent.Await
      import scala.concurrent.duration._

      implicit val timeout: Timeout = Timeout(5.seconds)

      def send(command: AnyRef): CustomCommandResponse[_] =
        Await.result((carts ? command).mapTo[CustomCommandResponse[_]], 5.seconds)

      val cartId = send(CreateShoppingCart(None, alice, "Groceries")) match {
        case SuccessResponse(id, _) => id
        case refused => throw new IllegalStateException("No cart: " + refused)
      }
      send(AddItem(alice, cartId, AggregateVersion(1), "milk"))       // SuccessResponse(Id(42),Version(2))

      Thread.sleep(500) // give the projection a moment; a better way comes later in this chapter

      val summary = Await.result((cartsList ? GetCartSummary(cartId)).mapTo[Option[CartSummary]], 5.seconds)
      // Some(CartSummary(Groceries,1,2))
      // #tryIt
      summary.forall(_ == CartSummary("Groceries", 1, 2)) mustBe true // on a busy machine, half a second may be too short
      awaitAssert(summaryOf(cartId) mustBe Some(CartSummary("Groceries", 1, 2)), 10.seconds, 100.millis)

      Then("its table holds the document, the other questions find it")
      DB.readOnly(implicit session =>
        sql"""SELECT document->>'name', (document->>'itemCount')::int, (document->>'version')::int
             |  FROM projection_carts_list WHERE id = ${cartId.asLong}""".stripMargin
          .map(rs => (rs.string(1), rs.int(2), rs.int(3))).single()) mustBe Some(("Groceries", 1, 2))
      cartsList.askFor[Vector[CartSummary]](FindCartsByName("Groceries")) must contain(CartSummary("Groceries", 1, 2))
      cartsList.askFor[Map[AggregateId, CartSummary]](GetAllCarts).get(cartId) mustBe Some(CartSummary("Groceries", 1, 2))
    }

    Scenario("the biggest carts first") {
      val five = cartWith("a", "b", "c", "d", "e")
      val three = cartWith("a", "b", "c")
      cartWith("a")
      awaitAssert(cartsList.askFor[Seq[(AggregateId, CartSummary)]](FindBigCarts(2)).take(2) mustBe
        Seq(five -> CartSummary("Groceries", 5, 6), three -> CartSummary("Groceries", 3, 4)), 10.seconds, 100.millis)
    }

    Scenario("reading your own write: the query waits until the projection has the version") {
      val cartId = cartWith("milk")
      // #readYourWrite
      val withBread = send(AddItem(alice, cartId, AggregateVersion(2), "bread")) match {
        case SuccessResponse(_, version) =>
          Await.result((cartsList ? GetCartSummaryAtLeast(cartId, version)).mapTo[Option[CartSummary]], 5.seconds)
          // Some(CartSummary(Groceries,2,3)), as soon as the projection has it
        case other => println("Not added: " + other)
      }
      // #readYourWrite
      withBread mustBe Some(CartSummary("Groceries", 2, 3))
    }
  }

  Feature("An activity log, from events") {

    Scenario("every event is a line of the cart's log, an undo included") {
      val activityStore = new PostgresDocumentStore[CartActivity]("cart_activity", mpjsons, new NoopDocumentStoreCache)
      val activity = system.actorOf(
        Props(new CartActivityProjection(subscriptionsState, activityStore)).withDispatcher(BlockingDispatcher.id), "CartActivity")
      val activityCarts = cartsOf(new ShoppingCartAggregateContext, "activityCarts", subscribers = List(activity))
      def sendToActivityCarts(command: AnyRef): CustomCommandResponse[_] =
        Await.result((activityCarts ? command).mapTo[CustomCommandResponse[_]], 5.seconds)
      val cartId = sendToActivityCarts(CreateShoppingCart(None, alice, "Groceries")).asInstanceOf[SuccessResponse].aggregateId
      sendToActivityCarts(AddItem(alice, cartId, AggregateVersion(1), "milk"))
      sendToActivityCarts(UndoShoppingCartChange(alice, cartId, AggregateVersion(2), 1))
      awaitAssert(activity.askFor[Option[CartActivity]](GetCartActivity(cartId)) mustBe Some(CartActivity(Vector(
        "v1, user 1: created \"Groceries\"", "v2, user 1: added milk", "v3, user 1: undid the last 1 change(s)"))),
        10.seconds, 100.millis)
    }
  }

  Feature("Options") {

    Scenario("grouped updates, and new carts that never wait") {
      // #options
      import io.reactivecqrs.core.projection.ProjectionActorOptions

      val options = ProjectionActorOptions.DEFAULT.withGroupUpdatesDelayMillis(200).withMinimumDelayVersion(2)
      // #options
      (options.groupUpdatesDelayMillis, options.minimumDelayVersion) mustBe ((200L, 2))
    }
  }
}

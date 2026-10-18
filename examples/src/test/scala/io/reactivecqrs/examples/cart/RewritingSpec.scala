package io.reactivecqrs.examples.cart

// Chapter 15's code, run against the test domain's carts and the chapter's orders: each block of the chapter is a region
// here, followed by the lines that check what the chapter says it does.

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor
import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.rebuild.ProjectionSupervisor.{Builds, GetBuilds}
import io.reactivecqrs.core.rebuild.{PostgresProjectionRegistry, Projections, ProjectionsSettings}
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

class RewritingSpec extends CartSpec {

  implicit val timeout: Timeout = Timeout(70.seconds)
  private val alice = UserId(1)

  private def await[T](question: Future[T]): T = Await.result(question, timeout.duration)

  private def succeeds(bus: ActorRef, command: AnyRef): SuccessResponse = await(bus ? command) match {
    case done: SuccessResponse => done
    case refused => fail(s"$command was answered $refused")
  }

  private def eventsOf[R](bus: ActorRef, id: AggregateId): List[EventInfo[R]] =
    bus.askFor[Try[List[EventInfo[R]]]](GetEventsForAggregate(id)).get

  // The orders of the complete rewrite, with a managed projection of who placed each order.
  private val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
  private val registry = new PostgresProjectionRegistry().initSchema()
  private val ordersContext = new OrdersContext
  private val ordersEvents = system.actorOf(Props(new EventsBusActor()), "ordersEvents")
  private val orders: ActorRef =
    system.actorOf(AggregateCommandBusActor(ordersContext, uidGenerator, eventStore, responseState, ordersEvents), "orders")
  private val ordersProjection: ActorRef =
    new Projections(system, ProjectionsSettings(eventStore, subscriptions, registry, Seq(ordersContext)))
      .start[OrderCustomers]("order-customers", 1, "orderCustomers") { build =>
        new OrderCustomers(subscriptions,
          new PostgresDocumentStore[OrderCustomer](build.documentStore("order_customers"), mpjsons, new NoopDocumentStoreCache))
      }
  ordersEvents ! RegisterSubscribers(Seq(ordersProjection))

  private def customerOf(orderId: AggregateId): Option[OrderCustomer] =
    ordersProjection.askFor[Option[OrderCustomer]](GetOrderCustomer(orderId), 5.seconds)

  private def outboxEmpty(id: AggregateId): Boolean = DB.readOnly { implicit session =>
    sql"SELECT count(*) FROM events_to_publish WHERE aggregate_id = ${id.asLong}".map(_.long(1)).single().get == 0
  }

  Feature("A history rewrite") {

    Scenario("a typo is fixed in the stored creation event, and a compensating event tells everyone else") {
      val carts = commandBus
      val cartId = succeeds(carts, CreateShoppingCart(None, alice, "Grocceries")).aggregateId
      succeeds(carts, AddItem(alice, cartId, AggregateVersion(1), "apples"))
      succeeds(carts, AddItem(alice, cartId, AggregateVersion(2), "oranges"))

      // #send
      import org.apache.pekko.pattern.ask
      import org.apache.pekko.util.Timeout
      import scala.concurrent.Future
      import scala.concurrent.duration._

      implicit val timeout: Timeout = Timeout(10.seconds)

      val answer: Future[CustomCommandResponse[_]] =
        (carts ? RewriteCartName(alice, cartId, AggregateVersion(3), "Groceries")).mapTo[CustomCommandResponse[_]]
      // SuccessResponse(cartId, AggregateVersion(4))
      // #send
      await(answer) mustBe SuccessResponse(cartId, AggregateVersion(4))
      eventsOf[ShoppingCart](carts, cartId).map(e => (e.version.asInt, e.event)) mustBe List(1 -> ShoppingCartCreated("Groceries"),
        2 -> ItemAdded("apples"), 3 -> ItemAdded("oranges"), 4 -> CartNameRewritten("Groceries"))
      carts.askFor[Try[Aggregate[ShoppingCart]]](GetAggregate(cartId)).get.aggregateRoot.get.name mustBe "Groceries"
    }

    Scenario("a rewritten event keeps its class; a rewrite without a new event is stored and keeps the version") {
      val classChanging = cartsOf(new ClassChangingCarts, "classChangingCarts")
      val changed = succeeds(classChanging, CreateShoppingCart(None, alice, "Grocceries")).aggregateId
      await(classChanging ? RewriteCartName(alice, changed, AggregateVersion(1), "Groceries")) mustBe an[EventHandlingError]
      eventsOf[ShoppingCart](classChanging, changed).map(_.event) mustBe List(ShoppingCartCreated("Grocceries"))

      val silent = cartsOf(new SilentlyRenamingCarts, "silentlyRenamingCarts")
      val renamed = succeeds(silent, CreateShoppingCart(None, alice, "Grocceries")).aggregateId
      await(silent ? RewriteCartName(alice, renamed, AggregateVersion(1), "Groceries")) mustBe
        SuccessResponse(renamed, AggregateVersion(1))
      eventsOf[ShoppingCart](silent, renamed).map(_.event) mustBe List(ShoppingCartCreated("Groceries"))
    }
  }

  Feature("The complete rewrite") {

    Scenario("a customer is erased from an order's events, its squash event, its archive and the read model") {
      awaitAssert({ // its first build done: a targeted rebuild is refused while a build runs
        val builds = ordersProjection.askFor[Builds](GetBuilds, 5.seconds)
        (builds.active.map(_.number), builds.building) mustBe ((Some(1), None))
      }, 60.seconds, 100.millis)

      Given("an order placed by a customer, squashed with the placing kept in the archive")
      val customerId = CustomerId(42)
      val operator = UserId(99)
      val orderId = succeeds(orders, PlaceOrder(alice, customerId, "Ann Example", Some("ann@example.com"))).aggregateId
      succeeds(orders, AddOrderLine(alice, orderId, AggregateVersion(1), "tea"))
      succeeds(orders, AddOrderLine(alice, orderId, AggregateVersion(2), "honey"))
      awaitAssert(outboxEmpty(orderId) mustBe true, 20.seconds, 50.millis)
      orders.askFor[SquashResponse](SquashAggregate(orderId, AggregateVersion(2))) mustBe AggregateSquashed(orderId, AggregateVersion(2))
      awaitAssert(customerOf(orderId) mustBe Some(OrderCustomer("Ann Example", Some("ann@example.com"))), 20.seconds, 50.millis)

      When("the customer is erased")
      // #run
      import io.reactivecqrs.core.rewrite.{HistoryRewriter, RewriteReport}

      val report: Future[RewriteReport] =
        new HistoryRewriter(orders, eventStore, projections = Map("orders" -> ordersProjection))
          .rewrite(orderId, EraseCustomer(customerId), operator)
      // #run
      val done = await(report)

      Then("the order, its archive and the read model hold the customer no more, and the rewrite is recorded")
      (done.complete, done.rebuilt) mustBe ((true, Seq("orders")))
      done.rewritten.map(r => (r.id, r.events, r.archived)) mustBe Seq((orderId, 1, 1))
      eventsOf[Order](orders, orderId).map(_.event) mustBe
        List(OrderSquashed(customerId, "erased", None, Vector("tea")), OrderLineAdded("honey"))
      orders.askFor[Try[ArchivedEvents[Order]]](GetArchivedEvents(orderId)).get.events.map(_.info.event) mustBe
        Seq(OrderPlaced(customerId, "erased", None))
      awaitAssert(customerOf(orderId) mustBe Some(OrderCustomer("erased", None)), 20.seconds, 50.millis)
      DB.readOnly(implicit session => sql"SELECT count(*) FROM history_rewrites WHERE aggregate_id = ${orderId.asLong}"
        .map(_.long(1)).single().get) mustBe 1L
    }
  }
}

/** A rewrite that would turn the creation into an item: refused, as a rewritten event keeps its class. */
class ClassChangingCarts extends ShoppingCartAggregateContext {
  override def rewriteHistoryCommandHandlers = (events, _) => {
    case c: RewriteCartName =>
      RewriteCommandSuccess(events.map(ev => EventWithVersion[ShoppingCart](ev.version, ItemAdded(c.name))).toSeq, CartNameRewritten(c.name))
  }
}

/** A rewrite with no new event: stored, at the same version, and published to nobody. */
class SilentlyRenamingCarts extends ShoppingCartAggregateContext {
  override def rewriteHistoryCommandHandlers = (events, _) => {
    case c: RewriteCartName =>
      RewriteCommandSuccess(events.map(ev => ev.event match {
        case e: ShoppingCartCreated => EventWithVersion(ev.version, e.copy(name = c.name))
        case _ => ev
      }).toSeq, Seq.empty[Event[ShoppingCart]])
  }
}

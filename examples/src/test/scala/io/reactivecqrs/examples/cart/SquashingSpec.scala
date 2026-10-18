package io.reactivecqrs.examples.cart

// Chapter 14's code, run against the test domain's carts: each block of the chapter is a region here, followed by the
// lines that check what the chapter says it does.

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.tools.StoredDataCheck.ClassUser
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

class SquashingSpec extends CartSpec {

  implicit val timeout: Timeout = Timeout(70.seconds)
  private val alice = UserId(1)

  private def await[T](question: Future[T]): T = Await.result(question, timeout.duration)

  private def succeeds(bus: ActorRef, command: AnyRef): SuccessResponse = await(bus ? command) match {
    case done: SuccessResponse => done
    case refused => fail(s"$command was answered $refused")
  }

  /** A Groceries cart with `items` added one by one, at versions 2 and on. */
  private def cartWith(bus: ActorRef, items: String*): AggregateId = {
    val cartId = succeeds(bus, CreateShoppingCart(None, alice, "Groceries")).aggregateId
    items.zipWithIndex.foreach { case (item, i) => succeeds(bus, AddItem(alice, cartId, AggregateVersion(i + 1), item)) }
    cartId
  }

  private def within20Seconds(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + 20.seconds.toNanos
    while (!condition) {
      if (System.nanoTime() > deadline) fail("Not within 20 seconds")
      Thread.sleep(100)
    }
  }

  /** A squash waits while the aggregate's events wait in the outbox ("still wait in the outbox"). */
  private def outboxEmpty(cartId: AggregateId): Boolean = DB.readOnly { implicit session =>
    sql"SELECT count(*) FROM events_to_publish WHERE aggregate_id = ${cartId.asLong}".map(_.long(1)).single().get == 0
  }

  private def squash(bus: ActorRef, request: SquashAggregate): SquashResponse = {
    within20Seconds(outboxEmpty(request.id))
    await((bus ? request).mapTo[SquashResponse])
  }

  private def eventsOf(bus: ActorRef, cartId: AggregateId): List[EventInfo[ShoppingCart]] =
    bus.askFor[Try[List[EventInfo[ShoppingCart]]]](GetEventsForAggregate(cartId)).get

  private def cart(bus: ActorRef, cartId: AggregateId): Aggregate[ShoppingCart] =
    bus.askFor[Try[Aggregate[ShoppingCart]]](GetAggregate(cartId)).get

  private val withAge =
    // #keepNewerThan
    AutomaticSquash(afterEvents = 1000, keepLast = 100, keepNewerThan = Some(30.days))
    // #keepNewerThan

  Feature("Squash on request") {

    Scenario("the history up to the point becomes one squash event; the version, the state and later events stay") {
      val carts = commandBus
      val cartId = cartWith(carts, "apple", "pear", "plum")
      succeeds(carts, RemoveItem(alice, cartId, AggregateVersion(4), 1))
      succeeds(carts, AddItem(alice, cartId, AggregateVersion(5), "fig"))
      val replacedAt = eventsOf(carts, cartId).find(_.version == AggregateVersion(5)).get.timestamp
      within20Seconds(outboxEmpty(cartId))

      // #request
      import io.reactivecqrs.api._
      import io.reactivecqrs.api.id.UserId
      import org.apache.pekko.pattern.ask
      import org.apache.pekko.util.Timeout
      import scala.concurrent.Future
      import scala.concurrent.duration._

      implicit val timeout: Timeout = Timeout(10.seconds)

      val answer: Future[SquashResponse] =
        (carts ? SquashAggregate(cartId, AggregateVersion(5), UserId(7))).mapTo[SquashResponse]
      // #request
      await(answer) mustBe AggregateSquashed(cartId, AggregateVersion(5))

      Then("the cart is as it was, and its history starts with the squash event")
      cart(carts, cartId) mustBe Aggregate(cartId, AggregateVersion(6),
        Some(ShoppingCart("Groceries", Vector(Item(2, "pear"), Item(3, "plum"), Item(4, "fig")))))
      val events = eventsOf(carts, cartId)
      events.map(e => (e.version.asInt, e.event)) mustBe
        List(5 -> CartSquashed(SpaceId(0), "Groceries", Vector(Item(2, "pear"), Item(3, "plum"))), 6 -> ItemAdded("fig"))
      (events.head.userId, events.head.timestamp) mustBe ((UserId(7), replacedAt))
      succeeds(carts, AddItem(alice, cartId, AggregateVersion(6), "kiwi")) mustBe SuccessResponse(cartId, AggregateVersion(7))

      Then("a read below the point is refused")
      // #old
      import scala.util.Try

      val old: Future[Try[Aggregate[ShoppingCart]]] =
        (carts ? GetAggregateForVersion(cartId, AggregateVersion(3))).mapTo[Try[Aggregate[ShoppingCart]]]
      // Failure(AggregateSquashedException): "The history of aggregate ... is squashed up to version 5;
      // version 3 cannot be read"
      // #old
      await(old).failed.get.getMessage mustBe
        s"The history of aggregate ${cartId.asLong} is squashed up to version 5; version 3 cannot be read"
    }

    Scenario("a squash event that does not give back the state is refused, unless it may change the state") {
      val forgetful = carts(new ForgetfulCarts, "forgetfulCarts")
      val forgotten = cartWith(forgetful, "apple")
      squash(forgetful, SquashAggregate(forgotten, AggregateVersion(2))) match {
        case SquashRefused(_, reason) => reason must include("does not give back the state it replaces")
        case answer => fail("Not refused: " + answer)
      }

      val archiving = carts(new ArchivingCarts, "archivingCarts")
      val cartId = cartWith(archiving, "apple", "pear", "plum")
      squash(archiving, SquashAggregate(cartId, AggregateVersion(3))) mustBe AggregateSquashed(cartId, AggregateVersion(3))
      cart(archiving, cartId).aggregateRoot.get.items.map(_.name) mustBe Vector("plum")
    }

    Scenario("undos and copies below the point are refused; at the point and after it they work") {
      val carts = commandBus
      val userId = alice
      val cartId = cartWith(carts, "apple", "pear", "plum")
      squash(carts, SquashAggregate(cartId, AggregateVersion(4))) mustBe AggregateSquashed(cartId, AggregateVersion(4))
      succeeds(carts, AddItem(alice, cartId, AggregateVersion(4), "fig"))
      // #belowThePoint
      val commands = Seq(
        UndoShoppingCartChange(userId, cartId, AggregateVersion(5), 2), // would reach version 4: EventHandlingError
        UndoShoppingCartChange(userId, cartId, AggregateVersion(5), 1), // SuccessResponse(cartId, AggregateVersion(6))
        DuplicateShoppingCart(userId, cartId, AggregateVersion(3)),     // below the point: EventHandlingError
        DuplicateShoppingCart(userId, cartId, AggregateVersion(4)))     // fine: the copy starts from the summary
      // #belowThePoint
      val answers = commands.map(command => await(carts ? command))
      answers(0) mustBe an[EventHandlingError]
      answers(1) mustBe SuccessResponse(cartId, AggregateVersion(6))
      answers(2) mustBe an[EventHandlingError]
      answers(3) must matchPattern { case SuccessResponse(_, AggregateVersion(1)) => }
    }
  }

  Feature("Squashing automatically") {

    Scenario("a rule needs a positive number of events; with an age, recent events stay too") {
      an[IllegalArgumentException] must be thrownBy AutomaticSquash(afterEvents = 0)
      withAge.keepNewerThan mustBe Some(30.days)
    }

    Scenario("the scheduler squashes what the rule selects, and counts it") {
      val carts = this.carts(new AutomaticallySquashedCarts, "automaticCarts")
      val cartId = succeeds(carts, CreateShoppingCart(None, alice, "Groceries")).aggregateId
      succeeds(carts, AddItems(alice, cartId, AggregateVersion(1), (1 to 1100).map("item " + _))) // version 1 101
      within20Seconds(outboxEmpty(cartId))

      // #scheduler
      import io.reactivecqrs.core.aggregate.SquashScheduler
      import io.reactivecqrs.core.aggregate.SquashScheduler.SquashTarget
      import io.reactivecqrs.core.cluster.Singletons
      import org.apache.pekko.actor.{ActorRef, Props}

      val squashScheduler: ActorRef = Singletons.start(system, "squashScheduler",
        Props(new SquashScheduler(eventStore, Seq(SquashTarget.of(new AutomaticallySquashedCarts, carts)))))
      // #scheduler
      squashScheduler ! SquashScheduler.Run
      within20Seconds(squashScheduler.askFor[SquashScheduler.SquashStatus](SquashScheduler.GetSquashStatus).squashed == 1)

      // #status
      val status: Future[SquashScheduler.SquashStatus] =
        (squashScheduler ? SquashScheduler.GetSquashStatus).mapTo[SquashScheduler.SquashStatus]
      // SquashStatus(squashed = 1, refused = 0, waiting = 0)
      // #status
      await(status) mustBe SquashScheduler.SquashStatus(squashed = 1, refused = 0, waiting = 0)
      eventsOf(carts, cartId).head.version mustBe AggregateVersion(1001) // 1 101 events, the newest 100 kept

      // #options
      import io.reactivecqrs.core.aggregate.SquashSchedulerOptions

      val withOptions = Props(new SquashScheduler(eventStore, Seq(SquashTarget.of(new AutomaticallySquashedCarts, carts)),
        SquashSchedulerOptions(firstRunAfter = 5.minutes, interval = 30.minutes, perRun = 500, answerTimeout = 2.minutes)))
      // #options
      withOptions must not be null
    }
  }

  Feature("The archive") {

    Scenario("kept events move to the archive; usersOf counts them in the history and in the archive") {
      val carts = this.carts(new AuditedCarts, "auditedCarts")
      val cartId = cartWith(carts, "apple", "pear")
      succeeds(carts, RemoveItem(alice, cartId, AggregateVersion(3), 1))
      succeeds(carts, AddItem(alice, cartId, AggregateVersion(4), "plum"))
      squash(carts, SquashAggregate(cartId, AggregateVersion(4))) mustBe AggregateSquashed(cartId, AggregateVersion(4))

      // #archived
      val archived: Future[Try[ArchivedEvents[ShoppingCart]]] =
        (carts ? GetArchivedEvents(cartId)).mapTo[Try[ArchivedEvents[ShoppingCart]]]
      // #archived
      await(archived).get.events.map(a => (a.info.version.asInt, a.info.event, a.undoneFrom)) mustBe
        Seq((2, ItemAdded("apple"), None), (3, ItemAdded("pear"), None))
      eventsOf(carts, cartId).map(_.event) mustBe
        List(CartSquashed(SpaceId(0), "Groceries", Vector(Item(2, "pear"))), ItemAdded("plum"))

      // #usersOf
      import io.reactivecqrs.core.tools.StoredDataCheck

      val users = StoredDataCheck.usersOf(classOf[ItemAdded].getName)
      // #usersOf
      users.className mustBe classOf[ItemAdded].getName
      StoredDataCheck.usersOf(classOf[ItemAdded].getName, limit = 100000).aggregates.find(_.aggregateId == cartId.asLong) mustBe
        Some(ClassUser(cartId.asLong, 1, 2))
    }

    Scenario("a retention policy: the last 90 days as they are, the audit events for good") {
      val policy = new RetainedCarts().squash.get
      (policy.automatic, policy.keep) mustBe
        ((Some(AutomaticSquash(afterEvents = 500, keepNewerThan = Some(90.days))), Set(classOf[ItemAdded])))
    }
  }
}

/** A squash event that leaves the items out without saying it may: the honesty check refuses it. */
class ForgetfulCarts extends ShoppingCartAggregateContext {
  override val squash: Option[SquashPolicy[ShoppingCart]] =
    Some(SquashPolicy(cart => CartSquashed(SpaceId(0), cart.name, Vector.empty)))
}

// #cartArchived
case class CartArchived(spaceId: SpaceId, name: String) extends FirstEvent[ShoppingCart] with SquashMayChangeState
// #cartArchived

class ArchivingCarts extends ShoppingCartAggregateContext {
  override def eventHandlers = (userId, timestamp, cart) => {
    // #cartArchivedHandler
    case e: CartArchived => ShoppingCart(e.name, Vector.empty)
    // #cartArchivedHandler
    case other => super.eventHandlers(userId, timestamp, cart)(other)
  }

  override val squash: Option[SquashPolicy[ShoppingCart]] = Some(SquashPolicy(cart => CartArchived(SpaceId(0), cart.name)))
}

class AutomaticallySquashedCarts extends ShoppingCartAggregateContext {
  // #automatic
  override val squash: Option[SquashPolicy[ShoppingCart]] = Some(SquashPolicy(
    toEvent = cart => CartSquashed(SpaceId(0), cart.name, cart.items),
    automatic = Some(AutomaticSquash(afterEvents = 1000, keepLast = 100))))
  // #automatic
}

class AuditedCarts extends ShoppingCartAggregateContext {
  // #keep
  override val squash: Option[SquashPolicy[ShoppingCart]] =
    Some(SquashPolicy(cart => CartSquashed(SpaceId(0), cart.name, cart.items), keep = Set(classOf[ItemAdded])))
  // #keep
}

class RetainedCarts extends ShoppingCartAggregateContext {
  // #retention
  override val squash: Option[SquashPolicy[ShoppingCart]] = Some(SquashPolicy(
    toEvent = cart => CartSquashed(SpaceId(0), cart.name, cart.items),
    automatic = Some(AutomaticSquash(afterEvents = 500, keepNewerThan = Some(90.days))),
    keep = Set(classOf[ItemAdded])))
  // #retention
}

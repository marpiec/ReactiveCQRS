package io.reactivecqrs.examples.cart

// Chapter 6's code, run against the test domain's carts: each block of the chapter is a region here, followed by the
// lines that check what the chapter says it answers.

import java.time.Instant

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Success, Try}

class EventsSpec extends CartSpec {

  // chapter 5's setup
  implicit val timeout: Timeout = Timeout(70.seconds)
  val alice = UserId(1)
  val bob = UserId(2)

  private def await[T](question: Future[T]): T = Await.result(question, timeout.duration)

  private def newCart(bus: ActorRef): AggregateId = await(bus ? CreateShoppingCart(None, alice, "Groceries")) match {
    case SuccessResponse(cartId, _) => cartId
    case refused => fail("No cart: " + refused)
  }

  private def succeeds(bus: ActorRef, command: AnyRef): AggregateVersion = await(bus ? command) match {
    case SuccessResponse(_, version) => version
    case refused => fail(s"$command was answered $refused")
  }

  private def cart(bus: ActorRef, cartId: AggregateId): Aggregate[ShoppingCart] =
    bus.askFor[Try[Aggregate[ShoppingCart]]](GetAggregate(cartId)).get

  private def itemsOf(cartId: AggregateId): Vector[String] = cart(commandBus, cartId).aggregateRoot.get.items.map(_.name)

  private def eventsOf(bus: ActorRef, cartId: AggregateId): Try[List[EventInfo[ShoppingCart]]] =
    bus.askFor[Try[List[EventInfo[ShoppingCart]]]](GetEventsForAggregate(cartId))

  private def within20Seconds(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + 20.seconds.toNanos
    while (!condition) {
      if (System.nanoTime() > deadline) fail("Not within 20 seconds")
      Thread.sleep(100)
    }
  }

  Feature("Event handlers") {

    Scenario("a loan is due fourteen days after it started, and its borrower is whoever started it") {
      val loans = new LoanContext
      val loan = loans.eventHandlers(bob, Instant.parse("2026-10-01T10:00:00Z"), loans.initialAggregateRoot)(LoanStarted("Dune"))
      loan mustBe Loan("Dune", bob, Instant.parse("2026-10-15T10:00:00Z"), returned = false)
      loans.eventHandlers(alice, Instant.parse("2026-10-14T09:00:00Z"), loan)(LoanExtended(7)).due mustBe
        Instant.parse("2026-10-22T10:00:00Z")
    }
  }

  Feature("First events") {

    Scenario("a first event without a space is refused, and so is a plain event as the first") {
      await(carts(new SpacelessCarts, "spacelessCarts") ? CreateShoppingCart(None, alice, "Groceries")) mustBe a[FailureResponse]
      await(carts(new PlainFirstCarts, "plainFirstCarts") ? CreateShoppingCart(None, alice, "Groceries")) mustBe an[EventHandlingError]
    }
  }

  Feature("Deleting, undoing, copying") {

    Scenario("a deleted cart reads as None, refuses commands, and keeps its history") {
      val cartId = newCart(commandBus)
      succeeds(commandBus, DeleteShoppingCart(None, alice, cartId, AggregateVersion(1))) mustBe AggregateVersion(2)
      cart(commandBus, cartId).aggregateRoot mustBe None
      await(commandBus ? AddItem(alice, cartId, AggregateVersion(2), "apples")) mustBe a[CommandHandlingError]
      eventsOf(commandBus, cartId).get.map(_.event) mustBe List(ShoppingCartCreated("Groceries"), ShoppingCartDeleted())
    }

    Scenario("an undo cancels the newest change still in effect; a copy starts from a version of its base") {
      Given("the drama: apples by Alice, oranges by Bob, and Bob's removal of item 1, the apples")
      val cartId = newCart(commandBus)
      succeeds(commandBus, AddItem(alice, cartId, AggregateVersion(1), "apples"))
      succeeds(commandBus, AddItem(bob, cartId, AggregateVersion(2), "oranges"))
      succeeds(commandBus, RemoveItem(bob, cartId, AggregateVersion(3), 1))

      When("Alice undoes it")
      // #undo
      val undone = commandBus ? UndoShoppingCartChange(alice, cartId, AggregateVersion(4), stepsToUndo = 1)
      // SuccessResponse(cartId, AggregateVersion(5)); the cart holds apples and oranges again
      // #undo
      await(undone) mustBe SuccessResponse(cartId, AggregateVersion(5))
      itemsOf(cartId) mustBe Vector("apples", "oranges")
      Then("version 4 still reads as it was then")
      commandBus.askFor[Try[Aggregate[ShoppingCart]]](GetAggregateForVersion(cartId, AggregateVersion(4)))
        .get.aggregateRoot.get.items.map(_.name) mustBe Vector("oranges")

      When("she copies the cart as it was at version 3")
      // #duplicate
      val copied = commandBus ? DuplicateShoppingCart(alice, cartId, AggregateVersion(3))
      // SuccessResponse(copyId, AggregateVersion(1)): a new cart with apples and oranges
      // #duplicate
      val copyId = await(copied) match {
        case SuccessResponse(id, version) if version == AggregateVersion(1) => id
        case refused => fail("No copy: " + refused)
      }
      itemsOf(copyId) mustBe Vector("apples", "oranges")
      Then("tomatoes added to the copy are its item 3, and the base never sees them")
      succeeds(commandBus, AddItem(alice, copyId, AggregateVersion(1), "tomatoes")) mustBe AggregateVersion(2)
      cart(commandBus, copyId).aggregateRoot.get.items.last mustBe Item(3, "tomatoes")
      itemsOf(cartId) mustBe Vector("apples", "oranges")
      Then("a copy above the base's version is refused")
      await(commandBus ? DuplicateShoppingCart(alice, cartId, AggregateVersion(10))) mustBe an[EventHandlingError]

      When("Alice undoes once more")
      succeeds(commandBus, UndoShoppingCartChange(alice, cartId, AggregateVersion(5), stepsToUndo = 1)) mustBe AggregateVersion(6)
      Then("it cancels version 3, the oranges")
      itemsOf(cartId) mustBe Vector("apples")
    }

    Scenario("a cart erased for good reads as one that never existed") {
      val erasableCarts = carts(new ErasableCarts, "erasableCarts")
      val cartId = newCart(erasableCarts)
      succeeds(erasableCarts, AddItem(alice, cartId, AggregateVersion(1), "apples"))
      succeeds(erasableCarts, EraseShoppingCart(alice, cartId, AggregateVersion(2))) mustBe AggregateVersion(3)
      within20Seconds(cart(erasableCarts, cartId).version == AggregateVersion.ZERO)
      cart(erasableCarts, cartId).aggregateRoot mustBe None
      eventsOf(erasableCarts, cartId) mustBe Success(Nil)
    }
  }

  Feature("The metadata envelope") {

    Scenario("an envelope travels with its event; an event without one reads as its user's") {
      val cartId = newCart(commandBus)
      // #onBehalf
      val onBehalf = EventMetadata("agent:planner", correlationId = Some("case 12"), custom = Map("channel" -> "api"))

      val added = commandBus ? AddItemOnBehalf(bob, cartId, AggregateVersion(1), "apple", onBehalf)
      // SuccessResponse(cartId, AggregateVersion(2)); the ItemAdded event carries the envelope
      // #onBehalf
      await(added) mustBe SuccessResponse(cartId, AggregateVersion(2))
      eventsOf(commandBus, cartId).get.map(_.metadata) mustBe List(EventMetadata.ofUser(alice), onBehalf)
    }
  }

  Feature("Changing event classes, and the limits") {

    Scenario("the stored data check reads every stored value, and usersOf counts a class's events") {
      newCart(commandBus)
      // #check
      import io.reactivecqrs.core.tools.StoredDataCheck

      val report = StoredDataCheck.run(mpjsons, typesNames)
      println(report.text)
      if (report.unreadable > 0) println("Not yet: some stored data cannot be read by this version")
      // #check
      report.text must startWith("Stored data check: ")

      // #usersOf
      val users = StoredDataCheck.usersOf(classOf[ItemAdded].getName)
      println(s"${users.events} stored and ${users.archived} archived ItemAdded events")
      users.aggregates.foreach(u => println(s"aggregate ${u.aggregateId}: ${u.events} stored, ${u.archived} archived"))
      // #usersOf
      users.events must be > 0L
    }

    Scenario("an event store with limits of its own refuses an event too large") {
      // #limits
      val eventStore = new PostgresEventStoreState(mpjsons, typesNames,
        aggregateVersionLimit = 20000,                                       // carts with very long lives
        eventSizeLimitPerClassName = Map(classOf[ItemAdded].getName -> 1000)  // an item name never needs more
      ).initSchema()
      // #limits
      val limitedCarts = carts(new ShoppingCartAggregateContext, "limitedCarts", eventStore)
      val cartId = newCart(limitedCarts)
      await(limitedCarts ? AddItem(alice, cartId, AggregateVersion(1), "x" * 2000)) mustBe an[EventHandlingError]
      succeeds(limitedCarts, AddItem(alice, cartId, AggregateVersion(1), "apples")) mustBe AggregateVersion(2)
    }
  }
}

// #cartOpened
case class CartOpened(spaceId: SpaceId, name: String) extends FirstEvent[ShoppingCart]
// #cartOpened

/** Carts whose first event names no space: refused before anything is stored. */
class SpacelessCarts extends ShoppingCartAggregateContext {
  override def commandHandlers = cart => {
    case c: CreateShoppingCart => CommandSuccess(CartOpened(SpaceId.unknown, c.name))
    case other => super.commandHandlers(cart)(other)
  }
  override def eventHandlers = (userId, timestamp, cart) => {
    case e: CartOpened => ShoppingCart(e.name, Vector.empty)
    case other => super.eventHandlers(userId, timestamp, cart)(other)
  }
}

/** Carts whose first event is a plain event: the store does not know where the cart belongs. */
class PlainFirstCarts extends ShoppingCartAggregateContext {
  override def commandHandlers = cart => {
    case c: CreateShoppingCart => CommandSuccess(ItemAdded(c.name))
    case other => super.commandHandlers(cart)(other)
  }
}

// #erase
case class ShoppingCartErased() extends PermanentDeleteEvent[ShoppingCart]

case class EraseShoppingCart(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion)
  extends Command[ShoppingCart, CustomCommandResponse[_]]

class ErasableCarts extends ShoppingCartAggregateContext {
  override def commandHandlers = cart => {
    case _: EraseShoppingCart => CommandSuccess(ShoppingCartErased())
    case other => super.commandHandlers(cart)(other)
  }

  override def eventHandlers = (userId, timestamp, cart) => {
    case _: ShoppingCartErased => cart
    case other => super.eventHandlers(userId, timestamp, cart)(other)
  }
}
// #erase

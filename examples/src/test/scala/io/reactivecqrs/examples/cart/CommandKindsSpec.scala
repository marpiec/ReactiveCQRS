package io.reactivecqrs.examples.cart

// Chapter 5's code, run against the test domain's carts: each block of the chapter is a region here, followed by the
// lines that check what the chapter says it answers.

// #imports
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._
// #imports

import java.util.UUID

import io.reactivecqrs.core.commandhandler.PostgresCommandResponseState
import org.apache.pekko.actor.ActorRef

import scala.concurrent.Await
import scala.util.Try

class CommandKindsSpec extends CartSpec {

  // #setup
  import system.dispatcher                             // runs the map and foreach callbacks below
  implicit val timeout: Timeout = Timeout(70.seconds)  // longer than the command timeout; see "Slow handlers"

  val alice = UserId(1)
  val bob = UserId(2)
  // #setup

  private def await[T](question: Future[T]): T = Await.result(question, timeout.duration)

  private def newCart(bus: ActorRef): AggregateId = await(bus ? CreateShoppingCart(None, alice, "Groceries")) match {
    case SuccessResponse(cartId, _) => cartId
    case refused => fail("No cart: " + refused)
  }

  private def itemsOf(cartId: AggregateId): Vector[String] =
    commandBus.askFor[Try[Aggregate[ShoppingCart]]](GetAggregate(cartId)).get.aggregateRoot.get.items.map(_.name)

  Feature("The kinds of command") {

    Scenario("FirstCommand: a new cart, at version 1") {
      // #create
      val answer = (commandBus ? CreateShoppingCart(None, alice, "Groceries")).mapTo[CustomCommandResponse[_]]
      answer.foreach {
        case SuccessResponse(cartId, version) => println(s"New cart $cartId at $version")  // version: AggregateVersion(1)
        case other                            => println(s"No cart: $other")
      }
      // #create
      await(answer) must matchPattern { case SuccessResponse(_, AggregateVersion(1)) => }
    }

    Scenario("Command: an outdated version is refused, and so is a cart that does not exist") {
      val groceries = newCart(commandBus)
      // #addItem
      def addItem(cartId: AggregateId, seen: AggregateVersion, item: String): Future[String] =
        (commandBus ? AddItem(bob, cartId, seen, item)).mapTo[CustomCommandResponse[_]].map {
          case SuccessResponse(_, version) =>
            s"Added $item; the cart is at version ${version.asInt} now"
          case AggregateConcurrentModificationError(_, _, expected, was) =>
            s"The cart changed while you looked (you saw version ${expected.asInt}, it is at ${was.asInt})"
          case FailureResponse(reasons) =>
            "Refused: " + reasons.mkString("; ")
          case other =>
            "Something went wrong: " + other
        }
      // #addItem
      await(addItem(groceries, AggregateVersion(1), "bread")) mustBe "Added bread; the cart is at version 2 now"
      await(addItem(groceries, AggregateVersion(1), "butter")) mustBe
        "The cart changed while you looked (you saw version 1, it is at 2)"

      val nowhere = await((commandBus ? ReserveAggregateId).mapTo[AggregateId]) // an id no cart has
      await(commandBus ? AddItem(bob, nowhere, AggregateVersion(1), "bread")) mustBe
        AggregateConcurrentModificationError(nowhere, AggregateType.of[ShoppingCart], AggregateVersion(1), AggregateVersion.ZERO)
    }

    Scenario("ConcurrentCommand: milk, whatever the version is") {
      val cartId = newCart(commandBus)
      // #concurrent
      val milk = commandBus ? AddItemConcurrently(alice, cartId, "milk")
      // answers SuccessResponse(cartId, AggregateVersion(n)), n being the version the milk landed on
      // #concurrent
      await(milk) mustBe SuccessResponse(cartId, AggregateVersion(2))
    }

    Scenario("an id chosen ahead: a copy cannot create a second cart, and with a key it gets the first answer") {
      // #reserve
      val created: Future[CustomCommandResponse[_]] = for {
        cartId   <- (commandBus ? ReserveAggregateId).mapTo[AggregateId]
        response <- (commandBus ? CreateShoppingCartWithId(None, alice, cartId, "Groceries")).mapTo[CustomCommandResponse[_]]
      } yield response   // SuccessResponse(cartId, AggregateVersion(1))
      // #reserve
      val groceries = await(created) match {
        case SuccessResponse(cartId, version) if version == AggregateVersion(1) => cartId
        case refused => fail("Not created: " + refused)
      }
      await(commandBus ? CreateShoppingCartWithId(None, alice, groceries, "Groceries")) mustBe
        AggregateConcurrentModificationError(groceries, AggregateType.of[ShoppingCart], AggregateVersion.ZERO, AggregateVersion(1))

      val reserved = await((commandBus ? ReserveAggregateId).mapTo[AggregateId])
      val keyed = CreateShoppingCartWithId(Some(ClientKey(UUID.randomUUID().toString)), alice, reserved, "Groceries")
      await(commandBus ? keyed) mustBe SuccessResponse(reserved, AggregateVersion(1))
      await(commandBus ? keyed) mustBe SuccessResponse(reserved, AggregateVersion(1))
    }

    Scenario("an id that is not allowed is refused before anything runs") {
      Seq(AggregateId(0), AggregateId(1L << 53), AggregateId(1000000000000L + 5)).foreach { id => // the last: installation 1's
        await(commandBus ? CreateShoppingCartWithId(None, alice, id, "Groceries")) mustBe a[FailureResponse]
      }
    }

    Scenario("an idempotent command: a plum added twice with one key") {
      val cartId = newCart(commandBus)
      // #plum
      val key = Some(ClientKey(java.util.UUID.randomUUID().toString))  // one key per user action
      val addPlum = AddItemWithReceipt(key, alice, cartId, AggregateVersion(1), "plum")

      val first  = commandBus ? addPlum  // CustomSuccessResponse(cartId, AggregateVersion(2), ItemReceipt("plum"))
      val second = commandBus ? addPlum  // the very same answer; there is one plum in the cart
      // #plum
      await(first) mustBe CustomSuccessResponse(cartId, AggregateVersion(2), ItemReceipt("plum"))
      await(second) mustBe CustomSuccessResponse(cartId, AggregateVersion(2), ItemReceipt("plum"))
      itemsOf(cartId) mustBe Vector("plum")
    }

    Scenario("responses kept for another period") {
      // #retention
      val responses = new PostgresCommandResponseState(mpjsons, typesNames, retention = 7.days).initSchema()
      // #retention
      responses must not be null
    }
  }

  Feature("What a handler can answer") {

    Scenario("several events, or none") {
      val cartId = newCart(commandBus)
      await(commandBus ? CheckCart(alice, cartId)) mustBe SuccessResponse(cartId, AggregateVersion(1))
      await(commandBus ? AddItems(alice, cartId, AggregateVersion(1), Seq("apple", "pear"))) mustBe
        SuccessResponse(cartId, AggregateVersion(3))
    }

    Scenario("an asynchronous handler asks a stock service first") {
      val stock = new Stock { def available(item: String): Future[Boolean] = Future.successful(item != "caviar") }
      val stockCheckingCarts = cartsOf(new StockCheckingCarts(stock), "stockCheckingCarts")
      val cartId = newCart(stockCheckingCarts)
      await(stockCheckingCarts ? AddItem(alice, cartId, AggregateVersion(1), "caviar")) mustBe
        FailureResponse(List("caviar is out of stock"))
      await(stockCheckingCarts ? AddItem(alice, cartId, AggregateVersion(1), "apples")) mustBe
        SuccessResponse(cartId, AggregateVersion(2))
    }

    Scenario("a message that is not a command fails the ask") {
      an[IllegalArgumentException] must be thrownBy await(commandBus ? "hello")
    }
  }

  Feature("Slow handlers and serial processing") {

    Scenario("a context with more time") {
      new PatientCarts().commandTimeout mustBe 2.minutes
    }

    Scenario("serial processing: the command one too many is rejected at once") {
      val busyCarts = cartsOf(new BusyCarts, "busyCarts")
      val cartId = newCart(busyCarts)
      val answers = await(Future.sequence((1 to 1000).map(i =>
        (busyCarts ? AddItemConcurrently(alice, cartId, "item " + i)).mapTo[CustomCommandResponse[_]])))
      answers.collect { case rejected: CommandRejected => rejected }.toSet mustBe
        Set(CommandRejected(cartId, "100 commands of the aggregate wait already"))
      answers.count(_.isInstanceOf[SuccessResponse]) must be >= 101
    }
  }
}

// #stock
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

/** Your own service, not part of the library. */
trait Stock {
  def available(item: String): Future[Boolean]
}

class StockCheckingCarts(stock: Stock) extends ShoppingCartAggregateContext {
  // a bounded pool of your own for the follow-up work
  private implicit val stockPool: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))

  override def commandHandlers = cart => {
    case c: AddItem =>
      AsyncCommandResult(stock.available(c.name).map {
        case true  => CommandSuccess(ItemAdded(c.name))
        case false => CommandFailure(c.name + " is out of stock")
      })
    case other => super.commandHandlers(cart)(other)
  }
}
// #stock

// #patient
class PatientCarts extends ShoppingCartAggregateContext {
  override val commandTimeout: FiniteDuration = 2.minutes
}
// #patient

// #busy
class BusyCarts extends ShoppingCartAggregateContext {
  override val commandProcessing: CommandProcessing = CommandProcessing.Serial(maxQueuePerAggregate = 100)
}
// #busy

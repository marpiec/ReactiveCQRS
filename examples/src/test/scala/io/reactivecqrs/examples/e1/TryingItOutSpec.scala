package io.reactivecqrs.examples.e1

import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

// Chapter 3's last blocks, as the chapter writes them, with what they print; and its rule that waits for a service.
class TryingItOutSpec extends AnyFeatureSpec with Matchers {

  Feature("Trying the cart out with a fold") {

    Scenario("the diary replays to the cart, and a decision is one call") {
      // #fold
      import java.time.Instant
      import io.reactivecqrs.api.id.UserId

      val context = new ShoppingCartContext
      val alice = UserId(1)
      val now = Instant.now()

      val diary = List(ShoppingCartCreated("Groceries"), ItemAdded("apples"), ItemAdded("oranges"), ItemRemoved(1))

      val cart = diary.foldLeft(context.initialAggregateRoot) { (state, event) =>
        context.eventHandlers(alice, now, state)(event)
      }
      // cart: ShoppingCart(Groceries,Vector(Item(2,oranges)))
      // #fold
      cart.toString mustBe "ShoppingCart(Groceries,Vector(Item(2,oranges)))"

      // #decision
      import io.reactivecqrs.api._
      import io.reactivecqrs.api.id.AggregateId

      val decision = context.commandHandlers(cart)(AddItem(alice, AggregateId(1001), AggregateVersion(4), "pears"))
      // decision: CommandSuccess(List(ItemAdded(pears)),NoInfo())
      // #decision
      decision.toString mustBe "CommandSuccess(List(ItemAdded(pears)),NoInfo())"
    }
  }

  Feature("A rule that waits") {

    Scenario("an item out of stock is refused once the stock service has answered") {
      import io.reactivecqrs.api._
      import io.reactivecqrs.api.id.{AggregateId, UserId}
      implicit val ec: ExecutionContext = ExecutionContext.global
      val carts = new StockAwareCarts(item => Future.successful(item != "caviar"))
      def decide(item: String): Any = carts.commandHandlers(ShoppingCart("Groceries", Vector.empty))(
        AddItem(UserId(1), AggregateId(1001), AggregateVersion(1), item)) match {
        case AsyncCommandResult(answer) => Await.result(answer, 5.seconds)
        case answered => answered
      }
      decide("caviar") mustBe CommandFailure("caviar is out of stock")
      decide("pears") mustBe CommandSuccess(ItemAdded("pears"))
    }
  }
}

/** Chapter 3's imaginary stock service: whether an item can be had. */
trait StockService {
  def isAvailable(item: String): Future[Boolean]
}

class StockAwareCarts(stock: StockService)(implicit ec: ExecutionContext) extends ShoppingCartContext {
  import io.reactivecqrs.api._

  override def commandHandlers = cart => {
    // #asyncAddItem
    case c: AddItem =>
      AsyncCommandResult(stock.isAvailable(c.name).map { available =>
        if (available) CommandSuccess(ItemAdded(c.name))
        else CommandFailure(c.name + " is out of stock")
      })
    // #asyncAddItem
    case other => super.commandHandlers(cart)(other)
  }
}

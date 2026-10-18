package io.reactivecqrs.examples.e11

// E11, testing (chapter 19): a cart's rules are a function of its context, tested without actors or a database.

// #rules
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.examples.e1.{AddItem, ItemAdded, ShoppingCart, ShoppingCartContext}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

class CartRulesSpec extends AnyFeatureSpec with Matchers {

  private val context = new ShoppingCartContext
  private val user = UserId(1)
  private val cartId = AggregateId(1001)

  /** What the handler decides for `command` on `cart`; a decision made in a Future is awaited. */
  private def decide(cart: ShoppingCart, command: AnyRef): GenericCommandResult[Any] =
    context.commandHandlers(cart)(command) match {
      case AsyncCommandResult(future) => Await.result(future, 5.seconds)
      case answered => answered
    }

  private def cartWith(items: String*): ShoppingCart = ShoppingCart("Groceries", items.toVector)

  Feature("Adding items") {

    Scenario("a cart of four items takes a fifth") {
      decide(cartWith("a", "b", "c", "d"), AddItem(user, cartId, AggregateVersion(5), "e")) mustBe
        CommandSuccess(ItemAdded("e"))
    }

    Scenario("a cart of five items refuses a sixth") {
      decide(cartWith("a", "b", "c", "d", "e"), AddItem(user, cartId, AggregateVersion(6), "f")) mustBe
        CommandFailure("Cannot have more than 5 items in your cart")
    }
  }
}
// #rules

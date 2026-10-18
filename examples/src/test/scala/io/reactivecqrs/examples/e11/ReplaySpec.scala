package io.reactivecqrs.examples.e11

// E11, testing (chapter 19): event handlers are functions too; a history replays with a fold.

// #replay
import java.time.Instant

import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.api.{AggregateContext, Event}
import io.reactivecqrs.examples.e1.{ItemAdded, ShoppingCart, ShoppingCartContext, ShoppingCartCreated}
import io.reactivecqrs.examples.e5.{ItemWritten, ListCreated, ShoppingListContext}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

class ReplaySpec extends AnyFeatureSpec with Matchers {

  /** The state that `events` make, as an owner makes it when it reads the aggregate. */
  private def replay[R](context: AggregateContext[R], events: Event[R]*): R =
    events.foldLeft(context.initialAggregateRoot) { (state, event) =>
      context.eventHandlers(UserId(1), Instant.EPOCH, state)(event)
    }

  Feature("Replaying a history") {

    Scenario("a cart's history replays to the cart") {
      replay(new ShoppingCartContext, ShoppingCartCreated("Groceries"), ItemAdded("apples"), ItemAdded("pears")) mustBe
        ShoppingCart("Groceries", Vector("apples", "pears"))
    }

    Scenario("a shopping list's squash event gives the list back (E5)") {
      val lists = new ShoppingListContext
      val list = replay(lists, ListCreated("Camping"), ItemWritten("tent"), ItemWritten("stove"))
      replay(lists, lists.squash.get.toEvent(list)) mustBe list
    }
  }
}
// #replay

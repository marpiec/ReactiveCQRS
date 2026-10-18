package io.reactivecqrs.examples.cart

// Chapter 7's code, run against the test domain's carts: each block of the chapter is a region here, followed by the
// lines that check what the chapter says it answers.

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.testdomain.shoppingcart._
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

// #imports
import java.time.Instant
import scala.util.{Failure, Success, Try}
// #imports

class PastSpec extends CartSpec {

  // chapter 5's setup
  import system.dispatcher
  implicit val timeout: Timeout = Timeout(70.seconds)
  val alice = UserId(1)
  val bob = UserId(2)

  private def await[T](question: Future[T]): T = Await.result(question, timeout.duration)

  private def succeeds(command: AnyRef): AggregateVersion = await(commandBus ? command) match {
    case SuccessResponse(_, version) => version
    case refused => fail(s"$command was answered $refused")
  }

  private def newCart(): AggregateId = await(commandBus ? CreateShoppingCart(None, alice, "Groceries")) match {
    case SuccessResponse(id, _) => id
    case refused => fail("No cart: " + refused)
  }

  /** The Groceries cart of chapters 6 and 7: apples by Alice, oranges by Bob, Bob's mistake undone, the oranges removed. */
  private lazy val cartId: AggregateId = {
    val id = newCart()
    succeeds(AddItem(alice, id, AggregateVersion(1), "apples"))
    succeeds(AddItem(bob, id, AggregateVersion(2), "oranges"))
    succeeds(RemoveItem(bob, id, AggregateVersion(3), 1))
    succeeds(UndoShoppingCartChange(alice, id, AggregateVersion(4), stepsToUndo = 1))
    succeeds(RemoveItem(alice, id, AggregateVersion(5), 2))
    id
  }

  private def eventsOf(id: AggregateId): List[EventInfo[ShoppingCart]] =
    commandBus.askFor[Try[List[EventInfo[ShoppingCart]]]](GetEventsForAggregate(id)).get

  private def at(version: Int): Try[Aggregate[ShoppingCart]] =
    commandBus.askFor[Try[Aggregate[ShoppingCart]]](GetAggregateForVersion(cartId, AggregateVersion(version)))

  private def within20Seconds(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + 20.seconds.toNanos
    while (!condition) {
      if (System.nanoTime() > deadline) fail("Not within 20 seconds")
      Thread.sleep(100)
    }
  }

  Feature("The diary") {

    Scenario("every event, with its version, its user and its envelope") {
      // #diary
      val diary = (commandBus ? GetEventsForAggregate(cartId)).mapTo[Try[List[EventInfo[ShoppingCart]]]]
      diary.foreach {
        case Success(events) =>
          events.foreach(e => println(f"${e.version.asInt}%2d  ${e.event.toString}%-30s  ${e.metadata.actor}"))
        case Failure(error) =>
          println("Cannot read the cart: " + error.getMessage)
      }
      // #diary
      await(diary).get.map(e => f"${e.version.asInt}%2d  ${e.event.toString}%-30s  ${e.metadata.actor}") mustBe List(
        " 1  ShoppingCartCreated(Groceries)  user:1",
        " 2  ItemAdded(apples)               user:1",
        " 3  ItemAdded(oranges)              user:2",
        " 4  ItemRemoved(1)                  user:2",
        " 5  ShoppingCartChangesUndone(1)    user:1",
        " 6  ItemRemoved(2)                  user:1")
    }

    Scenario("the diary up to a version") {
      // #upTo
      val firstThree = commandBus ? GetEventsForAggregateForVersion(cartId, AggregateVersion(3))
      // Success(List(...)) with the events of versions 1, 2 and 3
      // #upTo
      await(firstThree.mapTo[Try[List[EventInfo[ShoppingCart]]]]).get.map(_.version.asInt) mustBe List(1, 2, 3)
    }
  }

  Feature("The aggregate as it was") {

    Scenario("at a version, with the undos as they were then") {
      // #atVersion
      val atVersion4 = (commandBus ? GetAggregateForVersion(cartId, AggregateVersion(4))).mapTo[Try[Aggregate[ShoppingCart]]]
      atVersion4.foreach(println)
      // #atVersion
      await(atVersion4) mustBe
        Success(Aggregate(cartId, AggregateVersion(4), Some(ShoppingCart("Groceries", Vector(Item(2, "oranges"))))))
      at(5).get.aggregateRoot.get.items.map(_.name) mustBe Vector("apples", "oranges")
      at(6).get.aggregateRoot.get.items.map(_.name) mustBe Vector("apples")
      at(0) mustBe Success(Aggregate(cartId, AggregateVersion.ZERO, None))
      at(99).get.version mustBe AggregateVersion(6)
    }

    Scenario("at the version that deleted it, a cart reads as None") {
      val deleted = newCart()
      succeeds(DeleteShoppingCart(None, alice, deleted, AggregateVersion(1)))
      commandBus.askFor[Try[Aggregate[ShoppingCart]]](GetAggregateForVersion(deleted, AggregateVersion(2)))
        .get.aggregateRoot mustBe None
    }

    Scenario("at a moment: the newest version stored at or before it") {
      // #atInstant
      val whenOrdered = (commandBus ? GetAggregateAtInstant(cartId, Instant.parse("2026-10-16T18:00:00Z")))
        .mapTo[Try[Aggregate[ShoppingCart]]]
      whenOrdered.foreach {
        case Success(cart)  => println(s"At that moment: version ${cart.version.asInt}, ${cart.aggregateRoot}")
        case Failure(error) => println("Cannot read the cart: " + error.getMessage)
      }
      // #atInstant
      Then("an instant before the first event gives version 0 and no state")
      await(whenOrdered) mustBe Success(Aggregate(cartId, AggregateVersion.ZERO, None))
      Then("an instant between versions 4 and 5 gives the cart as Bob left it")
      val storedAt4 = eventsOf(cartId).find(_.version == AggregateVersion(4)).get.timestamp
      commandBus.askFor[Try[Aggregate[ShoppingCart]]](GetAggregateAtInstant(cartId, storedAt4)) mustBe
        Success(Aggregate(cartId, AggregateVersion(4), Some(ShoppingCart("Groceries", Vector(Item(2, "oranges"))))))
    }
  }

  Feature("Reading your own writes") {

    Scenario("the read waits for the version, and fails when it does not come within maxMillis") {
      // #minVersion
      val atLeast7 = (commandBus ? GetAggregateMinVersion(cartId, AggregateVersion(7), 2000)).mapTo[Try[Aggregate[ShoppingCart]]]
      atLeast7.foreach {
        case Success(cart) =>
          println(s"Up to date: version ${cart.version.asInt}")  // version 7 or later
        case Failure(e: AggregateInIncorrectVersionException) =>
          println(s"Still at version ${e.currentVersion.asInt}, waiting for ${e.requestedVersion.asInt}")
        case Failure(error) =>
          println("Cannot read the cart: " + error.getMessage)
      }
      // #minVersion
      await(atLeast7) match {
        case Failure(e: AggregateInIncorrectVersionException) =>
          (e.currentVersion, e.requestedVersion) mustBe ((AggregateVersion(6), AggregateVersion(7)))
        case answer => fail("Not the failure: " + answer)
      }

      Then("a version stored while the question waits answers it")
      val pears = newCart()
      val waiting = (commandBus ? GetAggregateMinVersion(pears, AggregateVersion(2), 10000)).mapTo[Try[Aggregate[ShoppingCart]]]
      succeeds(AddItem(alice, pears, AggregateVersion(1), "pears"))
      await(waiting).get.version mustBe AggregateVersion(2)
    }
  }

  Feature("Below the squash line") {

    Scenario("a read below the squash point fails; the events start with the squash event; the archive is empty") {
      val cartId = newCart()
      succeeds(AddItem(alice, cartId, AggregateVersion(1), "apples"))
      succeeds(AddItem(alice, cartId, AggregateVersion(2), "oranges"))
      within20Seconds(commandBus.askFor[SquashResponse](SquashAggregate(cartId, AggregateVersion(2))).isInstanceOf[AggregateSquashed])

      val belowTheLine = (commandBus ? GetAggregateForVersion(cartId, AggregateVersion(1))).mapTo[Try[Aggregate[ShoppingCart]]]
      belowTheLine.foreach {
        // #squashed
        case Failure(e: AggregateSquashedException) =>
          println(s"The history before version ${e.squashedUpTo.asInt} was folded away")
        // #squashed
        case answer => println(answer)
      }
      await(belowTheLine) match {
        case Failure(e: AggregateSquashedException) =>
          (e.squashedUpTo, e.requested) mustBe ((AggregateVersion(2), AggregateVersion(1)))
        case answer => fail("Not the failure: " + answer)
      }
      eventsOf(cartId).map(e => (e.version.asInt, e.event)) mustBe
        List(2 -> CartSquashed(SpaceId(0), "Groceries", Vector(Item(1, "apples"))), 3 -> ItemAdded("oranges"))

      // #archive
      val archived = (commandBus ? GetArchivedEvents(cartId)).mapTo[Try[ArchivedEvents[ShoppingCart]]]
      archived.foreach {
        case Success(archive) =>
          archive.events.foreach(a => println(s"${a.info.version.asInt}  ${a.info.event}  archived at ${a.archived}"))
        case Failure(error) =>
          println("Cannot read the archive: " + error.getMessage)
      }
      // #archive
      await(archived).get.events mustBe empty
    }
  }
}

package io.reactivecqrs.examples.e1

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets.UTF_8

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase, TestSystems}
import org.apache.pekko.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._

// Chapter 4's program as its main runs it, against the test database; then the chapter's futures and its two exercises.
class FirstProgramSpec extends CommonSpec {

  TestDatabase.init()

  /** What the chapter prints, whatever ids this test run's carts got: the chapter shows those of a fresh database. */
  private def asPrinted(text: String): String = text.replaceAll("Id\\(\\d+\\)", "Id(1001)")

  Feature("Chapter 4's program") {

    Scenario("it creates a cart, fills it, and prints what \"Running it\" shows") {
      val out = new ByteArrayOutputStream
      Console.withOut(out)(FirstProgram.run(system))
      asPrinted(new String(out.toByteArray, UTF_8)).split("\r?\n").toSeq mustBe Seq(
        "Created: SuccessResponse(Id(1001),Version(1))",
        "Now at: Version(3)",
        "The cart: Aggregate(Id(1001),Version(3),Some(ShoppingCart(Groceries,Vector(Item(1,apples), Item(2,oranges)))))")
    }

    Scenario("the same commands composed as futures, and the two ways to hear no") {
      implicit val timeout: Timeout = Timeout(70.seconds)
      val composedSystem = TestSystems.newSystem("composed") // the program's actors took their names in `system`
      try {
        val carts = BasicSetup.start(composedSystem).carts
        val cart = Await.result(ComposedCommands.composed(composedSystem, carts, UserId(1)), timeout.duration)
        cart.version mustBe AggregateVersion(3)
        cart.aggregateRoot mustBe Some(ShoppingCart("Groceries", Vector(Item(1, "apples"), Item(2, "oranges"))))

        def send(command: AnyRef): CustomCommandResponse[_] = carts.askFor[CustomCommandResponse[_]](command)
        def success(command: AnyRef): SuccessResponse = send(command) match {
          case done: SuccessResponse => done
          case answer => fail(s"$command was answered $answer")
        }
        val alice = UserId(1)
        val created = success(CreateShoppingCart(alice, "Groceries"))
        val apples = success(AddItem(alice, created.aggregateId, created.aggregateVersion, "apples"))

        Then("an old version is refused")
        asPrinted(send(AddItem(alice, created.aggregateId, created.aggregateVersion, "oranges")).toString) mustBe
          "AggregateConcurrentModificationError(Id(1001),AggregateType(io.reactivecqrs.examples.e1.ShoppingCart),Version(1),Version(2))"

        Then("so is a sixth item")
        val fifth = (2 to 5).foldLeft(apples)((last, i) => success(AddItem(alice, created.aggregateId, last.aggregateVersion, "item " + i)))
        send(AddItem(alice, created.aggregateId, fifth.aggregateVersion, "item 6")).toString mustBe
          "FailureResponse(List(Cannot have more than 5 items in your cart))"
      } finally Await.ready(composedSystem.terminate(), 30.seconds)
    }
  }
}

package io.reactivecqrs.examples.e6

import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.api.{CommandRejected, FailureResponse, SuccessResponse}
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.util.Timeout

import scala.concurrent.duration._

// E6 as its main runs it, against the test database.
class CommandsSpec extends CommonSpec {

  TestDatabase.init()

  private implicit val timeout: Timeout = Timeout(70.seconds)
  private val concerts = CommandsSetup.start(system)
  private val user = UserId(1)

  Feature("E6: command processing") {

    Scenario("a command sent twice with one key takes effect once, and both copies get the same answer") {
      val (first, again) = CommandsSetup.scheduleTwice(concerts, user, seats = 5)
      first mustBe a[SuccessResponse]
      again mustBe first
    }

    Scenario("bookings sent at once run one at a time: no seat is sold twice, and each booking wrote its ticket with its event") {
      val concertId = CommandsSetup.scheduleTwice(concerts, user, seats = 5)._1.asInstanceOf[SuccessResponse].aggregateId
      val answers = CommandsSetup.bookAtOnce(concerts, user, concertId, customers = 8)

      answers.count(_.isInstanceOf[SuccessResponse]) mustBe 5
      answers.count(_ == FailureResponse(List("Sold out"))) mustBe 3
      CommandsSetup.tickets(concertId) mustBe 5
    }

    Scenario("more bookings than may wait are answered CommandRejected at once, and may be sent again") {
      val concertId = CommandsSetup.scheduleTwice(concerts, user, seats = 100)._1.asInstanceOf[SuccessResponse].aggregateId
      val answers = CommandsSetup.bookAtOnce(concerts, user, concertId, customers = 60)

      val (booked, rejected) = (answers.count(_.isInstanceOf[SuccessResponse]), answers.count(_.isInstanceOf[CommandRejected]))
      (booked + rejected, rejected > 0) mustBe ((60, true)) // sent within a millisecond, each committed in several
      booked must be >= 21 // the one at work and the 20 that may wait
      CommandsSetup.tickets(concertId) mustBe booked
    }
  }
}

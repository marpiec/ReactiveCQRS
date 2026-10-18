package io.reactivecqrs.examples.e7

import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.util.Timeout

import scala.concurrent.duration._

// E7 as its main runs it, against the test database.
class MetadataSpec extends CommonSpec {

  TestDatabase.init()

  private implicit val timeout: Timeout = Timeout(70.seconds)
  private val desk = MetadataSetup.start(system)

  Feature("E7: metadata") {

    Scenario("the envelope a command carries reaches its events: in a read of the history and in the audit trail") {
      val ticketId = MetadataSetup.openAndNote(desk, UserId(1))

      MetadataSetup.history(desk, ticketId).map(e => (e.metadata.actor, e.metadata.correlationId)) mustBe
        List(("user:1", None), ("agent:assistant", Some("12")))
      awaitAssert(MetadataSetup.auditTrail(desk, ticketId) mustBe Some(AuditTrail(Vector(
        "user:1: opened Printer offline",
        "agent:assistant (case 12, trace 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01): noted restarted the spooler"))),
        10.seconds, 50.millis)
    }
  }
}

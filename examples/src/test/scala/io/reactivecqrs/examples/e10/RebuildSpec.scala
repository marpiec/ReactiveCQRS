package io.reactivecqrs.examples.e10

import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.projection.RebuildTarget
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase, TestSystems}
import org.apache.pekko.util.Timeout

import scala.concurrent.duration._

// E10 as its main runs it, against the test database; each release in an actor system of its own.
class RebuildSpec extends CommonSpec {

  TestDatabase.init()

  private implicit val timeout: Timeout = Timeout(70.seconds)

  Feature("E10: rebuilding projections") {

    Scenario("version 2 is built while version 1 serves and takes over; a damaged document is repaired by a targeted rebuild") {
      val release = RebuildSetup.release(() => TestSystems.newSystem("e10"), UserId(1))

      release.before mustBe Seq(Some("Groceries"), Some("Garden"), Some("Gifts"))
      (release.switched.active.map(b => (b.version, b.suffix)), release.switched.building) mustBe ((Some((2, "_v2")), None))
      release.after mustBe Seq(Some("GROCERIES"), Some("GARDEN"), Some("GIFTS"))
      release.damaged mustBe Some("???")
      release.rebuilt.target mustBe a[RebuildTarget.OneAggregate]
      release.rebuilt.aggregates mustBe 1
      release.repaired mustBe Some("GARDEN")
    }
  }
}

package io.reactivecqrs.examples.e8

import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.api.{AggregateVersion, EphemeralAggregateUnknown, Materialized}
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.concurrent.duration._

// E8 as its main runs it, against the test database.
class DraftsSpec extends CommonSpec {

  TestDatabase.init()

  private implicit val timeout: Timeout = Timeout(70.seconds)

  Feature("E8: ephemeral aggregates") {

    Scenario("a draft lives in memory under a negative id, and once materialized it is a stored list and the draft is gone") {
      val kept = DraftsSetup.draftAndKeep(DraftsSetup.start(system), UserId(1))

      kept.draftId.asLong must be < 0L
      DB.readOnly(implicit session => sql"SELECT count(*) FROM events WHERE aggregate_id = ${kept.draftId.asLong}".map(_.int(1)).single()) mustBe Some(0)
      kept.materialized mustBe a[Materialized]
      (kept.stored.aggregateRoot.map(_.items), kept.stored.version) mustBe ((Some(Vector("tent", "stove")), AggregateVersion(1)))
      kept.draftAfter mustBe EphemeralAggregateUnknown(kept.draftId)
    }
  }
}

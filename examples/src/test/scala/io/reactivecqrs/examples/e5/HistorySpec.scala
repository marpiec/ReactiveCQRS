package io.reactivecqrs.examples.e5

import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.api.{AggregateSquashed, AggregateSquashedException, AggregateVersion}
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.util.Timeout
import scalikejdbc._

import scala.concurrent.duration._

// E5 as its main runs it, against the test database.
class HistorySpec extends CommonSpec {

  TestDatabase.init()

  private implicit val timeout: Timeout = Timeout(70.seconds)

  Feature("E5: an aggregate's history") {

    Scenario("an undo, a copy and a squash: the list and its copy keep their states, and the history below the squash is gone") {
      val told = HistorySetup.story(HistorySetup.start(system), UserId(1))

      (told.list.aggregateRoot.map(_.items), told.list.version) mustBe ((Some(Vector("apples", "pears", "plums")), AggregateVersion(6)))
      told.copy.aggregateRoot.map(_.items) mustBe Some(Vector("apples", "pears", "bread"))
      told.squashed mustBe AggregateSquashed(told.listId, AggregateVersion(4))
      told.atVersion2.failed.get mustBe an[AggregateSquashedException]
      told.atVersion4.get.aggregateRoot.map(_.items) mustBe Some(Vector("apples", "pears", "plums"))
      DB.readOnly(implicit session => sql"SELECT count(*) FROM events WHERE aggregate_id = ${told.listId.asLong} AND version < 4"
        .map(_.int(1)).single()) mustBe Some(0)
    }
  }
}

package io.reactivecqrs.core.uid

import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.testkit.TestDatabase
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers
import scalikejdbc._

// D39: installations make aggregate ids in ranges of their own, below 2^53.
class IdRangeSpec extends AnyFeatureSpec with Matchers {

  TestDatabase.init()

  private var sequences = 0

  /** A sequence of the test's own, stepping by 10 000 from `start`, as the aggregate ids' does. */
  private def sequence(start: Long): String = {
    sequences += 1
    val name = "test_ids_" + System.nanoTime() + "_" + sequences
    DB.autoCommit(implicit session => SQL(s"CREATE SEQUENCE $name INCREMENT BY 10000 START $start").execute())
    name
  }

  Feature("Ranges (D39)") {

    Scenario("an installation's ids start at its number × 10^12, and the last range ends at 2^53") {
      (IdRange(0).from, IdRange(0).until) mustBe ((0L, 1000000000000L))
      (IdRange(7).from, IdRange(7).until) mustBe ((7000000000000L, 8000000000000L))
      IdRange(IdRange.MaxInstallation).until mustBe (1L << 53)
      an[IllegalArgumentException] must be thrownBy IdRange(IdRange.MaxInstallation + 1)
    }

    Scenario("a chosen id must be positive, below 2^53, and a system or legacy id or one of the installation's") {
      val range = IdRange(7)
      Seq(500L, 999999999999L, 7000000000001L).map(id => range.refusedChoice(AggregateId(id))) mustBe Seq(None, None, None)
      Seq(0L, -5L, 8000000000000L, 1L << 53).map(id => range.refusedChoice(AggregateId(id))).forall(_.isDefined) mustBe true
    }
  }

  Feature("The aggregate ids' generator (D39)") {

    Scenario("moves a sequence behind its range to the range's start, and never back") {
      val name = sequence(1001)
      new PostgresUidGenerator(name, Some(IdRange(7))).nextIdsPool.from mustBe 7000000000000L
      new PostgresUidGenerator(name, Some(IdRange(7))).nextIdsPool.from mustBe 7000000010000L
    }

    Scenario("refuses to start on a sequence past its range: a wrong installation number") {
      val name = sequence(8000000000000L)
      (the[IllegalStateException] thrownBy new PostgresUidGenerator(name, Some(IdRange(7)))).getMessage must include("installation number")
    }

    Scenario("refuses a pool past the end of its range, and one past 2^53") {
      val generator = new PostgresUidGenerator(sequence(8000000000000L - 15000), Some(IdRange(7)))
      generator.nextIdsPool.from mustBe 7999999985000L
      an[IllegalStateException] must be thrownBy generator.nextIdsPool
      an[IllegalStateException] must be thrownBy new PostgresUidGenerator(sequence((1L << 53) - 5000)).nextIdsPool
    }

    Scenario("without a range, takes pools as before") {
      new PostgresUidGenerator(sequence(1001)).nextIdsPool.from mustBe 1001
    }

    Scenario("F204 a node with a higher installation number refuses to start on ids given already, and leaves them") {
      val name = sequence(1001)
      new PostgresUidGenerator(name, Some(IdRange(0))).nextIdsPool.from mustBe 1001
      (the[IllegalStateException] thrownBy new PostgresUidGenerator(name, Some(IdRange(3)))).getMessage must include("installation number")
      new PostgresUidGenerator(name, Some(IdRange(0))).nextIdsPool.from mustBe 11001 // untouched
    }

    Scenario("F204 a step lowered while a generator runs gives pools of the new step, no id twice") {
      val name = sequence(1001)
      val (first, second) = (new PostgresUidGenerator(name), new PostgresUidGenerator(name))
      DB.autoCommit(implicit session => SQL(s"ALTER SEQUENCE $name INCREMENT BY 5000").execute())
      val pools = Seq(first.nextIdsPool, second.nextIdsPool, first.nextIdsPool)
      pools.map(p => (p.from, p.size)) mustBe Seq((1001L, 5000L), (6001L, 5000L), (11001L, 5000L))
    }
  }
}

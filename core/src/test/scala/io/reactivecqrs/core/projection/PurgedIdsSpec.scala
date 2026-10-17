package io.reactivecqrs.core.projection

import io.reactivecqrs.api.id.AggregateId
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

// R17: the aggregates a projection drops deliveries of, for a while after their purge.
class PurgedIdsSpec extends AnyFeatureSpec with Matchers {

  private def ids(longs: Long*): Seq[AggregateId] = longs.map(AggregateId(_))

  Scenario("holds the purged aggregates of every batch until the window after the last one, then none") {
    val now = 1000L
    val purged = PurgedIds.empty.adding(ids(9, 3, 7), now, 100).adding(ids(5, 3, 1), now + 50, 100)

    purged.size mustBe 5 // 3 came twice
    Seq(1L, 3L, 5L, 7L, 9L).map(id => purged.contains(AggregateId(id), now + 60)) mustBe Seq(true, true, true, true, true)
    Seq(2L, 4L, 10L).map(id => purged.contains(AggregateId(id), now + 60)) mustBe Seq(false, false, false)
    purged.contains(AggregateId(9), now + 149) mustBe true // the window runs from the last batch
    purged.contains(AggregateId(9), now + 150) mustBe false
    purged.adding(ids(4), now + 200, 100).size mustBe 1 // a batch after the window starts afresh
  }

  Scenario("works across the wrap of System.nanoTime, and lets a targeted rebuild take its aggregates out") {
    val now = Long.MaxValue - 10
    val purged = PurgedIds.empty.adding(ids(2, 1), now, 100)
    purged.contains(AggregateId(2), now + 50) mustBe true // past Long.MaxValue
    purged.without(Set(AggregateId(2))).contains(AggregateId(2), now) mustBe false
    purged.without(Set(AggregateId(2))).contains(AggregateId(1), now) mustBe true
  }
}

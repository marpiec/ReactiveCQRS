package io.reactivecqrs.core.projection

import java.util.Arrays

import io.reactivecqrs.api.id.AggregateId

/** R17: the aggregates purges removed, whose deliveries a projection acknowledges without applying them until `until`
  * (`System.nanoTime`), as one may still be on its way. One sorted array: 8 bytes per aggregate. */
private[projection] final class PurgedIds private (ids: Array[Long], until: Long) {

  def contains(id: AggregateId, now: Long): Boolean = now - until < 0 && Arrays.binarySearch(ids, id.asLong) >= 0

  /** These aggregates too, and all of them until `window` nanoseconds from `now`. */
  def adding(more: Seq[AggregateId], now: Long, window: Long): PurgedIds = {
    val kept = if (now - until < 0) ids else Array.emptyLongArray
    val added = more.map(_.asLong).toArray
    Arrays.sort(added)
    new PurgedIds(merged(kept, added), now + window)
  }

  /** Without the aggregates a targeted rebuild resets: their events come again from the store. */
  def without(reset: Set[AggregateId]): PurgedIds = new PurgedIds(ids.filterNot(id => reset.contains(AggregateId(id))), until)

  def size: Int = ids.length

  // Two sorted arrays into one, without repeats: linear, so a purge in many batches costs no sort of the whole.
  private def merged(a: Array[Long], b: Array[Long]): Array[Long] = {
    val out = new Array[Long](a.length + b.length)
    var i = 0
    var j = 0
    var n = 0
    while (i < a.length || j < b.length) {
      val next = if (j >= b.length || i < a.length && a(i) <= b(j)) { i += 1; a(i - 1) } else { j += 1; b(j - 1) }
      if (n == 0 || out(n - 1) != next) {
        out(n) = next
        n += 1
      }
    }
    Arrays.copyOf(out, n)
  }
}

private[projection] object PurgedIds {
  val empty: PurgedIds = new PurgedIds(Array.emptyLongArray, 0L)
}

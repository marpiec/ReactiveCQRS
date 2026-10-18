package io.reactivecqrs.core.eventbus

import scala.collection.mutable

/**
  * F212: the bus's deliveries in flight, kept so that a tick's work grows with what expires or stalls, not with all that
  * waits: in the order they were first sent (a deadline takes them from the head), with a queue of their last sends (a
  * stall check takes the old ones from its head), and a count of those held for a subscriber. `firstSent`, `lastSent`:
  * a delivery's times; `watched`: whether it may stall (awaits an answer, not logged yet); `held`: whether it is held
  * for someone. Not thread-safe: part of the bus's state.
  */
private[eventbus] final class InFlight[K, D](firstSent: D => Long, lastSent: D => Long, watched: D => Boolean, held: D => Boolean) {

  private val all = mutable.LinkedHashMap[K, D]()
  private val sends = mutable.Queue[(K, Long)]()
  private var heldCount = 0

  def size: Int = all.size

  /** How many are held for a subscriber. */
  def heldSize: Int = heldCount

  def get(key: K): Option[D] = all.get(key)

  def values: Iterator[D] = all.valuesIterator

  /** Puts `delivery` under `key`; one first sent anew (adopted) goes to the end. */
  def put(key: K, delivery: D): Unit = {
    val before = all.get(key)
    before.foreach { old =>
      if (held(old)) heldCount -= 1
      if (firstSent(old) != firstSent(delivery)) all.remove(key)
    }
    all(key) = delivery
    if (held(delivery)) heldCount += 1
    if (watched(delivery) && before.forall(old => !watched(old) || lastSent(old) != lastSent(delivery))) sends.enqueue(key -> lastSent(delivery))
  }

  def remove(key: K): Unit = all.remove(key).foreach(old => if (held(old)) heldCount -= 1)

  /** Those first sent at `cutoff` or before, the oldest first; only they are visited. */
  def firstSentBy(cutoff: Long): Seq[(K, D)] = all.iterator.takeWhile { case (_, delivery) => firstSent(delivery) <= cutoff }.toVector

  /** Those watched whose last send was at `cutoff` or before, each once for that send; only sends that old are visited. */
  def lastSentBy(cutoff: Long): Seq[(K, D)] = {
    val found = Vector.newBuilder[(K, D)]
    while (sends.nonEmpty && sends.head._2 <= cutoff) {
      val (key, at) = sends.dequeue()
      all.get(key).filter(delivery => watched(delivery) && lastSent(delivery) == at).foreach(delivery => found += key -> delivery)
    }
    found.result()
  }

  /** Those that `p` selects; every delivery is visited (a subscriber's stop or removal). */
  def entriesWhere(p: D => Boolean): Seq[(K, D)] = all.iterator.filter { case (_, delivery) => p(delivery) }.toVector
}

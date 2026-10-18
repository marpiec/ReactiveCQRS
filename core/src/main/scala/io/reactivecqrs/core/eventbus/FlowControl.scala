package io.reactivecqrs.core.eventbus

import io.reactivecqrs.api.AggregateType
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.eventbus.FlowControl._

import scala.collection.mutable

/**
  * F93: what the bus has sent to each subscriber and not yet had answered, and what it holds back beyond that. It is
  * the subscriber's mailbox that holds what is sent, so the window is per subscriber: `window` events and
  * `windowBytes` of their stored sizes (F126) in messages not yet answered, whichever comes first. A live publication
  * that does not fit is held, and its aggregate is behind for that subscriber (every later event of it is held too, so
  * the subscriber never sees a gap of the bus's making) until the bus pulls it: asks its owner to re-publish it to that
  * subscriber, once the subscriber's events in flight are down to half the window; while aggregates are behind, live
  * publications take at most that half, so the pulls always find the other. Its memory is bounded by the window,
  * the aggregates behind and `maxPulls`; the payloads of held events stay in the store. Not thread-safe: part of the
  * bus's state.
  */
final class FlowControl[S](window: Int, windowBytes: Long, maxPulls: Int, pullTimeoutNanos: Long, deadlineNanos: Long) {

  private final class Subscriber {
    val sent = mutable.LinkedHashMap[SentKey, Sent]() // oldest send first: a message sent again moves to the end
    var events = 0L
    var bytes = 0L
    val behind = mutable.LinkedHashMap[AggregateId, Behind]() // held first, pulled first
    val pulling = mutable.HashMap[AggregateId, Pulling]()
    var pullingEvents = 0L
    var pullingBytes = 0L
    def idle: Boolean = sent.isEmpty && behind.isEmpty && pulling.isEmpty
    def fits(moreEvents: Long, moreBytes: Long): Boolean = events + moreEvents <= window && bytes + moreBytes <= windowBytes
  }

  private val subscribers = mutable.HashMap[S, Subscriber]()

  /** Whether a live publication of `events` events of `aggregateId`, of `bytes` stored, goes to `subscriber` now; if
    * not, it is held. What pulls asked for and have not got yet counts as in flight: it is on its way. While aggregates
    * are behind, a live publication takes at most half the window: pulls start at half and take the rest, so the
    * oldest held aggregates move however busy the others are. With nothing in flight or asked for, a publication goes
    * whatever its size. */
  def admits(subscriber: S, aggregateId: AggregateId, events: Int, bytes: Long): Boolean = subscribers.get(subscriber).forall { s =>
    val share = if (s.behind.isEmpty) 1 else 2
    !s.behind.contains(aggregateId) && !s.pulling.contains(aggregateId) && (s.sent.isEmpty && s.pulling.isEmpty ||
      s.events + s.pullingEvents + events <= window / share && s.bytes + s.pullingBytes + bytes <= windowBytes / share)
  }

  /** Versions `from` to `upTo` of the aggregate, of `bytes` stored, are held for `subscriber`. */
  def hold(subscriber: S, aggregateType: AggregateType, aggregateId: AggregateId, from: Int, upTo: Int, bytes: Long, now: Long): Unit = {
    val s = subscribers.getOrElseUpdate(subscriber, new Subscriber)
    s.pulling.get(aggregateId) match {
      case Some(p) => s.pulling(aggregateId) = p.copy(behind = p.behind.including(from, upTo, bytes, now)) // past the part being pulled
      case None =>
        s.behind(aggregateId) = s.behind.get(aggregateId).map(_.including(from, upTo, bytes, now)).getOrElse(Behind(aggregateType, from, upTo, bytes, now))
    }
  }

  /** A message of `events` events, of `bytes` stored, was sent to `subscriber`. */
  def sent(subscriber: S, key: SentKey, events: Int, bytes: Long, now: Long): Unit = {
    val s = subscribers.getOrElseUpdate(subscriber, new Subscriber)
    s.sent.remove(key).foreach(earlier => take(s, earlier))
    s.sent(key) = Sent(events, bytes, now)
    s.events += events
    s.bytes += bytes
  }

  /** `subscriber` answered the message `key`; an answer to a message not known changes nothing. */
  def answered(subscriber: S, key: SentKey): Unit = subscribers.get(subscriber).foreach { s =>
    s.sent.remove(key).foreach(sent => take(s, sent))
    forgetIfIdle(subscriber, s)
  }

  /** Versions up to `last` of the aggregate were re-published to `subscriber` (a pull, or a gap it asked for): they are
    * no longer behind, and what was held past them is behind again. */
  def republished(subscriber: S, aggregateId: AggregateId, last: Int): Unit = subscribers.get(subscriber).foreach { s =>
    s.pulling.remove(aggregateId).foreach { p =>
      s.pullingEvents -= p.events
      s.pullingBytes -= p.bytes
      if (p.behind.upTo > last) s.behind(aggregateId) = p.behind.after(last)
    }
    s.behind.get(aggregateId).filter(_.from <= last).foreach { b =>
      if (b.upTo > last) s.behind(aggregateId) = b.after(last) else s.behind.remove(aggregateId)
    }
    forgetIfIdle(subscriber, s)
  }

  /** The aggregates to pull for `subscriber` now, oldest held first, each marked as pulled: none while its events in
    * flight are above half the window; then as many as fit the window, each counted as a part of at most `maxPartEvents`
    * and `maxPartBytes`, and at most `maxPulls` at once. */
  def pulls(subscriber: S, maxPartEvents: Int, maxPartBytes: Long, now: Long): Seq[Pull] = subscribers.get(subscriber) match {
    case Some(s) if s.behind.nonEmpty && s.events <= window / 2 && s.bytes <= windowBytes / 2 =>
      val taken = Vector.newBuilder[Pull]
      val oldest = s.behind.iterator
      while (oldest.hasNext && s.pulling.size < maxPulls && s.fits(s.pullingEvents, s.pullingBytes)) {
        val (aggregateId, behind) = oldest.next()
        val held = behind.upTo - behind.from + 1
        val events = math.min(held, maxPartEvents)
        val bytes = math.min(behind.bytes * events / held, maxPartBytes)
        s.pulling(aggregateId) = Pulling(behind, events, bytes, now)
        s.pullingEvents += events
        s.pullingBytes += bytes
        taken += Pull(behind.aggregateType, aggregateId, behind.from)
      }
      val pulled = taken.result()
      pulled.foreach(p => s.behind.remove(p.aggregateId))
      pulled
    case _ => Nil
  }

  /** The subscribers with aggregates behind. */
  def withBehind: Iterable[S] = subscribers.collect { case (subscriber, s) if s.behind.nonEmpty => subscriber }

  /** Messages not answered by the deadline are taken off (lost: their deliveries end then too), and pulls not answered
    * within the pull timeout are behind again. An aggregate last held a deadline ago is dropped: its held deliveries
    * ended by then, so its pulls may be answered through another node's bus (its owner moved), never this one; the
    * subscriber's gap requests and the outbox poller deliver it instead. */
  def expire(now: Long): Unit = subscribers.toSeq.foreach { case (subscriber, s) =>
    var oldest = s.sent.headOption
    while (oldest.exists { case (_, sent) => now - sent.at >= deadlineNanos }) {
      val (key, sent) = oldest.get
      s.sent.remove(key)
      take(s, sent)
      oldest = s.sent.headOption
    }
    s.pulling.toSeq.filter { case (_, p) => now - p.at >= pullTimeoutNanos }.foreach { case (aggregateId, p) =>
      s.pulling.remove(aggregateId)
      s.pullingEvents -= p.events
      s.pullingBytes -= p.bytes
      if (now - p.behind.heldAt < deadlineNanos) s.behind(aggregateId) = p.behind
    }
    s.behind.filterInPlace { case (_, b) => now - b.heldAt < deadlineNanos }
    forgetIfIdle(subscriber, s)
  }

  /** The subscriber stopped or was removed: nothing waits for it any more. */
  def remove(subscriber: S): Unit = subscribers.remove(subscriber)

  /** Per subscriber with anything in flight or held: its events and bytes in flight and its aggregates behind (pulled
    * ones included). */
  def status: Map[S, InFlight] = subscribers.map { case (subscriber, s) => subscriber -> InFlight(s.events, s.bytes, s.behind.size + s.pulling.size) }.toMap

  private def take(s: Subscriber, sent: Sent): Unit = {
    s.events -= sent.events
    s.bytes -= sent.bytes
  }

  private def forgetIfIdle(subscriber: S, s: Subscriber): Unit = if (s.idle) subscribers.remove(subscriber)
}

/** What the flow control keeps per message, aggregate and subscriber. */
object FlowControl {

  /** A message sent: its aggregate, its first version and its kind (each kind is answered on its own). */
  case class SentKey(aggregateId: AggregateId, firstVersion: Int, kind: Any)

  /** A message sent and not answered yet: its events, their stored bytes, and when it was sent (nanoseconds). */
  case class Sent(events: Int, bytes: Long, at: Long)

  /** Versions `from` to `upTo`, of about `bytes` stored, held for a subscriber; `heldAt`: the latest hold, a deadline
    * after which every delivery held for it has ended. */
  case class Behind(aggregateType: AggregateType, from: Int, upTo: Int, bytes: Long, heldAt: Long) {
    /** Versions `otherFrom` to `otherUpTo`, of `moreBytes`, held too at `at`. Only versions new to the range add their
      * share of the bytes: an event held again (re-published from the outbox while held) counts once. */
    def including(otherFrom: Int, otherUpTo: Int, moreBytes: Long, at: Long): Behind = {
      val below = math.max(math.min(otherUpTo, from - 1) - otherFrom + 1, 0)
      val above = math.max(otherUpTo - math.max(otherFrom, upTo + 1) + 1, 0)
      copy(from = math.min(from, otherFrom), upTo = math.max(upTo, otherUpTo),
        bytes = bytes + moreBytes * (below + above) / (otherUpTo - otherFrom + 1), heldAt = at)
    }
    /** What is left after versions up to `last` were re-published: the versions after it, with their share of the bytes. */
    def after(last: Int): Behind =
      if (last < from) this else copy(from = last + 1, bytes = bytes * (upTo - last) / (upTo - from + 1))
  }

  /** A pull under way: what it re-publishes, how much of the window it takes, and when it was asked. */
  case class Pulling(behind: Behind, events: Int, bytes: Long, at: Long)

  /** Ask the aggregate's owner to re-publish it from `from` to the subscriber. */
  case class Pull(aggregateType: AggregateType, aggregateId: AggregateId, from: Int)

  /** A subscriber's events and bytes sent and not answered, and how many of its aggregates are behind. */
  case class InFlight(events: Long, bytes: Long, aggregatesBehind: Int)
}

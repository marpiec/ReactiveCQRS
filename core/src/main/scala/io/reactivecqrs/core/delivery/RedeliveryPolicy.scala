package io.reactivecqrs.core.delivery

import java.time.{Duration, Instant}

import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.eventstore.WaitingInOutbox

import scala.concurrent.duration._

/**
  * When to ask an aggregate's owner to re-publish what waits in its outbox (D8, D20b). The first attempt is the live
  * publication at commit; re-publications follow it after each of `attemptsAfter` (3 and 6 minutes). Once they are
  * made, `giveUpAfter` (10 minutes) has passed, and the last of them is as old as the last one would be then (4 minutes:
  * one first seen late, re-published once for all its attempts, gets that long too, F268), the aggregate is reported
  * once as undelivered, and re-published
  * again every `retryGivenUpEvery` (30 minutes): its subscriber may be back without a restart (F123). Rows written
  * before this process started get a fresh set of attempts, the first one at once, because nothing delivers them
  * otherwise.
  *
  * At most `maxRepublishPerPoll` aggregates are re-published per poll, the longest waiting first, so a large backlog
  * (after an outage) does not start and restore every owner at once; the rest follow at the next polls (1 000 per
  * 30-second poll drains 100 000 aggregates in about 50 minutes). Pure: the outbox poller passes in the rows and the time.
  */
object RedeliveryPolicy {

  /** The policy's settings; see the object's description. */
  case class Options(attemptsAfter: Seq[FiniteDuration] = Seq(3.minutes, 6.minutes), giveUpAfter: FiniteDuration = 10.minutes,
                     maxRepublishPerPoll: Int = 1000, retryGivenUpEvery: FiniteDuration = 30.minutes)

  /** What the policy remembers about one aggregate between polls; `firstAttempt` is the live publication, or the first
    * re-publication for rows from before the start; `lastTry`, its last re-publication. */
  case class Tracked(waiting: WaitingInOutbox, firstAttempt: Instant, attemptsMade: Int, gaveUp: Boolean, lastTry: Option[Instant] = None)

  /** A poll's decision: what to re-publish now, what to report as undelivered, and what to remember for the next poll. */
  case class Decision(republish: Seq[WaitingInOutbox], gaveUp: Seq[WaitingInOutbox], memory: Map[AggregateId, Tracked])

  /** One aggregate's outcome of a poll: its state before (None if new) and after, and whether it is re-published. */
  private case class Step(waiting: WaitingInOutbox, before: Option[Tracked], after: Tracked, republish: Boolean, gaveUp: Boolean)

  /** Decides a poll: the aggregates waiting now, what was remembered from the poll before, when the poller started, and
    * the time now. */
  def decide(waiting: Seq[WaitingInOutbox], memory: Map[AggregateId, Tracked], startedAt: Instant, now: Instant, options: Options): Decision = {
    val steps = waiting.map(w => step(w, memory.get(w.aggregateId).filter(_.waiting.oldestVersion == w.oldestVersion), startedAt, now, options))
    // F141: retries of given-up aggregates take at most half the budget while others are due, so a large given-up backlog
    // (a subscriber stopped for long) never keeps the attempts of newer rows waiting; each takes what the other leaves.
    val budget = options.maxRepublishPerPoll
    val (retries, attempts) = steps.filter(_.republish).sortBy(_.waiting.since).partition(_.before.exists(_.gaveUp))
    val attemptsNow = attempts.take(budget - math.min(retries.size, budget / 2))
    val retriesNow = retries.take(budget - attemptsNow.size)
    val republishNow = (attemptsNow ++ retriesNow).sortBy(_.waiting.since)
    val postponed = (attempts.drop(attemptsNow.size) ++ retries.drop(retriesNow.size)).map(_.waiting.aggregateId).toSet

    // A postponed aggregate keeps its state from before this poll, so it is due again at the next one.
    val next = steps.flatMap { s =>
      if (postponed.contains(s.waiting.aggregateId)) s.before.map(s.waiting.aggregateId -> _)
      else Some(s.waiting.aggregateId -> s.after)
    }
    Decision(republishNow.map(_.waiting), steps.filter(_.gaveUp).map(_.waiting), next.toMap)
  }

  // A newer oldest version than the one tracked means the older rows were delivered: these rows get attempts of their own.
  private def step(w: WaitingInOutbox, before: Option[Tracked], startedAt: Instant, now: Instant, options: Options): Step = before match {
    case None if w.since.isBefore(startedAt) =>
      Step(w, None, Tracked(w, now, attemptsMade = 0, gaveUp = false, lastTry = Some(now)), republish = true, gaveUp = false)
    case None =>
      continue(w, None, Tracked(w, w.since, attemptsMade = 0, gaveUp = false), now, options)
    case Some(tracked) =>
      continue(w, before, tracked, now, options)
  }

  private def continue(w: WaitingInOutbox, before: Option[Tracked], tracked: Tracked, now: Instant, options: Options): Step = {
    val elapsed = Duration.between(tracked.firstAttempt, now).toMillis.millis
    val due = options.attemptsAfter.count(_ <= elapsed)
    if (tracked.gaveUp) {
      // F123: reported once, then tried again now and then, as its subscriber may be back without a restart.
      if (tracked.lastTry.forall(at => Duration.between(at, now).toMillis.millis >= options.retryGivenUpEvery)) {
        Step(w, before, tracked.copy(lastTry = Some(now)), republish = true, gaveUp = false)
      } else {
        Step(w, before, tracked, republish = false, gaveUp = false)
      }
    } else if (due > tracked.attemptsMade) {
      // one re-publication, however many attempts fell due since the last poll
      Step(w, before, tracked.copy(attemptsMade = due, lastTry = Some(now)), republish = true, gaveUp = false)
    } else if (due == options.attemptsAfter.size && elapsed >= options.giveUpAfter && lastTriedLongAgo(tracked, now, options)) {
      Step(w, before, tracked.copy(gaveUp = true, lastTry = Some(now)), republish = false, gaveUp = true)
    } else {
      Step(w, before, tracked, republish = false, gaveUp = false)
    }
  }

  // F268: the last re-publication has had the time the last of the attempts gets before the give-up.
  private def lastTriedLongAgo(tracked: Tracked, now: Instant, options: Options): Boolean = {
    val lastGets = options.giveUpAfter - options.attemptsAfter.lastOption.getOrElse(0.seconds)
    tracked.lastTry.forall(at => Duration.between(at, now).toMillis.millis >= lastGets)
  }
}

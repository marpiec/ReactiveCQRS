package io.reactivecqrs.core.delivery

import io.reactivecqrs.api.AggregateVersion

/**
  * Decides what a subscriber does with one delivery of an aggregate's events (D4). The subscriber's stored version
  * says how far it has applied the aggregate; a delivery carries a contiguous range of versions. Every listener kind
  * uses the same gate, so they all drop duplicates and detect gaps the same way.
  */
object SequenceGate {

  sealed trait Decision

  /** Every delivered version was applied before (a duplicate or a re-delivery): acknowledge, apply nothing. */
  case object AlreadyApplied extends Decision

  /** Apply versions `from` to `to`; delivered versions before `from` were applied before (F5). */
  case class Apply(from: AggregateVersion, to: AggregateVersion) extends Decision

  /** Versions between the applied one and the delivered ones are missing: they are needed again from `from`. */
  case class Gap(from: AggregateVersion) extends Decision

  /** `startsSequence`: nothing before `first` exists any more, so a subscriber behind it starts there: the delivery begins
    * with the aggregate's squash event (F98, D51), or holds the aggregate's whole state (a rebuild's final state, D62). */
  def decide(applied: AggregateVersion, first: AggregateVersion, last: AggregateVersion, startsSequence: Boolean = false): Decision = {
    require(first <= last, "a delivery holds at least one version, in order: " + first + ".." + last)
    if (last <= applied) AlreadyApplied
    else if (first <= applied.increment) Apply(applied.increment, last)
    else if (startsSequence) Apply(first, last)
    else Gap(applied.increment)
  }
}

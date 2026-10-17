package io.reactivecqrs.core.uid

import com.typesafe.config.Config
import io.reactivecqrs.api.id.AggregateId

/**
  * D39: the aggregate ids an installation makes, `installation × 10^12` up to the next installation's, so that ids stay
  * unique when the data of installations meet, and below 2^53, the largest whole number a JavaScript client reads exactly.
  * Installation 0 is the legacy range, where every id made before 1.0 lives; its first 1 000 ids are system ids.
  */
case class IdRange(installation: Int) {
  require(installation >= 0 && installation <= IdRange.MaxInstallation, "installation number must be between 0 and " + IdRange.MaxInstallation)

  val from: Long = installation.toLong * IdRange.Span
  val until: Long = math.min(from + IdRange.Span, IdRange.Limit)

  def contains(id: Long): Boolean = id >= from && id < until

  /** Why a caller may not choose this id for a new aggregate: not positive (negative ids are ephemeral aggregates', D77),
    * not below 2^53, or in another installation's range. System and legacy ids, below 10^12, are allowed. */
  def refusedChoice(id: AggregateId): Option[String] = id.asLong match {
    case n if n <= 0 => Some("aggregate id " + n + " is not positive")
    case n if n >= IdRange.Limit => Some("aggregate id " + n + " is not below 2^53")
    case n if n >= IdRange.Span && !contains(n) => Some("aggregate id " + n + " is in the range of installation " + n / IdRange.Span + ", not of this one (" + installation + ")")
    case _ => None
  }
}

object IdRange {
  val Span: Long = 1000000000000L // 10^12
  val Limit: Long = 1L << 53
  val MaxInstallation: Int = ((Limit - 1) / Span).toInt

  val Legacy: IdRange = IdRange(0)

  /** `reactivecqrs.installation-number`. */
  def of(config: Config): IdRange = IdRange(config.getInt("reactivecqrs.installation-number"))
}

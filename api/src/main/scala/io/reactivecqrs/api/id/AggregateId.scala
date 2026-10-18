package io.reactivecqrs.api.id

import io.reactivecqrs.api.AggregateVersion

object AggregateId {
  // F32: no pool of instances any more; it took a global lock on every id made, and ids compare by value.
  /** The id `asLong`. */
  def apply(asLong: Long): AggregateId = new AggregateId(asLong)

  def unapply(id: AggregateId): Option[Long] = Some(id.asLong)
}

/**
  * The id of one aggregate, unique across every aggregate type of the installation. The command bus assigns it to a
  * new aggregate from the installation's id range (or the caller reserves it, `ReserveAggregateId`); ephemeral
  * aggregates have negative ids (D77). Ids stay below 2^53, so they survive JavaScript numbers.
  *
  * @param asLong the id's number.
  */
class AggregateId(val asLong: Long) extends Serializable {
  override def equals(obj: Any): Boolean = obj.isInstanceOf[AggregateId] && obj.asInstanceOf[AggregateId].asLong == asLong

  override def hashCode(): Int = java.lang.Long.hashCode(asLong)

  override def toString: String = "Id(" + asLong + ")"
}

/** An aggregate at a version. */
case class AggregateIdWithVersion(id: AggregateId, version: AggregateVersion)

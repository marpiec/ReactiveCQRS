package io.reactivecqrs.api.id

object SpaceId {

  /** `-1`: the space of aggregates stored before the store kept spaces, until `backfillSpaces` gives them theirs (F49,
    * D38); old first events may carry it. No new aggregate is made in it: such a first event is refused. */
  val unknown: SpaceId = SpaceId(-1L)

  /** The space whose id is the id of the aggregate that stands for it (an organization, say). */
  def apply(id: AggregateId): SpaceId = new SpaceId(id.asLong)

  /** The space `asLong`. */
  def apply(asLong: Long): SpaceId = new SpaceId(asLong)

  def unapply(id: SpaceId): Option[Long] = Some(id.asLong)
}

/**
  * The organization (space) an aggregate belongs to, given by its first event (`FirstEvent.spaceId`): a projection
  * keeps it with each document, a purge, an export or an import works on one space at a time. `SpaceId(0)` when the
  * application has no organizations.
  *
  * @param asLong the space's id, often the id of the aggregate that stands for the organization.
  */
class SpaceId(val asLong: Long) extends Serializable {
  override def equals(obj: Any): Boolean = obj.isInstanceOf[SpaceId] && obj.asInstanceOf[SpaceId].asLong == asLong

  override def hashCode(): Int = java.lang.Long.hashCode(asLong)

  override def toString: String = "SpaceId(" + asLong + ")"
}

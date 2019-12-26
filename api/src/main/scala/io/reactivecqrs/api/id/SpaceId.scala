package io.reactivecqrs.api.id

object SpaceId {

  private var pool = Map[Long, SpaceId]()

  val unknown: SpaceId = create(-1L)

  def apply(id: AggregateId): SpaceId = {
    create(id.asLong)
  }

  def apply(asLong: Long): SpaceId = {
    create(asLong)
  }

  private def create(asLong: Long): SpaceId = {
    pool.getOrElse(asLong, {
      synchronized {
        val space = new SpaceId(asLong)
        pool += asLong -> space
        space
      }
    })
  }

  def unapply(id: SpaceId): Option[Long] = Some(id.asLong)

}

/**
  * Globally unique id that identifies single aggregate in whole application.
  *
  * @param asLong unique long identifier across aggregates.
  */
class SpaceId(val asLong: Long) extends Serializable {
  override def equals(obj: Any): Boolean = obj.isInstanceOf[SpaceId] && obj.asInstanceOf[SpaceId].asLong == asLong

  override def hashCode(): Int = java.lang.Long.hashCode(asLong)

  override def toString: String = "SpaceId("+asLong+")"

}

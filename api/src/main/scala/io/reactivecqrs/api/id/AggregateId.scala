package io.reactivecqrs.api.id

import scala.collection.JavaConversions._
import scala.collection.mutable
import java.util.Collections.synchronizedMap

import io.reactivecqrs.api.AggregateVersion

class LRUCache[K, V](maxEntries: Int) extends java.util.LinkedHashMap[K, V](1000, .75f, true) {
  override def removeEldestEntry(eldest: java.util.Map.Entry[K, V]): Boolean = size > maxEntries
}
object LRUCache {
  def apply[K, V](maxEntries: Int): mutable.Map[K, V] = synchronizedMap(new LRUCache[K, V](maxEntries))
}

object AggregateId {

  private var pool = LRUCache[Long, AggregateId](1000)

  def apply(asLong: Long): AggregateId = {

    pool.getOrElse(asLong, {
      val id = new AggregateId(asLong)
      pool += asLong -> id
      id
    })
  }

  def unapply(id: AggregateId): Option[Long] = Some(id.asLong)
}

/**
 * Globally unique id that identifies single aggregate in whole application.
 *
 * @param asLong unique long identifier across aggregates.
 */
class AggregateId(val asLong: Long) extends Serializable {
  override def equals(obj: Any): Boolean = obj.isInstanceOf[AggregateId] && obj.asInstanceOf[AggregateId].asLong == asLong

  override def hashCode(): Int = java.lang.Long.hashCode(asLong)

  override def toString: String = "Id("+asLong+")"

}

case class AggregateIdWithVersion(id: AggregateId, version: AggregateVersion)


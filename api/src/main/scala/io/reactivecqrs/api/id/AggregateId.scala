package io.reactivecqrs.api.id

import java.util.Collections
import scala.jdk.CollectionConverters._
import scala.collection.mutable
import io.reactivecqrs.api.AggregateVersion

class LRUCache[K, V](maxEntries: Int) extends java.util.LinkedHashMap[K, V](1000, .75f, true) {
  override def removeEldestEntry(eldest: java.util.Map.Entry[K, V]): Boolean = size > maxEntries
}
object LRUCache {
  def apply[K, V](maxEntries: Int): mutable.Map[K, V] = Collections.synchronizedMap[K, V](new LRUCache[K, V](maxEntries)).asScala
}

object AggregateId {
  private var pool: java.util.Map[Long, AggregateId] = LRUCache[Long, AggregateId](1000).asJava
  def apply(asLong: Long): AggregateId = {
    val fromPool = pool.get(asLong)
    if(fromPool == null) {
      val id = new AggregateId(asLong)
      pool.put(asLong, id)
      id
    } else {
      fromPool
    }
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


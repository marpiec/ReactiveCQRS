package io.reactivecqrs.api.id

import java.util.Collections
import scala.jdk.CollectionConverters._
import scala.collection.mutable
import io.reactivecqrs.api.AggregateVersion

@deprecated("Not part of the library; copy it if you use it", "1.0")
class LRUCache[K, V](maxEntries: Int) extends java.util.LinkedHashMap[K, V](1000, .75f, true) {
  override def removeEldestEntry(eldest: java.util.Map.Entry[K, V]): Boolean = size > maxEntries
}
@deprecated("Not part of the library; copy it if you use it", "1.0")
object LRUCache {
  def apply[K, V](maxEntries: Int): mutable.Map[K, V] = Collections.synchronizedMap[K, V](new LRUCache[K, V](maxEntries)).asScala
}

object AggregateId {
  // F32: no pool of instances any more; it took a global lock on every id made, and ids compare by value.
  def apply(asLong: Long): AggregateId = new AggregateId(asLong)

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


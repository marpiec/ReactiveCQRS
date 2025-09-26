package io.reactivecqrs.core.documentstore

sealed trait CacheEntry[+T] {
  /**
    * @return Some for InCache, None for InCacheEmpty, Some(default) for NotInCache
    */
  def getOrElse[B >: T](default: => Option[B]): Option[B]
}

case class InCache[+T](value: T) extends CacheEntry[T] {
  def getOrElse[B >: T](default: => Option[B]): Option[B] = Some(value)
}
case object InCacheEmpty extends CacheEntry[Nothing] {
  def getOrElse[B](default: => Option[B]): Option[B] = None
}

case object NotInCache extends CacheEntry[Nothing] {
  def getOrElse[B](default: => Option[B]): Option[B] = default
}


trait DocumentStoreCache[D <: AnyRef] {

  def put(key: Long, value: Option[VersionedDocument[D]]): Unit
  def get(key: Long): CacheEntry[VersionedDocument[D]]
  def getAll(keys: Set[Long]): Map[Long, CacheEntry[VersionedDocument[D]]]
  def remove(key: Long): Unit
  def clear(): Unit

}


class NoopDocumentStoreCache[D <: AnyRef, M <: AnyRef] extends DocumentStoreCache[D] {

  override def put(key: Long, value: Option[VersionedDocument[D]]): Unit = ()
  override def get(key: Long): CacheEntry[VersionedDocument[D]] = NotInCache
  override def clear(): Unit = {}
  override def remove(key: Long): Unit = {}
  override def getAll(keys: Set[Long]): Map[Long, CacheEntry[VersionedDocument[D]]] = Map.empty

}

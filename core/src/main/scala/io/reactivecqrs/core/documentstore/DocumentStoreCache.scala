package io.reactivecqrs.core.documentstore

/** What a document cache knows of a key. */
sealed trait CacheEntry[+T] {
  /**
    * @return Some for InCache, None for InCacheEmpty, Some(default) for NotInCache
    */
  def getOrElse[B >: T](default: => Option[B]): Option[B]
}

/** The key's document is cached. */
case class InCache[+T](value: T) extends CacheEntry[T] {
  def getOrElse[B >: T](default: => Option[B]): Option[B] = Some(value)
}
/** The key is known to have no document. */
case object InCacheEmpty extends CacheEntry[Nothing] {
  def getOrElse[B](default: => Option[B]): Option[B] = None
}

/** The cache knows nothing of the key: the store reads it. */
case object NotInCache extends CacheEntry[Nothing] {
  def getOrElse[B](default: => Option[B]): Option[B] = default
}


/** A cache of a document store's documents, which the store keeps current: it puts what it read or wrote once the
  * transaction commits, and removes what it changes. One cache per store, and per build of a managed projection. */
trait DocumentStoreCache[D <: AnyRef] {

  /** Keeps the key's document, or that it has none. */
  def put(key: Long, value: Option[VersionedDocument[D]]): Unit
  /** What the cache knows of the key. */
  def get(key: Long): CacheEntry[VersionedDocument[D]]
  /** What the cache knows of each key. */
  def getAll(keys: Set[Long]): Map[Long, CacheEntry[VersionedDocument[D]]]
  /** Forgets the key. */
  def remove(key: Long): Unit
  /** Forgets everything. */
  def clear(): Unit

}


/** No cache: every read goes to the store. */
class NoopDocumentStoreCache[D <: AnyRef, M <: AnyRef] extends DocumentStoreCache[D] {

  override def put(key: Long, value: Option[VersionedDocument[D]]): Unit = ()
  override def get(key: Long): CacheEntry[VersionedDocument[D]] = NotInCache
  override def clear(): Unit = {}
  override def remove(key: Long): Unit = {}
  override def getAll(keys: Set[Long]): Map[Long, CacheEntry[VersionedDocument[D]]] = Map.empty

}

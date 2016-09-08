package io.reactivecqrs.core.documentstore


trait DocumentStoreCache[D <: AnyRef, M <: AnyRef] {

  def putIfAbsent(key: Long, document: VersionedDocument[D, M]): Option[VersionedDocument[D, M]]

  def get(key: Long): Option[VersionedDocument[D, M]]

  def replace(key: Long, newValue: VersionedDocument[D, M], oldValue: VersionedDocument[D, M]): Boolean

  def getAll(keys: Set[Long]): Map[Long, VersionedDocument[D, M]]

  def remove(key: Long): Unit

  def clear(): Unit

}


class NoopDocumentStoreCache[D <: AnyRef, M <: AnyRef] extends DocumentStoreCache[D, M] {

  override def putIfAbsent(key: Long, document: VersionedDocument[D, M]): Option[VersionedDocument[D, M]] = None

  override def get(key: Long): Option[VersionedDocument[D, M]] = None

  override def replace(key: Long, newValue: VersionedDocument[D, M], oldValue: VersionedDocument[D, M]): Boolean = true

  override def clear(): Unit = {}

  override def remove(key: Long): Unit = {}

  override def getAll(keys: Set[Long]): Map[Long, VersionedDocument[D, M]] = Map.empty
}
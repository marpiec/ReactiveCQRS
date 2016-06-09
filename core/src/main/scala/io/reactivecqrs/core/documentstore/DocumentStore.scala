package io.reactivecqrs.core.documentstore

import scalikejdbc.DBSession

case class DocumentWithMetadata[T <: AnyRef, M <: AnyRef](document: T, metadata: M)

sealed abstract class AbstractDocumentStore[T <: AnyRef, M <: AnyRef] {

  def findDocumentByPath(path: Seq[String], value: String)(implicit session: DBSession = null): Map[Long, DocumentWithMetadata[T,M]]

  def findDocumentsByPathWithOneOfTheValues(path: Seq[String], values: Set[String])(implicit session: DBSession = null): Map[Long, DocumentWithMetadata[T,M]]

  def findDocumentByObjectInArray[V](arrayPath: Seq[String], objectPath: Seq[String], value: V)(implicit session: DBSession = null): Map[Long, DocumentWithMetadata[T, M]]

  def findDocumentByMetadataObjectInArray[V](arrayPath: Seq[String], objectPath: Seq[String], value: V)(implicit session: DBSession = null): Map[Long, DocumentWithMetadata[T,M]]

  def updateDocument(key: Long, document: T, metadata: M)(implicit session: DBSession): Unit

  def getDocument(key: Long)(implicit session: DBSession = null): Option[DocumentWithMetadata[T, M]]

  def getDocuments(keys: List[Long])(implicit session: DBSession = null): Map[Long, DocumentWithMetadata[T, M]]

  def removeDocument(key: Long)(implicit session: DBSession): Unit

  def findAll()(implicit session: DBSession = null): Map[Long, DocumentWithMetadata[T, M]]

}

abstract class DocumentStore[T <: AnyRef, M <: AnyRef] extends AbstractDocumentStore[T, M] {
  def insertDocument(key: Long, document: T, metadata: M)(implicit session: DBSession): Unit
}

abstract class DocumentStoreAutoId[T <: AnyRef, M <: AnyRef] extends AbstractDocumentStore[T, M] {
  def insertDocument(document: T, metadata: M)(implicit session: DBSession): Long
}
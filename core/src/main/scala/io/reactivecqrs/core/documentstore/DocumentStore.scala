package io.reactivecqrs.core.documentstore

case class DocumentWithMetadata[T <: AnyRef, M <: AnyRef](document: T, metadata: M)

sealed abstract class AbstractDocumentStore[T <: AnyRef, M <: AnyRef] {

  def findDocumentByPath(path: Seq[String], value: String): Map[Long, DocumentWithMetadata[T,M]]

  def findDocumentsByPathWithOneOfTheValues(path: Seq[String], values: Set[String]): Map[Long, DocumentWithMetadata[T,M]]

  def findDocumentByObjectInArray[V](arrayPath: Seq[String], objectPath: Seq[String], value: V): Map[Long, DocumentWithMetadata[T, M]]

  def findDocumentByMetadataObjectInArray[V](arrayPath: Seq[String], objectPath: Seq[String], value: V): Map[Long, DocumentWithMetadata[T,M]]

  def updateDocument(key: Long, document: T, metadata: M): Unit

  def getDocument(key: Long): Option[DocumentWithMetadata[T, M]]

  def getDocuments(keys: List[Long]): Map[Long, DocumentWithMetadata[T, M]]

  def removeDocument(key: Long): Unit

  def findAll(): Map[Long, DocumentWithMetadata[T, M]]

}

abstract class DocumentStore[T <: AnyRef, M <: AnyRef] extends AbstractDocumentStore[T, M] {
  def insertDocument(key: Long, document: T, metadata: M): Unit
}

abstract class DocumentStoreAutoId[T <: AnyRef, M <: AnyRef] extends AbstractDocumentStore[T, M] {
  def insertDocument(document: T, metadata: M): Unit
}
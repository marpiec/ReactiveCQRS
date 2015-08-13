package io.reactivecqrs.core.documentstore

case class DocumentWithMetadata[T <: AnyRef, M <: AnyRef](document: T, metadata: M)

abstract class DocumentStore[T <: AnyRef, M <: AnyRef] {

  def findDocumentByPath(path: Seq[String], value: String): Map[Long, DocumentWithMetadata[T,M]]

  def findDocumentsByPathWithOneOfTheValues(path: Seq[String], values: Set[String]): Map[Long, DocumentWithMetadata[T,M]]

  def findDocumentByPathWithOneArray[V](array: String, objectPath: Seq[String], value: V): Map[Long, DocumentWithMetadata[T,M]]

  def insertDocument(key: Long, document: T, metadata: M): Unit

  def updateDocument(key: Long, document: T, metadata: M): Unit

  def getDocument(key: Long): Option[DocumentWithMetadata[T, M]]

  // def getJson(key: Long): String

  // def getJsons(keys: List[Long]): Map[Long, String]

  def getDocuments(keys: List[Long]): Map[Long, DocumentWithMetadata[T, M]]

  def removeDocument(key: Long): Unit

  def findAll(): Map[Long, DocumentWithMetadata[T, M]]

}

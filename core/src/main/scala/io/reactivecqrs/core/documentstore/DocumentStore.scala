package io.reactivecqrs.core.documentstore

import scalikejdbc.DBSession

case class VersionedDocument[T <: AnyRef, M <: AnyRef](version: Int, document: T, metadata: M)

case class Document[T <: AnyRef, M <: AnyRef](document: T, metadata: M)

sealed trait Index

case class MultipleIndex(path: Seq[String]) extends Index

case class UniqueIndex(path: Seq[String]) extends Index

case class ExpectedValue(path: Seq[String], value: String)

import scala.reflect.runtime.universe._

sealed abstract class AbstractDocumentStore[T <: AnyRef, M <: AnyRef] {

  def findDocumentByPath(path: Seq[String], value: String)(implicit session: DBSession = null): Map[Long, Document[T,M]]

  def findDocumentByPaths(values: ExpectedValue*)(implicit session: DBSession = null): Map[Long, Document[T,M]]

  def findDocumentPartByPaths[P: TypeTag](part: List[String], values: ExpectedValue*)(implicit session: DBSession = null): Map[Long, P]

  def findDocument2PartsByPaths[P1: TypeTag, P2: TypeTag](part1: List[String], part2: List[String], values: ExpectedValue*)(implicit session: DBSession = null): Map[Long, (P1, P2)]

  def findDocument3PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag](part1: List[String], part2: List[String], part3: List[String], values: ExpectedValue*)(implicit session: DBSession = null): Map[Long, (P1, P2, P3)]

  def findDocument4PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag, P4: TypeTag](part1: List[String], part2: List[String], part3: List[String], part4: List[String], values: ExpectedValue*)(implicit session: DBSession = null): Map[Long, (P1, P2, P3, P4)]

  def findDocumentsByPathWithOneOfTheValues(path: Seq[String], values: Set[String])(implicit session: DBSession = null): Map[Long, Document[T,M]]

  def findDocumentByObjectInArray[V](arrayPath: Seq[String], objectPath: Seq[String], value: V)(implicit session: DBSession = null): Map[Long, Document[T, M]]

  def findDocumentByMetadataObjectInArray[V](arrayPath: Seq[String], objectPath: Seq[String], value: V)(implicit session: DBSession = null): Map[Long, Document[T,M]]

  def overwriteDocument(key: Long, document: T, metadata: M)(implicit session: DBSession): Unit

  def updateDocument(key: Long, modify: Option[Document[T, M]] => Document[T, M])(implicit session: DBSession): Document[T, M]

  def getDocument(key: Long)(implicit session: DBSession = null): Option[Document[T, M]]

  def getDocuments(keys: List[Long])(implicit session: DBSession = null): Map[Long, Document[T, M]]

  def removeDocument(key: Long)(implicit session: DBSession): Unit

  def findAll()(implicit session: DBSession = null): Map[Long, Document[T, M]]

  def countAll()(implicit session: DBSession = null): Int

  def clearAllData()(implicit session: DBSession = null): Unit

}

abstract class DocumentStore[T <: AnyRef, M <: AnyRef] extends AbstractDocumentStore[T, M] {
  def insertDocument(key: Long, document: T, metadata: M)(implicit session: DBSession): Unit
}

abstract class DocumentStoreAutoId[T <: AnyRef, M <: AnyRef] extends AbstractDocumentStore[T, M] {
  def insertDocument(document: T, metadata: M)(implicit session: DBSession): Long
}
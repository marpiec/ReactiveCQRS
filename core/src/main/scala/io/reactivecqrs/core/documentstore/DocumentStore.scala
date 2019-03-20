package io.reactivecqrs.core.documentstore

import scalikejdbc.DBSession

case class VersionedDocument[T <: AnyRef](version: Int, document: T)

case class Document[T <: AnyRef](document: T)

sealed trait Index

case class MultipleIndex(path: Seq[String]) extends Index

case class UniqueIndex(path: Seq[String]) extends Index

sealed trait ExpectedValue

case class ExpectedNoValue(path: Seq[String]) extends ExpectedValue

case class ExpectedSingleValue(path: Seq[String], value: String) extends ExpectedValue

case class ExpectedMultipleValues(path: Seq[String], values: Set[String]) extends ExpectedValue

import scala.reflect.runtime.universe._

sealed abstract class AbstractDocumentStore[T <: AnyRef] {

  def findDocumentByPath(path: Seq[String], value: String)(implicit session: DBSession = null): Map[Long, Document[T]]

  def findDocumentByPaths(values: ExpectedValue*)(implicit session: DBSession = null): Map[Long, Document[T]]

  def findDocumentPartByPaths[P: TypeTag](part: List[String], values: ExpectedValue*)(implicit session: DBSession = null): Map[Long, P]

  def findDocument2PartsByPaths[P1: TypeTag, P2: TypeTag](part1: List[String], part2: List[String], values: ExpectedValue*)(implicit session: DBSession = null): Map[Long, (P1, P2)]

  def findDocument3PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag](part1: List[String], part2: List[String], part3: List[String], values: ExpectedValue*)(implicit session: DBSession = null): Map[Long, (P1, P2, P3)]

  def findDocument4PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag, P4: TypeTag](part1: List[String], part2: List[String], part3: List[String], part4: List[String], values: ExpectedValue*)(implicit session: DBSession = null): Map[Long, (P1, P2, P3, P4)]

  def findDocumentsByPathWithOneOfTheValues(path: Seq[String], values: Set[String])(implicit session: DBSession = null): Map[Long, Document[T]]

  def findDocumentByObjectInArray[V](arrayPath: Seq[String], objectPath: Seq[String], value: V)(implicit session: DBSession = null): Map[Long, Document[T]]

  def overwriteDocument(key: Long, document: T)(implicit session: DBSession): Unit

  def updateDocument(spaceId: Long, key: Long, modify: Option[Document[T]] => Document[T])(implicit session: DBSession): Document[T]

  def getDocument(key: Long)(implicit session: DBSession = null): Option[Document[T]]

  def getDocuments(keys: List[Long])(implicit session: DBSession = null): Map[Long, Document[T]]

  def removeDocument(key: Long)(implicit session: DBSession): Unit

  def findAll()(implicit session: DBSession = null): Map[Long, Document[T]]

  def countAll()(implicit session: DBSession = null): Int

  def clearAllData()(implicit session: DBSession = null): Unit

}

abstract class DocumentStore[T <: AnyRef] extends AbstractDocumentStore[T] {
  def insertDocument(spaceId: Long, key: Long, document: T)(implicit session: DBSession): Unit
}

abstract class DocumentStoreAutoId[T <: AnyRef] extends AbstractDocumentStore[T] {
  def insertDocument(spaceId: Long, document: T)(implicit session: DBSession): Long
}
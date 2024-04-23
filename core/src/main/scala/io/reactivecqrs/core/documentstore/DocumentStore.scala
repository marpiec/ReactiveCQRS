package io.reactivecqrs.core.documentstore

import scalikejdbc.DBSession

import scala.collection.immutable.ListMap

case class VersionedDocument[T <: AnyRef](version: Int, document: T)

case class Document[T <: AnyRef](document: T)

sealed trait Index {
  val uniqueId: Int
}

sealed trait IndexPath
case class TextIndexPath(path: Seq[String]) extends IndexPath
case class LongIndexPath(path: Seq[String]) extends IndexPath
case class IntIndexPath(path: Seq[String]) extends IndexPath

case class UniqueCombinedIndex(uniqueId: Int, paths: Seq[IndexPath]) extends Index

case class MultipleCombinedIndex(uniqueId: Int, paths: Seq[IndexPath]) extends Index

case class MultipleTextIndex(uniqueId: Int, path: Seq[String]) extends Index

case class UniqueTextIndex(uniqueId: Int, path: Seq[String]) extends Index

case class MultipleLongIndex(uniqueId: Int, path: Seq[String]) extends Index

case class UniqueLongIndex(uniqueId: Int, path: Seq[String]) extends Index

case class MultipleIntIndex(uniqueId: Int, path: Seq[String]) extends Index

case class UniqueIntIndex(uniqueId: Int, path: Seq[String]) extends Index

case class MultipleTextArrayIndex(uniqueId: Int, path: Seq[String]) extends Index

sealed trait ExpectedValue

case class ExpectedNoValue(path: Seq[String]) extends ExpectedValue

case class ExpectedSingleTextValueLike(path: Seq[String], pattern: String) extends ExpectedValue

case class ExpectedSingleTextValue(path: Seq[String], value: String) extends ExpectedValue

case class ExpectedSingleTextValueInArray(path: Seq[String], value: String) extends ExpectedValue

case class ExpectedMultipleTextValuesInArray(path: Seq[String], values: Iterable[String]) extends ExpectedValue

case class ExpectedSingleIntValue(path: Seq[String], value: Int) extends ExpectedValue

case class ExpectedSingleLongValue(path: Seq[String], value: Long) extends ExpectedValue

case class ExpectedSingleBooleanValue(path: Seq[String], value: Boolean) extends ExpectedValue

case class ExpectedGreaterThanIntValue(path: Seq[String], value: Int) extends ExpectedValue

case class ExpectedLessThanIntValue(path: Seq[String], value: Int) extends ExpectedValue

case class ExpectedMultipleTextValues(path: Seq[String], values: Iterable[String]) extends ExpectedValue

case class ExpectedMultipleIntValues(path: Seq[String], values: Iterable[Int]) extends ExpectedValue

case class ExpectedMultipleLongValues(path: Seq[String], values: Iterable[Long]) extends ExpectedValue

import scala.reflect.runtime.universe._

object DocumentStoreQuery {
  def basic(where: Seq[ExpectedValue]) = DocumentStoreQuery(where, Seq.empty, 0, 10000)
}

sealed trait Sort

object SortAscInt {
  def apply(name: String): Sort = SortAscInt(Seq(name))
}
case class SortAscInt(path: Seq[String]) extends Sort


object SortAscText {
  def apply(name: String): Sort = SortAscText(Seq(name))
}
case class SortAscText(path: Seq[String]) extends Sort

object SortDescInt {
  def apply(name: String): Sort = SortDescInt(Seq(name))
}
case class SortDescInt(path: Seq[String]) extends Sort

object SortDescText {
  def apply(name: String): Sort = SortDescText(Seq(name))
}
case class SortDescText(path: Seq[String]) extends Sort



case class DocumentStoreQuery(where: Seq[ExpectedValue],
                              sortBy: Seq[Sort],
                              offset: Int,
                              limit: Int)

sealed abstract class AbstractDocumentStore[T <: AnyRef] {

  def findDocumentByPath(path: Seq[String], value: String)(implicit session: DBSession = null): Map[Long, Document[T]] =
    findDocument(DocumentStoreQuery.basic(Seq(ExpectedSingleTextValue(path, value)))).toMap

  def findDocumentByPath(value: ExpectedValue): Map[Long, Document[T]] =
    findDocument(DocumentStoreQuery.basic(Seq(value))).toMap

  def findDocumentByPaths(values: ExpectedValue*): Map[Long, Document[T]] =
    findDocument(DocumentStoreQuery.basic(values)).toMap

  def findDocument(query: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, Document[T])]

  def findDocumentWithTransform[TT <: AnyRef](query: DocumentStoreQuery, transform: T => TT)(implicit session: DBSession = null): Seq[(Long, Document[TT])]

  def findDocumentPartByPaths[P: TypeTag](part: Seq[String], query: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, P)]

  def findDocument2PartsByPaths[P1: TypeTag, P2: TypeTag](part1: Seq[String], part2: Seq[String], query: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, (P1, P2))]

  def findDocument3PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag](part1: Seq[String], part2: Seq[String], part3: Seq[String], query: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, (P1, P2, P3))]

  def findDocument4PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag, P4: TypeTag](part1: Seq[String], part2: Seq[String], part3: Seq[String], part4: Seq[String], query: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, (P1, P2, P3, P4))]

  def findDocumentsByPathWithOneOfTheValues(path: Seq[String], values: Set[String])(implicit session: DBSession = null): Map[Long, Document[T]] = {
    if(values.isEmpty) {
      Map.empty
    } else {
      findDocument(DocumentStoreQuery.basic(Seq(ExpectedMultipleTextValues(path, values)))).toMap
    }
  }

  def findDocumentByObjectInArray[V](arrayPath: Seq[String], objectPath: Seq[String], value: V)(implicit session: DBSession = null): Map[Long, Document[T]]

  def overwriteDocument(key: Long, document: T)(implicit session: DBSession): Unit

  def updateExistingDocument(key: Long, modify: Document[T] => Document[T])(implicit session: DBSession): Document[T]

  def updateDocument(spaceId: Long, key: Long, modify: Option[Document[T]] => Document[T])(implicit session: DBSession): Document[T]

  def getDocument(key: Long)(implicit session: DBSession = null): Option[Document[T]]

  def getDocuments(keys: Iterable[Long])(implicit session: DBSession = null): Map[Long, Document[T]]

  def removeDocument(key: Long)(implicit session: DBSession): Unit

  def findAll()(implicit session: DBSession = null): Map[Long, Document[T]]

  def countAll()(implicit session: DBSession = null): Int

  def clearAllData()(implicit session: DBSession = null): Unit

  def clearSpace(spaceId: Long)(implicit session: DBSession): Unit
}

abstract class DocumentStore[T <: AnyRef] extends AbstractDocumentStore[T] {
  def insertDocument(spaceId: Long, key: Long, document: T)(implicit session: DBSession): Unit
}

abstract class DocumentStoreAutoId[T <: AnyRef] extends AbstractDocumentStore[T] {
  def insertDocument(spaceId: Long, document: T)(implicit session: DBSession): Long
}
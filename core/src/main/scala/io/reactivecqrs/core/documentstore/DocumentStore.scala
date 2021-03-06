package io.reactivecqrs.core.documentstore

import scalikejdbc.DBSession

case class VersionedDocument[T <: AnyRef](version: Int, document: T)

case class Document[T <: AnyRef](document: T)

sealed trait Index {
  val uniqueId: Int
}

case class MultipleIndex(uniqueId: Int, path: Seq[String]) extends Index

case class UniqueIndex(uniqueId: Int, path: Seq[String]) extends Index

sealed trait ExpectedValue

case class ExpectedNoValue(path: Seq[String]) extends ExpectedValue

case class ExpectedSingleValueLike(path: Seq[String], pattern: String) extends ExpectedValue

case class ExpectedSingleValue(path: Seq[String], value: String) extends ExpectedValue

case class ExpectedSingleValueInArray(path: Seq[String], value: String) extends ExpectedValue

case class ExpectedMultipleValuesInArray(path: Seq[String], values: Iterable[String]) extends ExpectedValue

case class ExpectedSingleIntValue(path: Seq[String], value: Int) extends ExpectedValue

case class ExpectedSingleLongValue(path: Seq[String], value: Long) extends ExpectedValue

case class ExpectedGreaterThanIntValue(path: Seq[String], value: Int) extends ExpectedValue

case class ExpectedLessThanIntValue(path: Seq[String], value: Int) extends ExpectedValue

case class ExpectedMultipleValues(path: Seq[String], values: Iterable[String]) extends ExpectedValue

case class ExpectedMultipleIntValues(path: Seq[String], values: Iterable[Int]) extends ExpectedValue

case class ExpectedMultipleLongValues(path: Seq[String], values: Iterable[Long]) extends ExpectedValue

import scala.reflect.runtime.universe._

object DocumentStoreQuery {
  def basic(where: Seq[ExpectedValue]) = DocumentStoreQuery(where, Seq.empty, 0, 10000)
}

sealed trait Sort
object SortAsc {
  def apply(name: String): Sort = SortAsc(Seq(name))
}
case class SortAsc(path: Seq[String]) extends Sort
object SortDesc {
  def apply(name: String): Sort = SortDesc(Seq(name))
}
case class SortDesc(path: Seq[String]) extends Sort

case class DocumentStoreQuery(where: Seq[ExpectedValue],
                              sortBy: Seq[Sort],
                              offset: Int,
                              limit: Int)

sealed abstract class AbstractDocumentStore[T <: AnyRef] {

  def findDocumentByPath(path: Seq[String], value: String)(implicit session: DBSession = null): Map[Long, Document[T]] =
    findDocument(DocumentStoreQuery.basic(Seq(ExpectedSingleValue(path, value))))

  def findDocumentByPaths(values: ExpectedValue*): Map[Long, Document[T]] =
    findDocument(DocumentStoreQuery.basic(values))

  def findDocument(query: DocumentStoreQuery)(implicit session: DBSession = null): Map[Long, Document[T]]

  def findDocumentWithTransform[TT <: AnyRef](query: DocumentStoreQuery, transform: T => TT)(implicit session: DBSession = null): Map[Long, Document[TT]]

  def findDocumentPartByPaths[P: TypeTag](part: List[String], query: DocumentStoreQuery)(implicit session: DBSession = null): Map[Long, P]

  def findDocument2PartsByPaths[P1: TypeTag, P2: TypeTag](part1: List[String], part2: List[String], query: DocumentStoreQuery)(implicit session: DBSession = null): Map[Long, (P1, P2)]

  def findDocument3PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag](part1: List[String], part2: List[String], part3: List[String], query: DocumentStoreQuery)(implicit session: DBSession = null): Map[Long, (P1, P2, P3)]

  def findDocument4PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag, P4: TypeTag](part1: List[String], part2: List[String], part3: List[String], part4: List[String], query: DocumentStoreQuery)(implicit session: DBSession = null): Map[Long, (P1, P2, P3, P4)]

  def findDocumentsByPathWithOneOfTheValues(path: Seq[String], values: Set[String])(implicit session: DBSession = null): Map[Long, Document[T]] = {
    if(values.isEmpty) {
      Map.empty
    } else {
      findDocument(DocumentStoreQuery.basic(Seq(ExpectedMultipleValues(path, values))))
    }
  }

  def findDocumentByObjectInArray[V](arrayPath: Seq[String], objectPath: Seq[String], value: V)(implicit session: DBSession = null): Map[Long, Document[T]]

  def overwriteDocument(key: Long, document: T)(implicit session: DBSession): Unit

  def updateExistingDocument(key: Long, modify: Document[T] => Document[T])(implicit session: DBSession): Document[T]

  def updateDocument(spaceId: Long, key: Long, modify: Option[Document[T]] => Document[T])(implicit session: DBSession): Document[T]

  def getDocument(key: Long)(implicit session: DBSession = null): Option[Document[T]]

  def getDocuments(keys: List[Long])(implicit session: DBSession = null): Map[Long, Document[T]]

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
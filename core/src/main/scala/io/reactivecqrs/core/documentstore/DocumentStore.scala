package io.reactivecqrs.core.documentstore

import scalikejdbc.DBSession

/** A document with the version of its row: 1 when inserted, one more at every write. */
case class VersionedDocument[T <: AnyRef](version: Int, document: T)

/** A document of a read model, as a store gives it back. */
case class Document[T <: AnyRef](document: T)

/** An index of a store's table over paths in its documents' JSON, declared when the store is made: the store creates
  * the missing ones, builds again one whose definition changed, and drops those no longer declared (F171). */
sealed trait Index {
  /** The index's number, part of its name: unique within the store, and kept for as long as the index is. */
  val uniqueId: Int
}

/** A path in a combined index, with the type its value is indexed as. */
sealed trait IndexPath
/** A text value at `path`. */
case class TextIndexPath(path: Seq[String]) extends IndexPath
/** A number at `path`, indexed as a `bigint`. */
case class LongIndexPath(path: Seq[String]) extends IndexPath
/** A number at `path`, indexed as an `int`. */
case class IntIndexPath(path: Seq[String]) extends IndexPath

/** A unique index over several paths: no two documents share all their values. */
case class UniqueCombinedIndex(uniqueId: Int, paths: Seq[IndexPath]) extends Index

/** An index over several paths. */
case class MultipleCombinedIndex(uniqueId: Int, paths: Seq[IndexPath]) extends Index

/** An index of the text at `path`. */
case class MultipleTextIndex(uniqueId: Int, path: Seq[String]) extends Index

/** A unique index of the text at `path`. */
case class UniqueTextIndex(uniqueId: Int, path: Seq[String]) extends Index

/** An index of the number at `path`, as a `bigint`. */
case class MultipleLongIndex(uniqueId: Int, path: Seq[String]) extends Index

/** A unique index of the number at `path`, as a `bigint`. */
case class UniqueLongIndex(uniqueId: Int, path: Seq[String]) extends Index

/** An index of the number at `path`, as an `int`. */
case class MultipleIntIndex(uniqueId: Int, path: Seq[String]) extends Index

/** A unique index of the number at `path`, as an `int`. */
case class UniqueIntIndex(uniqueId: Int, path: Seq[String]) extends Index

/** A GIN index of the array of texts at `path`, for `ExpectedSingleTextValueInArray` and
  * `ExpectedMultipleTextValuesInArray`. */
case class MultipleTextArrayIndex(uniqueId: Int, path: Seq[String]) extends Index

/** A condition on the value at a path of a document's JSON (a path is the names of the fields, outermost first). A
  * query's conditions must all hold. */
sealed trait ExpectedValue

/** No value at `path`: the field is missing or `null`. */
case class ExpectedNoValue(path: Seq[String]) extends ExpectedValue

/** The text at `path` matches `pattern`, case-insensitively (SQL `ILIKE`: `%` any text, `_` one character). */
case class ExpectedSingleTextValueLike(path: Seq[String], pattern: String) extends ExpectedValue

/** The text at `path` is `value`. */
case class ExpectedSingleTextValue(path: Seq[String], value: String) extends ExpectedValue

/** The array of texts at `path` holds `value`. */
case class ExpectedSingleTextValueInArray(path: Seq[String], value: String) extends ExpectedValue

/** The array of texts at `path` holds at least one of `values`. */
case class ExpectedMultipleTextValuesInArray(path: Seq[String], values: Iterable[String]) extends ExpectedValue

/** The number at `path` is `value`. */
case class ExpectedSingleIntValue(path: Seq[String], value: Int) extends ExpectedValue

/** The number at `path` is `value`. */
case class ExpectedSingleLongValue(path: Seq[String], value: Long) extends ExpectedValue

/** The boolean at `path` is `value`. */
case class ExpectedSingleBooleanValue(path: Seq[String], value: Boolean) extends ExpectedValue

/** The number at `path` is greater than `value`. */
case class ExpectedGreaterThanIntValue(path: Seq[String], value: Int) extends ExpectedValue

/** The number at `path` is less than `value`. */
case class ExpectedLessThanIntValue(path: Seq[String], value: Int) extends ExpectedValue

/** The text at `path` is one of `values`. */
case class ExpectedMultipleTextValues(path: Seq[String], values: Iterable[String]) extends ExpectedValue

/** The number at `path` is one of `values`. */
case class ExpectedMultipleIntValues(path: Seq[String], values: Iterable[Int]) extends ExpectedValue

/** The number at `path` is one of `values`. */
case class ExpectedMultipleLongValues(path: Seq[String], values: Iterable[Long]) extends ExpectedValue

import scala.reflect.runtime.universe._

object DocumentStoreQuery {
  /** The documents for which every condition of `where` holds, in no particular order, all of them. */
  def basic(where: Seq[ExpectedValue]): DocumentStoreQuery = DocumentStoreQuery(where, Seq.empty, 0, -1)
}

/** An order of a query's results, by the value at a path. */
sealed trait Sort

object SortAscInt {
  /** By the number in the top-level field `name`, smallest first. */
  def apply(name: String): Sort = SortAscInt(Seq(name))
}
/** By the number at `path`, as an `int`, smallest first. */
case class SortAscInt(path: Seq[String]) extends Sort

object SortAscLong {
  /** By the number in the top-level field `name`, smallest first. */
  def apply(name: String): Sort = SortAscLong(Seq(name))
}
/** By the number at `path`, as a `bigint`, smallest first. */
case class SortAscLong(path: Seq[String]) extends Sort

object SortAscText {
  /** By the text in the top-level field `name`, in the database's order. */
  def apply(name: String): Sort = SortAscText(Seq(name))
}
/** By the text at `path`, in the database's order. */
case class SortAscText(path: Seq[String]) extends Sort

object SortDescInt {
  /** By the number in the top-level field `name`, largest first. */
  def apply(name: String): Sort = SortDescInt(Seq(name))
}
/** By the number at `path`, as an `int`, largest first. */
case class SortDescInt(path: Seq[String]) extends Sort

object SortDescLong {
  /** By the number in the top-level field `name`, largest first. */
  def apply(name: String): Sort = SortDescLong(Seq(name))
}
/** By the number at `path`, as a `bigint`, largest first. */
case class SortDescLong(path: Seq[String]) extends Sort

object SortDescText {
  /** By the text in the top-level field `name`, in the database's reverse order. */
  def apply(name: String): Sort = SortDescText(Seq(name))
}
/** By the text at `path`, in the database's reverse order. */
case class SortDescText(path: Seq[String]) extends Sort

/** A query of a store: the documents for which every condition of `where` holds, ordered by `sortBy`, from `offset`,
  * at most `limit` of them (`-1`: all). */
case class DocumentStoreQuery(where: Seq[ExpectedValue],
                              sortBy: Seq[Sort],
                              offset: Int,
                              limit: Int)

/** The transaction a finder reads in, for the one that cannot take `(implicit session: DBSession = null)` as the others
  * do, since only one alternative of an overloaded method may have default arguments (F160): the caller's implicit
  * `DBSession`, such as a listener's, or without one none, and the store reads on a connection of its own. */
final class CallerSession private[documentstore] (val session: DBSession)

object CallerSession extends NoCallerSession {
  /** The caller's session, when it has one in implicit scope. */
  implicit def callers(implicit session: DBSession): CallerSession = new CallerSession(session)
}

sealed trait NoCallerSession {
  /** No session: the store reads on a connection of its own. */
  implicit val none: CallerSession = new CallerSession(null)
}

/**
  * The documents of a read model, one per key (often the id of the aggregate it shows), each in an organization's
  * space, stored as JSON (`PostgresDocumentStore`: a table `projection_<name>` with a JSONB column; `MemoryDocumentStore`
  * for tests). Every method takes the transaction it runs in as its implicit `session`: a projection's listener passes
  * its own, so its writes commit or roll back with the subscription's version. A read without a session (`null`, the
  * default) runs on a connection of its own. A store may keep a cache of documents (`DocumentStoreCache`); writes reach
  * it only after their transaction commits.
  */
sealed abstract class AbstractDocumentStore[T <: AnyRef] {

  /** The documents whose text at `path` is `value`, by key. */
  def findDocumentByPath(path: Seq[String], value: String)(implicit session: DBSession = null): Map[Long, Document[T]] =
    findDocument(DocumentStoreQuery.basic(Seq(ExpectedSingleTextValue(path, value)))).toMap

  /** The documents for which `value` holds, by key; read in the caller's transaction when it has one (F160). */
  def findDocumentByPath(value: ExpectedValue)(implicit caller: CallerSession): Map[Long, Document[T]] =
    findDocument(DocumentStoreQuery.basic(Seq(value)))(caller.session).toMap

  /** The documents for which every one of `values` holds, by key. */
  def findDocumentByPaths(values: ExpectedValue*)(implicit session: DBSession = null): Map[Long, Document[T]] =
    findDocument(DocumentStoreQuery.basic(values)).toMap

  /** The documents `query` selects, with their keys, in its order. */
  def findDocument(query: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, Document[T])]

  /** As `findDocument`, each document changed by `transform` (to leave out what the caller does not need). */
  def findDocumentWithTransform[TT <: AnyRef](query: DocumentStoreQuery, transform: T => TT)(implicit session: DBSession = null): Seq[(Long, Document[TT])]

  /** The value at the path `part` of each document `query` selects, read as a `P`, without reading the rest. */
  def findDocumentPartByPaths[P: TypeTag](part: Seq[String], query: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, P)]

  /** The values at two paths of each document `query` selects. */
  def findDocument2PartsByPaths[P1: TypeTag, P2: TypeTag](part1: Seq[String], part2: Seq[String], query: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, (P1, P2))]

  /** The values at three paths of each document `query` selects. */
  def findDocument3PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag](part1: Seq[String], part2: Seq[String], part3: Seq[String], query: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, (P1, P2, P3))]

  /** The values at four paths of each document `query` selects. */
  def findDocument4PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag, P4: TypeTag](part1: Seq[String], part2: Seq[String], part3: Seq[String], part4: Seq[String], query: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, (P1, P2, P3, P4))]

  /** The documents whose text at `path` is one of `values`, by key; none for no values. */
  def findDocumentsByPathWithOneOfTheValues(path: Seq[String], values: Set[String])(implicit session: DBSession = null): Map[Long, Document[T]] = {
    if(values.isEmpty) {
      Map.empty
    } else {
      findDocument(DocumentStoreQuery.basic(Seq(ExpectedMultipleTextValues(path, values)))).toMap
    }
  }

  /** At most `limit` documents whose array at `arrayPath` holds an object with `value` at `objectPath`, by key. */
  def findDocumentByObjectInArray[V](arrayPath: Seq[String], objectPath: Seq[String], value: V, limit: Int)(implicit session: DBSession = null): Map[Long, Document[T]]

  /** Replaces the document of `key`, which must exist (otherwise it throws). */
  def overwriteDocument(key: Long, document: T)(implicit session: DBSession): Unit

  /** Changes the document of `key` with `modify`, which must exist (otherwise it throws), with optimistic locking: when
    * another transaction wrote it meanwhile, `modify` runs again on the newer document, at most 10 times. Returns the
    * document written. */
  def updateExistingDocument(key: Long, modify: Document[T] => Document[T])(implicit session: DBSession): Document[T]

  /** As `updateExistingDocument`, and when there is no document of `key`, inserts `modify(None)` in the space `spaceId`. */
  def updateDocument(spaceId: Long, key: Long, modify: Option[Document[T]] => Document[T])(implicit session: DBSession): Document[T]

  /** The document of `key`, if there is one. */
  def getDocument(key: Long)(implicit session: DBSession = null): Option[Document[T]]

  /** The documents of the `keys` that have one, by key. */
  def getDocuments(keys: Iterable[Long])(implicit session: DBSession = null): Map[Long, Document[T]]

  /** Removes the document of `key`, if there is one. */
  def removeDocument(key: Long)(implicit session: DBSession): Unit

  /** Every document, by key: for small read models only. */
  def findAll()(implicit session: DBSession = null): Map[Long, Document[T]]

  /** How many documents there are. */
  def countAll()(implicit session: DBSession = null): Int

  /** Removes every document. */
  def clearAllData()(implicit session: DBSession = null): Unit

  /** Removes the documents of the space `spaceId` (an organization's purge, `SpacePurge`). */
  def clearSpace(spaceId: Long)(implicit session: DBSession): Unit
}

/** A document to insert under `key`. */
case class DocumentToInsert[T <: AnyRef](key: Long, document: T)

/** A store whose keys the caller gives, usually the aggregate's id. */
abstract class DocumentStore[T <: AnyRef] extends AbstractDocumentStore[T] {
  /** Inserts `document` under `key` in the space `spaceId`; a key that has a document already fails. */
  def insertDocument(spaceId: Long, key: Long, document: T)(implicit session: DBSession): Unit
  /** Inserts `documents` in the space `spaceId`, in one batch. */
  def insertDocuments(spaceId: Long, documents: Seq[DocumentToInsert[T]])(implicit session: DBSession): Unit
}

/** A store that gives its documents their keys, from a sequence of its own: for read models with several documents
  * per aggregate. */
abstract class DocumentStoreAutoId[T <: AnyRef] extends AbstractDocumentStore[T] {
  /** Inserts `document` in the space `spaceId`, and returns the key it got. */
  def insertDocument(spaceId: Long, document: T)(implicit session: DBSession): Long
  /** Inserts `documents` in the space `spaceId`, in one batch. */
  def insertDocuments(spaceId: Long, documents: Seq[T])(implicit session: DBSession): Unit
}

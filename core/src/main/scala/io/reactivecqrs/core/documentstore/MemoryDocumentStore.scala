package io.reactivecqrs.core.documentstore

import java.lang.reflect.Field
import java.util.regex.Pattern

import scalikejdbc.DBSession

import scala.annotation.nowarn
import scala.collection.parallel.mutable

import scala.reflect.runtime.universe._

// The sessions are the document store's interface; in memory there is none to use.
@nowarn("cat=unused-params")
sealed trait MemoryDocumentStoreTrait[T <: AnyRef] {

  val store = mutable.ParHashMap[Long, Document[T]]()
  /** As `PostgresDocumentStoreTrait.maxRows` (F211). */
  val maxRows: Int
  // F156: each document's space, as the space_id column keeps it: set when the document is inserted, kept after.
  protected val spaces = mutable.ParHashMap[Long, Long]()

  protected def stored(spaceId: Long, key: Long, document: Document[T]): Unit = {
    store += key -> document
    spaces += key -> spaceId
  }


  // Conditions, order, offset, limit and maxRows mirror PostgresDocumentStore.createQuery, so tests using this store see
  // the same results (F273).
  def findDocument(query: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, Document[T])] = {
    val paged = ordered(found(query), query.sortBy).drop(query.offset)
    if (query.limit >= 0) paged.take(query.limit)
    else if (paged.size > maxRows) throw new TooManyDocumentsException("the memory store", maxRows)
    else paged
  }

  // As PostgreSQL orders: numbers by value, texts by their characters (PostgreSQL's collation may differ), a missing
  // value last when ascending and first when descending; equal ones by key, which PostgreSQL leaves unspecified.
  private def ordered(documents: Seq[(Long, Document[T])], sortBy: Seq[Sort]): Seq[(Long, Document[T])] = {
    def compare(path: Seq[String], numbers: Boolean)(a: Document[T], b: Document[T]): Int =
      (valueAtPath(a.document, path).map(_.toString), valueAtPath(b.document, path).map(_.toString)) match {
        case (None, None) => 0
        case (None, _) => 1
        case (_, None) => -1
        case (Some(x), Some(y)) => if (numbers) BigDecimal(x).compare(BigDecimal(y)) else x.compareTo(y)
      }
    val comparisons = sortBy.map {
      case SortAscInt(path) => compare(path, numbers = true) _
      case SortAscLong(path) => compare(path, numbers = true) _
      case SortAscText(path) => compare(path, numbers = false) _
      case SortDescInt(path) => (a: Document[T], b: Document[T]) => -compare(path, numbers = true)(a, b)
      case SortDescLong(path) => (a: Document[T], b: Document[T]) => -compare(path, numbers = true)(a, b)
      case SortDescText(path) => (a: Document[T], b: Document[T]) => -compare(path, numbers = false)(a, b)
    }
    documents.sortBy(_._1).sortWith { (a, b) => comparisons.iterator.map(_(a._2, b._2)).find(_ != 0).exists(_ < 0) }
  }

  private def found(query: DocumentStoreQuery): Seq[(Long, Document[T])] = {
    store.filter(keyValuePair => {
      val document = keyValuePair._2.asInstanceOf[Document[AnyRef]].document
      query.where.forall {
        case ExpectedNoValue(path) => valueAtPath(document, path).isEmpty
        case ExpectedSingleTextValue(path, value) => matches(document, path, value)
        case ExpectedSingleTextValueLike(path, pattern) => valueAtPath(document, path).exists(value => ilike(value.toString, pattern))
        case ExpectedSingleTextValueInArray(path, value) => arrayMatchSeq(document, path).exists(_.toString == value)
        case ExpectedMultipleTextValuesInArray(path, values) => arrayMatchSeq(document, path).exists(element => values.exists(_ == element.toString))
        case ExpectedMultipleTextValues(path, vals) => vals.exists(value => matches(document, path, value))
        case ExpectedMultipleIntValues(path, vals) => vals.exists(value => matches(document, path, value))
        case ExpectedMultipleLongValues(path, vals) => vals.exists(value => matches(document, path, value))
        case ExpectedSingleIntValue(path, value) => matches(document, path, value)
        case ExpectedSingleLongValue(path, value) => matches(document, path, value)
        case ExpectedSingleBooleanValue(path, value) => matches(document, path, value)
        case ExpectedGreaterThanIntValue(path, value) => valueAtPath(document, path).exists(_.toString.toInt > value)
        case ExpectedLessThanIntValue(path, value) => valueAtPath(document, path).exists(_.toString.toInt < value)
      }
    }).seq.toSeq
  }

  /** Value at the path, like `document #> path`: `None` for null, a missing option value, or an absent field value. */
  protected def valueAtPath(element: AnyRef, path: Seq[String]): Option[AnyRef] = {
    if (element == null) {
      None
    } else if (path.isEmpty) {
      Some(element)
    } else {
      element match {
        case None => None
        case Some(inner) if path.head == "value" => valueAtPath(inner.asInstanceOf[AnyRef], path.tail)
        case _ =>
          try {
            valueAtPath(getPrivateValue(element, element.getClass.getDeclaredField(path.head)), path.tail)
          } catch {
            case _: NoSuchFieldException => throw new IllegalArgumentException(s"No field [${path.head}] found in type [${element.getClass}] for element $element")
          }
      }
    }
  }

  /** SQL ILIKE: `%` is any sequence, `_` is one character, case-insensitive. */
  private def ilike(value: String, pattern: String): Boolean = {
    val regex = pattern.map {
      case '%' => ".*"
      case '_' => "."
      case c => Pattern.quote(c.toString)
    }.mkString
    Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.DOTALL).matcher(value).matches()
  }

  def findDocumentWithTransform[TT <: AnyRef](query: DocumentStoreQuery, transform: T => TT)(implicit session: DBSession = null): Seq[(Long, Document[TT])] = {
    findDocument(query).map(v => (v._1, v._2.copy(document = transform(v._2.document))))
  }

  def findDocumentPartByPaths[P: TypeTag](part: Seq[String], query: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, P)] = {
    findDocument(query).map(d => d._1 -> valueAt[P](d._2.document, part))
  }

  def findDocument2PartsByPaths[P1: TypeTag, P2: TypeTag](part1: Seq[String], part2: Seq[String], query: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, (P1, P2))] = {
    findDocument(query).map(d => d._1 -> (valueAt[P1](d._2.document, part1), valueAt[P2](d._2.document, part2)))
  }

  def findDocument3PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag](part1: Seq[String], part2: Seq[String], part3: Seq[String], query: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, (P1, P2, P3))] = {
    findDocument(query).map(d => d._1 -> (valueAt[P1](d._2.document, part1), valueAt[P2](d._2.document, part2), valueAt[P3](d._2.document, part3)))
  }

  def findDocument4PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag, P4: TypeTag](part1: Seq[String], part2: Seq[String], part3: Seq[String], part4: Seq[String], query: DocumentStoreQuery)(implicit session: DBSession = null): Seq[(Long, (P1, P2, P3, P4))] = {
    findDocument(query).map(d => d._1 -> (valueAt[P1](d._2.document, part1), valueAt[P2](d._2.document, part2), valueAt[P3](d._2.document, part3), valueAt[P4](d._2.document, part4)))
  }

  def findDocumentByObjectInArray[V](arrayPath: Seq[String], objectPath: Seq[String], value: V, limit: Int)(implicit session: DBSession = null): Map[Long, Document[T]] = {
    store.filter(keyValuePair => arrayMatchSeq(keyValuePair._2.asInstanceOf[Document[AnyRef]].document, arrayPath).exists(matches(_, objectPath, value)))
      .seq.toSeq.sortBy(_._1).take(limit).toMap // F273: at most `limit`, as PostgreSQL finds them
  }

  def findAll()(implicit session: DBSession = null): Map[Long, Document[T]] = {
    store.seq.toMap
  }

  def countAll()(implicit session: DBSession = null): Int = {
    store.size
  }

  protected def arrayMatchSeq(element: AnyRef, arrayPath: Seq[String]): Seq[AnyRef] = {
    val innerElement: AnyRef = element match {
      case None if arrayPath.head == "value" => return Seq()
      case Some(_) if arrayPath.head == "value" => element.asInstanceOf[Option[AnyRef]].get
      case _ =>
        val field = element.getClass.getDeclaredField(arrayPath.head)
        getPrivateValue(element, field)
    }
    val tail = arrayPath.tail
    if (tail.isEmpty) {
      innerElement match {
        case iter: Iterable[_] => iter.asInstanceOf[Iterable[AnyRef]].toSeq
        case _ => Seq()
      }
    } else {
      arrayMatchSeq(innerElement, tail)
    }
  }

  protected def matches[V](element: AnyRef, path: Seq[String], value: V): Boolean = {
    try {
      val innerElement: AnyRef = element match {
        case None if path.head == "value" => return false
        case Some(_) if path.head == "value" => element.asInstanceOf[Option[AnyRef]].get
        case _ =>
          val field = element.getClass.getDeclaredField(path.head)
          getPrivateValue(element, field)
      }
      val tail = path.tail
      if(tail.isEmpty) {
        innerElement.toString == value.toString
      } else {
        matches(innerElement, tail, value)
      }
    } catch {
      case e: NoSuchFieldException => throw new IllegalArgumentException(s"No field [${path.head}] found in type [${element.getClass}] for element $element")
    }
  }

  protected def valueAt[V](element: AnyRef, path: Seq[String]): V = {
    try {
      val field = element.getClass.getDeclaredField(path.head)
      val innerElement = getPrivateValue(element, field)
      val tail = path.tail
      if (tail.isEmpty) {
        innerElement.asInstanceOf[V]
      } else {
        valueAt(innerElement, tail)
      }
    } catch {
      case e: NoSuchFieldException => throw new IllegalArgumentException(s"No field [${path.head}] found in type [${element.getClass}] for element $element")
    }
  }

  protected def matchesMultiple(element: AnyRef, path: Seq[String], values: Set[String]): Boolean = {
    try {
      val innerElement: AnyRef = element match {
        case None if path.head == "value" => return false
        case Some(_) if path.head == "value" => element.asInstanceOf[Option[AnyRef]].get
        case _ =>
          val field = element.getClass.getDeclaredField(path.head)
          getPrivateValue(element, field)
      }
      val tail = path.tail
      if(tail.isEmpty) {
        values.contains(innerElement.toString)
      } else {
        matchesMultiple(innerElement, tail, values)
      }
    } catch {
      case e: NoSuchFieldException => throw new IllegalArgumentException(s"No field [${path.head}] found in type [${element.getClass}] for element $element")
    }
  }

  // Left accessible: switching it back raced with another thread reading the same field.
  def getPrivateValue(element: AnyRef, field: Field): AnyRef = {
    field.trySetAccessible()
    field.get(element)
  }

  def getDocument(key: Long)(implicit session: DBSession = null): Option[Document[T]] = store.get(key)

  def removeDocument(key: Long)(implicit session: DBSession = null): Unit = {
    store -= key
    spaces -= key
  }

  def getDocuments(keys: Iterable[Long])(implicit session: DBSession = null): Map[Long, Document[T]] = (store filterKeys keys.toSet).seq.toMap

  def overwriteDocument(key: Long, document: T)(implicit session: DBSession = null): Unit = {
    if (store.contains(key)) {
      store += key -> Document[T](document)
    } else {
      throw new IllegalStateException("Attempting update on non-existing document with key " + key)
    }
  }

  def updateDocument(spaceId: Long, key: Long, modify: Option[Document[T]] => Document[T])(implicit session: DBSession = null): Document[T] = {
    val existing = store.get(key)
    val modified: Document[T] = modify(existing)
    if (existing.isEmpty) stored(spaceId, key, modified) else store += key -> modified
    modified
  }

  def updateExistingDocument(key: Long, modify: Document[T] => Document[T])(implicit session: DBSession = null): Document[T] = {
    val modified: Document[T] = modify(store(key))
    store += key -> modified
    modified
  }

  def clearAllData()(implicit session: DBSession): Unit = {
    store.clear()
    spaces.clear()
  }

  def clearSpace(spaceId: Long)(implicit session: DBSession): Unit =
    spaces.filter(_._2 == spaceId).keys.seq.foreach(key => removeDocument(key))


}

class MemoryDocumentStore[T <: AnyRef](val maxRows: Int = DocumentStoreQuery.MaxRows) extends DocumentStore[T] with MemoryDocumentStoreTrait[T] {

  def insertDocument(spaceId: Long, key: Long, document: T)(implicit session: DBSession = null): Unit =
    if (store.contains(key)) {
      throw new IllegalStateException("Attempting to re-insert document with key " + key)
    } else {
      stored(spaceId, key, Document[T](document))
    }

  override def insertDocuments(spaceId: Long, documents: Seq[DocumentToInsert[T]])(implicit session: DBSession): Unit =
    documents.foreach(document => insertDocument(spaceId, document.key, document.document))
}

class MemoryDocumentStoreAutoId[T <: AnyRef](val maxRows: Int = DocumentStoreQuery.MaxRows) extends DocumentStoreAutoId[T] with MemoryDocumentStoreTrait[T] {

  val random = new scala.util.Random(System.nanoTime)

  private def generateNextId: Long = {
    var id: Long = 0
    do {
      id = random.nextLong()
    } while (store.contains(id))
    id
  }

  def insertDocument(spaceId: Long, document: T)(implicit session: DBSession = null): Long = {
    val key = generateNextId
    stored(spaceId, key, Document[T](document))
    key
  }

  override def insertDocuments(spaceId: Long, documents: Seq[T])(implicit session: DBSession): Unit =
    documents.foreach(document => insertDocument(spaceId, document))
}

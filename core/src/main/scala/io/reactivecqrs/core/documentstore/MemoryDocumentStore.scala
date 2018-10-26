package io.reactivecqrs.core.documentstore

import java.lang.reflect.Field

import scalikejdbc.DBSession

import scala.collection.parallel.mutable

import scala.reflect.runtime.universe._

sealed trait MemoryDocumentStoreTrait[T <: AnyRef, M <: AnyRef] {

  val store = mutable.ParHashMap[Long, Document[T,M]]()


  def findDocumentByPath(path: Seq[String], value: String)(implicit session: DBSession = null): Map[Long, Document[T,M]] = {
    store.filter(keyValuePair => matches(keyValuePair._2.asInstanceOf[Document[AnyRef, AnyRef]].document, path, value)).seq.toMap
  }

  def findDocumentByPaths(values: ExpectedValue*)(implicit session: DBSession = null): Map[Long, Document[T,M]] = {
    store.filter(keyValuePair => values.forall {
      case ExpectedSingleValue(path, value) => matches(keyValuePair._2.asInstanceOf[Document[AnyRef, AnyRef]].document, path, value)
      case ExpectedMultipleValues(path, vals) => vals.exists(value => matches(keyValuePair._2.asInstanceOf[Document[AnyRef, AnyRef]].document, path, value))
    }).seq.toMap
  }

  def findDocumentPartByPaths[P: TypeTag](part: List[String], values: ExpectedValue*)(implicit session: DBSession = null): Map[Long, P] = {
    findDocumentByPaths(values:_*).map(d => d._1 -> valueAt[P](d._2.document, part))
  }

  def findDocument2PartsByPaths[P1: TypeTag, P2: TypeTag](part1: List[String], part2: List[String], values: ExpectedValue*)(implicit session: DBSession = null): Map[Long, (P1, P2)] = {
    findDocumentByPaths(values:_*).map(d => d._1 -> (valueAt[P1](d._2.document, part1), valueAt[P2](d._2.document, part2)))
  }

  def findDocument3PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag](part1: List[String], part2: List[String], part3: List[String], values: ExpectedValue*)(implicit session: DBSession = null): Map[Long, (P1, P2, P3)] = {
    findDocumentByPaths(values:_*).map(d => d._1 -> (valueAt[P1](d._2.document, part1), valueAt[P2](d._2.document, part2), valueAt[P3](d._2.document, part3)))
  }

  def findDocument4PartsByPaths[P1: TypeTag, P2: TypeTag, P3: TypeTag, P4: TypeTag](part1: List[String], part2: List[String], part3: List[String], part4: List[String], values: ExpectedValue*)(implicit session: DBSession = null): Map[Long, (P1, P2, P3, P4)] = {
    findDocumentByPaths(values:_*).map(d => d._1 -> (valueAt[P1](d._2.document, part1), valueAt[P2](d._2.document, part2), valueAt[P3](d._2.document, part3), valueAt[P4](d._2.document, part4)))
  }

  def findDocumentsByPathWithOneOfTheValues(path: Seq[String], values: Set[String])(implicit session: DBSession = null): Map[Long, Document[T,M]] = {
    store.filter(keyValuePair => matchesMultiple(keyValuePair._2.asInstanceOf[Document[AnyRef, AnyRef]].document, path, values)).seq.toMap
  }

  def findDocumentByObjectInArray[V](arrayPath: Seq[String], objectPath: Seq[String], value: V)(implicit session: DBSession = null): Map[Long, Document[T, M]] = {
    store.filter(keyValuePair => arrayMatchSeq(keyValuePair._2.asInstanceOf[Document[AnyRef, AnyRef]].document, arrayPath).exists(matches(_, objectPath, value))).seq.toMap
  }

  def findDocumentByMetadataObjectInArray[V](arrayPath: Seq[String], objectPath: Seq[String], value: V)(implicit session: DBSession = null): Map[Long, Document[T, M]] = {
    store.filter(keyValuePair => arrayMatchSeq(keyValuePair._2.asInstanceOf[Document[AnyRef, AnyRef]].metadata, arrayPath).exists(matches(_, objectPath, value))).seq.toMap
  }

  def findAll()(implicit session: DBSession = null): Map[Long, Document[T,M]] = {
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

  def getPrivateValue(element: AnyRef, field: Field): AnyRef = {
    val accessible = field.isAccessible
    if(!accessible) {
      field.setAccessible(true)
    }
    val value = field.get(element)
    if(!accessible) {
      field.setAccessible(false)
    }
    value
  }

  def getDocument(key: Long)(implicit session: DBSession = null): Option[Document[T,M]] = store.get(key)

  def removeDocument(key: Long)(implicit session: DBSession = null): Unit = store -= key

  def getDocuments(keys: List[Long])(implicit session: DBSession = null): Map[Long, Document[T,M]] = (store filterKeys keys.toSet).seq.toMap

  def overwriteDocument(key: Long, document: T, metadata: M)(implicit session: DBSession = null): Unit = {
    if (store.contains(key)) {
      store += key -> Document[T, M](document, metadata)
    } else {
      throw new IllegalStateException("Attempting update on non-existing document with key " + key)
    }
  }

  def updateDocument(key: Long, modify: Option[Document[T, M]] => Document[T, M])(implicit session: DBSession = null): Document[T, M] = {
    val modified: Document[T, M] = modify(store.get(key))
    store += key -> modified
    modified
  }

  def clearAllData()(implicit session: DBSession): Unit = {
    store.clear()
  }


}

class MemoryDocumentStore[T <: AnyRef, M <: AnyRef] extends DocumentStore[T,M] with MemoryDocumentStoreTrait[T, M] {

  def insertDocument(key: Long, document: T, metadata: M)(implicit session: DBSession = null): Unit =
    if (store.contains(key)) {
      throw new IllegalStateException("Attempting to re-insert document with key " + key)
    } else {
      store += key -> Document[T, M](document, metadata)
    }
}

class MemoryDocumentStoreAutoId[T <: AnyRef, M <: AnyRef] extends DocumentStoreAutoId[T,M] with MemoryDocumentStoreTrait[T, M] {

  val random = new scala.util.Random(System.nanoTime)

  private def generateNextId: Long = {
    var id: Long = 0
    do {
      id = random.nextLong()
    } while (store.contains(id))
    id
  }

  def insertDocument(document: T, metadata: M)(implicit session: DBSession = null): Long = {
    val key = generateNextId
    store += key -> Document[T, M](document, metadata)
    key
  }
}

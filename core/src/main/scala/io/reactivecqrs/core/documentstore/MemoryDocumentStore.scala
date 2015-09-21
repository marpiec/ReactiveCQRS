package io.reactivecqrs.core.documentstore

import java.lang.reflect.Field

import scala.collection.parallel.mutable
import scala.reflect.runtime.universe._

sealed trait MemoryDocumentStoreTrait[T <: AnyRef, M <: AnyRef] {

  val store = mutable.ParHashMap[Long, DocumentWithMetadata[T,M]]()


  def findDocumentByPath(path: Seq[String], value: String): Map[Long, DocumentWithMetadata[T,M]] = {
    store.filter(keyValuePair => matches(keyValuePair._2.asInstanceOf[DocumentWithMetadata[AnyRef, AnyRef]].document, path, value)).seq.toMap
  }

  def findDocumentsByPathWithOneOfTheValues(path: Seq[String], values: Set[String]): Map[Long, DocumentWithMetadata[T,M]] = {
    store.filter(keyValuePair => matchesMultiple(keyValuePair._2.asInstanceOf[DocumentWithMetadata[AnyRef, AnyRef]].document, path, values)).seq.toMap
  }


  def findDocumentByPathWithOneArray[V](array: String, objectPath: Seq[String], value: V): Map[Long, DocumentWithMetadata[T, M]] = {
    store.filter(keyValuePair => arrayMatch(keyValuePair._2.asInstanceOf[DocumentWithMetadata[AnyRef, AnyRef]].document, array).exists(matches(_, objectPath, value))).seq.toMap
  }

  def findDocumentByPathWithOneArrayAnywhere[V](arrayPath: Seq[String], objectPath: Seq[String], value: V): Map[Long, DocumentWithMetadata[T, M]] = {
    store.filter(keyValuePair => arrayMatchSeq(keyValuePair._2.asInstanceOf[DocumentWithMetadata[AnyRef, AnyRef]].document, arrayPath).exists(matches(_, objectPath, value))).seq.toMap
  }

  def findDocumentByMetadataPathWithOneArray[V](array: String, objectPath: Seq[String], value: V): Map[Long, DocumentWithMetadata[T, M]] = {
    store.filter(keyValuePair => arrayMatch(keyValuePair._2.asInstanceOf[DocumentWithMetadata[AnyRef, AnyRef]].metadata, array).exists(matches(_, objectPath, value))).seq.toMap
  }


  def findAll(): Map[Long, DocumentWithMetadata[T,M]] = {
    store.seq.toMap
  }

  private def arrayMatch(element: AnyRef, array: String): Seq[AnyRef] = {
    val field = element.getClass.getDeclaredField(array)
    val innerElement: AnyRef = getPrivateValue(element, field)

    innerElement match {
      case seq: Seq[_] => seq.asInstanceOf[Seq[AnyRef]]
      case _ => Seq()
    }
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
        case seq: Seq[_] => seq.asInstanceOf[Seq[AnyRef]]
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

  def getDocument(key: Long): Option[DocumentWithMetadata[T,M]] = store.get(key)

  def removeDocument(key: Long): Unit = store -= key

  def getDocuments(keys: List[Long]): Map[Long, DocumentWithMetadata[T,M]] = (store filterKeys keys.toSet).seq.toMap

  def updateDocument(key: Long, document: T, metadata: M): Unit = {
    if (store.contains(key)) {
      store += key -> DocumentWithMetadata[T, M](document, metadata)
    } else {
      throw new IllegalStateException("Attempting update on non-existing document with key " + key)
    }
  }


}

class MemoryDocumentStore[T <: AnyRef, M <: AnyRef] extends DocumentStore[T,M] with MemoryDocumentStoreTrait[T, M] {

  def insertDocument(key: Long, document: T, metadata: M): Unit =
    if (store.contains(key)) {
      throw new IllegalStateException("Attempting to re-insert document with key " + key)
    } else {
      store += key -> DocumentWithMetadata[T, M](document, metadata)
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

  def insertDocument(document: T, metadata: M): Unit = {
    val key = generateNextId
    store += key -> DocumentWithMetadata[T, M](document, metadata)
  }
}

package io.reactivecqrs.core.documentstore

import java.lang.reflect.Field

import scala.collection.parallel.mutable
import scala.reflect.runtime.universe._

class MemoryDocumentStore[T <: AnyRef : TypeTag, M <: AnyRef : TypeTag] extends DocumentStore[T,M] {

  val store = mutable.ParHashMap[Long, DocumentWithMetadata[T,M]]()


  override def findDocumentByPath(path: Seq[String], value: String): Map[Long, DocumentWithMetadata[T,M]] = {
    store.filter(keyValuePair => matches(keyValuePair._2.asInstanceOf[AnyRef], path, value)).seq.toMap
  }

  override def findDocumentsByPathWithOneOfTheValues(path: Seq[String], values: Set[String]): Map[Long, DocumentWithMetadata[T,M]] = {
    store.filter(keyValuePair => matchesMultiple(keyValuePair._2.asInstanceOf[AnyRef], path, values)).seq.toMap
  }


  override def findDocumentByPathWithOneArray[V](array: String, objectPath: Seq[String], value: V): Map[Long, DocumentWithMetadata[T,M]] =
    store.filter(keyValuePair => arrayMatch(keyValuePair._2.asInstanceOf[AnyRef], array).exists(matches(_, objectPath, value))).seq.toMap


  override def findAll(): Map[Long, DocumentWithMetadata[T,M]] = {
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

  private def matches[V](element: AnyRef, path: Seq[String], value: V): Boolean = {
    val field = element.getClass.getDeclaredField(path.head)
    val innerElement: AnyRef = getPrivateValue(element, field)
    val tail = path.tail
    if(tail.isEmpty) {
      innerElement.toString == value.toString
    } else {
      matches(innerElement, tail, value)
    }
  }

  private def matchesMultiple(element: AnyRef, path: Seq[String], values: Set[String]): Boolean = {
    val field = element.getClass.getDeclaredField(path.head)
    val innerElement: AnyRef = getPrivateValue(element, field)
    val tail = path.tail
    if(tail.isEmpty) {
      values.contains(innerElement.toString)
    } else {
      matchesMultiple(innerElement, tail, values)
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

  override def getDocument(key: Long): Option[DocumentWithMetadata[T,M]] = store.get(key)

  override def removeDocument(key: Long): Unit = store -= key

  override def getDocuments(keys: List[Long]): Map[Long, DocumentWithMetadata[T,M]] = (store filterKeys keys.toSet).seq.toMap

  override def insertDocument(key: Long, document: T, metadata: M): Unit =
    if (store.contains(key)) {
      throw new IllegalStateException("Attempting to re-insert document with key " + key)
    } else {
      store += key -> DocumentWithMetadata[T, M](document, metadata)
    }

  override def updateDocument(key: Long, document: T, metadata: M): Unit = {
    if (store.contains(key)) {
      store += key -> DocumentWithMetadata[T, M](document, metadata)
    } else {
      throw new IllegalStateException("Attempting update on non-existing document with key " + key)
    }
  }


}

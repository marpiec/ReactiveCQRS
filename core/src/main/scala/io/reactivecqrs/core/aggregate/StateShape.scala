package io.reactivecqrs.core.aggregate

import java.lang.reflect.{Field, Modifier, ParameterizedType, Type, WildcardType}
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

/**
  * F196: the shape of a state class, as a snapshot is stored with it: a digest of the names and types of its fields and
  * of the fields of the classes they hold (type arguments included: `Vector[Item]` reaches `Item`), by Java reflection.
  * A field added, removed, renamed or retyped anywhere in the state changes it, so a snapshot of the old shape is not
  * used (mpjsons reads a missing field as zero, false or null, silently), and the class's name is part of it, so a
  * snapshot of another type is not either (F197). Classes of the JDK and Scala's library count by name only.
  */
object StateShape {

  private val shapes = new ConcurrentHashMap[String, String]()

  /** The shape of the class named, or "" when it cannot be loaded. */
  def of(className: String): String = if (className.isEmpty) "" else shapes.computeIfAbsent(className, name =>
    scala.util.Try(Class.forName(name, false, getClass.getClassLoader)).map(shapeOf).getOrElse(""))

  private def shapeOf(root: Class[_]): String =
    MessageDigest.getInstance("SHA-256").digest(describe(root).getBytes("UTF-8")).take(16).map(b => "%02x".format(b & 0xff)).mkString

  /** The text the shape is a digest of: each class reached, with its fields' names and types. */
  private[aggregate] def describe(root: Class[_]): String = {
    val text = new StringBuilder
    val seen = mutable.Set[Class[_]]()
    def library(c: Class[_]): Boolean = c.isPrimitive || c.isArray && library(c.getComponentType) ||
      Seq("java.", "javax.", "scala.", "jdk.", "sun.").exists(c.getName.startsWith)
    def visitType(t: Type): Unit = t match {
      case c: Class[_] => visitClass(c)
      case p: ParameterizedType =>
        visitType(p.getRawType)
        p.getActualTypeArguments.foreach(visitType)
      case w: WildcardType => (w.getUpperBounds ++ w.getLowerBounds).foreach(visitType)
      case _ => ()
    }
    def visitClass(c: Class[_]): Unit = if (!library(c) && seen.add(c)) {
      text.append(c.getName).append('{')
      val fields = fieldsOf(c)
      fields.foreach(f => text.append(f.getName).append(':').append(f.getGenericType.getTypeName).append(';'))
      text.append('}')
      fields.foreach(f => visitType(f.getGenericType))
    }
    text.append(root.getName).append('|')
    visitClass(root)
    text.toString
  }

  // The stored fields of a class and its superclasses, in a stable order.
  private def fieldsOf(c: Class[_]): Seq[Field] =
    Iterator.iterate[Class[_]](c)(_.getSuperclass).takeWhile(k => k != null && k != classOf[Object]).toSeq.reverse
      .flatMap(_.getDeclaredFields.toSeq.filterNot(f => Modifier.isStatic(f.getModifiers) || Modifier.isTransient(f.getModifiers)).sortBy(_.getName))
}

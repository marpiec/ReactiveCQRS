package io.reactivecqrs.core.cluster

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, OutputStream}
import java.lang.reflect.Modifier
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import io.mpjsons.{MPJsons, NullPolicy}
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.IdempotencyKeyTakenException
import io.reactivecqrs.core.metrics.ReactiveMetrics
import org.apache.pekko.actor.{ActorRef, ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Status}
import org.apache.pekko.event.Logging
import org.apache.pekko.serialization.{Serialization, SerializerWithStringManifest}

import scala.util.{Failure, Success, Try}
import io.reactivecqrs.core.util.Errors

/** An exception from another node whose class is not one of the library's: its class name, and its message as it was. */
class RemoteFailure(val className: String, message: String) extends RuntimeException(message) {
  override def fillInStackTrace(): Throwable = this
  override def toString: String = "RemoteFailure(" + className + "): " + getMessage
}

/**
  * The JSON the cluster serializer writes the application's values with (D45): an `MPJsons` that reads tolerantly
  * (D68: unknown fields are skipped, a missing one reads as zero, null or `None`). A host that registers converters for
  * its types sets one configured the same way before its nodes exchange messages; stored events keep their own,
  * strict reader.
  */
class ClusterSerialization extends Extension {
  @volatile var mpjsons: MPJsons = new MPJsons(true, true, NullPolicy.Omit)
}

/** `ClusterSerialization(system)`: the mpjsons instance of messages between nodes. */
object ClusterSerialization extends ExtensionId[ClusterSerialization] with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): ClusterSerialization = new ClusterSerialization
  override def lookup: ClusterSerialization.type = ClusterSerialization
  override def get(system: ActorSystem): ClusterSerialization = super.get(system)
}

/**
  * D45: the one serializer of the messages between nodes, bound to `java.io.Serializable` (`reference.conf`), so case
  * classes and Scala collections that Pekko has no more specific serializer for come here; Java serialization stays
  * off. A message is written as one value, which says what it is:
  *
  *  - scalars, `Instant`, `UUID`, `BigDecimal`, an `ActorRef` (its serialized path, with its node's address);
  *  - `Option`, `Try`, lists, vectors, sets, maps and tuples, each element written the same way;
  *  - a sequence of events (`EventInfo`), as deliveries and re-publications carry them, in a compact form: the event
  *    classes once, then each event's version, user, time, class and JSON, about 30 bytes besides the JSON (F170);
  *  - the library's case classes field by field, with the fields' names, rebuilt with their constructor, so a field
  *    typed `Any`, a type parameter or an `ActorRef` keeps its value (mpjsons types by declaration, F64) and a node
  *    reads what a node of another version wrote (D68); case objects by name;
  *  - the library's exceptions with their fields, also inside a `Status.Failure` (they have bindings of their own,
  *    as Pekko binds exceptions to Java serialization); any other exception as a `RemoteFailure`;
  *  - anything else (the application's commands, events, states, queries and answers) as JSON of its runtime class,
  *    through mpjsons, as stored events are.
  *
  * Messages that carry functions cannot cross nodes and are kept inside one (a projection's supervisor and its
  * instances are one singleton).
  *
  * F121: a message larger than `MessageSize.limit`, which Artery would drop unseen, is written as a marker that the
  * receiving node reads as `Status.Failure(MessageTooLargeException)`: an asker fails at once instead of timing out, and
  * this node logs what it was.
  */
class ClusterSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {

  override val identifier: Int = 76143011

  private lazy val limit = MessageSize(system).limit
  private val log = Logging(system, classOf[ClusterSerializer])
  private val lastLogged = new ConcurrentHashMap[String, java.lang.Long]()

  override def manifest(o: AnyRef): String = o.getClass.getName

  override def toBinary(o: AnyRef): Array[Byte] = {
    val bytes = written(o)
    if (bytes.length <= limit) bytes else tooLarge(o.getClass.getName, bytes.length)
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    new ClusterSerializer.Reader(new DataInputStream(new ByteArrayInputStream(bytes)), system, ClusterSerialization(system).mpjsons).read().asInstanceOf[AnyRef]

  /** How many bytes `o` takes, written without keeping them (F121). */
  def sizeOf(o: AnyRef): Int = {
    val out = new DataOutputStream(OutputStream.nullOutputStream())
    new ClusterSerializer.Writer(out, ClusterSerialization(system).mpjsons).write(o)
    out.size()
  }

  private def written(o: AnyRef): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    new ClusterSerializer.Writer(out, ClusterSerialization(system).mpjsons).write(o)
    out.flush()
    bytes.toByteArray
  }

  // Logged at most once per class every 10 seconds: one aggregate's large state goes out with each of its events.
  private def tooLarge(className: String, size: Int): Array[Byte] = {
    ReactiveMetrics(system).metrics.count("reactivecqrs.message.too.large", Map("class" -> className), 1L)
    val now = System.nanoTime()
    val last = lastLogged.get(className)
    if (last == null || now - last > 10000000000L) {
      lastLogged.put(className, now)
      log.error("A {} of {} bytes is too large to be sent between nodes; the limit is {} bytes (from pekko.remote.artery.advanced." +
        "maximum-frame-size). Its recipient receives a MessageTooLargeException instead. Others of the class in the next 10 s are not logged.",
        className, size, limit)
    }
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    new ClusterSerializer.Writer(out, ClusterSerialization(system).mpjsons).tooLarge(className, size, limit)
    out.flush()
    bytes.toByteArray
  }
}

/** The kinds of values the serializer writes, and how. */
object ClusterSerializer {

  // The value's kind, written before it.
  private final val Null = 0
  private final val Str = 1
  private final val IntV = 2
  private final val LongV = 3
  private final val BoolV = 4
  private final val DoubleV = 5
  private final val FloatV = 6
  private final val ShortV = 7
  private final val ByteV = 8
  private final val CharV = 9
  private final val InstantV = 10
  private final val UuidV = 11
  private final val BigDec = 12
  private final val Ref = 13
  private final val NoneV = 14
  private final val SomeV = 15
  private final val SuccessV = 16
  private final val FailureV = 17
  private final val ListV = 18
  private final val VectorV = 19
  private final val SetV = 20
  private final val MapV = 21
  private final val Module = 22
  private final val ProductV = 23
  private final val Json = 24
  private final val LibraryError = 25
  private final val OtherError = 26
  private final val TupleV = 27
  private final val TooLarge = 28 // F121: in place of a message larger than the limit
  private final val EventsV = 29 // F170: a sequence of events, compact; 1.0's first nodes read it

  /** The library's own classes, written field by field; the application's go through mpjsons. */
  private[cluster] def isLibrary(className: String): Boolean =
    className.startsWith("io.reactivecqrs.api.") || className.startsWith("io.reactivecqrs.core.")

  private val modules = new ClassValue[java.lang.Boolean] {
    override protected def computeValue(clazz: Class[_]): java.lang.Boolean =
      clazz.getName.endsWith("$") && clazz.getFields.exists(_.getName == "MODULE$")
  }

  private def isModule(value: Any): Boolean = modules.get(value.getClass)

  /**
    * D68: how a library case class is rebuilt from the fields that a node of another version wrote. By name: a field the
    * class no longer has is skipped, one it gained takes its Scala default, else `None`, zero or null. A field whose
    * type changed fails the message; such a change needs a new class.
    */
  private final class Shape(clazz: Class[_]) {
    private val constructor = clazz.getConstructors.maxBy(_.getParameterCount)
    private val types = constructor.getParameterTypes
    // A case class keeps its parameters as its first fields, in their order. One defined inside a class (an actor's own
    // message) takes that class's instance too, so it cannot leave its node.
    private val names = {
      val fields = clazz.getDeclaredFields.filterNot(f => Modifier.isStatic(f.getModifiers)).take(types.length)
      if (fields.map(_.getType).toSeq != types.toSeq) throw new IllegalArgumentException(clazz.getName + " cannot go to another " +
        "node: its constructor does not take just its fields, as one of a case class defined inside a class takes that class's instance")
      fields.map(_.getName)
    }
    private val defaults: Array[() => AnyRef] = types.indices.map(default).toArray

    def apply(written: Map[String, Any]): AnyRef = {
      val values = names.indices.map(i => written.get(names(i)) match {
        case Some(value) => value.asInstanceOf[AnyRef]
        case None => defaults(i)()
      })
      constructor.newInstance(values: _*).asInstanceOf[AnyRef]
    }

    private def default(i: Int): () => AnyRef = {
      val companion = Try(clazz.getClassLoader.loadClass(clazz.getName + "$").getField("MODULE$").get(null)).toOption
      companion.flatMap(c => c.getClass.getMethods.find(_.getName == "$lessinit$greater$default$" + (i + 1)).map(m => () => m.invoke(c))).getOrElse {
        val zero: AnyRef =
          if (types(i).isPrimitive) java.lang.reflect.Array.get(java.lang.reflect.Array.newInstance(types(i), 1), 0)
          else if (types(i) == classOf[Option[_]]) None
          else null
        () => zero
      }
    }
  }

  private val shapes = new ClassValue[Shape] {
    override protected def computeValue(clazz: Class[_]): Shape = new Shape(clazz)
  }

  private[cluster] final class Writer(out: DataOutputStream, json: MPJsons) {

    def write(value: Any): Unit = value match {
      case null => out.writeByte(Null)
      case s: String => out.writeByte(Str); str(s)
      case i: Int => out.writeByte(IntV); out.writeInt(i)
      case l: Long => out.writeByte(LongV); out.writeLong(l)
      case b: Boolean => out.writeByte(BoolV); out.writeBoolean(b)
      case d: Double => out.writeByte(DoubleV); out.writeDouble(d)
      case f: Float => out.writeByte(FloatV); out.writeFloat(f)
      case s: Short => out.writeByte(ShortV); out.writeShort(s)
      case b: Byte => out.writeByte(ByteV); out.writeByte(b)
      case c: Char => out.writeByte(CharV); out.writeChar(c)
      case t: Instant => out.writeByte(InstantV); out.writeLong(t.getEpochSecond); out.writeInt(t.getNano)
      case u: UUID => out.writeByte(UuidV); out.writeLong(u.getMostSignificantBits); out.writeLong(u.getLeastSignificantBits)
      case b: BigDecimal => out.writeByte(BigDec); str(b.toString)
      case r: ActorRef => out.writeByte(Ref); str(Serialization.serializedActorPath(r))
      case None => out.writeByte(NoneV)
      case Some(x) => out.writeByte(SomeV); write(x)
      case Success(x) => out.writeByte(SuccessV); write(x)
      case Failure(e) => out.writeByte(FailureV); write(e)
      case events: Seq[_] if events.nonEmpty && events.forall(_.isInstanceOf[EventInfo[_]]) => this.events(events.asInstanceOf[Seq[EventInfo[Any]]])
      case l: List[_] => out.writeByte(ListV); elements(l)
      case s: Set[_] => out.writeByte(SetV); elements(s)
      case m: Map[_, _] =>
        out.writeByte(MapV)
        out.writeInt(m.size)
        m.foreach { case (k, v) => write(k); write(v) }
      case s: Seq[_] => out.writeByte(VectorV); elements(s) // any other sequence reads back as a vector
      case e: Throwable => error(e)
      case p: Product if isModule(p) => out.writeByte(Module); str(p.getClass.getName)
      case t: Product if t.getClass.getName.startsWith("scala.Tuple") => out.writeByte(TupleV); elements(t.productIterator.toSeq)
      case p: Product if isLibrary(p.getClass.getName) => product(p.getClass.getName, p)
      case other =>
        val className = other.getClass.getName
        out.writeByte(Json)
        str(className)
        str(json.serialize[Any](other, className))
    }

    /** A library case class, field by field with the fields' names (D68); `className` differs from its own only in tests.
      * One that no node could rebuild fails here, on the sender. */
    private[cluster] def product(className: String, p: Product): Unit = {
      shapes.get(p.getClass)
      out.writeByte(ProductV)
      str(className)
      out.writeInt(p.productArity)
      p.productElementNames.zip(p.productIterator).foreach { case (name, value) => str(name); write(value) }
    }

    // F170: field by field, each event took about 275 bytes besides its JSON: its class and five field names, and the
    // version and user as named products. Read back as a vector, as other sequences.
    private def events(events: Seq[EventInfo[Any]]): Unit = {
      out.writeByte(EventsV)
      val classes = events.map(_.event.getClass.getName).distinct
      out.writeInt(classes.size)
      classes.foreach(str)
      val index = classes.zipWithIndex.toMap
      out.writeInt(events.size)
      events.foreach { e =>
        val className = e.event.getClass.getName
        out.writeInt(e.version.asInt)
        out.writeLong(e.userId.asLong)
        out.writeLong(e.timestamp.getEpochSecond)
        out.writeInt(e.timestamp.getNano)
        out.writeInt(index(className))
        str(json.serialize[Any](e.event, className))
        write(e.storedMetadata)
      }
    }

    /** The marker written in place of a message larger than `limit` (F121). */
    def tooLarge(className: String, size: Int, limit: Int): Unit = {
      out.writeByte(TooLarge)
      str(className)
      out.writeInt(size)
      out.writeInt(limit)
    }

    private def elements(values: Iterable[_]): Unit = {
      out.writeInt(values.size)
      values.foreach(write)
    }

    // The library's exceptions keep their class, which callers match on; the fields that rebuild each one follow.
    private def error(e: Throwable): Unit = e match {
      case r: AggregateRestoreFailedException => library(r, r.aggregateId, r.aggregateType, r.getCause)
      case v: AggregateInIncorrectVersionException => library(v, v.aggregateId, v.aggregateType, v.currentVersion, v.requestedVersion)
      case s: AggregateSquashedException => library(s, s.aggregateId, s.squashedUpTo, s.requested, s.at)
      case n: NoEventsForAggregateException => library(n, n.aggregateId, n.aggregateType)
      case l: EventTooLargeException => library(l, l.aggregateId, l.aggregateTypeName, l.currentVersion, l.eventTypeName, l.eventSize, l.maxSize)
      case t: TooManyEventsException => library(t, t.aggregateId, t.aggregateTypeName, t.maxCount)
      case m: MessageTooLargeException => library(m, m.className, m.size, m.limit)
      case k: IdempotencyKeyTakenException => library(k, k.key)
      case r: RemoteFailure => out.writeByte(OtherError); str(r.className); write(r.getMessage)
      case other => out.writeByte(OtherError); str(other.getClass.getName); write(Errors.message(other))
    }

    private def library(e: Throwable, fields: Any*): Unit = {
      out.writeByte(LibraryError)
      str(e.getClass.getName)
      out.writeInt(fields.size)
      fields.foreach(write)
    }

    private def str(s: String): Unit = {
      val bytes = s.getBytes(UTF_8)
      out.writeInt(bytes.length)
      out.write(bytes)
    }
  }

  private[cluster] final class Reader(in: DataInputStream, system: ExtendedActorSystem, json: MPJsons) {

    def read(): Any = in.readByte() match {
      case Null => null
      case Str => str()
      case IntV => in.readInt()
      case LongV => in.readLong()
      case BoolV => in.readBoolean()
      case DoubleV => in.readDouble()
      case FloatV => in.readFloat()
      case ShortV => in.readShort()
      case ByteV => in.readByte()
      case CharV => in.readChar()
      case InstantV => Instant.ofEpochSecond(in.readLong(), in.readInt())
      case UuidV => new UUID(in.readLong(), in.readLong())
      case BigDec => BigDecimal(str())
      case Ref => system.provider.resolveActorRef(str())
      case NoneV => None
      case SomeV => Some(read())
      case SuccessV => Success(read())
      case FailureV => Failure(read().asInstanceOf[Throwable])
      case ListV => elements().toList
      case VectorV => elements()
      case SetV => elements().toSet
      case MapV => (1 to in.readInt()).map(_ => read() -> read()).toMap
      case Module => trusted(str(), library = false).getField("MODULE$").get(null)
      case TupleV =>
        val values = elements()
        loaded("scala.Tuple" + values.size).getConstructors.head.newInstance(values.map(_.asInstanceOf[AnyRef]): _*)
      case ProductV =>
        val shape = shapes.get(trusted(str(), library = true))
        shape((1 to in.readInt()).map(_ => str() -> read()).toMap)
      case Json =>
        val className = trusted(str(), library = false).getName
        json.deserialize[Any](str(), className)
      case LibraryError => libraryError(str(), (1 to in.readInt()).map(_ => read()))
      case OtherError => new RemoteFailure(str(), read().asInstanceOf[String])
      case TooLarge =>
        val className = str()
        val size = in.readInt()
        Status.Failure(new MessageTooLargeException(className, size, in.readInt()))
      case EventsV => events()
      case unknown => throw new IllegalArgumentException("Unknown value kind " + unknown + " in a cluster message")
    }

    private def libraryError(className: String, f: Seq[Any]): Throwable = {
      def id = f.head.asInstanceOf[AggregateId]
      className match {
        case "io.reactivecqrs.api.AggregateRestoreFailedException" =>
          new AggregateRestoreFailedException(id, f(1).asInstanceOf[AggregateType], f(2).asInstanceOf[Throwable])
        case "io.reactivecqrs.api.AggregateInIncorrectVersionException" =>
          new AggregateInIncorrectVersionException(id, f(1).asInstanceOf[AggregateType], f(2).asInstanceOf[AggregateVersion], f(3).asInstanceOf[AggregateVersion])
        case "io.reactivecqrs.api.AggregateSquashedException" =>
          new AggregateSquashedException(id, f(1).asInstanceOf[AggregateVersion], f(2).asInstanceOf[AggregateVersion], f(3).asInstanceOf[Option[Instant]])
        case "io.reactivecqrs.api.NoEventsForAggregateException" => new NoEventsForAggregateException(id, f(1).asInstanceOf[AggregateType])
        case "io.reactivecqrs.api.EventTooLargeException" =>
          new EventTooLargeException(id, f(1).asInstanceOf[String], f(2).asInstanceOf[AggregateVersion], f(3).asInstanceOf[String], f(4).asInstanceOf[Int], f(5).asInstanceOf[Int])
        case "io.reactivecqrs.api.TooManyEventsException" => new TooManyEventsException(id, f(1).asInstanceOf[String], f(2).asInstanceOf[Int])
        case "io.reactivecqrs.api.MessageTooLargeException" =>
          new MessageTooLargeException(f.head.asInstanceOf[String], f(1).asInstanceOf[Int], f(2).asInstanceOf[Int])
        case "io.reactivecqrs.core.commandhandler.IdempotencyKeyTakenException" => new IdempotencyKeyTakenException(f.head.asInstanceOf[String])
        case other => new RemoteFailure(other, f.mkString(", "))
      }
    }

    private def elements(): Vector[Any] = (1 to in.readInt()).map(_ => read()).toVector

    private def events(): Vector[EventInfo[Any]] = {
      val classes = Vector.fill(in.readInt())(str())
      Vector.fill(in.readInt()) {
        val version = AggregateVersion(in.readInt())
        val userId = UserId(in.readLong())
        val timestamp = Instant.ofEpochSecond(in.readLong(), in.readInt())
        val className = trusted(classes(in.readInt()), library = false).getName
        val event = json.deserialize[Any](str(), className).asInstanceOf[Event[Any]]
        EventInfo[Any](version, event, userId, timestamp, read().asInstanceOf[Option[EventMetadata]])
      }
    }

    private def loaded(className: String): Class[_] = system.dynamicAccess.getClassFor[AnyRef](className).get

    // F240: a class named in the bytes is loaded, not initialized, and used only when it is the library's (written field
    // by field), or as JSON a Scala case class or object, a collection, a string, number, boolean or character (an
    // application's message, event or state): no other class is made or initialized.
    private def trusted(className: String, library: Boolean): Class[_] = {
      val clazz = loaded(className)
      def plainValue = classOf[Product].isAssignableFrom(clazz) || clazz == classOf[String] || classOf[Number].isAssignableFrom(clazz) ||
        clazz == classOf[java.lang.Boolean] || clazz == classOf[Character] || className.startsWith("scala.collection.")
      if (!isLibrary(className) && (library || !plainValue))
        throw new IllegalArgumentException("A cluster message names " + className + ", which it may not carry")
      clazz
    }

    // F240: a length is never past the message's end, so a corrupt or crafted one allocates nothing.
    private def str(): String = {
      val length = in.readInt()
      if (length < 0 || length > in.available()) throw new IllegalArgumentException("A length of " + length + " in a cluster message, past its end")
      val bytes = new Array[Byte](length)
      in.readFully(bytes)
      new String(bytes, UTF_8)
    }
  }
}

package io.reactivecqrs.core.cluster

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import com.typesafe.config.ConfigFactory
import io.reactivecqrs.api.MessageTooLargeException
import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem, Status}
import org.apache.pekko.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

import scala.util.Failure

// One library message in two versions, and one whose field changed type.
case class OldShape(id: Int, name: String, dropped: List[String])
case class NewShape(id: Int, name: String, added: Option[Int], counted: Long, withDefault: String = "given")
case class ChangedShape(id: String, name: String)
// 1.0.1-RC1's shapes of the messages 1.0.1-RC2 added a field to (CR-2).
case class BuildStatusOfRc1(phase: String, position: Long, eventsSent: Long, gapsFilled: Long, handedOver: Long, unreadable: Long)
// 1.0.2's shape of the build status, which 1.0.3 added the timings to (CR-8).
case class BuildStatusOf102(phase: String, position: Long, eventsSent: Long, gapsFilled: Long, handedOver: Long, unreadable: Long,
                            heldBy: Option[String], reportedAt: Option[java.time.Instant], statesRestored: Long)
// An actor's own messages are defined inside its class, so they hold its instance.
class Holder { case class Inner(id: Int) }

// D68: a node reads the library's messages that a node of another version wrote.
class ClusterSerializerSpec extends AnyFeatureSpec with Matchers with BeforeAndAfterAll {

  private val system = ActorSystem("serializer").asInstanceOf[ExtendedActorSystem]
  // A frame of 64 KiB, so the limit is 56 KiB (F121).
  private val smallFrame = ActorSystem("small-frame", ConfigFactory.parseString("pekko.remote.artery.advanced.maximum-frame-size = 64 KiB")
    .withFallback(ConfigFactory.load())).asInstanceOf[ExtendedActorSystem]

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    TestKit.shutdownActorSystem(smallFrame)
  }

  /** Writes `value` as the node that has it as class `as` would have, and reads it back. */
  private def asWrittenFor(value: Product, as: Class[_]): Any = {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    new ClusterSerializer.Writer(out, ClusterSerialization(system).mpjsons).product(as.getName, value)
    out.flush()
    new ClusterSerializer.Reader(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray)), system, ClusterSerialization(system).mpjsons).read()
  }

  Feature("Library messages between versions (D68)") {

    Scenario("a newer node skips the fields it no longer has and gives the new ones their defaults") {
      asWrittenFor(OldShape(1, "a", List("x")), classOf[NewShape]) mustBe NewShape(1, "a", None, 0L, "given")
    }

    Scenario("an older node skips the fields it does not know") {
      asWrittenFor(NewShape(1, "a", Some(2), 3L), classOf[OldShape]) mustBe OldShape(1, "a", null)
    }

    Scenario("1.0.1-RC2's messages: a status of RC1 reads without its hold, and the new answers travel") {
      import io.reactivecqrs.api.AggregateVersion
      import io.reactivecqrs.api.id.AggregateId
      import io.reactivecqrs.core.rebuild.ProjectionBuilder.BuildStatus
      asWrittenFor(BuildStatusOfRc1("Bulk", 5, 6, 7, 8, 9), classOf[BuildStatus]) mustBe BuildStatus("Bulk", 5, 6, 7, 8, 9, None)
      val serializer = new ClusterSerializer(system)
      Seq(BuildStatus("Waiting", 1, 0, 0, 0, 0, Some("the event keys are not complete")), io.reactivecqrs.core.delivery.CannotRestore(AggregateId(7), "a cause"),
        io.reactivecqrs.core.projection.SkipAggregate(AggregateId(7)), io.reactivecqrs.core.projection.AggregateSkipped(AggregateId(7), AggregateVersion(3), 1)
      ).foreach(message => serializer.fromBinary(serializer.toBinary(message), serializer.manifest(message)) mustBe message)
    }

    Scenario("1.0.3's build status: one of 1.0.2 reads without its timings, and the timings travel (CR-8)") {
      import io.reactivecqrs.core.rebuild.ProjectionBuilder.BuildStatus
      val at = Some(java.time.Instant.parse("2026-10-23T10:00:00Z"))
      asWrittenFor(BuildStatusOf102("Bulk", 5, 6, 7, 8, 9, None, at, 3), classOf[BuildStatus]) mustBe BuildStatus("Bulk", 5, 6, 7, 8, 9, None, at, 3)
      val timed = BuildStatus("Bulk", 5, 6, 7, 8, 9, None, at, 3, readingMillis = 10, replayingMillis = 20, waitingMillis = 30)
      val serializer = new ClusterSerializer(system)
      serializer.fromBinary(serializer.toBinary(timed), serializer.manifest(timed)) mustBe timed
    }

    Scenario("a field whose type changed fails the message") {
      an[IllegalArgumentException] must be thrownBy asWrittenFor(OldShape(1, "a", Nil), classOf[ChangedShape])
    }
  }

  Feature("The bytes on the wire (D94)") {

    Scenario("an application's value goes as JSON in the same bytes as before: its class, then its JSON's UTF-8 bytes") {
      val value = example.NoteValue(7, "zażółć gęślą jaźń")
      val mpjsons = ClusterSerialization(system).mpjsons
      val written = new ByteArrayOutputStream()
      val out = new DataOutputStream(written)
      new ClusterSerializer.Writer(out, mpjsons).write(value)
      out.flush()

      // 1.0.3's writer: the JSON built as a string, then encoded.
      val before = new ByteArrayOutputStream()
      val old = new DataOutputStream(before)
      def str(s: String): Unit = { val b = s.getBytes("UTF-8"); old.writeInt(b.length); old.write(b) }
      old.writeByte(24) // as JSON
      str(value.getClass.getName)
      str(mpjsons.serialize[Any](value, value.getClass.getName))
      old.flush()

      written.toByteArray mustBe before.toByteArray
    }
  }

  Feature("The limits of messages between nodes") {

    Scenario("F249 with large-message destinations, an ordinary destination keeps the ordinary frame's limit") {
      val large = ActorSystem("large-frames", ConfigFactory.parseString(
        """pekko.remote.artery.advanced.maximum-frame-size = 64 KiB
          |pekko.remote.artery.advanced.maximum-large-frame-size = 2 MiB
          |pekko.remote.artery.large-message-destinations = ["/user/large"]""".stripMargin).withFallback(ConfigFactory.load()))
      try {
        val sizes = MessageSize(large)
        (sizes.frameLimit, sizes.limit) mustBe ((56 * 1024, 2 * 1024 * 1024 - 8 * 1024))
      } finally TestKit.shutdownActorSystem(large)
    }
  }

  Feature("Frames the reader refuses (F240)") {

    def read(write: DataOutputStream => Unit): Any = {
      val bytes = new ByteArrayOutputStream()
      val out = new DataOutputStream(bytes)
      write(out)
      out.flush()
      new ClusterSerializer.Reader(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray)), system, ClusterSerialization(system).mpjsons).read()
    }
    def str(out: DataOutputStream, s: String): Unit = { val b = s.getBytes("UTF-8"); out.writeInt(b.length); out.write(b) }

    Scenario("F240 a frame naming a class that is neither the library's nor a case class is refused, the class not made") {
      an[IllegalArgumentException] must be thrownBy read { out => out.writeByte(24); str(out, "java.lang.ProcessBuilder"); str(out, "{}") } // as JSON
      an[IllegalArgumentException] must be thrownBy read { out => out.writeByte(23); str(out, "java.util.Date"); out.writeInt(0) } // as a library class
    }

    Scenario("F240 a length past the frame's end is refused without allocating it") {
      an[IllegalArgumentException] must be thrownBy read { out => out.writeByte(1); out.writeInt(Int.MaxValue) }
    }
  }

  Feature("Messages that cannot leave their node") {

    Scenario("F314 a value that no node would read is refused by the sending node, not dropped by the receiving one") {
      val refused = the[IllegalArgumentException] thrownBy new ClusterSerializer(system).toBinary(new example.PlainValue(1))
      refused.getMessage must include("cannot go to another node")
    }

    Scenario("F314 java.time values, numbers and collections travel on their own") {
      val serializer = new ClusterSerializer(system)
      Seq(java.time.LocalDate.of(2026, 10, 18), java.time.Duration.ofMinutes(90), java.time.ZonedDateTime.parse("2026-10-18T22:45:00Z"),
        BigInt(12), new java.math.BigDecimal("1.25"), List(java.time.LocalDate.of(2026, 1, 1))).foreach { value =>
        serializer.fromBinary(serializer.toBinary(value), "") mustBe value
      }
    }

    Scenario("a message defined inside a class is refused by the sending node") {
      val refused = the[IllegalArgumentException] thrownBy new ClusterSerializer(system).toBinary(new Holder().Inner(1))
      refused.getMessage must include("cannot go to another node")
    }
  }

  Feature("Messages larger than a message between nodes may be (F121)") {

    Scenario("one larger than the limit is read on the receiving node as a failure naming its class, written no further than the limit (F227)") {
      val serializer = new ClusterSerializer(smallFrame)
      val large = OldShape(1, "x" * 100000, Nil)
      val size = serializer.sizeOf(large)
      size must be > 100000
      serializer.fromBinary(serializer.toBinary(large), classOf[OldShape].getName) match {
        // F227: written only up to the limit, so the size it names is "more than the limit"
        case Status.Failure(e: MessageTooLargeException) => (e.className, e.size, e.limit) mustBe ((classOf[OldShape].getName, 56 * 1024 + 1, 56 * 1024))
        case other => fail("Not a failure: " + other)
      }
      val fitting = OldShape(1, "x" * 1000, Nil)
      serializer.fromBinary(serializer.toBinary(fitting), classOf[OldShape].getName) mustBe fitting
    }

    Scenario("the failure keeps its class when it is sent on") {
      val serializer = new ClusterSerializer(system)
      serializer.fromBinary(serializer.toBinary(Failure(new MessageTooLargeException("a.B", 5, 4))), "") match {
        case Failure(e: MessageTooLargeException) => (e.className, e.size, e.limit) mustBe (("a.B", 5, 4))
        case other => fail("Not the failure: " + other)
      }
    }
  }
}

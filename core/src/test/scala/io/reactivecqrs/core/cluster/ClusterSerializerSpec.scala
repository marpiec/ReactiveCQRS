package io.reactivecqrs.core.cluster

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem}
import org.apache.pekko.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

// One library message in two versions, and one whose field changed type.
case class OldShape(id: Int, name: String, dropped: List[String])
case class NewShape(id: Int, name: String, added: Option[Int], counted: Long, withDefault: String = "given")
case class ChangedShape(id: String, name: String)
// An actor's own messages are defined inside its class, so they hold its instance.
class Holder { case class Inner(id: Int) }

// D68: a node reads the library's messages that a node of another version wrote.
class ClusterSerializerSpec extends AnyFeatureSpec with Matchers with BeforeAndAfterAll {

  private val system = ActorSystem("serializer").asInstanceOf[ExtendedActorSystem]

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

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

    Scenario("a field whose type changed fails the message") {
      an[IllegalArgumentException] must be thrownBy asWrittenFor(OldShape(1, "a", Nil), classOf[ChangedShape])
    }
  }

  Feature("Messages that cannot leave their node") {

    Scenario("a message defined inside a class is refused by the sending node") {
      val refused = the[IllegalArgumentException] thrownBy new ClusterSerializer(system).toBinary(new Holder().Inner(1))
      refused.getMessage must include("cannot go to another node")
    }
  }
}

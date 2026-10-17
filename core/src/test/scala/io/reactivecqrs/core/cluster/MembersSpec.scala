package io.reactivecqrs.core.cluster

import com.typesafe.config.ConfigFactory
import io.reactivecqrs.api.{AggregateContext, Event}
import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem}
import org.apache.pekko.testkit.TestKit
import org.apache.pekko.util.Version
import org.scalatest.BeforeAndAfterAll
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

case class Notes(notes: Vector[String])
case class NoteAdded(note: String) extends Event[Notes]
case class NoteAddedV2(note: String, by: String) extends Event[Notes]

/** Notes whose version 2.0.0 adds `NoteAddedV2`. */
class NotesContext extends AggregateContext[Notes] {
  override val version: Int = 1
  override def initialAggregateRoot: Notes = Notes(Vector())
  override def commandHandlers: CommandHandler = _ => PartialFunction.empty
  override def eventHandlers: EventHandler = (_, _, notes) => {
    case NoteAdded(note) => notes.copy(notes = notes.notes :+ note)
    case NoteAddedV2(note, _) => notes.copy(notes = notes.notes :+ note)
  }
  override val writtenFrom: Map[Class[_], String] = Map(classOf[NoteAddedV2] -> "2.0.0")
  def writesV2: Boolean = everyNodeRuns("2.0.0")
}

// D67: an event class added in a rolling upgrade is written once every member of the cluster runs its version.
class MembersSpec extends AnyFeatureSpec with Matchers with BeforeAndAfterAll {

  private val system = ActorSystem("members") // not a cluster: the test sets the members' versions itself

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  Feature("The write switch (D67)") {

    Scenario("a new class is written once the lowest app-version among the members reaches its version") {
      val (versions, notes) = (new Members(system.asInstanceOf[ExtendedActorSystem]), new NotesContext)
      versions.track(notes)
      notes.notWrittenYet(NoteAddedV2("a", "b")) mustBe None // outside a cluster everything is written

      versions.membersChanged(Seq(Version("1.0.0"), Version("2.0.0")))
      notes.notWrittenYet(NoteAddedV2("a", "b")).get must include("2.0.0")
      notes.writesV2 mustBe false
      notes.notWrittenYet(NoteAdded("a")) mustBe None

      versions.membersChanged(Seq(Version("2.0.0"), Version("2.1.0")))
      notes.notWrittenYet(NoteAddedV2("a", "b")) mustBe None
      notes.writesV2 mustBe true
    }

    Scenario("members without an app-version (Pekko's 0.0.0) are not managed when all are so; among versioned ones they count as the oldest") {
      val (versions, notes) = (new Members(system.asInstanceOf[ExtendedActorSystem]), new NotesContext)
      versions.track(notes)
      versions.membersChanged(Seq(Version.Zero, Version.Zero))
      notes.writesV2 mustBe true
      versions.membersChanged(Seq(Version.Zero, Version("2.0.0")))
      notes.writesV2 mustBe false
    }

    Scenario("a node of a cluster writes no new class while it knows no member, itself included (F138)") {
      val unjoined = ActorSystem("members-unjoined", ConfigFactory.parseString(
        """pekko.actor.provider = cluster
          |pekko.remote.artery.canonical { hostname = "127.0.0.1", port = 0 }
          |pekko.cluster.jmx.multi-mbeans-in-same-jvm = on""".stripMargin).withFallback(ConfigFactory.load()))
      try {
        val (members, notes) = (new Members(unjoined.asInstanceOf[ExtendedActorSystem]), new NotesContext)
        members.track(notes)
        notes.writesV2 mustBe false
        members.membersChanged(Seq(Version("2.0.0")))
        notes.writesV2 mustBe true
        members.membersChanged(Nil) // removed: it knows no member again
        notes.writesV2 mustBe false
      } finally TestKit.shutdownActorSystem(unjoined)
    }
  }

  Feature("Whether the node is alone (F131)") {

    Scenario("outside a cluster, or as the cluster's only member; not while it knows no member") {
      val members = new Members(system.asInstanceOf[ExtendedActorSystem])
      members.alone mustBe true
      members.membersChanged(Seq(Version.Zero, Version.Zero))
      members.alone mustBe false
      members.membersChanged(Seq(Version.Zero))
      members.alone mustBe true
      members.membersChanged(Nil)
      members.alone mustBe false
    }
  }
}

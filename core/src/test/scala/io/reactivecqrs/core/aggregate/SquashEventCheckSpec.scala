package io.reactivecqrs.core.aggregate

import java.time.Instant

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import io.reactivecqrs.core.eventstore.{MemoryEventStoreState, NewEvents}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.must.Matchers

case class Note(text: String, edits: Int, editor: Long)

case class NoteWritten(text: String) extends FirstEvent[Note] { def spaceId: SpaceId = SpaceId(0) }
case class NoteEdited(text: String) extends Event[Note]
case class NoteCopied(spaceId: SpaceId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion) extends DuplicationEvent[Note]
/** Gives the state back: its handler takes the editor from the user it is stored by. */
case class NoteRestated(text: String, edits: Int) extends FirstEvent[Note] { def spaceId: SpaceId = SpaceId(0) }
/** Leaves out the edits, without saying so. */
case class NoteTrimmed(text: String) extends FirstEvent[Note] { def spaceId: SpaceId = SpaceId(0) }
/** Leaves out the edits, on purpose. */
case class NoteArchived(text: String) extends FirstEvent[Note] with SquashMayChangeState { def spaceId: SpaceId = SpaceId(0) }
/** No event handler takes it. */
case class NoteUnreadable() extends FirstEvent[Note] { def spaceId: SpaceId = SpaceId(0) }

// D85: a squash event must give back exactly the state it replaces, unless its class says it may differ.
class SquashEventCheckSpec extends AnyFeatureSpec with Matchers {

  private val (writer, editor) = (UserId(1), UserId(2))

  private val handlers: (UserId, Instant, Note) => PartialFunction[Any, Note] = (userId, _, note) => {
    case e: NoteWritten => Note(e.text, 0, userId.asLong)
    case e: NoteEdited => Note(e.text, note.edits + 1, userId.asLong)
    case _: NoteCopied => note
    case e: NoteRestated => Note(e.text, e.edits, userId.asLong)
    case e: NoteTrimmed => Note(e.text, 0, userId.asLong)
    case e: NoteArchived => Note(e.text, 0, userId.asLong)
  }

  private class Fixture {
    val store = new MemoryEventStoreState
    val restorer = new AggregateRestorer[Note](store, handlers, () => Note("", 0, 0))
    val noteId = AggregateId(10)
    persist(noteId, writer, 0, NoteWritten("draft"))
    persist(noteId, editor, 1, NoteEdited("text"))

    def persist(id: AggregateId, userId: UserId, expected: Int, event: Event[Note]): Unit =
      store.localTx(session => store.persistEvents(id, NewEvents[Note](userId, AggregateVersion(expected), Instant.now, Seq(event)))(session)).get

    def squashEvent(toEvent: Note => FirstEvent[Note], by: UserId = editor): Either[String, FirstEvent[Note]] =
      restorer.squashEvent(noteId, AggregateVersion(2), SquashPolicy(toEvent), Some(by))
  }

  Feature("The squash event is checked against the state it replaces") {

    Scenario("one that gives the state back is used") {
      val f = new Fixture
      f.squashEvent(note => NoteRestated(note.text, note.edits)) mustBe Right(NoteRestated("text", 1))
    }

    Scenario("it is applied as a restore will apply it: by the user it is stored by") {
      val f = new Fixture
      f.squashEvent(note => NoteRestated(note.text, note.edits), by = writer).left.toOption.get must include("does not give back the state")
    }

    Scenario("one that gives another state is refused, unless its class says it may") {
      val f = new Fixture
      f.squashEvent(note => NoteTrimmed(note.text)).left.toOption.get must include("NoteTrimmed does not give back the state it replaces")
      f.squashEvent(note => NoteArchived(note.text)) mustBe Right(NoteArchived("text"))
    }

    Scenario("one the event handlers fail on is refused, even when it may change the state") {
      val f = new Fixture
      f.squashEvent(_ => NoteUnreadable()).left.toOption.get must include("the event handlers fail on the squash event NoteUnreadable")
      f.squashEvent(_ => throw new IllegalStateException("no event")).left.toOption.get must include("making the squash event failed")
    }

    Scenario("a duplicate is detached with a checked event of its state at version 1, applied as by the user of its first event") {
      val f = new Fixture
      val copyId = AggregateId(11)
      f.persist(copyId, writer, 0, NoteCopied(SpaceId(0), f.noteId, AggregateVersion(2)))
      // The copy's state at version 1 is the base's, edited by `editor`; its first event was stored by `writer`.
      f.restorer.detachEvents(Seq(copyId), SquashPolicy(note => NoteRestated(note.text, note.edits))).left.toOption.get must
        include("duplicate 11: the squash event NoteRestated does not give back the state")
      f.restorer.detachEvents(Seq(copyId), SquashPolicy(note => NoteArchived(note.text))) mustBe Right(Seq(copyId -> NoteArchived("text")))
    }
  }
}

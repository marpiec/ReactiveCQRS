package io.reactivecqrs.core.eventstore

import java.time.Instant

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import scalikejdbc.DBSession

import scala.util.Try

/** The events of one command, stored after `expectedVersion` with one user and one timestamp (D10). */
case class NewEvents[AGGREGATE_ROOT](userId: UserId, expectedVersion: AggregateVersion, timestamp: Instant, events: Seq[Event[AGGREGATE_ROOT]])

/** A performance snapshot (D30): the aggregate's state at `version`, usable only with the context and schema versions
  * it was made with. */
case class Snapshot[AGGREGATE_ROOT](version: AggregateVersion, contextVersion: Int, schemaVersion: Int, state: AGGREGATE_ROOT)

/** An aggregate with rows waiting in the outbox: its oldest waiting version, and when that event was written. */
case class WaitingInOutbox(aggregateType: AggregateType, aggregateId: AggregateId, oldestVersion: AggregateVersion, since: Instant)

abstract class EventStoreState {
  def countAllEvents(): Int
  def countEventsForAggregateTypes(strings: Seq[String]): Int
  def persistEvents[AGGREGATE_ROOT](aggregateId: AggregateId, eventsEnvelope: NewEvents[AGGREGATE_ROOT])(implicit session: DBSession): Try[Seq[(Event[AGGREGATE_ROOT], AggregateVersion)]]
  def overwriteEvents[AGGREGATE_ROOT](aggregateId: AggregateId, events: Iterable[EventWithVersion[AGGREGATE_ROOT]])(implicit session: DBSession): Unit
  def readAndProcessEvents[AGGREGATE_ROOT](aggregateId: AggregateId, version: Option[Either[AggregateVersion, Instant]])(eventHandler: (UserId, Instant, Event[AGGREGATE_ROOT], AggregateId, Int, Boolean) => Unit): Unit
  /** The aggregate's own events after version `after`, up to `upTo` (or its newest), in order, each with whether it
    * was undone as of that version: the tail after a snapshot. A duplicate's base events all come before its own. */
  def readAndProcessOwnEvents[AGGREGATE_ROOT](aggregateId: AggregateId, after: AggregateVersion, upTo: Option[AggregateVersion])
                                             (eventHandler: (UserId, Instant, Event[AGGREGATE_ROOT], Int, Boolean) => Unit): Unit

  /** The aggregate's snapshot (D30), if it has one; `Failure` when the stored state cannot be read as `stateClassName`.
    * An undo, a history rewrite and a permanent delete remove the snapshots they make wrong, in their own transaction. */
  def readSnapshot[AGGREGATE_ROOT](aggregateId: AggregateId, stateClassName: String): Option[Try[Snapshot[AGGREGATE_ROOT]]]

  /** Stores the aggregate's snapshot in a transaction of its own, in place of the one it had. */
  def writeSnapshot[AGGREGATE_ROOT](aggregateId: AggregateId, snapshot: Snapshot[AGGREGATE_ROOT], stateClassName: String): Unit

  def readAndProcessAllEvents(aggregateType: String,
                              batchPerAggregate: Boolean, eventHandler: (Seq[EventInfo[_]], AggregateId, AggregateType) => Unit): Unit

  /** Removes delivered versions from the outbox. It may return before the rows are gone (the PostgreSQL store batches
    * these deletes, F26); a delete that fails only leaves rows that the outbox poller re-publishes later. */
  def deletePublished(aggregateId: AggregateId, versions: Seq[AggregateVersion]): Unit

  /** Carries out a `PermanentDeleteEvent` once it is delivered: removes the aggregate, its events, its outbox rows and
    * its subscriptions in one transaction. Returns false, deleting nothing, while a subscription of the aggregate is
    * failed (D20a): the delete would remove the record of that failure. */
  def deleteAggregatePermanently(aggregateId: AggregateId): Boolean

  /** Every aggregate with rows waiting in the outbox, for the outbox poller (D8). */
  def readWaitingInOutbox(): Seq[WaitingInOutbox]

  /** The aggregate's own events with versions `from` to `to`, in order, undone ones included, as live delivery sent
    * them (D60): what a re-publication sends. */
  def readEventsRange[AGGREGATE_ROOT](aggregateId: AggregateId,
                                      from: AggregateVersion, to: AggregateVersion): Seq[EventInfo[AGGREGATE_ROOT]]

  /** The oldest version of the aggregate still waiting in the outbox, if any. */
  def oldestVersionToPublish(aggregateId: AggregateId): Option[AggregateVersion]

  def localTx[A](block: DBSession => A): A

  def readNotYetPublishedEvents(): Map[AggregateId, AggregateVersion]
}

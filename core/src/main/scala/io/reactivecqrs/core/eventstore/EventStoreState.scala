package io.reactivecqrs.core.eventstore

import java.time.Instant

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import scalikejdbc.DBSession

import scala.util.Try

/** The events of one command, stored after `expectedVersion` with one user and one timestamp (D10) and the
  * command's metadata envelope, if it has one (D32). */
case class NewEvents[AGGREGATE_ROOT](userId: UserId, expectedVersion: AggregateVersion, timestamp: Instant, events: Seq[Event[AGGREGATE_ROOT]],
                                     metadata: Option[EventMetadata] = None)

/** A performance snapshot (D30): the aggregate's state at `version`, usable only with the context and schema versions
  * it was made with. */
case class Snapshot[AGGREGATE_ROOT](version: AggregateVersion, contextVersion: Int, schemaVersion: Int, state: AGGREGATE_ROOT)

/** An aggregate with rows waiting in the outbox: its oldest waiting version, and when that event was written. */
case class WaitingInOutbox(aggregateType: AggregateType, aggregateId: AggregateId, oldestVersion: AggregateVersion, since: Instant)

/** A stored event in commit order (D59): its row id, its aggregate's type and squash point (D51), and the event, or why
  * it cannot be read. */
case class CommittedEvent(id: Long, aggregateType: AggregateType, aggregateId: AggregateId, version: AggregateVersion,
                          squashedUpTo: Option[AggregateVersion], event: Either[String, EventInfo[Any]])

/** A stored aggregate: its type, current version and squash point. */
case class StoredAggregate(aggregateType: AggregateType, aggregateId: AggregateId, version: AggregateVersion, squashedUpTo: Option[AggregateVersion])

/** Which aggregates of the types a read covers: all, the ones given, or those of one organization (D38, D63). */
sealed trait AggregateScope
object AggregateScope {
  case object All extends AggregateScope
  case class Only(aggregateIds: Set[AggregateId]) extends AggregateScope
  case class InSpace(spaceId: SpaceId) extends AggregateScope
}

abstract class EventStoreState {
  def countAllEvents(): Int
  def countEventsForAggregateTypes(strings: Seq[String]): Int
  def persistEvents[AGGREGATE_ROOT](aggregateId: AggregateId, eventsEnvelope: NewEvents[AGGREGATE_ROOT])(implicit session: DBSession): Try[Seq[(Event[AGGREGATE_ROOT], AggregateVersion)]]
  def overwriteEvents[AGGREGATE_ROOT](aggregateId: AggregateId, events: Iterable[EventWithVersion[AGGREGATE_ROOT]])(implicit session: DBSession): Unit
  def readAndProcessEvents[AGGREGATE_ROOT](aggregateId: AggregateId, version: Option[Either[AggregateVersion, Instant]])(eventHandler: (UserId, Instant, Event[AGGREGATE_ROOT], AggregateId, Int, Boolean) => Unit): Unit
  /** Like `readAndProcessEvents`, with each event's metadata envelope (D32): for answers that return events. */
  def readAndProcessEventsWithMetadata[AGGREGATE_ROOT](aggregateId: AggregateId, version: Option[Either[AggregateVersion, Instant]])
                                                      (eventHandler: (EventInfo[AGGREGATE_ROOT], AggregateId, Boolean) => Unit): Unit

  /** The aggregate's own events after version `after`, up to `upTo` (or its newest), in order, each with whether it
    * was undone as of that version: the tail after a snapshot. A duplicate's base events all come before its own. */
  def readAndProcessOwnEvents[AGGREGATE_ROOT](aggregateId: AggregateId, after: AggregateVersion, upTo: Option[AggregateVersion])
                                             (eventHandler: (UserId, Instant, Event[AGGREGATE_ROOT], Int, Boolean) => Unit): Unit

  /** The aggregate's snapshot (D30), if it has one; `Failure` when the stored state cannot be read as `stateClassName`.
    * An undo, a history rewrite and a permanent delete remove the snapshots they make wrong, in their own transaction. */
  def readSnapshot[AGGREGATE_ROOT](aggregateId: AggregateId, stateClassName: String): Option[Try[Snapshot[AGGREGATE_ROOT]]]

  /** Stores the aggregate's snapshot in a transaction of its own, in place of the one it had; only while the aggregate
    * is stored at that version or later, so nothing is written for an aggregate deleted for good. */
  def writeSnapshot[AGGREGATE_ROOT](aggregateId: AggregateId, snapshot: Snapshot[AGGREGATE_ROOT], stateClassName: String): Unit

  def readAndProcessAllEvents(aggregateType: String,
                              batchPerAggregate: Boolean, eventHandler: (Seq[EventInfo[_]], AggregateId, AggregateType) => Unit): Unit

  // ************** Rebuild (D58, D59)

  /** Up to `limit` events of the aggregates of `types` with row ids above `after`, in commit order (row id), each under
  * its own aggregate (a duplicate's own events, not its base's). Undone events and undo events too, as live delivery
  * sent them (D60). A row that cannot be read is returned with the reason, so one bad row does not stop a rebuild. */
  def readCommitted(types: Seq[AggregateType], after: Long, limit: Int, scope: AggregateScope = AggregateScope.All): Seq[CommittedEvent]

  /** Up to `limit` aggregates of `types` with ids above `after`, in id order. */
  def aggregatesOf(types: Seq[AggregateType], after: AggregateId, limit: Int, scope: AggregateScope = AggregateScope.All): Seq[StoredAggregate]

  /** Removes delivered versions from the outbox. It may return before the rows are gone (the PostgreSQL store batches
    * these deletes, F26); a delete that fails only leaves rows that the outbox poller re-publishes later. */
  def deletePublished(aggregateId: AggregateId, versions: Seq[AggregateVersion]): Unit

  /** Carries out a `PermanentDeleteEvent` once it is delivered: removes the aggregate, its events, its outbox rows and
    * its subscriptions in one transaction, after detaching the duplicates given (see `squash`; F76). Deletes nothing,
    * giving the reason, while a subscription of the aggregate is failed (D20a: the delete would remove the record of
    * that failure), or when the duplicates given are not all those that read the aggregate (one was made meanwhile). */
  def deleteAggregatePermanently(aggregateId: AggregateId, detach: Seq[(AggregateId, FirstEvent[_])] = Nil): Either[String, Unit]

  // ************** Squash (D47–D52)

  /** The version the aggregate's history is squashed up to, if it was. Reads below it fail with
    * `AggregateSquashedException`, and so does a duplication of the aggregate below it. */
  def squashedVersion(aggregateId: AggregateId): Option[AggregateVersion]

  /** Why the aggregate cannot be squashed up to `upTo` now (D51), if it cannot: it is squashed that far already; events
    * at or below `upTo` still wait in the outbox; a subscription of it is failed; an undo after `upTo` undid events at
    * or below it; or a duplicate the squash would detach has its first event waiting in the outbox or a failed
    * subscription (the detach changes that event). `squash` checks it again. */
  def squashBlocker(aggregateId: AggregateId, upTo: AggregateVersion): Option[String]

  /** The aggregates whose duplication chain reads `baseId`'s events, at any depth; with `below`, only those that read
    * fewer than `below` of them: a squash up to `below` must detach those (D50). */
  def duplicatesReading(baseId: AggregateId, below: Option[AggregateVersion]): Seq[AggregateId]

  /** In one transaction (D47, D50): detaches each duplicate given (its version-1 event becomes the given first event,
    * holding its state at version 1, and its chain rows to other aggregates go); then the event at `upTo` becomes
    * `squashEvent` (keeping its id and time, so commit order holds), the events before it and the noop rows at or
    * below it go, as do the snapshot at or below it, the aggregate's own chain rows to other aggregates and its outbox
    * rows at or below it; the squash point is recorded. The events are checked by the caller (D85); one that may change
    * the state (`SquashMayChangeState`) takes every snapshot of its aggregate, or of the duplicate it detaches. Changes nothing, giving the reason, when `squashBlocker` finds
    * one, or when the duplicates given are not all those that read the aggregate below `upTo` (one was made
    * meanwhile): the caller may try again. A duplication of the aggregate made at the same time waits for the
    * transaction and is then refused below the squash point. */
  def squash[AGGREGATE_ROOT](aggregateId: AggregateId, upTo: AggregateVersion, squashEvent: FirstEvent[AGGREGATE_ROOT], userId: UserId,
                             metadata: Option[EventMetadata], detach: Seq[(AggregateId, FirstEvent[_])]): Either[String, Unit]

  /** For the squash scheduler (D52): up to `limit` aggregates of the type with ids above `after`, in id order, that
    * `policy` says to squash now, each with the version to squash it up to. */
  def squashCandidates(aggregateType: AggregateType, policy: AutomaticSquash, after: AggregateId, limit: Int): Seq[(AggregateId, AggregateVersion)]

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

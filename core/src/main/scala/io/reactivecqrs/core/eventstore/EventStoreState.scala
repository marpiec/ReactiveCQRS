package io.reactivecqrs.core.eventstore

import java.time.Instant

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}
import scalikejdbc.DBSession

import scala.util.Try

/** The events of one command, stored after `expectedVersion` with one user and one timestamp (D10) and the
  * command's metadata envelope, if it has one (D32). `expectedGeneration`: the history generation the command's state was
  * read at; when given, the events are refused if the stored history changed since, though the version did not (F195). */
case class NewEvents[AGGREGATE_ROOT](userId: UserId, expectedVersion: AggregateVersion, timestamp: Instant, events: Seq[Event[AGGREGATE_ROOT]],
                                     metadata: Option[EventMetadata] = None, expectedGeneration: Option[Int] = None)

/** A performance snapshot (D30): the aggregate's state at `version`, usable only with the context and schema versions
  * it was made with, and the state class's shape (`StateShape`, F196; "" for one written by 1.0.0). */
case class Snapshot[AGGREGATE_ROOT](version: AggregateVersion, contextVersion: Int, schemaVersion: Int, state: AGGREGATE_ROOT, shape: String = "")

/** An aggregate with rows waiting in the outbox: its oldest waiting version, and when that event was written. */
case class WaitingInOutbox(aggregateType: AggregateType, aggregateId: AggregateId, oldestVersion: AggregateVersion, since: Instant)

/** A stored event in commit order (D59): its row id, its aggregate's type and squash point (D51), and the event, or why
  * it cannot be read; `size`: its stored JSON's length (F126), 0 when the store does not know it. */
case class CommittedEvent(id: Long, aggregateType: AggregateType, aggregateId: AggregateId, version: AggregateVersion,
                          squashedUpTo: Option[AggregateVersion], event: Either[String, EventInfo[Any]], size: Int = 0)

/** A page of events in commit order; `cut`: its bytes ended it before its limit of events (F126), so a shorter page does
  * not mean that no more events are stored. */
case class CommittedPage(events: Seq[CommittedEvent], cut: Boolean)

/** An event as stored: its version, and its stored JSON's bytes, which bound batches (F126, F142); 0 when the
  * store does not know it. */
case class StoredEvent[AGGREGATE_ROOT](event: Event[AGGREGATE_ROOT], version: AggregateVersion, size: Int)

/** A stored aggregate: its type, current version and squash point. */
case class StoredAggregate(aggregateType: AggregateType, aggregateId: AggregateId, version: AggregateVersion, squashedUpTo: Option[AggregateVersion])

/** What a complete rewrite wrote again (D76): the events and the archived events, their classes, and the fields that
  * changed (case class field names; `metadata.` and the envelope's field for a changed envelope). */
case class RewriteCount(events: Int, archived: Int, classes: Set[String], fields: Set[String], unreadable: Int = 0)

object RewriteCount {
  /** The fields whose values differ between two events (or envelopes, with the prefix `metadata.`) of one case class;
    * the whole value's name when they are not of one case class (an envelope added or removed). */
  def fieldsChanged(before: Any, after: Any, prefix: String = ""): Set[String] = (before, after) match {
    case _ if before == after => Set.empty
    case (b: Product, a: Product) if b.getClass == a.getClass =>
      b.productElementNames.zip(b.productIterator.zip(a.productIterator)).collect { case (name, (x, y)) if x != y => prefix + name }.toSet
    case _ => Set(if (prefix.isEmpty) "event" else prefix.stripSuffix("."))
  }
}

/** Which aggregates of the types a read covers: all, the ones given, or those of one organization (D38, D63). */
sealed trait AggregateScope
object AggregateScope {
  /** Every aggregate. */
  case object All extends AggregateScope
  /** These aggregates. */
  case class Only(aggregateIds: Set[AggregateId]) extends AggregateScope
  /** The aggregates of one organization. */
  case class InSpace(spaceId: SpaceId) extends AggregateScope
}

/** Where events are stored (`PostgresEventStoreState`; `MemoryEventStoreState` for tests): what owners write and
  * restore, the outbox, snapshots, squash and its archive, and the reads in commit order that rebuilds take. Hosts make
  * one per installation and give it to the command buses, the outbox poller and `Projections`. */
abstract class EventStoreState {
  /** Stores the events after `expectedVersion`: each with its version and stored size. */
  def persist[AGGREGATE_ROOT](aggregateId: AggregateId, eventsEnvelope: NewEvents[AGGREGATE_ROOT])(implicit session: DBSession): Try[Seq[StoredEvent[AGGREGATE_ROOT]]]
  /** `persist`, giving each event with its version. */
  final def persistEvents[AGGREGATE_ROOT](aggregateId: AggregateId, eventsEnvelope: NewEvents[AGGREGATE_ROOT])(implicit session: DBSession): Try[Seq[(Event[AGGREGATE_ROOT], AggregateVersion)]] =
    persist(aggregateId, eventsEnvelope).map(_.map(stored => (stored.event, stored.version)))
  /** The aggregate's history generation and its type's name, from its own row (F197); the type is None when the store
    * does not know it. */
  def generationAndType(aggregateId: AggregateId): (Int, Option[String])
  /** Writes these events over the stored ones of their versions (a history-rewriting command). With `expected`, the
    * version and history generation the command's state was read at, it throws `HistoryChangedException` when the
    * stored aggregate differs (F195). */
  def overwriteEvents[AGGREGATE_ROOT](aggregateId: AggregateId, events: Iterable[EventWithVersion[AGGREGATE_ROOT]],
                                      expected: Option[(AggregateVersion, Int)] = None)(implicit session: DBSession): Unit
  /** Calls `eventHandler` with each event of the aggregate, oldest first (a duplicate's base's first), up to a version
    * or an instant when one is given: its user, time, event, the aggregate that stored it, its version, and whether an
    * undo cancelled it. */
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
    * is stored at that version or later, so nothing is written for an aggregate deleted for good, and while its history
    * is at `generation`, the one the state was read at (F119). */
  def writeSnapshot[AGGREGATE_ROOT](aggregateId: AggregateId, snapshot: Snapshot[AGGREGATE_ROOT], stateClassName: String, generation: Int): Unit

  /** How many times the aggregate's history changed so that its snapshots could be wrong (an undo, a rewrite of it or of
    * its base, a squash or a detach that may change its state, F119); 0 for an aggregate not stored yet. Read it before
    * the history, so that a state is never paired with a later generation than its own. */
  def historyGeneration(aggregateId: AggregateId): Int

  /** The aggregate's stored version and history generation, by one key lookup; None for an aggregate not stored (yet, or
    * any more). An owner that may share its aggregate with another node's owner compares it with its own (F131). */
  def storedVersion(aggregateId: AggregateId): Option[(AggregateVersion, Int)]

  // ************** Rebuild (D58, D59)

  /** Up to `limit` events of the aggregates of `types` with row ids above `after`, in commit order (row id), each under
  * its own aggregate (a duplicate's own events, not its base's), and no more than `maxBytes` of stored JSON, but at least
  * one (F126). Undone events and undo events too, as live delivery sent them (D60). A row that cannot be read is
  * returned with the reason, so one bad row does not stop a rebuild. */
  def readCommittedPage(types: Seq[AggregateType], after: Long, limit: Int, scope: AggregateScope, maxBytes: Long): CommittedPage

  /** `readCommittedPage` without a limit of bytes. */
  final def readCommitted(types: Seq[AggregateType], after: Long, limit: Int, scope: AggregateScope = AggregateScope.All): Seq[CommittedEvent] =
    readCommittedPage(types, after, limit, scope, Long.MaxValue).events

  /** Up to `limit` aggregates of `types` with ids above `after`, in id order. */
  def aggregatesOf(types: Seq[AggregateType], after: AggregateId, limit: Int, scope: AggregateScope = AggregateScope.All): Seq[StoredAggregate]

  /** Removes delivered versions from the outbox. It may return before the rows are gone (the PostgreSQL store batches
    * these deletes, F26); a delete that fails only leaves rows that the outbox poller re-publishes later. */
  def deletePublished(aggregateId: AggregateId, versions: Seq[AggregateVersion]): Unit

  /** Carries out a `PermanentDeleteEvent` once it is delivered: removes the aggregate, its events, its archive, its outbox
    * rows and its subscriptions in one transaction, after detaching the duplicates given (see `squash`; F76), whose
    * replaced first events of the classes in `archive` are archived (R11). Deletes nothing, giving the reason, while a
    * subscription of the aggregate is failed (D20a: the delete would remove the record of that failure), while a detach
    * must wait (F143), or when the duplicates given are not all those that read the aggregate (one was made meanwhile). */
  def deleteAggregatePermanently(aggregateId: AggregateId, detach: Seq[(AggregateId, FirstEvent[_])] = Nil, archive: Set[String] = Set.empty): Either[String, Unit]

  /** R17: detaches the given duplicates of the aggregate, each with its new first event (see `squash`; D50), in one
    * transaction under the chain lock, archiving the replaced first events of the classes in `archive`. Changes nothing,
    * giving the reason, when one of them no longer reads the aggregate, or when a detach must wait (F143: its first event
    * was undone, still waits in the outbox, or a subscription of it is failed). */
  def detachDuplicates(baseId: AggregateId, detach: Seq[(AggregateId, FirstEvent[_])], archive: Set[String] = Set.empty): Either[String, Unit]

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
    * the state (`SquashMayChangeState`) takes every snapshot of its aggregate, or of the duplicate it detaches. Changes
    * nothing, giving the reason, when `squashBlocker` finds one, or when the duplicates given are not all those that
    * read the aggregate below `upTo` (one was made meanwhile): the caller may try again. A duplication of the aggregate
    * made at the same time waits for the transaction and is then refused below the squash point. The aggregate's own
    * events at or below `upTo` of the classes in `archive` (and a detached duplicate's first event of one) move to its
    * archive with their undo marks instead of going (R11). */
  def squash[AGGREGATE_ROOT](aggregateId: AggregateId, upTo: AggregateVersion, squashEvent: FirstEvent[AGGREGATE_ROOT], userId: UserId,
                             metadata: Option[EventMetadata], detach: Seq[(AggregateId, FirstEvent[_])], archive: Set[String] = Set.empty): Either[String, Unit]

  /** The aggregate's archived events (R11), oldest first. */
  def readArchive[AGGREGATE_ROOT](aggregateId: AggregateId): Seq[ArchivedEvent[AGGREGATE_ROOT]]

  // ************** Complete rewrite (D76)

  /** In the session's transaction, under the chain lock: applies `alter` to each of the aggregate's own events and
    * archived events (not its bases'), with its metadata envelope, and writes again those it changes, checked for size
    * (D76); raises the history generation of the aggregate and of the duplicates reading it, whose snapshots go; records
    * an audit row of the request. Writes nothing, giving the reason, when `alter` changes an event's class, or changes an
    * undo or a duplication event. */
  def rewriteEverywhere[AGGREGATE_ROOT](aggregateId: AggregateId, userId: UserId, change: HistoryChange,
                                       alter: (Event[AGGREGATE_ROOT], Option[EventMetadata]) => (Event[AGGREGATE_ROOT], Option[EventMetadata]))
                                      (implicit session: DBSession): Either[String, RewriteCount]

  /** The aggregates copied from this one, at any depth: the duplicates reading its events, and those detached from it,
    * which hold a copy of its state (the lineage every detach records, D76). */
  def copiesOf(aggregateId: AggregateId): Seq[AggregateId]

  /** The aggregates whose events this one reads: the bases of its duplication chain, the nearest first. */
  def basesOf(aggregateId: AggregateId): Seq[AggregateId]

  /** For the squash scheduler (D52): up to `limit` aggregates of the type with ids above `after`, in id order, that
    * `policy` says to squash at `now`, each with the version to squash it up to. */
  def squashCandidates(aggregateType: AggregateType, policy: AutomaticSquash, after: AggregateId, limit: Int,
                       now: Instant = Instant.now): Seq[(AggregateId, AggregateVersion)]

  /** The aggregates with rows waiting in the outbox, for the outbox poller (D8): at most `limit`, those waiting longest first (F122). */
  def readWaitingInOutbox(limit: Int = Int.MaxValue): Seq[WaitingInOutbox]

  /** F260: as `readWaitingInOutbox`, those waiting the shortest first. */
  def readNewestWaitingInOutbox(limit: Int): Seq[WaitingInOutbox]

  /** The aggregate's own events with versions `from` to `to`, in order, undone ones included, as live delivery sent
    * them (D60), each with its stored size, and no more than `maxBytes` of them, but at least one: a re-publication's
    * part (F126). */
  def readPart[AGGREGATE_ROOT](aggregateId: AggregateId, from: AggregateVersion, to: AggregateVersion,
                               maxBytes: Long): Seq[(EventInfo[AGGREGATE_ROOT], Int)]

  /** The aggregate's own events with versions `from` to `to`, in order, undone ones included (D60). */
  final def readEventsRange[AGGREGATE_ROOT](aggregateId: AggregateId,
                                            from: AggregateVersion, to: AggregateVersion): Seq[EventInfo[AGGREGATE_ROOT]] =
    readPart[AGGREGATE_ROOT](aggregateId, from, to, Long.MaxValue).map(_._1)

  /** The oldest version of the aggregate still waiting in the outbox, if any. */
  def oldestVersionToPublish(aggregateId: AggregateId): Option[AggregateVersion]

  /** Runs `block` in a transaction of the store; what was registered with `AfterCommit` in it runs once it commits. */
  def localTx[A](block: DBSession => A): A

  // ************** CR-4: aggregates that cannot be restored. A store that keeps no records (the ephemeral one) records
  // nothing.

  /** Records, or counts once more, that the aggregate could not be restored there; true when the aggregate had no record
    * before, from anywhere (the caller then logs a warning). */
  def recordRestoreFailure(failure: RestoreFailureSeen): Boolean = false

  /** The aggregate restores again, or no longer exists, there: its record there goes. */
  def clearRestoreFailure(aggregateId: AggregateId, source: RestoreFailureSource): Unit = ()

  /** Every record of a projection's build goes (the build was dropped). */
  def clearBuildRestoreFailures(projection: String, build: Int): Unit = ()

  /** An operator skipped the aggregate in that build (`SkipAggregate`): its record there says so from now on; one is
    * made when there is none. */
  def restoreFailureSkipped(aggregateId: AggregateId, aggregateType: Option[AggregateType], source: RestoreFailureSource.Build): Unit = ()

  /** The records, the oldest first, at most `limit`. */
  def restoreFailures(filter: RestoreFailureFilter = RestoreFailureFilter(), limit: Int = 1000): Seq[RestoreFailure] = Seq.empty

  /** How many aggregates have a record, from anywhere. */
  def restoreFailuresCount(): Long = 0L
}

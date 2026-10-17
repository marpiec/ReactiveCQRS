package io.reactivecqrs.core.eventstore

import java.time.Instant

import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.api._
import io.reactivecqrs.core.eventstore.MemoryEventStoreState.EventRow
import scalikejdbc.{DBSession, NoSession}

import scala.util.Try

object MemoryEventStoreState {
  case class EventRow(eventId: Long, aggregateId: AggregateId, aggregateVersion: AggregateVersion, aggregateType: AggregateType,
                      event: Event[_], userId: UserId, timestamp: Instant, metadata: Option[EventMetadata] = None)
}

case class EventStoreEntry[AGGREGATE_ROOT](userId: UserId, timestamp: Instant, version: AggregateVersion, event: Event[AGGREGATE_ROOT],
                                           metadata: Option[EventMetadata] = None)

// State is read and changed under the object's lock: owners, the outbox poller and the replayer call the store from
// their own threads. Callbacks run outside the lock, on a snapshot. `commitOrder`: keep every event in one list in
// commit order, for rebuilds; an ephemeral store (D77) keeps none, so dropping an aggregate does not walk them all.
class MemoryEventStoreState(commitOrder: Boolean = true) extends EventStoreState {

  private var eventsRows: List[EventRow] = List.empty
  private var eventStore: Map[AggregateId, Vector[EventStoreEntry[_]]] = Map.empty
  private var eventsToPublish: Map[(AggregateId, AggregateVersion), (UserId, Instant, Event[_], Long)] = Map.empty
  private var eventIdSeq: Long = 0
  private var snapshots: Map[AggregateId, Snapshot[_]] = Map.empty
  private var squashedVersions: Map[AggregateId, AggregateVersion] = Map.empty
  private var generations: Map[AggregateId, Int] = Map.empty
  private var archives: Map[AggregateId, Vector[ArchivedEvent[_]]] = Map.empty
  private var lineage: Map[AggregateId, Set[AggregateId]] = Map.empty // D76: a detached copy and the bases it read

  // The memory store keeps objects, not JSON: the sizes are not known (0).
  override def persist[AGGREGATE_ROOT](aggregateId: AggregateId, eventsEnvelope: NewEvents[AGGREGATE_ROOT])(implicit session: DBSession): Try[Seq[StoredEvent[AGGREGATE_ROOT]]] = Try { synchronized {

    var eventsForAggregate: Vector[EventStoreEntry[_]] = eventStore.getOrElse(aggregateId, Vector())
    val currentVersion = eventsForAggregate.lastOption.map(_.version.asInt).getOrElse(0) // not the count: a squash removes events

    if (eventsEnvelope.expectedVersion.asInt != currentVersion) {
      throw new IllegalStateException("Incorrect version for event, expected " + eventsEnvelope.expectedVersion.asInt + " but was " + currentVersion)
    }
    // Each event is checked against the history before it, the command's earlier events included, as the procedures
    // store them one after another; all are checked before anything changes, as a failed transaction rolls back.
    val entries = eventsEnvelope.events.zipWithIndex.map { case (event, i) =>
      EventStoreEntry(eventsEnvelope.userId, eventsEnvelope.timestamp, AggregateVersion(currentVersion + i + 1), event, eventsEnvelope.metadata)
    }
    entries.foldLeft(eventsForAggregate) { (before, entry) =>
      entry.event match {
        case undo: UndoEvent[_] => refuseUndoIntoSquashed(aggregateId, before, undo.eventsCount)
        case duplication: DuplicationEvent[_] => refuseDuplicationOfSquashed(duplication)
        case _ => ()
      }
      before :+ entry
    }
    val eventsWithVersions = entries.map { entry =>
      val (event, version) = (entry.event, entry.version)
      eventsForAggregate :+= entry
      eventIdSeq += 1
      if (commitOrder) eventsRows ::= EventRow(eventIdSeq, aggregateId, version,
                    AggregateType.ofClass(event.aggregateRootClass), event, eventsEnvelope.userId, eventsEnvelope.timestamp, eventsEnvelope.metadata)
      val key = (aggregateId, version)

      val value = (eventsEnvelope.userId, eventsEnvelope.timestamp, event, eventIdSeq)
      eventsToPublish += key -> value
      StoredEvent(event, version, 0)
    }

    eventStore += aggregateId -> eventsForAggregate
    if (eventsEnvelope.events.exists(_.isInstanceOf[UndoEvent[_]])) historyChangedFor(Set(aggregateId)) // as in PostgreSQL (D30, F119)
    eventsWithVersions
  }}


  override def readAndProcessEvents[AGGREGATE_ROOT](aggregateId: AggregateId, upToVersion: Option[Either[AggregateVersion, Instant]])(eventHandler: (UserId, Instant, Event[AGGREGATE_ROOT], AggregateId, Int, Boolean) => Unit): Unit =
    processEvents[AGGREGATE_ROOT](aggregateId, upToVersion)((entry, eventAggregateId, noop) =>
      eventHandler(entry.userId, entry.timestamp, entry.event, eventAggregateId, entry.version.asInt, noop))

  override def readAndProcessEventsWithMetadata[AGGREGATE_ROOT](aggregateId: AggregateId, upToVersion: Option[Either[AggregateVersion, Instant]])
                                                               (eventHandler: (EventInfo[AGGREGATE_ROOT], AggregateId, Boolean) => Unit): Unit =
    processEvents[AGGREGATE_ROOT](aggregateId, upToVersion)((entry, eventAggregateId, noop) =>
      eventHandler(EventInfo(entry.version, entry.event, entry.userId, entry.timestamp, entry.metadata), eventAggregateId, noop))

  private def processEvents[AGGREGATE_ROOT](aggregateId: AggregateId, upToVersion: Option[Either[AggregateVersion, Instant]])
                                           (eventHandler: (EventStoreEntry[AGGREGATE_ROOT], AggregateId, Boolean) => Unit): Unit = {
    var eventsForAggregate: Vector[EventStoreEntry[AGGREGATE_ROOT]] = synchronized(eventStore.getOrElse(aggregateId, Vector())).asInstanceOf[Vector[EventStoreEntry[AGGREGATE_ROOT]]]

    if(upToVersion.isDefined) {
      upToVersion.get match {
        case Left(version) =>
          // D51, as in PostgreSQL: the history below the squash point is gone.
          synchronized(squashedVersions.get(aggregateId)).filter(squashed => version > AggregateVersion.ZERO && squashed > version)
            .foreach(squashed => throw new AggregateSquashedException(aggregateId, squashed, version))
          eventsForAggregate = eventsForAggregate.filter(_.version <= version)
        case Right(instant) => throw new IllegalStateException("Memory event store by instant not yet supported")
      }
    }

    val eventsWithNoop = withNoopFlags(eventsForAggregate)

    // A duplicated aggregate starts with its base's history up to the duplicated version, as the
    // aggregates chain does in PostgreSQL; base events are reported with the base's id.
    eventsForAggregate.headOption.map(_.event).foreach {
      case duplication: DuplicationEvent[_] =>
        processEvents[AGGREGATE_ROOT](duplication.baseAggregateId, Some(Left(duplication.baseAggregateVersion)))(eventHandler)
      case _ => ()
    }

    eventsWithNoop.foreach { case (entry, noop) => eventHandler(entry, aggregateId, noop) }
  }

  // As add_undo_event: an undo event marks itself and the newest `eventsCount` events not undone yet as noop.
  private def withNoopFlags[AGGREGATE_ROOT](entries: Vector[EventStoreEntry[AGGREGATE_ROOT]]): Vector[(EventStoreEntry[AGGREGATE_ROOT], Boolean)] = {
    val noop = Array.fill(entries.size)(false)
    entries.indices.foreach { i =>
      entries(i).event match {
        case undo: UndoEvent[_] =>
          (0 until i).reverseIterator.filterNot(noop(_)).take(undo.eventsCount).foreach(noop(_) = true)
          noop(i) = true
        case _ => ()
      }
    }
    entries.zip(noop)
  }

  // D49, as add_undo_event: an undo may not reach the squashed history.
  private def refuseUndoIntoSquashed(aggregateId: AggregateId, entries: Vector[EventStoreEntry[_]], eventsCount: Int): Unit =
    squashedVersions.get(aggregateId).foreach { squashed =>
      val undone = withNoopFlags(entries.asInstanceOf[Vector[EventStoreEntry[Any]]]).reverseIterator.collect { case (entry, false) => entry.version }.take(eventsCount)
      if (undone.exists(_ <= squashed)) {
        throw new IllegalStateException("Undo cannot reach the squashed history: aggregate " + aggregateId.asLong + ", squashed up to version " + squashed.asInt)
      }
    }

  // As add_duplication_event: the base exists, has reached the duplicated version (F147), and its history there is still
  // there (D50).
  private def refuseDuplicationOfSquashed(duplication: DuplicationEvent[_]): Unit = {
    val base = duplication.baseAggregateId
    if (!eventStore.contains(base)) throw new IllegalStateException("Duplication of aggregate " + base.asLong + ", which does not exist")
    val current = eventStore(base).lastOption.map(_.version).getOrElse(AggregateVersion.ZERO)
    if (duplication.baseAggregateVersion > current) throw new IllegalStateException("Duplication of aggregate " + base.asLong + " at version " +
      duplication.baseAggregateVersion.asInt + ", which it has not reached (it is at version " + current.asInt + ")")
    squashedVersions.get(base).filter(_ > duplication.baseAggregateVersion).foreach(squashed => throw new IllegalStateException(
      "Duplication cannot read the squashed history: aggregate " + base.asLong + ", squashed up to version " + squashed.asInt +
        ", duplicated at version " + duplication.baseAggregateVersion.asInt))
  }

  override def readAndProcessOwnEvents[AGGREGATE_ROOT](aggregateId: AggregateId, after: AggregateVersion, upTo: Option[AggregateVersion])
                                                      (eventHandler: (UserId, Instant, Event[AGGREGATE_ROOT], Int, Boolean) => Unit): Unit = {
    val entries = synchronized(eventStore.getOrElse(aggregateId, Vector())).asInstanceOf[Vector[EventStoreEntry[AGGREGATE_ROOT]]]
    withNoopFlags(upTo.fold(entries)(version => entries.filter(_.version <= version)))
      .filter { case (entry, _) => entry.version > after }
      .foreach { case (entry, noop) => eventHandler(entry.userId, entry.timestamp, entry.event, entry.version.asInt, noop) }
  }

  override def readSnapshot[AGGREGATE_ROOT](aggregateId: AggregateId, stateClassName: String): Option[Try[Snapshot[AGGREGATE_ROOT]]] =
    synchronized(snapshots.get(aggregateId)).map(snapshot => Try(snapshot.asInstanceOf[Snapshot[AGGREGATE_ROOT]]))

  // As in PostgreSQL: only while the aggregate is stored at that version, at the history generation of the state (F119).
  override def writeSnapshot[AGGREGATE_ROOT](aggregateId: AggregateId, snapshot: Snapshot[AGGREGATE_ROOT], stateClassName: String, generation: Int): Unit =
    synchronized {
      if (eventStore.get(aggregateId).exists(_.lastOption.exists(_.version >= snapshot.version)) && generations.getOrElse(aggregateId, 0) == generation)
        snapshots += aggregateId -> snapshot
    }

  override def historyGeneration(aggregateId: AggregateId): Int = synchronized(generations.getOrElse(aggregateId, 0))

  override def storedVersion(aggregateId: AggregateId): Option[(AggregateVersion, Int)] = synchronized {
    eventStore.get(aggregateId).flatMap(_.lastOption).map(last => (last.version, generations.getOrElse(aggregateId, 0)))
  }

  // As in PostgreSQL (D30, F119): their snapshots may be wrong; owners holding the states of before must not write them.
  private def historyChangedFor(aggregateIds: Set[AggregateId]): Unit = {
    snapshots --= aggregateIds
    aggregateIds.foreach(id => generations += id -> (generations.getOrElse(id, 0) + 1))
  }

  // The aggregates whose duplication chain includes `aggregateId`, at any depth, as the aggregates rows hold it in PostgreSQL.
  private def duplicatesOf(aggregateId: AggregateId): Set[AggregateId] = {
    val direct = eventStore.collect {
      case (id, entries) if entries.headOption.exists(entry => isDuplicateOf(entry.event, aggregateId)) => id
    }.toSet
    direct ++ direct.flatMap(duplicatesOf)
  }

  private def isDuplicateOf(event: Event[_], base: AggregateId): Boolean = event match {
    case duplication: DuplicationEvent[_] => duplication.baseAggregateId == base
    case _ => false
  }

  // ************** Squash (D47–D52), as in PostgreSQL

  override def squashedVersion(aggregateId: AggregateId): Option[AggregateVersion] = synchronized(squashedVersions.get(aggregateId))

  // Subscription failures live in the subscriptions state, which this store does not see.
  override def squashBlocker(aggregateId: AggregateId, upTo: AggregateVersion): Option[String] = synchronized {
    val entries = eventStore.getOrElse(aggregateId, Vector()).asInstanceOf[Vector[EventStoreEntry[Any]]]
    val undoneThen = withNoopFlags(entries.filter(_.version <= upTo)).map { case (entry, noop) => entry.version -> noop }.toMap
    val squashed = squashedVersions.get(aggregateId)
    if (squashed.exists(_ >= upTo)) {
      Some("already squashed up to version " + squashed.get.asInt)
    } else if (waiting(aggregateId, upTo)) {
      Some("events at or below version " + upTo.asInt + " still wait in the outbox")
    } else if (withNoopFlags(entries).exists { case (entry, noop) => entry.version <= upTo && noop != undoneThen(entry.version) }) {
      Some("an undo after version " + upTo.asInt + " undid events at or below it")
    } else detachBlocker(duplicatesReading(aggregateId, Some(upTo)), "the squash")
  }

  private def waiting(id: AggregateId, upTo: AggregateVersion): Boolean = eventsToPublish.keys.exists { case (i, version) => i == id && version <= upTo }

  // As in PostgreSQL (D51, F143), but for subscription failures, which this store does not see.
  private def detachBlocker(detached: Seq[AggregateId], by: String): Option[String] =
    detached.find(firstEventUndone).map(d => "the first event of duplicate " + d.asLong + ", which " + by + " detaches, was undone")
      .orElse(detached.find(waiting(_, AggregateVersion(1)))
        .map(d => "the first event of duplicate " + d.asLong + ", which " + by + " detaches, still waits in the outbox"))

  // As in PostgreSQL: a detach replaces the first event, and one that was undone cannot be.
  private def firstEventUndone(aggregateId: AggregateId): Boolean =
    withNoopFlags(eventStore.getOrElse(aggregateId, Vector()).asInstanceOf[Vector[EventStoreEntry[Any]]]).headOption.exists(_._2)

  private def detachingAll(found: Seq[AggregateId], detach: Seq[(AggregateId, FirstEvent[_])]): Either[String, Unit] =
    Either.cond(found.toSet == detach.map(_._1).toSet, (), "the duplicates of the aggregate changed meanwhile")

  // Without `below`, every duplicate at any depth, as in PostgreSQL. With it, only the direct ones that read the base
  // below that version: a duplicate's chain is followed through its first event here, so a deeper one reads the base
  // through them, and once they are detached it reads their squash events.
  override def duplicatesReading(baseId: AggregateId, below: Option[AggregateVersion]): Seq[AggregateId] = synchronized {
    below match {
      case None => duplicatesOf(baseId).toSeq.sortBy(_.asLong)
      case Some(version) => eventStore.collect {
        case (id, entries) if entries.headOption.exists(_.event match {
          case duplication: DuplicationEvent[_] => duplication.baseAggregateId == baseId && duplication.baseAggregateVersion < version
          case _ => false
        }) => id
      }.toSeq.sortBy(_.asLong)
    }
  }

  override def squash[AGGREGATE_ROOT](aggregateId: AggregateId, upTo: AggregateVersion, squashEvent: FirstEvent[AGGREGATE_ROOT], userId: UserId,
                                      metadata: Option[EventMetadata], detach: Seq[(AggregateId, FirstEvent[_])], archive: Set[String]): Either[String, Unit] = synchronized {
    for {
      _ <- squashBlocker(aggregateId, upTo).toLeft(())
      _ <- detachingAll(duplicatesReading(aggregateId, Some(upTo)), detach)
    } yield {
      detach.foreach(detachDuplicate(_, archive))
      archived(aggregateId, AggregateVersion(1), upTo, archive)
      eventStore += aggregateId -> eventStore.getOrElse(aggregateId, Vector()).filterNot(_.version < upTo)
      eventsRows = eventsRows.filterNot(row => row.aggregateId == aggregateId && row.aggregateVersion < upTo)
      replaceEvent(aggregateId, upTo, squashEvent, Some(userId), Some(metadata))
      eventsToPublish = eventsToPublish.filterNot { case ((id, version), _) => id == aggregateId && version <= upTo }
      // D85: an event that may change the state takes the snapshots of the aggregate and of the duplicates still reading it.
      if (squashEvent.isInstanceOf[SquashMayChangeState]) historyChangedFor(duplicatesOf(aggregateId) + aggregateId)
      else if (snapshots.get(aggregateId).exists(_.version <= upTo)) snapshots -= aggregateId
      squashedVersions += aggregateId -> upTo
    }
  }

  // As in PostgreSQL: the duplicate's first event becomes `firstEvent`; one that may change the state takes its snapshot (D85).
  // The bases it read are recorded first (D76).
  private def detachDuplicate(duplicate: (AggregateId, FirstEvent[_]), archive: Set[String]): Unit = {
    val (duplicateId, firstEvent) = duplicate
    lineage += duplicateId -> (lineage.getOrElse(duplicateId, Set.empty) ++ chainOf(duplicateId).toSet)
    archived(duplicateId, AggregateVersion(1), AggregateVersion(1), archive)
    replaceEvent(duplicateId, AggregateVersion(1), firstEvent, None, None)
    if (firstEvent.isInstanceOf[SquashMayChangeState]) historyChangedFor(Set(duplicateId))
  }

  // The bases a duplicate reads, nearest first, through the first events of its chain.
  private def chainOf(aggregateId: AggregateId): List[AggregateId] = eventStore.get(aggregateId).flatMap(_.headOption).map(_.event) match {
    case Some(duplication: DuplicationEvent[_]) => duplication.baseAggregateId :: chainOf(duplication.baseAggregateId)
    case _ => Nil
  }

  // The event at `version` becomes `event`, keeping its place and time; the user and metadata change when given.
  private def replaceEvent(aggregateId: AggregateId, version: AggregateVersion, event: Event[_], userId: Option[UserId], metadata: Option[Option[EventMetadata]]): Unit = {
    eventStore += aggregateId -> eventStore.getOrElse(aggregateId, Vector()).map(entry =>
      if (entry.version == version) entry.asInstanceOf[EventStoreEntry[Any]].copy(event = event.asInstanceOf[Event[Any]],
        userId = userId.getOrElse(entry.userId), metadata = metadata.getOrElse(entry.metadata)) else entry)
    eventsRows = eventsRows.map(row =>
      if (row.aggregateId == aggregateId && row.aggregateVersion == version) row.copy(event = event, userId = userId.getOrElse(row.userId),
        metadata = metadata.getOrElse(row.metadata)) else row)
  }

  // The sizes are not known here (0), so no page is cut by its bytes.
  override def readCommittedPage(types: Seq[AggregateType], after: Long, limit: Int, scope: AggregateScope, maxBytes: Long): CommittedPage = synchronized {
    val wanted = types.toSet
    CommittedPage(eventsRows.reverseIterator.filter(row => row.eventId > after && wanted.contains(row.aggregateType) && inScope(row.aggregateId, scope)).take(limit).map { row =>
      CommittedEvent(row.eventId, row.aggregateType, row.aggregateId, row.aggregateVersion, squashedVersions.get(row.aggregateId),
        Right(EventInfo[Any](row.aggregateVersion, row.event.asInstanceOf[Event[Any]], row.userId, row.timestamp, row.metadata)))
    }.toVector, cut = false)
  }

  override def aggregatesOf(types: Seq[AggregateType], after: AggregateId, limit: Int, scope: AggregateScope = AggregateScope.All): Seq[StoredAggregate] = synchronized {
    val wanted = types.toSet
    eventsRows.reverseIterator.filter(row => row.aggregateId.asLong > after.asLong && wanted.contains(row.aggregateType))
      .map(row => row.aggregateId -> row.aggregateType).toVector.distinct.filter(a => inScope(a._1, scope)).sortBy(_._1.asLong).take(limit)
      .map { case (id, aggregateType) => StoredAggregate(aggregateType, id, eventStore(id).last.version, squashedVersions.get(id)) }
  }

  // The space is the first event's, as PostgreSQL keeps it with the aggregate (D38).
  private def inScope(id: AggregateId, scope: AggregateScope): Boolean = scope match {
    case AggregateScope.All => true
    case AggregateScope.Only(ids) => ids.contains(id)
    case AggregateScope.InSpace(spaceId) => eventStore.get(id).flatMap(_.headOption).exists(_.event match {
      case first: FirstEvent[_] => first.spaceId == spaceId
      case _ => false
    })
  }

  // Like the SQL DELETE: deleting an event that is no longer waiting is a no-op.
  override def deletePublished(aggregateId: AggregateId, versions: Seq[AggregateVersion]): Unit = synchronized {
    eventsToPublish --= versions.map(version => (aggregateId, version))
  }

  // R11, as in PostgreSQL: the events from `from` to `upTo` of the classes in `archive`, with their undo marks.
  private def archived(aggregateId: AggregateId, from: AggregateVersion, upTo: AggregateVersion, archive: Set[String]): Unit = if (archive.nonEmpty) {
    val entries = eventStore.getOrElse(aggregateId, Vector()).asInstanceOf[Vector[EventStoreEntry[Any]]]
    val undone = undoneFrom(entries)
    val now = Instant.now
    val existing = archives.getOrElse(aggregateId, Vector())
    val kept = entries.filter(e => e.version >= from && e.version <= upTo && archive.contains(e.event.getClass.getName) &&
        !existing.exists(_.info.version == e.version))
      .map(e => ArchivedEvent[Any](EventInfo(e.version, e.event, e.userId, e.timestamp, e.metadata), undone.get(e.version), now))
    archives += aggregateId -> (existing ++ kept).sortBy(_.info.version.asInt)
  }

  // As add_undo_event: the version of the undo that marked each undone event, and of each undo event itself.
  private def undoneFrom(entries: Vector[EventStoreEntry[Any]]): Map[AggregateVersion, AggregateVersion] = {
    var marks = Map[AggregateVersion, AggregateVersion]()
    entries.indices.foreach { i =>
      entries(i).event match {
        case undo: UndoEvent[_] =>
          (0 until i).reverseIterator.filterNot(j => marks.contains(entries(j).version)).take(undo.eventsCount)
            .foreach(j => marks += entries(j).version -> entries(i).version)
          marks += entries(i).version -> entries(i).version
        case _ => ()
      }
    }
    marks
  }

  override def readArchive[AGGREGATE_ROOT](aggregateId: AggregateId): Seq[ArchivedEvent[AGGREGATE_ROOT]] = synchronized {
    archives.getOrElse(aggregateId, Vector()).asInstanceOf[Vector[ArchivedEvent[AGGREGATE_ROOT]]]
  }

  // ************** Complete rewrite (D76), as in PostgreSQL; the audit rows are not kept here.

  override def rewriteEverywhere[AGGREGATE_ROOT](aggregateId: AggregateId, userId: UserId, change: HistoryChange,
                                                alter: (Event[AGGREGATE_ROOT], Option[EventMetadata]) => (Event[AGGREGATE_ROOT], Option[EventMetadata]))
                                               (implicit session: DBSession): Either[String, RewriteCount] = synchronized {
    val entries = eventStore.getOrElse(aggregateId, Vector()).asInstanceOf[Vector[EventStoreEntry[AGGREGATE_ROOT]]]
    val archived = archives.getOrElse(aggregateId, Vector()).asInstanceOf[Vector[ArchivedEvent[AGGREGATE_ROOT]]]
    // (archived, version, before, after)
    val altered = entries.map(e => (false, e.version, (e.event, e.metadata), alter(e.event, e.metadata))) ++
      archived.map(a => (true, a.info.version, (a.info.event, a.info.storedMetadata), alter(a.info.event, a.info.storedMetadata)))
    val changed = altered.filter { case (_, _, before, after) => before != after }
    changed.collectFirst {
      case (inArchive, version, (before, _), (after, _)) if after.getClass != before.getClass =>
        "the change makes " + (if (inArchive) "archived event " else "event ") + version.asInt + " of aggregate " + aggregateId.asLong + " a " + after.getClass.getName + " (D17)"
      case (inArchive, version, (before, _), (after, _)) if after != before && (before.isInstanceOf[UndoEvent[_]] || before.isInstanceOf[DuplicationEvent[_]]) =>
        "the change alters " + (if (inArchive) "archived event " else "event ") + version.asInt + " of aggregate " + aggregateId.asLong +
          ", an undo or a duplication, whose meaning the store keeps elsewhere"
    }.toLeft(()).map { _ =>
      changed.foreach {
        case (false, version, _, (event, metadata)) => replaceEvent(aggregateId, version, event, None, Some(metadata))
        case (true, version, _, (event, metadata)) => archives += aggregateId -> archives(aggregateId).map(a =>
          if (a.info.version == version) a.asInstanceOf[ArchivedEvent[AGGREGATE_ROOT]].copy(info = a.info.asInstanceOf[EventInfo[AGGREGATE_ROOT]]
            .copy(event = event, storedMetadata = metadata)) else a)
      }
      if (changed.nonEmpty) historyChangedFor(duplicatesOf(aggregateId) + aggregateId)
      RewriteCount(changed.count(!_._1), changed.count(_._1), changed.map(_._3._1.getClass.getName).toSet,
        changed.flatMap { case (_, _, (before, beforeMetadata), (after, afterMetadata)) =>
          RewriteCount.fieldsChanged(before, after) ++ RewriteCount.fieldsChanged(beforeMetadata.orNull, afterMetadata.orNull, "metadata.")
        }.toSet)
    }
  }

  /** Whether the store has events of the aggregate. */
  def holds(aggregateId: AggregateId): Boolean = synchronized(eventStore.contains(aggregateId))

  /** How many aggregates the store has events of. */
  def aggregatesHeld: Int = synchronized(eventStore.size)

  /** Drops everything the store keeps of the aggregate (D77: an ephemeral aggregate dropped or materialized). */
  def forgetAggregate(aggregateId: AggregateId): Unit = synchronized {
    eventStore -= aggregateId
    if (commitOrder) eventsRows = eventsRows.filterNot(_.aggregateId == aggregateId)
    eventsToPublish = eventsToPublish.filterNot { case ((id, _), _) => id == aggregateId }
    snapshots -= aggregateId
    squashedVersions -= aggregateId
    generations -= aggregateId
    archives -= aggregateId
    lineage -= aggregateId
  }

  override def copiesOf(aggregateId: AggregateId): Seq[AggregateId] = synchronized {
    (duplicatesOf(aggregateId) ++ lineage.collect { case (copy, bases) if bases.contains(aggregateId) => copy }).toSeq.sortBy(_.asLong)
  }

  override def basesOf(aggregateId: AggregateId): Seq[AggregateId] = synchronized(chainOf(aggregateId))

  // As in PostgreSQL: only the duplicates given, each of which must still read the aggregate.
  override def detachDuplicates(baseId: AggregateId, detach: Seq[(AggregateId, FirstEvent[_])], archive: Set[String]): Either[String, Unit] = synchronized {
    val detached = detach.map(_._1)
    for {
      _ <- Either.cond(detached.forall(duplicatesReading(baseId, None).contains), (), "the duplicates of the aggregate changed meanwhile")
      _ <- detachBlocker(detached, "the purge").toLeft(())
    } yield detach.foreach(detachDuplicate(_, archive))
  }

  // Subscription failures live in the subscriptions state, which this store does not see.
  override def deleteAggregatePermanently(aggregateId: AggregateId, detach: Seq[(AggregateId, FirstEvent[_])], archive: Set[String]): Either[String, Unit] = synchronized {
    val duplicates = duplicatesReading(aggregateId, None)
    for {
      _ <- detachBlocker(duplicates, "the delete").toLeft(())
      _ <- detachingAll(duplicates, detach)
    } yield {
      detach.foreach(detachDuplicate(_, archive))
      snapshots -= aggregateId
      archives -= aggregateId
      eventsRows = eventsRows.filterNot(_.aggregateId == aggregateId)
      eventStore -= aggregateId
      eventsToPublish = eventsToPublish.filterNot { case ((eventAggregateId, _), _) => eventAggregateId == aggregateId }
      squashedVersions -= aggregateId
      lineage = (lineage - aggregateId).map { case (copy, bases) => copy -> (bases - aggregateId) }.filter(_._2.nonEmpty) // D76
    }
  }

  override def readWaitingInOutbox(limit: Int = Int.MaxValue): Seq[WaitingInOutbox] = synchronized {
    val typeOf = eventsRows.map(row => row.aggregateId -> row.aggregateType).toMap
    eventsToPublish.toSeq.groupBy { case ((aggregateId, _), _) => aggregateId }.toSeq.map { case (aggregateId, rows) =>
      val oldest = rows.minBy { case ((_, version), _) => version.asInt }
      WaitingInOutbox(typeOf(aggregateId), aggregateId, oldest._1._2, rows.map { case (_, (_, timestamp, _, _)) => timestamp }.min)
    }.sortBy(w => (w.since, w.aggregateId.asLong)).take(limit)
  }

  // The sizes are not known here (0), so every event of the range fits.
  override def readPart[AGGREGATE_ROOT](aggregateId: AggregateId, from: AggregateVersion, to: AggregateVersion,
                                        maxBytes: Long): Seq[(EventInfo[AGGREGATE_ROOT], Int)] = {
    synchronized(eventStore.getOrElse(aggregateId, Vector.empty))
      .filter(entry => entry.version >= from && entry.version <= to)
      .map(entry => (EventInfo[AGGREGATE_ROOT](entry.version, entry.event.asInstanceOf[Event[AGGREGATE_ROOT]], entry.userId, entry.timestamp, entry.metadata), 0))
  }

  override def oldestVersionToPublish(aggregateId: AggregateId): Option[AggregateVersion] = synchronized {
    val versions = eventsToPublish.keys.collect { case (eventAggregateId, version) if eventAggregateId == aggregateId => version.asInt }
    if (versions.isEmpty) None else Some(AggregateVersion(versions.min))
  }

  override def localTx[A](block: (DBSession) => A): A = {
    block(NoSession)
  }

  override def squashCandidates(aggregateType: AggregateType, policy: AutomaticSquash, after: AggregateId, limit: Int,
                                now: Instant): Seq[(AggregateId, AggregateVersion)] = synchronized {
    eventsRows.collect { case row if row.aggregateType == aggregateType && row.aggregateId.asLong > after.asLong => row.aggregateId }
      .distinct.sortBy(_.asLong).iterator.flatMap { id =>
        val entries = eventStore(id)
        // R11, as in PostgreSQL: the newest version older than `keepNewerThan`, if that is lower.
        val olderThan = policy.keepNewerThan.map(age => now.minusMillis(age.toMillis))
        val byAge = olderThan.map(at => entries.filter(_.timestamp.isBefore(at)).lastOption.map(_.version.asInt).getOrElse(0))
        val upTo = math.min(entries.last.version.asInt - policy.keepLast, byAge.getOrElse(Int.MaxValue))
        if (upTo - squashedVersions.get(id).map(_.asInt).getOrElse(0) >= policy.afterEvents) Some(id -> AggregateVersion(upTo)) else None
      }.take(limit).toSeq
  }

  // The SQL version updates the stored event in place; restore and re-publication then read the new one.
  override def overwriteEvents[AGGREGATE_ROOT](aggregateId: AggregateId, events: Iterable[EventWithVersion[AGGREGATE_ROOT]])(implicit session: DBSession): Unit = synchronized {
    // D17, as in PostgreSQL: a rewritten event keeps its class. Checked first: there is no transaction to roll back.
    events.find(rewritten => !eventStore.getOrElse(aggregateId, Vector()).exists(entry =>
      entry.version == rewritten.version && entry.event.getClass == rewritten.event.getClass)).foreach { rewritten =>
      throw new RewriteChangesClassException(aggregateId, rewritten.version, rewritten.event.getClass.getName)
    }
    historyChangedFor(duplicatesOf(aggregateId) + aggregateId) // as in PostgreSQL (D30, F119)
    events.foreach { rewritten =>
      eventStore += aggregateId -> eventStore.getOrElse(aggregateId, Vector()).map(entry =>
        if (entry.version == rewritten.version) entry.asInstanceOf[EventStoreEntry[AGGREGATE_ROOT]].copy(event = rewritten.event) else entry)
      eventsRows = eventsRows.map(row =>
        if (row.aggregateId == aggregateId && row.aggregateVersion == rewritten.version) row.copy(event = rewritten.event) else row)
      eventsToPublish.get((aggregateId, rewritten.version)).foreach { case (userId, timestamp, _, eventId) =>
        eventsToPublish += (aggregateId, rewritten.version) -> (userId, timestamp, rewritten.event, eventId)
      }
    }
  }

}

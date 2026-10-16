package io.reactivecqrs.core.aggregate

import java.time.Instant

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.eventstore.{EventStoreState, Snapshot}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/** An aggregate's version and root as read from the store; the root is null once the aggregate was deleted.
  * `fromSnapshot`: the version of the snapshot the state was read from, if one was used. */
case class Restored[AGGREGATE_ROOT](version: AggregateVersion, root: AGGREGATE_ROOT, fromSnapshot: Option[AggregateVersion] = None)

/**
  * Reads aggregates of one type from the event store: their state at the newest version, at a version or at an
  * instant, and the events behind it. It hides how the store keeps a history: duplication chains (a duplicate starts
  * with its base's events), undone events and snapshots (D30): with a snapshot policy it starts from the aggregate's
  * snapshot when that snapshot fits the context and schema versions and lies at or before the version asked for, and
  * reads only the events after it; a missing, unfitting or unreadable snapshot means reading every event, never a
  * failure (R10). The owner restores through it, and the command bus answers historical reads with it. Synchronous: it
  * reads on the caller's thread.
  */
class AggregateRestorer[AGGREGATE_ROOT](eventStore: EventStoreState,
                                        eventHandlers: (UserId, Instant, AGGREGATE_ROOT) => PartialFunction[Any, AGGREGATE_ROOT],
                                        initialState: () => AGGREGATE_ROOT,
                                        snapshots: Option[SnapshotPolicy] = None,
                                        contextVersion: Int = 0,
                                        stateClassName: String = "") {

  private val log = LoggerFactory.getLogger(classOf[AggregateRestorer[_]])

  /** The aggregate at its newest version, or at `at`. A version counts only the aggregate's own events, not those of
    * the base it was duplicated from. Fails when an event cannot be read or handled. */
  def restore(aggregateId: AggregateId, at: Option[Either[AggregateVersion, Instant]] = None): Restored[AGGREGATE_ROOT] = {
    usableSnapshot(aggregateId, at) match {
      case Some(snapshot) =>
        var version = snapshot.version
        var root = snapshot.state
        eventStore.readAndProcessOwnEvents[AGGREGATE_ROOT](aggregateId, snapshot.version, at.flatMap(_.left.toOption)) { (userId, timestamp, event, eventVersion, noop) =>
          if (!noop) root = eventHandlers(userId, timestamp, root)(event)
          version = AggregateVersion(eventVersion)
        }
        Restored(version, root, Some(snapshot.version))
      case None =>
        var version = AggregateVersion.ZERO
        var root = initialState()
        // The version is the last own event's: after a squash (D47) the own events start at the squash point.
        eventStore.readAndProcessEvents[AGGREGATE_ROOT](aggregateId, at) { (userId, timestamp, event, eventAggregateId, eventVersion, noop) =>
          if (!noop) root = eventHandlers(userId, timestamp, root)(event)
          if (eventAggregateId == aggregateId) version = AggregateVersion(eventVersion)
        }
        Restored(version, root)
    }
  }

  // Reads at an instant do not use snapshots: a snapshot does not record when its version was reached.
  private def usableSnapshot(aggregateId: AggregateId, at: Option[Either[AggregateVersion, Instant]]): Option[Snapshot[AGGREGATE_ROOT]] = snapshots match {
    case Some(policy) if at.forall(_.isLeft) =>
      Try(eventStore.readSnapshot[AGGREGATE_ROOT](aggregateId, stateClassName)) match {
        case Success(Some(Success(snapshot))) =>
          Some(snapshot).filter(s => s.contextVersion == contextVersion && s.schemaVersion == policy.schemaVersion &&
            at.forall(_.left.forall(s.version <= _)))
        case Success(Some(Failure(e))) =>
          log.warn("The snapshot of aggregate " + aggregateId.asLong + " cannot be read; reading all of its events instead", e)
          None
        case Success(None) => None
        case Failure(e) =>
          log.warn("Reading the snapshot of aggregate " + aggregateId.asLong + " failed; reading all of its events instead", e)
          None
      }
    case _ => None
  }

  /** Stores the aggregate's state at `version` as its snapshot, if the context takes snapshots. A failure is logged and
    * changes nothing else: the aggregate is then restored from an older snapshot or from its events. */
  def writeSnapshot(aggregateId: AggregateId, version: AggregateVersion, root: AGGREGATE_ROOT): Boolean = snapshots.exists { policy =>
    Try(eventStore.writeSnapshot(aggregateId, Snapshot(version, contextVersion, policy.schemaVersion, root), stateClassName)) match {
      case Success(_) => true
      case Failure(e) =>
        log.warn("Writing the snapshot of aggregate " + aggregateId.asLong + " at version " + version.asInt + " failed", e)
        false
    }
  }

  /** Every event of the aggregate up to its newest version or `at`, its base's included, undone ones too, in order,
    * with their metadata envelopes (D32). */
  def events(aggregateId: AggregateId, at: Option[Either[AggregateVersion, Instant]] = None): List[EventInfo[AGGREGATE_ROOT]] = {
    var events = List[EventInfo[AGGREGATE_ROOT]]()
    eventStore.readAndProcessEventsWithMetadata[AGGREGATE_ROOT](aggregateId, at)((event, _, _) => events ::= event)
    events.reverse
  }

  /** The aggregate's events of the given classes, for a history rewrite. */
  def eventsOfClasses(aggregateId: AggregateId, classNames: Set[String]): List[EventWithVersion[AGGREGATE_ROOT]] = {
    var events = List[EventWithVersion[AGGREGATE_ROOT]]()
    eventStore.readAndProcessEvents[AGGREGATE_ROOT](aggregateId, None) { (_, _, event, _, eventVersion, _) =>
      if (classNames.contains(event.getClass.getName)) events ::= EventWithVersion(AggregateVersion(eventVersion), event)
    }
    events.reverse
  }
}

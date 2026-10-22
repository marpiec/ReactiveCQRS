package io.reactivecqrs.core.rebuild

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.delivery.NothingToRepublish
import io.reactivecqrs.core.eventbus.EventsBusActor.{MessageAck, MessageFailed, RequestRepublish}
import io.reactivecqrs.core.eventstore.{AggregateScope, CommittedEvent, EventStoreState, RestoreFailedAtEventException, RestoreFailureSeen, RestoreFailureSource, StoredAggregate}
import io.reactivecqrs.core.projection._
import io.reactivecqrs.core.rebuild.ProjectionBuilder._
import io.reactivecqrs.core.util.MyActorLogging
import org.apache.pekko.actor.{Actor, ActorRef, Status, Terminated}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.control.NonFatal
import io.reactivecqrs.core.util.Errors

/** The builder's messages and settings: it builds a managed projection from the event store (D57, chapter 10). */
object ProjectionBuilder {

  /** What a build delivers: the projection's subscriptions, one per aggregate type and listener kind (D58). */
  case class BuildPlan(subscriptions: Seq[(AggregateType, SubscriptionType)]) {
    val types: Seq[AggregateType] = subscriptions.map(_._1).distinct
    /** D62: a projection whose listeners all receive the state only is built with each aggregate's final state. */
    val finalStates: Boolean = subscriptions.nonEmpty && subscriptions.forall(_._2 == SubscriptionType.AGGREGATES)
    def kindsOf(aggregateType: AggregateType): Seq[SubscriptionType] = subscriptions.collect { case (`aggregateType`, kind) => kind }
  }

  /** How far a build got, kept so it continues after a crash (D61): the last event (or, building final states, the
    * last aggregate) of the bulk pass that the projection applied, or that the bulk pass is done. */
  case class BuildPosition(bulkDone: Boolean, position: Long)
  object BuildPosition {
    /** Nothing applied yet. */
    val Start: BuildPosition = BuildPosition(bulkDone = false, 0L)
  }

  /** Where a build's position is kept: the registry, per build. */
  trait BuildPositions {
    /** The position saved last. */
    def read(): BuildPosition
    /** Saves the position. */
    def save(position: BuildPosition): Unit
  }

  /** @param pageSize events (or aggregates) read from the store at once, and at most `pageBytes` of events (F126)
    * @param chunkEvents events per chunk, one transaction of the projection, and at most `chunkBytes` of events (F126)
    * @param chunksInFlight chunks sent and not yet applied: the next page is read while the projection applies them
    * @param maxReplayStates aggregate states kept open by a commit-order replay (D59)
    * @param retryAfter the wait after the store or the projection failed */
  case class BuilderSettings(pageSize: Int = 1000, chunkEvents: Int = 1000, chunksInFlight: Int = 2, maxReplayStates: Int = 10000,
                             retryAfter: FiniteDuration = 5.seconds, pageBytes: Long = 64L * 1024 * 1024, chunkBytes: Long = 16L * 1024 * 1024)

  /** To `reportTo`: the projection is live and has applied every event committed before it went live, or recorded a
    * failure for it (D20a). */
  case object BuildCaughtUp

  /** Answered by a projection's supervisor too (F152): CR-7, at once, with the status its running builder reported last. */
  case object GetBuildStatus
  /** `phase`: `Bulk`, `GoingLive`, `Reconciling` or `CaughtUp` from a builder (`Starting` before its first report); from
    * the supervisor without one, `Waiting` (a build waits for its turn, a restart, or, with `heldBy` saying why, a hold of
    * full builds: CR-2) or `NoBuild`. `position`: how far the bulk pass got. `reportedAt` (CR-7): when the builder
    * reported these numbers, which it does after each chunk and every 2 s; a builder in a long read reports late.
    * `statesRestored`: replay states read again from the store as more were open than `maxReplayStates` (D59). */
  case class BuildStatus(phase: String, position: Long, eventsSent: Long, gapsFilled: Long, handedOver: Long, unreadable: Long,
                         heldBy: Option[String] = None, reportedAt: Option[java.time.Instant] = None, statesRestored: Long = 0)
  object BuildStatus {
    /** No build runs. */
    val NoBuild: BuildStatus = BuildStatus("NoBuild", 0, 0, 0, 0, 0)
    /** A build waits, at `position`, held for `heldBy` if given. */
    def waiting(position: Long, heldBy: Option[String] = None): BuildStatus = BuildStatus("Waiting", position, 0, 0, 0, 0, heldBy)
  }

  /** CR-7: to its `reportTo`, the builder's status now. */
  private[rebuild] case class BuildStatusReport(status: BuildStatus)

  private case object Retry
  private case object ReportStatus
}

/**
  * Builds a projection from the event store, delivering only to it (D58), in chunks of many aggregates (D61):
  *
  *  1. The bulk pass reads the events of the projection's aggregate types in commit order (D59), every event as live
  *     delivery sent it, undone and undo events too, with each aggregate's state as it was at the last version of each
  *     run (D60), from the bounded `ReplayStates`. A projection whose listeners all receive the state only gets each
  *     aggregate's final state instead (D62). Meanwhile the projection acknowledges the bus's deliveries without applying
  *     them (`BuildFromStore`): their events are in the store.
  *  2. `GoLive`: from then on the projection applies the bus's deliveries.
  *  3. The reconciliation compares every aggregate's version with the projection's and delivers what it lacks: the events
  *     committed after the bulk pass read past them, and rows a commit made visible late (row ids are not commit order
  *     between concurrent transactions).
  *
  * Missing versions the projection reports (`ChunkApplied.gaps`, `RequestRepublish`) are read again by version. The
  * position of the bulk pass is saved after each chunk the projection applied, so a build continues after a crash
  * and repeats at most the chunks in flight; the projection's own versions make that repetition harmless. Reads are
  * synchronous on the builder's thread: create it on the blocking dispatcher.
  *
  * CR-4: with `restoreFailures` (the build it builds), an aggregate it cannot restore or read is recorded in the store
  * (`EventStoreState.recordRestoreFailure`), in any phase. CR-7: with `reportStatus`, it tells `reportTo` its status
  * (`BuildStatusReport`) as it starts, after each chunk applied, at each phase and every 2 s, so its supervisor answers
  * status questions without waiting on its reads.
  */
class ProjectionBuilder(target: ActorRef, subscriberName: String, plan: BuildPlan, eventStore: EventStoreState,
                        subscriptionsState: SubscriptionsState, sources: Map[AggregateType, ReplaySource], positions: BuildPositions,
                        reportTo: ActorRef, settings: BuilderSettings = BuilderSettings(), targeted: Option[AggregateScope] = None,
                        restoreFailures: Option[RestoreFailureSource.Build] = None, reportStatus: Boolean = false)
  extends Actor with MyActorLogging {

  // A targeted rebuild (D63) delivers the aggregates in scope to a projection that is live already: no BuildFromStore,
  // no GoLive.
  private val scope = targeted.getOrElse(AggregateScope.All)

  private sealed trait Phase
  private case object Bulk extends Phase
  private case object GoingLive extends Phase
  private case object Reconciling extends Phase
  private case object CaughtUp extends Phase

  private val states = new ReplayStates(sources, settings.maxReplayStates)

  private var phase: Phase = Bulk
  private var position = 0L // the pass's read position: a row id, or an aggregate id
  private var readAll = false
  private var waitingToRetry = false
  // Chunks to send with the position the bulk pass reaches once each is applied, and those sent, oldest first.
  private val ready = mutable.Queue[(ApplyChunk, Long)]()
  private val inFlight = mutable.Queue[(ApplyChunk, Long)]()
  // F223: what the gaps and the reconciliation lack (the gaps first), read a chunk's worth of aggregates at a time when
  // nothing else is ready, so memory holds a few chunks, not every missing event of a page. Its chunks, and those to send
  // again, go before a bulk page's (`first`); they save no position.
  private val work = mutable.Queue[() => Seq[ChunkItem]]()
  private val first = mutable.Queue[(ApplyChunk, Long)]()
  private var chunkIds = 0L
  private var outstanding = 0 // items the projection handed to its queues, not answered yet

  private var eventsSent = 0L
  private var gapsFilled = 0L
  private var handedOver = 0L
  private var unreadable = 0L

  private val reports = if (reportStatus) Some(context.system.scheduler.scheduleWithFixedDelay(2.seconds, 2.seconds, self, ReportStatus)(context.dispatcher)) else None

  override def postStop(): Unit = reports.foreach(_.cancel())

  private def status: BuildStatus = BuildStatus(phase.toString, position, eventsSent, gapsFilled, handedOver, unreadable,
    reportedAt = Some(java.time.Instant.now), statesRestored = states.restored)

  private def report(): Unit = if (reportStatus) reportTo ! BuildStatusReport(status)

  override def preStart(): Unit = {
    report()
    context.watch(target)
    val start = positions.read()
    if (start.bulkDone) {
      goLive()
    } else {
      position = start.position
      if (targeted.isEmpty) target ! BuildFromStore(self)
      log.info((if (targeted.isDefined) "Rebuilding " + scope + " of " else "Building ") + subscriberName +
        (if (position > 0) " from position " + position else "") +
        (if (plan.finalStates) " from the final states of its aggregates (D62)" else " from the events in commit order (D59)"))
      pump()
    }
  }

  override def receive: Receive = {
    case ChunkApplied(id, gaps, handed) =>
      inFlight.dequeueFirst(_._1.id == id).foreach { case (_, reached) =>
        if (phase == Bulk && reached >= 0) save(BuildPosition(bulkDone = false, reached))
      }
      outstanding += handed
      handedOver += handed
      fill(gaps.map(gap => (gap.aggregateType, gap.aggregateId, gap.from)))
      pump()
      report()
    case _: MessageAck | _: MessageFailed =>
      outstanding -= 1
      pump()
    case RequestRepublish(_, aggregateType, aggregateId, from) =>
      fill(Seq((aggregateType, aggregateId, from)))
      pump()
    case ProjectionLive if phase == GoingLive =>
      log.info("Projection " + subscriberName + " is live; reconciling it with the store")
      reconcile()
    case Status.Failure(e) =>
      // The projection could not take a chunk (its store is down): the chunks in flight are sent again, after a wait.
      log.warning("Projection " + subscriberName + " failed on a chunk, sending it again in " + settings.retryAfter + ": " + Errors.describe(e))
      inFlight.reverseIterator.foreach(sent => first.prepend(sent)) // re-sent first, in their order
      inFlight.clear()
      retryLater()
    case Retry =>
      waitingToRetry = false
      pump()
    case GetBuildStatus => sender() ! status
    case ReportStatus => report()
    case Terminated(`target`) =>
      log.warning("Projection " + subscriberName + " stopped; its build stops")
      context.stop(self)
  }

  // Sends what is ready while the window has room, reads the next page when nothing is (also past pages that give
  // nothing to send, such as aggregates the projection has whole), and moves to the next phase when the pass is read
  // and every chunk and handed-over item is answered.
  private def pump(): Unit = if (!waitingToRetry) {
    var more = true
    while (more) {
      var next = if (inFlight.size < settings.chunksInFlight) nextChunk() else None
      while (next.isDefined) {
        val sent = next.get
        target ! sent._1
        eventsSent += sent._1.items.map(item => math.max(item.events.size, 1)).sum
        inFlight.enqueue(sent)
        next = if (inFlight.size < settings.chunksInFlight) nextChunk() else None
      }
      more = inFlight.size < settings.chunksInFlight && nothingToSend && !readAll && readNextPage()
    }
    if (readAll && nothingToSend && inFlight.isEmpty && outstanding == 0) phase match {
      case Bulk => goLive()
      case Reconciling =>
        phase = CaughtUp
        log.info("Projection " + subscriberName + " is built: " + eventsSent + " events delivered, " + gapsFilled + " gaps filled, " +
          handedOver + " items applied one at a time, " + unreadable + " unreadable")
        report()
        reportTo ! BuildCaughtUp
        context.stop(self) // with its replay states
      case _ => ()
    }
  }

  private def nothingToSend: Boolean = first.isEmpty && ready.isEmpty && work.isEmpty

  // The next chunk to send: one sent before and not taken, one made from the next aggregates missing versions, a page's.
  // F312: when the store cannot be read for what an aggregate lacks, nothing more is sent until it is read again (the
  // gaps go before a page's chunks).
  private def nextChunk(): Option[(ApplyChunk, Long)] = {
    var readable = true
    while (readable && first.isEmpty && work.nonEmpty) {
      val (items, read) = aChunkOfWork()
      readable = read
      chunked(items.map(_ -> -1L), -1L).foreach(first.enqueue)
    }
    if (first.nonEmpty) Some(first.dequeue()) else if (readable && ready.nonEmpty) Some(ready.dequeue()) else None
  }

  // The items of the next aggregates waiting, until they fill a chunk: small aggregates share one. F312: a transient
  // failure of the store leaves the aggregate waiting, read again after `retryAfter` (false), as a page is; before
  // F223's lazy reads it was a page's, and a builder stopped by it would start the reconciliation again.
  private def aChunkOfWork(): (Seq[ChunkItem], Boolean) = {
    val items = Vector.newBuilder[ChunkItem]
    var (events, bytes) = (0, 0L)
    var read = true
    while (read && work.nonEmpty && events < settings.chunkEvents && bytes < settings.chunkBytes) {
      try {
        val made = work.head()
        work.dequeue()
        items ++= made
        events += made.map(item => math.max(item.events.size, 1)).sum
        bytes += made.map(_.bytes).sum
      } catch {
        case NonFatal(e) =>
          log.error(e, "Building " + subscriberName + " could not read what an aggregate lacks; trying again in " + settings.retryAfter)
          retryLater()
          read = false
      }
    }
    (items.result(), read)
  }

  private def goLive(): Unit = {
    save(BuildPosition(bulkDone = true, 0L))
    if (targeted.isDefined) reconcile() else {
      phase = GoingLive
      report()
      target ! GoLive
    }
  }

  private def reconcile(): Unit = {
    phase = Reconciling
    report()
    position = 0L
    readAll = false
    pump()
  }

  // Reads one page of the current pass into `ready`; false when the store failed, to be tried again later.
  private def readNextPage(): Boolean = try {
    var cut = false // a page its bytes ended is short, but not the last (F126)
    val (items, reached, count) = phase match {
      case Bulk if plan.finalStates => finalStates(eventStore.aggregatesOf(plan.types, AggregateId(position), settings.pageSize, scope))
      case Bulk =>
        val page = eventStore.readCommittedPage(plan.types, position, settings.pageSize, scope, settings.pageBytes)
        cut = page.cut
        committed(page.events)
      case _ => behind(eventStore.aggregatesOf(plan.types, AggregateId(position), settings.pageSize, scope))
    }
    readAll = count < settings.pageSize && !cut
    position = reached
    chunked(items, reached).foreach(ready.enqueue)
    true
  } catch {
    case NonFatal(e) =>
      log.error(e, "Building " + subscriberName + " could not read the store at position " + position + "; trying again in " + settings.retryAfter)
      retryLater()
      false
  }

  private def retryLater(): Unit = if (!waitingToRetry) {
    waitingToRetry = true
    context.system.scheduler.scheduleOnce(settings.retryAfter, self, Retry)(context.dispatcher)
  }

  // ************** The bulk pass

  // Consecutive events of one aggregate become one run, delivered to each of its listener kinds. A run ends where a
  // version is missing (a row committed late), so the projection sees the gap and it is read again by version, and
  // where it fills a chunk, so it fits one (F140).
  private def committed(rows: Seq[CommittedEvent]): (Seq[(ChunkItem, Long)], Long, Int) = {
    val items = Vector.newBuilder[(ChunkItem, Long)]
    var run = Vector[CommittedEvent]()
    var runBytes = 0L
    def endRun(): Unit = if (run.nonEmpty) {
      items ++= itemsOf(run).map(item => item.copy(bytes = runBytes) -> run.last.id)
      run = Vector.empty
      runBytes = 0L
    }
    rows.foreach { row =>
      if (run.nonEmpty && (run.head.aggregateId != row.aggregateId || run.last.event.isLeft || row.version.asInt != run.last.version.asInt + 1 ||
        run.size >= settings.chunkEvents || runBytes + row.size > settings.chunkBytes)) endRun()
      run :+= row
      runBytes += row.size
    }
    endRun()
    (items.result(), rows.lastOption.map(_.id).getOrElse(position), rows.size)
  }

  private def itemsOf(run: Vector[CommittedEvent]): Seq[ChunkItem] = {
    val head = run.head
    val readable = run.takeWhile(_.event.isRight).map(_.event.toOption.get)
    val startsAtSquash = head.squashedUpTo.contains(head.version)
    val kinds = plan.kindsOf(head.aggregateType)
    val forReadable = if (readable.isEmpty) Nil else {
      try {
        val state = if (kinds.exists(_ != SubscriptionType.EVENTS)) Option(states.after(head.aggregateType, head.aggregateId, readable, head.squashedUpTo)) else None
        kinds.map(kind => item(kind, head.aggregateType, head.aggregateId, readable, state, startsAtSquash))
      } catch {
        case NonFatal(e) if !Errors.transient(e) =>
          record(head.aggregateType, head.aggregateId, e)
          unreadableItems(head, failureText(e))
      }
    }
    // A row that cannot be read ends its run; the projection fails the subscription there (F82).
    val forUnreadable = run.find(_.event.isLeft).toSeq.flatMap { bad =>
      val error = bad.event.left.toOption.get
      recordSeen(bad.aggregateType, bad.aggregateId, build => RestoreFailureSeen(bad.aggregateId, bad.aggregateType, build, bad.version.asInt, Some(bad.id),
        bad.aggregateId, "(unreadable event)", error))
      unreadableItems(bad, "a stored event cannot be read: " + error)
    }
    forReadable ++ forUnreadable
  }

  // F117: a store that cannot be read now (the connection lost, the database restarting or short of resources) is read
  // again later, as a page is; only what cannot be read at all fails its subscription. In a page this retries the page;
  // in a gap fill the builder stops and its supervisor starts it again from the saved position.

  // CR-4: what the subscription's failure says: the aggregate cannot be restored (a handler failed on one of its events),
  // or one of its events cannot be read.
  private def failureText(e: Throwable): String = {
    val at = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(10).collectFirst { case r: RestoreFailedAtEventException => r }
    (if (at.exists(!_.unreadable)) "the aggregate cannot be restored: " else "a stored event cannot be read: ") +
      Errors.describe(at.flatMap(r => Option(r.getCause)).getOrElse(e))
  }

  private def record(aggregateType: AggregateType, id: AggregateId, e: Throwable): Unit =
    recordSeen(aggregateType, id, build => RestoreFailureSeen.of(id, aggregateType, build, e))

  private def recordSeen(aggregateType: AggregateType, id: AggregateId, seen: RestoreFailureSource.Build => RestoreFailureSeen): Unit = restoreFailures.foreach { build =>
    try {
      val failure = seen(build)
      if (eventStore.recordRestoreFailure(failure)) log.warning("Building " + subscriberName + ": " + aggregateType.simpleName + " " + id.asLong +
        (if (failure.errorClass == "missing versions") ": " + failure.error else " cannot be restored") + "; it is recorded (restoreFailures)")
    } catch {
      case NonFatal(failure) => log.warning("Building " + subscriberName + " could not record that aggregate " + id.asLong + " cannot be restored: " + Errors.describe(failure))
    }
  }

  private def unreadableItems(at: CommittedEvent, error: String): Seq[ChunkItem] = {
    unreadable += 1
    log.error("Building " + subscriberName + ": " + at.aggregateType.simpleName + " " + at.aggregateId.asLong + " version " + at.version.asInt +
      " (event " + at.id + ") cannot be read: " + error)
    plan.kindsOf(at.aggregateType).map(kind => ChunkItem(kind, at.aggregateType, at.aggregateId, at.version, at.version, Nil, None, unreadable = Some(error)))
  }

  private def item(kind: SubscriptionType, aggregateType: AggregateType, id: AggregateId, events: Seq[EventInfo[Any]], state: Option[Any],
                   startsAtSquash: Boolean): ChunkItem = {
    val withEvents = if (kind == SubscriptionType.AGGREGATES) Nil else events
    ChunkItem(kind, aggregateType, id, events.head.version, events.last.version, withEvents, state, startsAtSquash)
  }

  // D62: one item per aggregate, its final state (snapshot plus tail), which starts the subscription.
  private def finalStates(aggregates: Seq[StoredAggregate]): (Seq[(ChunkItem, Long)], Long, Int) = {
    val items = aggregates.map(a => itemsFrom(a.aggregateType, a.aggregateId, SubscriptionType.AGGREGATES, AggregateVersion(1), a.squashedUpTo, Some(a.version))
      .map(_ -> a.aggregateId.asLong)).flatten
    (items, aggregates.lastOption.map(_.aggregateId.asLong).getOrElse(position), aggregates.size)
  }

  // ************** The reconciliation, and missing versions

  // Every aggregate whose stored version is above the projection's, for each listener kind.
  private def behind(aggregates: Seq[StoredAggregate]): (Seq[(ChunkItem, Long)], Long, Int) = {
    val missing = plan.subscriptions.flatMap { case (aggregateType, kind) =>
      val ofType = aggregates.filter(_.aggregateType == aggregateType)
      val applied = subscriptionsState.lastVersions(subscriberName, kind, ofType.map(_.aggregateId))
      ofType.collect { case a if a.version > applied(a.aggregateId) => (a, kind, applied(a.aggregateId)) }
    }
    missing.foreach { case (aggregate, kind, applied) =>
      work.enqueue(() => itemsFrom(aggregate.aggregateType, aggregate.aggregateId, kind, AggregateVersion(applied.asInt + 1), aggregate.squashedUpTo,
        Some(aggregate.version)))
    }
    (Nil, aggregates.lastOption.map(_.aggregateId.asLong).getOrElse(position), aggregates.size)
  }

  // The versions of one aggregate from `from` on, read by version; a range from below the squash point starts at the
  // squash event (D51), which starts the subscription (F98). A state listener gets the state at `version` (read when not
  // given) as a whole state, which needs nothing before it.
  private def itemsFrom(aggregateType: AggregateType, id: AggregateId, kind: SubscriptionType, from: AggregateVersion,
                        squashedUpTo: Option[AggregateVersion], version: Option[AggregateVersion] = None): Seq[ChunkItem] =
    try {
      if (kind == SubscriptionType.AGGREGATES) {
        val current = version.orElse(eventStore.aggregatesOf(Seq(aggregateType), AggregateId(id.asLong - 1), 1).find(_.aggregateId == id).map(_.version))
        current.toSeq.map(v => ChunkItem(kind, aggregateType, id, from, v, Nil, Option(sources(aggregateType).stateAt(id, Some(v))), startsSequence = true))
      } else {
        // In parts of at most a chunk's events and bytes (F126, F140), one item each. Nothing is stored below the squash
        // point: the range starts at it, and goes on from it when a squash since then left a part empty.
        var parts = Vector[ChunkItem]()
        var squashed = squashedUpTo
        var next = squashed.filter(_ > from).getOrElse(from)
        var missing = Option.empty[(AggregateVersion, AggregateVersion)]
        var more = true
        while (more) {
          val part = eventStore.readPart[Any](id, next, AggregateVersion((next.asInt.toLong + settings.chunkEvents - 1).min(Int.MaxValue).toInt), settings.chunkBytes)
          val firstStored = part.headOption.map(_._1.version).orElse(storedFrom(id, next))
          if (firstStored.forall(_ > next)) squashed = eventStore.squashedVersion(id)
          firstStored match {
            case _ if squashed.exists(_ > next) => next = squashed.get // a squash moved the start since: read from it
            case None => more = false
            case Some(stored) if stored > next =>
              // CR-6, D92: versions below one that is stored are never committed late (one owner writes them in order),
              // so `next` to `stored - 1` are missing from the store.
              missing = Some((next, AggregateVersion(stored.asInt - 1)))
              more = false
            case Some(_) if part.isEmpty => () // committed since the part was read: read it again
            case Some(_) =>
              val events = part.map(_._1)
              parts :+= item(kind, aggregateType, id, events, None, squashed.contains(events.head.version)).copy(bytes = part.map(_._2.toLong).sum)
              next = events.last.version.increment
          }
        }
        // Every part carries the state after the last, restored once, as an owner's re-published parts do.
        val delivered = if (kind == SubscriptionType.AGGREGATES_WITH_EVENTS && parts.nonEmpty) {
          val state = Option(sources(aggregateType).stateAt(id, Some(parts.last.last)))
          parts.map(_.copy(aggregateRoot = state))
        } else parts
        delivered ++ missing.map { case (start, end) => missingItem(kind, aggregateType, id, start, end, MissingVersions.fromStore(start, end)) }
      }
    } catch {
      case NonFatal(e) if !Errors.transient(e) =>
        unreadable += 1
        log.error(e, "Building " + subscriberName + ": " + aggregateType.simpleName + " " + id.asLong + " cannot be read from version " + from.asInt)
        record(aggregateType, id, e)
        Seq(ChunkItem(kind, aggregateType, id, from, from, Nil, None, unreadable = Some(failureText(e))))
    }

  // CR-6: the first version of the aggregate stored from `from` on, when the part read from it was empty: a gap longer
  // than a part, or missing versions at the end (then the stored version, past which nothing is).
  private def storedFrom(id: AggregateId, from: AggregateVersion): Option[AggregateVersion] =
    eventStore.storedVersion(id).map(_._1).filter(_ >= from).map { stored =>
      eventStore.readPart[Any](id, from, stored, 1L).headOption.map(_._1.version).getOrElse(stored.increment)
    }

  // CR-6: the versions `start` to `end` of the aggregate cannot be delivered: the subscription fails there (D20a), with a
  // "versions from" error that `SkipFailedVersion(pastMissing = true)` moves past; recorded, one warning per aggregate.
  private def missingItem(kind: SubscriptionType, aggregateType: AggregateType, id: AggregateId, start: AggregateVersion, end: AggregateVersion,
                          error: String): ChunkItem = {
    recordSeen(aggregateType, id, build => RestoreFailureSeen(id, aggregateType, build, start.asInt, None, id, "missing versions", error))
    ChunkItem(kind, aggregateType, id, start, end, Nil, None, unreadable = Some(error))
  }

  // CR-6: how often each gap was filled; one asked for again and again without the projection's version moving fails.
  private val fills = mutable.Map[(AggregateId, AggregateVersion), Int]()

  // Missing versions are read by version and sent before anything else; a gap means a row was committed late, so the
  // rest of that aggregate's runs in later chunks may be gaps too, and are read again the same way. CR-6: a gap asked for
  // more often than chunks can be in flight with it, without moving on, ends as a failure, so no fill is repeated for
  // ever; versions missing from the store end so at the first read (`itemsFrom`).
  private def fill(gaps: Seq[(AggregateType, AggregateId, AggregateVersion)]): Unit = if (gaps.nonEmpty) {
    gapsFilled += gaps.size
    gaps.distinct.reverseIterator.foreach { case (aggregateType, id, from) =>
      val asked = fills.getOrElse((id, from), 0) + 1
      fills((id, from)) = asked
      if (asked > settings.chunksInFlight + 2) work.prepend(() => plan.kindsOf(aggregateType).map(kind => missingItem(kind, aggregateType, id, from, from,
        MissingVersions.Prefix + from.asInt + " were asked for " + asked + " times in this build without arriving")))
      else work.prepend(() => {
        val squashed = eventStore.squashedVersion(id)
        val items = plan.kindsOf(aggregateType).flatMap(kind => itemsFrom(aggregateType, id, kind, from, squashed))
        // CR-4: an aggregate removed meanwhile (by an operator, chapter 18) has nothing to give: a retry of its failure
        // clears it, as the owner's answer does after the build.
        if (items.isEmpty && eventStore.storedVersion(id).isEmpty) target ! NothingToRepublish(id, AggregateVersion.ZERO)
        items
      })
    }
  }

  // Items in chunks of at most `chunkEvents` events and `chunkBytes` of them (F126, F140), each with the position reached
  // once it is applied. An item fits a chunk (runs and parts are cut so), except one event larger than its bytes.
  private def chunked(items: Seq[(ChunkItem, Long)], reached: Long): Seq[(ApplyChunk, Long)] = {
    val chunks = Vector.newBuilder[(ApplyChunk, Long)]
    var current = Vector[(ChunkItem, Long)]()
    var events = 0
    var bytes = 0L
    def endChunk(): Unit = if (current.nonEmpty) {
      chunkIds += 1
      chunks += ApplyChunk(chunkIds, current.map(_._1)) -> current.map(_._2).max
      current = Vector.empty
      events = 0
      bytes = 0L
    }
    items.foreach { case (item, at) =>
      val itemEvents = math.max(item.events.size, 1)
      if (events + itemEvents > settings.chunkEvents || bytes + item.bytes > settings.chunkBytes) endChunk()
      current :+= item -> at
      events += itemEvents
      bytes += item.bytes
    }
    endChunk()
    val result = chunks.result()
    // The last chunk of a page reaches the page's end, also when the page's last rows gave no items.
    if (result.isEmpty) result else result.init :+ (result.last._1 -> math.max(result.last._2, reached))
  }

  private def save(reached: BuildPosition): Unit =
    try positions.save(reached) catch {
      case NonFatal(e) => log.warning("Building " + subscriberName + " could not save its position " + reached + "; a restart repeats more: " + Errors.describe(e))
    }
}

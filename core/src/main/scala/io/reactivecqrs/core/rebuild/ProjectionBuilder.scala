package io.reactivecqrs.core.rebuild

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.eventbus.EventsBusActor.{MessageAck, MessageFailed, RequestRepublish}
import io.reactivecqrs.core.eventstore.{CommittedEvent, EventStoreState, StoredAggregate}
import io.reactivecqrs.core.projection._
import io.reactivecqrs.core.rebuild.ProjectionBuilder._
import io.reactivecqrs.core.util.MyActorLogging
import org.apache.pekko.actor.{Actor, ActorRef, Status, Terminated}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.control.NonFatal

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
    val Start: BuildPosition = BuildPosition(bulkDone = false, 0L)
  }

  trait BuildPositions {
    def read(): BuildPosition
    def save(position: BuildPosition): Unit
  }

  /** @param pageSize events (or aggregates) read from the store at once
    * @param chunkEvents events per chunk, one transaction of the projection
    * @param chunksInFlight chunks sent and not yet applied: the next page is read while the projection applies them
    * @param maxReplayStates aggregate states kept open by a commit-order replay (D59)
    * @param retryAfter the wait after the store or the projection failed */
  case class BuilderSettings(pageSize: Int = 1000, chunkEvents: Int = 1000, chunksInFlight: Int = 2, maxReplayStates: Int = 10000,
                             retryAfter: FiniteDuration = 5.seconds)

  /** To `reportTo`: the projection is live and has applied every event committed before it went live, or recorded a
    * failure for it (D20a). */
  case object BuildCaughtUp

  case object GetBuildStatus
  case class BuildStatus(phase: String, position: Long, eventsSent: Long, gapsFilled: Long, handedOver: Long, unreadable: Long)

  private case object Retry
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
  */
class ProjectionBuilder(target: ActorRef, subscriberName: String, plan: BuildPlan, eventStore: EventStoreState,
                        subscriptionsState: SubscriptionsState, sources: Map[AggregateType, ReplaySource], positions: BuildPositions,
                        reportTo: ActorRef, settings: BuilderSettings = BuilderSettings()) extends Actor with MyActorLogging {

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
  private var chunkIds = 0L
  private var outstanding = 0 // items the projection handed to its queues, not answered yet

  private var eventsSent = 0L
  private var gapsFilled = 0L
  private var handedOver = 0L
  private var unreadable = 0L

  override def preStart(): Unit = {
    context.watch(target)
    val start = positions.read()
    if (start.bulkDone) {
      goLive()
    } else {
      position = start.position
      target ! BuildFromStore(self)
      log.info("Building " + subscriberName + (if (position > 0) " from position " + position else "") +
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
    case _: MessageAck | _: MessageFailed =>
      outstanding -= 1
      pump()
    case RequestRepublish(_, aggregateType, aggregateId, from) =>
      fill(Seq((aggregateType, aggregateId, from)))
      pump()
    case ProjectionLive if phase == GoingLive =>
      log.info("Projection " + subscriberName + " is live; reconciling it with the store")
      phase = Reconciling
      position = 0L
      readAll = false
      pump()
    case Status.Failure(e) =>
      // The projection could not take a chunk (its store is down): the chunks in flight are sent again, after a wait.
      log.warning("Projection " + subscriberName + " failed on a chunk, sending it again in " + settings.retryAfter + ": " + e)
      inFlight.reverseIterator.foreach(sent => ready.prepend(sent)) // re-sent in their order
      inFlight.clear()
      retryLater()
    case Retry =>
      waitingToRetry = false
      pump()
    case GetBuildStatus =>
      sender() ! BuildStatus(phase.toString, position, eventsSent, gapsFilled, handedOver, unreadable)
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
      while (inFlight.size < settings.chunksInFlight && ready.nonEmpty) {
        val sent = ready.dequeue()
        target ! sent._1
        eventsSent += sent._1.items.map(item => math.max(item.events.size, 1)).sum
        inFlight.enqueue(sent)
      }
      more = inFlight.size < settings.chunksInFlight && ready.isEmpty && !readAll && readNextPage()
    }
    if (readAll && ready.isEmpty && inFlight.isEmpty && outstanding == 0) phase match {
      case Bulk => goLive()
      case Reconciling =>
        phase = CaughtUp
        log.info("Projection " + subscriberName + " is built: " + eventsSent + " events delivered, " + gapsFilled + " gaps filled, " +
          handedOver + " items applied one at a time, " + unreadable + " unreadable")
        reportTo ! BuildCaughtUp
        context.stop(self) // with its replay states
      case _ => ()
    }
  }

  private def goLive(): Unit = {
    save(BuildPosition(bulkDone = true, 0L))
    phase = GoingLive
    target ! GoLive
  }

  // Reads one page of the current pass into `ready`; false when the store failed, to be tried again later.
  private def readNextPage(): Boolean = try {
    val (items, reached, count) = phase match {
      case Bulk if plan.finalStates => finalStates(eventStore.aggregatesOf(plan.types, AggregateId(position), settings.pageSize))
      case Bulk => committed(eventStore.readCommitted(plan.types, position, settings.pageSize))
      case _ => behind(eventStore.aggregatesOf(plan.types, AggregateId(position), settings.pageSize))
    }
    readAll = count < settings.pageSize
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
  // version is missing (a row committed late), so the projection sees the gap and it is read again by version.
  private def committed(rows: Seq[CommittedEvent]): (Seq[(ChunkItem, Long)], Long, Int) = {
    val items = Vector.newBuilder[(ChunkItem, Long)]
    var run = Vector[CommittedEvent]()
    def endRun(): Unit = if (run.nonEmpty) {
      items ++= itemsOf(run).map(_ -> run.last.id)
      run = Vector.empty
    }
    rows.foreach { row =>
      if (run.nonEmpty && (run.head.aggregateId != row.aggregateId || run.last.event.isLeft || row.version.asInt != run.last.version.asInt + 1)) endRun()
      run :+= row
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
        case NonFatal(e) => unreadableItems(head, "its state cannot be restored: " + e.getClass.getSimpleName + ": " + e.getMessage)
      }
    }
    // A row that cannot be read ends its run; the projection fails the subscription there (F82).
    val forUnreadable = run.find(_.event.isLeft).toSeq.flatMap(bad => unreadableItems(bad, bad.event.left.toOption.get))
    forReadable ++ forUnreadable
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
    val items = missing.flatMap { case (aggregate, kind, applied) =>
      itemsFrom(aggregate.aggregateType, aggregate.aggregateId, kind, AggregateVersion(applied.asInt + 1), aggregate.squashedUpTo, Some(aggregate.version))
    }
    (items.map(_ -> 0L), aggregates.lastOption.map(_.aggregateId.asLong).getOrElse(position), aggregates.size)
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
        val events = eventStore.readEventsRange[Any](id, from, AggregateVersion(Int.MaxValue))
        if (events.isEmpty) Nil else {
          val state = if (kind == SubscriptionType.AGGREGATES_WITH_EVENTS) Option(sources(aggregateType).stateAt(id, Some(events.last.version))) else None
          Seq(item(kind, aggregateType, id, events, state, squashedUpTo.contains(events.head.version)))
        }
      }
    } catch {
      case NonFatal(e) =>
        unreadable += 1
        log.error(e, "Building " + subscriberName + ": " + aggregateType.simpleName + " " + id.asLong + " cannot be read from version " + from.asInt)
        Seq(ChunkItem(kind, aggregateType, id, from, from, Nil, None, unreadable = Some(e.getClass.getSimpleName + ": " + e.getMessage)))
    }

  // Missing versions are read by version and sent before anything else; a gap means a row was committed late, so the
  // rest of that aggregate's runs in later chunks may be gaps too, and are read again the same way.
  private def fill(gaps: Seq[(AggregateType, AggregateId, AggregateVersion)]): Unit = if (gaps.nonEmpty) {
    gapsFilled += gaps.size
    val items = gaps.distinct.flatMap { case (aggregateType, id, from) =>
      val squashed = eventStore.squashedVersion(id)
      plan.kindsOf(aggregateType).flatMap(kind => itemsFrom(aggregateType, id, kind, from, squashed))
    }
    // A filled gap does not move the bulk pass: it saves no position.
    chunked(items.map(_ -> -1L), -1L).reverseIterator.foreach(ready.prepend)
  }

  // Items in chunks of about `chunkEvents` events, each with the position reached once it is applied.
  private def chunked(items: Seq[(ChunkItem, Long)], reached: Long): Seq[(ApplyChunk, Long)] = {
    val chunks = Vector.newBuilder[(ApplyChunk, Long)]
    var current = Vector[(ChunkItem, Long)]()
    var events = 0
    def endChunk(): Unit = if (current.nonEmpty) {
      chunkIds += 1
      chunks += ApplyChunk(chunkIds, current.map(_._1)) -> current.map(_._2).max
      current = Vector.empty
      events = 0
    }
    items.foreach { case (item, at) =>
      current :+= item -> at
      events += math.max(item.events.size, 1)
      if (events >= settings.chunkEvents) endChunk()
    }
    endChunk()
    val result = chunks.result()
    // The last chunk of a page reaches the page's end, also when the page's last rows gave no items.
    if (result.isEmpty) result else result.init :+ (result.last._1 -> math.max(result.last._2, reached))
  }

  private def save(reached: BuildPosition): Unit =
    try positions.save(reached) catch {
      case NonFatal(e) => log.warning("Building " + subscriberName + " could not save its position " + reached + "; a restart repeats more: " + e)
    }
}

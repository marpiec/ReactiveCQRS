package io.reactivecqrs.core.rebuild

import io.reactivecqrs.api.{AggregateContext, AggregateType}
import io.reactivecqrs.api.id.{AggregateId, SpaceId}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.eventstore.{AggregateScope, EventStoreState}
import io.reactivecqrs.core.projection._
import io.reactivecqrs.core.rebuild.ProjectionBuilder._
import io.reactivecqrs.core.rebuild.ProjectionSupervisor._
import io.reactivecqrs.core.util.{BlockingDispatcher, MyActorLogging}
import org.apache.pekko.actor.{Actor, ActorRef, ActorRefFactory, OneForOneStrategy, Props, Stash, Status, SupervisorStrategy, Terminated}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

/**
  * @param contexts the aggregate contexts whose states projections receive, for their builds (D59) and for the context
  *                 versions a build records (D63)
  * @param buildsAtOnce how many builds run at the same time; the others wait
  */
case class ProjectionsSettings(eventStore: EventStoreState, subscriptionsState: SubscriptionsState, registry: ProjectionRegistry,
                               eventBus: ActorRef, contexts: Seq[AggregateContext[_]], builder: BuilderSettings = BuilderSettings(),
                               buildsAtOnce: Int = 2, restartAfter: FiniteDuration = 10.seconds)

/**
  * Runs managed projections (D57, D70): `start` creates a projection's supervisor, whose `ActorRef` the host uses where it
  * used the projection's (queries, `RegisterSubscribers`). The supervisor runs one instance of the projection per build
  * it needs, made by `create` with the build's table names, builds a new version from the store while the active one
  * serves, and switches to it in one update once it caught up. See docs/design/M4-rebuild.md.
  */
class Projections(actors: ActorRefFactory, val settings: ProjectionsSettings) {

  private[rebuild] val sources: Map[AggregateType, ReplaySource] = ReplaySource.of(settings.contexts, settings.eventStore)
  private[rebuild] val contextVersions: Map[String, Int] = settings.contexts.map(c => c.aggregateType.typeName -> c.version).toMap
  private[rebuild] val slots: ActorRef = actors.actorOf(Props(new BuildSlots(settings.buildsAtOnce)))

  /** Starts the projection `name` at `version`; its supervisor is named `actorName`. The instance `create` makes for a
    * build must take its stores' table names from that build (`build.documentStore(...)`). `P`, the projection's class,
    * names the subscriptions of a projection 0.12 built, which is adopted as it is (D81). */
  def start[P <: ProjectionActor : ClassTag](name: String, version: Int, actorName: String, whileBuilding: WhileBuilding = ServeLive)
                                           (create: ProjectionBuild => P): ActorRef = {
    val spec = ProjectionSpec(name, version, classTag[P].runtimeClass.getName, whileBuilding)
    actors.actorOf(Props(new ProjectionSupervisor(spec, create, this)).withDispatcher(BlockingDispatcher.id), actorName)
  }

  /** Drops every build of a projection removed from the code: its tables, its subscription versions and its failures, which
    * would keep permanent deletes waiting (F94). */
  def retire(name: String): Unit = {
    settings.registry.builds(name).foreach { build =>
      settings.subscriptionsState.clearSubscriptionsInfo(build.subscriber)
      settings.registry.drop(name, build.number)
    }
    settings.registry.remove(name)
  }
}

case class ProjectionSpec(name: String, version: Int, legacySubscriber: String, whileBuilding: WhileBuilding)

object ProjectionSupervisor {

  /** What the active build does while a new version of the projection is built on this node (D57, D70). */
  sealed trait WhileBuilding
  /** It keeps applying live events to its tables, with the projection's current code: its queries, parked ones too, see
    * fresh data. For a version whose code can run on the documents of the version before. */
  case object ServeLive extends WhileBuilding
  /** It serves its tables unchanged, taking no deliveries. */
  case object ServeStale extends WhileBuilding

  /** Starts a build of the projection's current version, which replaces the active build once caught up (D63). */
  case object RebuildProjection
  case class RebuildStarted(build: Int)
  case class RebuildRefused(reason: String)

  /** Rebuilds, in place, what the active build derived from the target (D63, R14): the projection clears it
    * (`clearForRebuild`) and forgets its versions of the target's aggregates in one transaction, then gets their events
    * again from the store, in commit order. Answered with `AggregatesRebuilt`, or `RebuildRefused`; one that a stop
    * interrupted runs again at the next start. An organization's aggregates need their space (F49's backfill). */
  case class RebuildAggregates(target: RebuildTarget)
  case class AggregatesRebuilt(target: RebuildTarget, aggregates: Int)

  /** Forwards `message` to the instance of the build in progress, such as `RetryAllFailedSubscriptions` for failures
    * that keep the build from replacing the active one. */
  case class ToBuildInProgress(message: Any)

  case object GetBuilds
  /** `caughtUp`: the build in progress has every event; it replaces the active build once it has no failed subscription. */
  case class Builds(name: String, active: Option[BuildRecord], building: Option[BuildRecord], caughtUp: Boolean)

  private[rebuild] case object RequestBuildSlot
  private[rebuild] case object BuildSlotGranted
  private[rebuild] case object ReleaseBuildSlot

  private case object Start
  private case object CheckSwitch
  private case object RunTargetedBuilder
  private case object RestartBuild

  private[rebuild] def encode(target: RebuildTarget): String = target match {
    case RebuildTarget.OneAggregate(id) => "aggregate:" + id.asLong
    case RebuildTarget.OneType(aggregateType) => "type:" + aggregateType.typeName
    case RebuildTarget.OneSpace(spaceId) => "space:" + spaceId.asLong
  }

  private[rebuild] def decode(text: String): Option[RebuildTarget] = text.split(":", 2) match {
    case Array("aggregate", id) => Some(RebuildTarget.OneAggregate(AggregateId(id.toLong)))
    case Array("type", name) => Some(RebuildTarget.OneType(AggregateType(name)))
    case Array("space", id) => Some(RebuildTarget.OneSpace(SpaceId(id.toLong)))
    case _ => None
  }
}

/** A targeted rebuild's builder reads nothing back after a stop: the rebuild runs again whole. */
private[rebuild] object NoPositions extends BuildPositions {
  override def read(): BuildPosition = BuildPosition.Start
  override def save(position: BuildPosition): Unit = ()
}

/** At most `slots` builds at once, granted in the order they were asked for; a supervisor holds or waits for one at most. */
private[rebuild] class BuildSlots(slots: Int) extends Actor {
  private var holders = Set[ActorRef]()
  private val waiting = mutable.Queue[ActorRef]()

  override def receive: Receive = {
    case RequestBuildSlot =>
      val supervisor = sender()
      if (!holders.contains(supervisor) && !waiting.contains(supervisor)) {
        context.watch(supervisor)
        if (holders.size < slots) grant(supervisor) else waiting.enqueue(supervisor)
      }
    case ReleaseBuildSlot =>
      waiting.dequeueAll(_ == sender())
      release(sender())
    case Terminated(supervisor) =>
      waiting.dequeueAll(_ == supervisor)
      release(supervisor)
  }

  private def grant(supervisor: ActorRef): Unit = {
    holders += supervisor
    supervisor ! BuildSlotGranted
  }

  private def release(supervisor: ActorRef): Unit = if (holders.contains(supervisor)) {
    holders -= supervisor
    if (waiting.nonEmpty) grant(waiting.dequeue())
  }
}

/** The position of a build in the registry (D61). */
private[rebuild] class RegistryBuildPositions(registry: ProjectionRegistry, name: String, build: Int) extends BuildPositions {
  override def read(): BuildPosition = registry.projection(name).flatMap(p => (p.building ++ p.active).find(_.number == build))
    .map(_.position).getOrElse(BuildPosition.Start)
  override def save(position: BuildPosition): Unit = registry.savePosition(name, build, position)
}

/**
  * The supervisor of one managed projection (D57, D70): it reads the registry, runs an instance of the projection for
  * the active build and one for the build in progress (each with its own tables and subscriptions), forwards every
  * other message to the instance that serves, builds through a `ProjectionBuilder` and switches. JDBC runs on its thread:
  * it is created on the blocking dispatcher.
  */
class ProjectionSupervisor(spec: ProjectionSpec, create: ProjectionBuild => ProjectionActor, runtime: Projections)
  extends Actor with Stash with MyActorLogging {

  private val settings = runtime.settings
  private val registry = settings.registry

  private case class Instance(record: BuildRecord, ref: ActorRef, subscribed: Boolean)
  private case class Targeted(target: RebuildTarget, requester: ActorRef, types: Seq[AggregateType], aggregates: Int, builder: Option[ActorRef])

  private var active: Option[Instance] = None
  private var building: Option[Instance] = None // the same instance as `active` while a new projection is built first
  private var listeners: Seq[(AggregateType, SubscriptionType)] = Nil
  private var bus: Option[ActorRef] = None
  private var answeredBus = false
  private var builder: Option[ActorRef] = None
  private var caughtUp = false
  private val retiring = mutable.Map[ActorRef, BuildRecord]()
  private var targeted: Option[Targeted] = None
  private var buildStopped = false // the build's instance stopped by itself; a new one starts after a wait
  // F110: instances that stopped, kept in the bus as stopped subscribers (their events stay in the outbox) until their
  // replacements are subscribed; then they leave it, and their outbox rows go to the replacements.
  private var replaced = Vector[ActorRef]()
  private var children = 0 // names each child once, so a new one never meets the name of one still stopping

  // AR7: an instance that fails to start (its stores cannot be made) is stopped, not restarted, and the projection
  // starts again after a wait.
  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() { case _ => SupervisorStrategy.Stop }

  override def preStart(): Unit = self ! Start

  override def receive: Receive = guarded(starting)

  // AR7: a message the supervisor fails on (the registry or the store unavailable) is logged and answered with the
  // failure. A restart would stop the instances, and the new ones would never be subscribed: the bus asks once.
  private def guarded(handler: Receive): Receive = {
    case message if handler.isDefinedAt(message) =>
      try handler(message) catch {
        case NonFatal(e) =>
          log.error(e, "Projection " + spec.name + " failed on " + message.getClass.getSimpleName)
          sender() ! Status.Failure(e)
      }
  }

  private def serving: Option[Instance] = active.orElse(building)

  private def runningBuild: Option[Instance] = building.filterNot(_ => buildStopped)

  private def childName(prefix: String, build: Int): String = {
    children += 1
    prefix + "-" + build + "-" + children
  }

  // ************** Start

  private def starting: Receive = {
    case Start =>
      try decideBuilds() catch {
        case NonFatal(e) =>
          log.error(e, "Projection " + spec.name + " could not read its builds; trying again in " + settings.restartAfter)
          (active ++ building).foreach(i => context.stop(i.ref)) // made before the failure; the next try makes its own
          active = None
          building = None
          context.system.scheduler.scheduleOnce(settings.restartAfter, self, Start)(context.dispatcher)
      }
    case Listeners(subscriptions) if serving.isDefined =>
      listeners = subscriptions
      logged("could not check the versions of its contexts; it checks them at the next start")(rebuildForChangedContexts())
      connect()
      context.become(guarded(running))
      unstashAll()
      // A targeted rebuild a stop interrupted runs again (a build in progress includes its target anyway).
      if (building.isEmpty) logged("could not read its pending targeted rebuild; it runs at the next start") {
        registry.projection(spec.name).flatMap(_.pendingTarget).flatMap(decode).foreach(target => rebuildTarget(target, context.system.deadLetters))
      }
    case GetSubscriptionRequests =>
      bus = Some(sender())
    case Terminated(ref) => terminated(ref)
    case _ => stash()
  }

  private def decideBuilds(): Unit = {
    var registered = registry.projection(spec.name).getOrElse(adoptOrBuild())
    // A build of a version the code no longer has is superseded.
    registered.building.filter(_.version != spec.version).foreach { stale =>
      log.info("Projection " + spec.name + " abandons build " + stale.number + " of version " + stale.version + ": the code is at version " + spec.version)
      registry.abandon(spec.name, stale.number)
      drop(stale)
      registered = registry.projection(spec.name).get
    }
    if (registered.building.isEmpty && registered.active.forall(_.version != spec.version)) {
      registry.startBuild(spec.name, spec.version, runtime.contextVersions)
      registered = registry.projection(spec.name).get
    }
    // F116: a build retired or abandoned before a stop let it be dropped is dropped now.
    val kept = (registered.active ++ registered.building).map(_.number).toSet
    registry.builds(spec.name).filterNot(b => kept.contains(b.number)).foreach(drop)
    active = registered.active.map(instanceOf)
    building = registered.building.map(b => active.filter(_.record.number == b.number).getOrElse(instanceOf(b)))
    log.info("Projection " + spec.name + " " + describe)
    serving.foreach(_.ref ! GetListeners)
  }

  // D81: a projection 0.12 built is adopted at the version 0.12 recorded, or, when it recorded none but its subscriptions
  // are there, at the code's version; one with no trace is built.
  private def adoptOrBuild(): RegisteredProjection = {
    val legacy = registry.legacyVersions()
    legacy.projections.get(spec.name) match {
      case Some(version) => registry.adopt(spec.name, version, spec.legacySubscriber, runtime.contextVersions ++ legacy.aggregates)
      case None if settings.subscriptionsState.hasSubscriptions(spec.legacySubscriber) =>
        registry.adopt(spec.name, spec.version, spec.legacySubscriber, runtime.contextVersions)
      case None => registry.startBuild(spec.name, spec.version, runtime.contextVersions)
    }
    registry.projection(spec.name).get
  }

  private def instanceOf(record: BuildRecord): Instance = {
    val build = new ProjectionBuild(record, registry)
    val ref = context.watch(context.actorOf(Props(create(build)).withDispatcher(BlockingDispatcher.id), childName("build", record.number)))
    ref ! BindSubscriber(record.subscriber)
    Instance(record, ref, subscribed = false)
  }

  // D63: a build records the context versions it was built with; the projection is built again when a context whose
  // states it receives changed.
  private def rebuildForChangedContexts(): Unit = if (building.isEmpty) active.foreach { a =>
    val stateTypes = listeners.collect { case (t, kind) if kind != SubscriptionType.EVENTS => t.typeName }.toSet
    val changed = stateTypes.filter(t => runtime.contextVersions.get(t).exists(v => a.record.contexts.get(t).exists(_ != v)))
    if (changed.nonEmpty) {
      log.info("Projection " + spec.name + " is built again: the version of " + changed.mkString(", ") + " changed (D63)")
      building = Some(instanceOf(registry.startBuild(spec.name, spec.version, runtime.contextVersions)))
    }
  }

  // The instances that take deliveries are subscribed: the build in progress, and the active build unless it serves
  // unchanged meanwhile. The build in progress acknowledges deliveries unapplied from the start: its builder reads them.
  private def connect(): Unit = {
    runningBuild.foreach(_.ref ! BuildFromStore(self))
    subscribeTakers()
    runningBuild.foreach(_ => runtime.slots ! RequestBuildSlot)
  }

  private def subscribeTakers(): Unit = {
    def takesDeliveries(i: Instance) = building.isEmpty || building.exists(_.ref == i.ref) || spec.whileBuilding == ServeLive
    val takers = (active ++ runningBuild).toSeq.distinct.filter(i => !i.subscribed && takesDeliveries(i))
    if (takers.nonEmpty) subscribe(takers)
  }

  private def subscribe(instances: Seq[Instance]): Unit = bus.orElse(if (answeredBus) Some(settings.eventBus) else None) match {
    case Some(b) =>
      instances.foreach(_.ref ! ConnectedTo(b))
      val requests = instances.flatMap(requestsOf)
      if (answeredBus) b ! AddSubscriptions(requests) else b ! SubscriptionRequests(requests)
      answeredBus = true
      markSubscribed(instances)
      replaced.foreach(old => b ! RemoveSubscriber(old, keepOutbox = true))
      replaced = Vector.empty
    case None => () // the bus has not asked yet; it is answered when it does
  }

  private def markSubscribed(instances: Seq[Instance]): Unit = {
    def marked(i: Instance) = if (instances.exists(_.ref == i.ref)) i.copy(subscribed = true) else i
    active = active.map(marked)
    building = building.map(marked)
  }

  private def requestsOf(instance: Instance): Seq[SubscribeRequest] = listeners.map {
    case (aggregateType, SubscriptionType.EVENTS) => SubscribeForEvents(aggregateType, instance.ref)
    case (aggregateType, SubscriptionType.AGGREGATES) => SubscribeForAggregates(aggregateType, instance.ref)
    case (aggregateType, _) => SubscribeForAggregatesWithEvents(aggregateType, instance.ref)
  }

  // ************** Running

  private def running: Receive = {
    case GetSubscriptionRequests =>
      bus = Some(sender())
      subscribeTakers()
    case BuildSlotGranted =>
      if (runningBuild.isDefined && builder.isEmpty) startBuilder()
      else if (builder.isEmpty) runtime.slots ! ReleaseBuildSlot // nothing to build any more
    case RestartBuild => if (buildStopped) building.foreach { stopped =>
      buildStopped = false
      building = Some(instanceOf(stopped.record))
      connect()
    }
    case BuildCaughtUp if targeted.exists(_.builder.contains(sender())) => targetRebuilt()
    case BuildCaughtUp =>
      caughtUp = true
      runtime.slots ! ReleaseBuildSlot
      trySwitch()
    case RebuildAggregates(target) => rebuildTarget(target, sender())
    case ResetForRebuildDone(aggregates) if targeted.isDefined =>
      targeted = targeted.map(_.copy(aggregates = aggregates))
      startTargetedBuilder()
    case RunTargetedBuilder => startTargetedBuilder()
    case Status.Failure(e) if targeted.exists(_.builder.isEmpty) && active.exists(_.ref == sender()) =>
      log.error("Projection " + spec.name + " cannot rebuild " + targeted.get.target + ": " + e.getMessage)
      clearPendingTarget()
      targeted.get.requester ! RebuildRefused(e.getMessage)
      targeted = None
    case CheckSwitch => trySwitch()
    case status: ProjectionStatus if building.exists(_.ref == sender()) =>
      if (status.failures.isEmpty && status.retrying.isEmpty) switchNow() else {
        log.warning("Projection " + spec.name + ": build " + building.get.record.number + " caught up with " + (status.failures.size + status.retrying.size) +
          " failed subscriptions; it replaces the active build once they are retried or skipped (ToBuildInProgress)")
        context.system.scheduler.scheduleOnce(30.seconds, self, CheckSwitch)(context.dispatcher)
      }
    case SubscriberRemoved(old) => retiringActive(old)
    case RebuildProjection => rebuild()
    case ToBuildInProgress(message) => runningBuild match {
      case Some(b) => b.ref.forward(message)
      case None => sender() ! RebuildRefused("no build is running")
    }
    case GetBuilds => sender() ! Builds(spec.name, active.map(_.record), building.map(_.record), caughtUp)
    case request: RequestRepublish => builder.foreach(_.forward(request)) // from the build in progress, before its builder runs
    case Terminated(ref) => terminated(ref)
    case message => serving.foreach(_.ref.forward(message))
  }

  private def startBuilder(): Unit = runningBuild.foreach { b =>
    val missing = listeners.collect { case (t, kind) if kind != SubscriptionType.EVENTS && !runtime.sources.contains(t) => t.typeName }.distinct
    if (missing.nonEmpty) {
      log.error("Projection " + spec.name + " cannot be built: no aggregate context of " + missing.mkString(", ") + " was given to Projections")
      runtime.slots ! ReleaseBuildSlot
    } else {
      val positions = new RegistryBuildPositions(registry, spec.name, b.record.number)
      builder = Some(context.watch(context.actorOf(Props(new ProjectionBuilder(b.ref, b.record.subscriber, BuildPlan(listeners), settings.eventStore,
        settings.subscriptionsState, runtime.sources, positions, self, settings.builder)).withDispatcher(BlockingDispatcher.id), childName("builder", b.record.number))))
    }
  }

  // A build with failed subscriptions does not replace the active one until they are repaired.
  private def trySwitch(): Unit = if (caughtUp) building.foreach(_.ref ! GetProjectionStatus)

  private def switchNow(): Unit = building.foreach { b =>
    val switched = try Some(registry.switch(spec.name, b.record.number)) catch {
      case NonFatal(e) =>
        log.error(e, "Projection " + spec.name + " could not switch to build " + b.record.number + "; it tries again in " + settings.restartAfter)
        context.system.scheduler.scheduleOnce(settings.restartAfter, self, CheckSwitch)(context.dispatcher)
        None
    }
    if (switched.contains(true)) {
      log.info("Projection " + spec.name + " switched to build " + b.record.number + " (version " + b.record.version + ")")
      b.ref ! Activate
      val old = active.filterNot(_.record.number == b.record.number)
      active = Some(b)
      building = None
      caughtUp = false
      builder = None
      old.foreach { o =>
        retiring += o.ref -> o.record
        if (o.subscribed) settings.eventBus ! RemoveSubscriber(o.ref) else o.ref ! Retire(active.map(_.ref))
      }
    } else if (switched.contains(false)) {
      log.error("Projection " + spec.name + ": build " + b.record.number + " is no longer the build in progress in the registry; it does not switch")
    }
  }

  // The bus no longer waits for the old instance: it hands its push subscriptions to the new one and stops.
  private def retiringActive(old: ActorRef): Unit = if (retiring.contains(old)) old ! Retire(active.map(_.ref))

  // ************** Targeted rebuild (D63, R14)

  private def rebuildTarget(target: RebuildTarget, requester: ActorRef): Unit = {
    val types = listeners.map(_._1).distinct.filter(t => target match {
      case RebuildTarget.OneType(aggregateType) => aggregateType == t
      case _ => true
    })
    (building, targeted, active) match {
      case (Some(b), _, _) => requester ! RebuildRefused("build " + b.record.number + " is in progress, and replaces the active one whole")
      case (_, Some(t), _) => requester ! RebuildRefused("the rebuild of " + t.target + " is in progress")
      case (_, _, Some(_)) if types.nonEmpty && target.isInstanceOf[RebuildTarget.OneSpace] &&
        settings.eventStore.aggregatesOf(types, AggregateId(Long.MinValue), 1, AggregateScope.InSpace(SpaceId(-1))).nonEmpty =>
        requester ! RebuildRefused("aggregates of " + types.map(_.simpleName).mkString(", ") + " have no space yet: run backfillSpaces first (F49)")
      case (_, _, Some(a)) if types.nonEmpty =>
        registry.setPendingTarget(spec.name, Some(encode(target)))
        targeted = Some(Targeted(target, requester, types, 0, None))
        log.info("Projection " + spec.name + " rebuilds " + target)
        val (eventStore, scope) = (settings.eventStore, scopeOf(target))
        // F112: listed by the projection when it resets, after the deliveries it took before.
        a.ref ! ResetForRebuild(target, () => Iterator.unfold(AggregateId(Long.MinValue)) { after =>
          val page = eventStore.aggregatesOf(types, after, 1000, scope)
          page.lastOption.map(last => (page.map(_.aggregateId), last.aggregateId))
        }.flatten.toVector)
      case _ => requester ! RebuildRefused(s"$target is not of an aggregate type the projection receives")
    }
  }

  private def scopeOf(target: RebuildTarget): AggregateScope = target match {
    case RebuildTarget.OneAggregate(id) => AggregateScope.Only(Set(id))
    case RebuildTarget.OneType(_) => AggregateScope.All
    case RebuildTarget.OneSpace(spaceId) => AggregateScope.InSpace(spaceId)
  }

  private def startTargetedBuilder(): Unit = for (t <- targeted; a <- active) {
    val plan = BuildPlan(listeners.filter { case (aggregateType, _) => t.types.contains(aggregateType) })
    val builder = context.watch(context.actorOf(Props(new ProjectionBuilder(a.ref, a.record.subscriber, plan, settings.eventStore,
      settings.subscriptionsState, runtime.sources, NoPositions, self, settings.builder, Some(scopeOf(t.target)))).withDispatcher(BlockingDispatcher.id)))
    targeted = Some(t.copy(builder = Some(builder)))
  }

  private def targetRebuilt(): Unit = targeted.foreach { t =>
    clearPendingTarget()
    log.info("Projection " + spec.name + " rebuilt " + t.target)
    t.requester ! AggregatesRebuilt(t.target, t.aggregates)
    targeted = None
  }

  // A target left in the registry runs again at the next start, which repeats a finished rebuild harmlessly.
  private def clearPendingTarget(): Unit = logged("could not clear its targeted rebuild in the registry; it runs again at the next start") {
    registry.setPendingTarget(spec.name, None)
  }

  private def logged(what: String)(work: => Unit): Unit = try work catch {
    case NonFatal(e) => log.error(e, "Projection " + spec.name + " " + what)
  }

  private def rebuild(): Unit = (building, targeted) match {
    case (Some(b), _) => sender() ! RebuildRefused("build " + b.record.number + " is in progress")
    case (_, Some(t)) => sender() ! RebuildRefused("the rebuild of " + t.target + " is in progress")
    case _ =>
      val b = instanceOf(registry.startBuild(spec.name, spec.version, runtime.contextVersions))
      building = Some(b)
      log.info("Projection " + spec.name + " is built again on request (build " + b.record.number + ")")
      connect()
      sender() ! RebuildStarted(b.record.number)
  }

  private def terminated(ref: ActorRef): Unit = {
    retiring.remove(ref) match {
      case Some(retired) => drop(retired)
      case None if targeted.exists(_.builder.contains(ref)) =>
        log.warning("Projection " + spec.name + ": the rebuild of " + targeted.get.target + " stopped; it runs again in " + settings.restartAfter)
        targeted = targeted.map(_.copy(builder = None))
        context.system.scheduler.scheduleOnce(settings.restartAfter, self, RunTargetedBuilder)(context.dispatcher)
      case None if builder.contains(ref) =>
        builder = None
        if (!caughtUp) {
          log.warning("Projection " + spec.name + ": the builder of build " + building.map(_.record.number).getOrElse("-") + " stopped; it continues in " + settings.restartAfter)
          runtime.slots ! ReleaseBuildSlot
          context.system.scheduler.scheduleOnce(settings.restartAfter, runtime.slots, RequestBuildSlot)(context.dispatcher, self)
        }
      case None if building.exists(_.ref == ref) && !active.exists(_.ref == ref) =>
        // The build's instance stopped by itself (its stores could not be made): the active build serves on (D57), and
        // the build starts again in a new instance after a wait.
        log.error("Projection " + spec.name + ": the instance of build " + building.get.record.number + " stopped; it starts again in " + settings.restartAfter)
        if (building.exists(_.subscribed)) replaced :+= ref
        builder.foreach(context.stop)
        builder = None
        caughtUp = false
        buildStopped = true
        runtime.slots ! ReleaseBuildSlot
        context.system.scheduler.scheduleOnce(settings.restartAfter, self, RestartBuild)(context.dispatcher)
      case None if (active ++ building).exists(_.ref == ref) =>
        // The active instance stopped by itself: every instance starts again after a wait. The bus keeps their
        // subscriptions as stopped subscribers meanwhile, so no event leaves the outbox without them (F110).
        log.error("Projection " + spec.name + ": an instance stopped; the projection starts again in " + settings.restartAfter)
        (active ++ building).toSeq.distinct.foreach { i =>
          if (i.subscribed) replaced :+= i.ref
          if (i.ref != ref) context.stop(i.ref)
        }
        (builder ++ targeted.flatMap(_.builder)).foreach(context.stop)
        targeted.foreach(t => t.requester ! RebuildRefused("the projection restarts; the rebuild of " + t.target + " runs again then"))
        targeted = None
        runtime.slots ! ReleaseBuildSlot
        active = None
        building = None
        builder = None
        caughtUp = false
        buildStopped = false
        context.become(guarded(starting))
        context.system.scheduler.scheduleOnce(settings.restartAfter, self, Start)(context.dispatcher)
      case None => ()
    }
  }

  // A retired build's tables, subscription versions and failures go (F94). Its versions and failures first: a stop between
  // the two leaves the build in the registry, which is dropped at the next start (F116).
  private def drop(retired: BuildRecord): Unit =
    try {
      settings.subscriptionsState.clearSubscriptionsInfo(retired.subscriber)
      registry.drop(spec.name, retired.number)
      log.info("Projection " + spec.name + ": build " + retired.number + " is dropped")
    } catch {
      case NonFatal(e) => log.error(e, "Projection " + spec.name + ": build " + retired.number + " could not be dropped; its tables stay")
    }

  private def describe: String =
    "serves build " + active.map(a => s"${a.record.number} (version ${a.record.version})").getOrElse("-") +
      building.map(b => s", builds ${b.record.number} (version ${b.record.version})").getOrElse("")
}

package io.reactivecqrs.core.rebuild

import java.net.URLEncoder

import io.reactivecqrs.api.{AggregateContext, AggregateType}
import io.reactivecqrs.api.id.{AggregateId, SpaceId}
import io.reactivecqrs.core.cluster.{Members, Singletons}
import io.reactivecqrs.core.eventbus.EventsBusActor._
import io.reactivecqrs.core.eventstore.{AggregateScope, EventStoreState}
import io.reactivecqrs.core.projection._
import io.reactivecqrs.core.rebuild.ProjectionBuilder._
import io.reactivecqrs.core.rebuild.ProjectionSupervisor._
import io.reactivecqrs.core.util.{BlockingDispatcher, MyActorLogging, SchemaLock}
import org.apache.pekko.actor.{Actor, ActorContext, ActorRef, ActorRefFactory, ActorSystem, OneForOneStrategy, Props, Stash, Status, SupervisorStrategy, Terminated}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.reflect.{ClassTag, classTag}
import scala.util.Try
import scala.util.control.NonFatal
import io.reactivecqrs.core.util.Errors

/**
  * @param contexts the aggregate contexts whose states projections receive, for their builds (D59) and for the context
  *                 versions a build records (D63)
  * @param buildsAtOnce how many builds run at the same time; the others wait
  */
case class ProjectionsSettings(eventStore: EventStoreState, subscriptionsState: SubscriptionsState, registry: ProjectionRegistry,
                               contexts: Seq[AggregateContext[_]], builder: BuilderSettings = BuilderSettings(),
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
    * names the subscriptions of a projection 0.12 built, which is adopted as it is (D81). In a cluster (started on an
    * actor system with the cluster provider) the supervisor is a singleton (D53): every node starts it, and the
    * `ActorRef` returned is this node's proxy to it, for queries and for this node's event bus (`RegisterSubscribers`). */
  def start[P <: ProjectionActor : ClassTag](name: String, version: Int, actorName: String, whileBuilding: WhileBuilding = ServeLive)
                                           (create: ProjectionBuild => P): ActorRef = {
    val spec = ProjectionSpec(name, version, classTag[P].runtimeClass.getName, whileBuilding)
    val props = Props(new ProjectionSupervisor(spec, create, this)).withDispatcher(BlockingDispatcher.id)
    actors match {
      case system: ActorSystem => Singletons.start(system, actorName, props)
      case _ => actors.actorOf(props, actorName)
    }
  }

  /** Drops every build of a projection removed from the code: its tables, its subscription versions and its failures, which
    * would keep permanent deletes waiting (F94). Under the schema lock: every node of a cluster calls it as it starts.
    * While members of an older app-version run (a rolling upgrade, D65), they may still serve the projection and write
    * it: the drop waits until every member runs this node's version or a later one, looked at every 10 s (F132). */
  def retire(name: String): Unit =
    if (Members(system).olderVersionsRun) {
      if (!retiresLater.contains(name)) Projections.log.info("Projection " + name + " is retired once every node runs this version")
      retiresLater += name
      Try(system.scheduler.scheduleOnce(10.seconds)(retire(name))(system.dispatchers.lookup(BlockingDispatcher.id))) // none once the system stops
    } else SchemaLock {
      settings.registry.builds(name).foreach { build =>
        settings.subscriptionsState.clearSubscriptionsInfo(build.subscriber)
        settings.registry.drop(name, build.number)
      }
      settings.registry.remove(name)
    }

  private val retiresLater = java.util.concurrent.ConcurrentHashMap.newKeySet[String]().asScala

  private def system: ActorSystem = actors match {
    case system: ActorSystem => system
    case context: ActorContext => context.system
    case other => throw new IllegalArgumentException("Projections runs in an actor system or an actor's context, not in " + other)
  }
}

object Projections {
  private val log = LoggerFactory.getLogger(classOf[Projections])
}

/** A managed projection as `Projections.start` describes it to its supervisor: its name and version, the subscriber name
  * of its 0.12 form (adopted as build 0), and what its active build does while a new one is built. */
case class ProjectionSpec(name: String, version: Int, legacySubscriber: String, whileBuilding: WhileBuilding)

/** The messages of a managed projection's supervisor: the `ActorRef` that `Projections.start` returns (chapter 10). */
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
  /** The answer to `RebuildProjection`: the build that started. */
  case class RebuildStarted(build: Int)
  /** The answer to a rebuild that cannot run now, for `reason`. */
  case class RebuildRefused(reason: String)

  /** Rebuilds, in place, what the active build derived from the target (D63, R14): the projection clears it
    * (`clearForRebuild`) and forgets its versions of the target's aggregates in one transaction, then gets their events
    * again from the store, in commit order. Answered with `AggregatesRebuilt`, or `RebuildRefused`; one that a stop
    * interrupted runs again at the next start. An organization's aggregates need their space (F49's backfill). */
  case class RebuildAggregates(target: RebuildTarget)
  /** The answer to `RebuildAggregates`: how many aggregates were rebuilt. */
  case class AggregatesRebuilt(target: RebuildTarget, aggregates: Int)

  /** Forwards `message` to the instance of the build in progress, such as `RetryAllFailedSubscriptions` for failures
    * that keep the build from replacing the active one. */
  case class ToBuildInProgress(message: Any)

  /** Asks the supervisor for its `Builds`. */
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
  // F192: a targeted rebuild to do: of the active build (`build` None), or of the build in progress (its number) once it
  // caught up, before it switches in, as its bulk pass may have read the aggregates before they changed. Kept in the
  // registry until done, so a stop loses none; `requesters` are answered when it is done.
  private case class Job(target: RebuildTarget, build: Option[Int], requesters: Vector[ActorRef])
  private case class Targeted(job: Job, types: Seq[AggregateType], aggregates: Int, builder: Option[ActorRef]) {
    def target: RebuildTarget = job.target
  }

  private var active: Option[Instance] = None
  private var building: Option[Instance] = None // the same instance as `active` while a new projection is built first
  private var listeners: Seq[(AggregateType, SubscriptionType)] = Nil
  // M5: the bus of every node asks (through this supervisor's singleton proxy, which it names), and each is sent the
  // later changes. `unanswered`: buses that asked before the instances were known, or asked again.
  private var buses = Map[ActorRef, ActorRef]()
  private var unanswered = Map[ActorRef, ActorRef]()
  // A retired build's instance stops only once every bus that delivered to it removed it.
  private val removing = mutable.Map[ActorRef, Set[ActorRef]]()
  private var builder: Option[ActorRef] = None
  private var caughtUp = false
  private val retiring = mutable.Map[ActorRef, BuildRecord]()
  private var targeted: Option[Targeted] = None // the job running
  private var jobs = Vector[Job]() // the jobs waiting, in order
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

  // F258: the projection's name first, so the bus's status, gauges and logs, which name a subscriber by its actor's name,
  // tell the projections apart; a name an actor path cannot hold is encoded.
  private def childName(prefix: String, build: Int): String = {
    children += 1
    URLEncoder.encode(spec.name, "UTF-8") + "-" + prefix + "-" + build + "-" + children
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
      // The targeted rebuilds a stop interrupted run again; those of a build that is no longer in progress are not needed:
      // a later build reads the store as it is now.
      logged("could not read its pending targeted rebuilds; they run at the next start") {
        jobs = registry.projection(spec.name).flatMap(_.pendingTarget).toVector.flatMap(_.split("\n")).flatMap(decodeJob)
          .filter(_.build.forall(n => building.exists(_.record.number == n)))
        saveJobs()
        runNextJob()
      }
    case GetSubscriptionRequests(registered) => asked(sender(), registered) // answered once the instances are known
    case Terminated(ref) => terminated(ref)
    case _ => stash()
  }

  private def asked(bus: ActorRef, registered: ActorRef): Unit = {
    context.watch(bus)
    unanswered += bus -> registered
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
    val build = new ProjectionBuild(record, registry, current = record.version == spec.version)
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

  // Buses answered before get the new takers' subscriptions; buses that asked get every subscription. Until a bus has
  // asked, nothing is subscribed: it is answered when it does.
  private def subscribeTakers(): Unit = (buses.keys ++ unanswered.keys).headOption.foreach { anyBus =>
    def takesDeliveries(i: Instance) = building.isEmpty || building.exists(_.ref == i.ref) || spec.whileBuilding == ServeLive
    val takers = (active ++ runningBuild).toSeq.distinct.filter(i => !i.subscribed && takesDeliveries(i))
    takers.foreach(_.ref ! ConnectedTo(anyBus)) // where it asks for re-publications until a delivery names its bus
    markSubscribed(takers)
    val added = takers.flatMap(requestsOf)
    if (added.nonEmpty) buses.foreach { case (bus, registered) => bus ! AddSubscriptions(added, Some(registered)) }
    val all = (active ++ building).toSeq.distinct.filter(_.subscribed).flatMap(requestsOf)
    unanswered.foreach { case (bus, registered) => bus ! SubscriptionRequests(all, registered) }
    buses ++= unanswered
    unanswered = Map.empty
    for (bus <- buses.keys; old <- replaced) bus ! RemoveSubscriber(old, keepOutbox = true)
    replaced = Vector.empty
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
    case GetSubscriptionRequests(registered) =>
      asked(sender(), registered)
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
      runNextJob() // the build's own targeted rebuilds, before it switches in
      trySwitch()
    case RebuildAggregates(target) => rebuildTarget(target, sender())
    // R17: a purge waits for builds; its other messages go to the active build.
    case check: PurgeCheck => (building, targeted.map(_.job).orElse(jobs.headOption), active) match {
      case (Some(b), _, _) => sender() ! PurgeCheckAnswer(spec.name, Some("build " + b.record.number + " of projection " + spec.name + " is in progress"))
      case (_, Some(j), _) => sender() ! PurgeCheckAnswer(spec.name, Some("the rebuild of " + j.target + " by projection " + spec.name + " is in progress"))
      case (_, _, Some(a)) => a.ref.forward(check)
      case _ => sender() ! PurgeCheckAnswer(spec.name, Some("projection " + spec.name + " has no active build"))
    }
    case message @ (_: PurgedAggregates | _: PurgeSpace) => active match {
      case Some(a) => a.ref.forward(message)
      case None => sender() ! Status.Failure(new IllegalStateException("Projection " + spec.name + " has no active build"))
    }
    case ResetForRebuildDone(aggregates) if targeted.isDefined =>
      targeted = targeted.map(_.copy(aggregates = aggregates))
      startTargetedBuilder()
    case RunTargetedBuilder => startTargetedBuilder()
    case Status.Failure(e) if targeted.exists(t => t.builder.isEmpty && instanceOfJob(t.job).exists(_.ref == sender())) =>
      val t = targeted.get
      log.error("Projection " + spec.name + " cannot rebuild " + t.target + ": " + Errors.describe(e))
      targeted = None
      saveJobs() // before the answer: the registry says what is left once the requester hears
      t.job.requesters.foreach(_ ! RebuildRefused(Errors.message(e)))
      runNextJob()
      trySwitch()
    case CheckSwitch => trySwitch()
    case status: ProjectionStatus if building.exists(_.ref == sender()) =>
      if (buildJobsWaiting) () // the last of them tries again
      else if (status.failures.isEmpty && status.retrying.isEmpty) switchNow() else {
        log.warning("Projection " + spec.name + ": build " + building.get.record.number + " caught up with " + (status.failures.size + status.retrying.size) +
          " failed subscriptions; it replaces the active build once they are retried or skipped (ToBuildInProgress)")
        context.system.scheduler.scheduleOnce(30.seconds, self, CheckSwitch)(context.dispatcher)
      }
    case SubscriberRemoved(old) => removedFrom(old, sender())
    case RebuildProjection => rebuild()
    case ToBuildInProgress(message) => runningBuild match {
      case Some(b) => b.ref.forward(message)
      case None => sender() ! RebuildRefused("no build is running")
    }
    case GetBuilds => sender() ! Builds(spec.name, active.map(_.record), building.map(b => saved(b.record)), caughtUp)
    // F152: the running builder answers, a full build's or a targeted rebuild's.
    case GetBuildStatus => builder.orElse(targeted.flatMap(_.builder)) match {
      case Some(b) => b.forward(GetBuildStatus)
      case None => sender() ! building.fold(BuildStatus.NoBuild)(b => BuildStatus.waiting(saved(b.record).position.position))
    }
    case request: RequestRepublish => builder.foreach(_.forward(request)) // from the build in progress, before its builder runs
    case Terminated(ref) => terminated(ref)
    case message => serving.foreach(_.ref.forward(message))
  }

  // F152: an instance's record keeps the position its build started from; the builder saves its progress in the registry.
  // Without the registry the answer has the position the build started from.
  private def saved(record: BuildRecord): BuildRecord =
    Try(registry.builds(spec.name)).toOption.flatMap(_.find(_.number == record.number)).getOrElse(record)

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

  // A build with failed subscriptions does not replace the active one until they are repaired, nor one with targeted
  // rebuilds still to do (F192).
  private def trySwitch(): Unit = if (caughtUp && !buildJobsWaiting) building.foreach(_.ref ! GetProjectionStatus)

  private def buildJobsWaiting: Boolean = (targeted.map(_.job) ++ jobs).exists(_.build.isDefined)

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
        if (o.subscribed && buses.nonEmpty) {
          removing += o.ref -> buses.keySet
          buses.keys.foreach(_ ! RemoveSubscriber(o.ref))
        } else o.ref ! Retire(active.map(_.ref))
      }
    } else if (switched.contains(false)) {
      log.error("Projection " + spec.name + ": build " + b.record.number + " is no longer the build in progress in the registry; it does not switch")
    }
  }

  // No bus waits for the old instance any more: it hands its push subscriptions to the new one and stops.
  private def removedFrom(old: ActorRef, bus: ActorRef): Unit = removing.get(old).foreach { waiting =>
    val left = waiting - bus
    if (left.nonEmpty) removing += old -> left else {
      removing -= old
      if (retiring.contains(old)) old ! Retire(active.map(_.ref))
    }
  }

  // A bus that ended (its node left) is neither answered nor waited for.
  private def busEnded(bus: ActorRef): Unit = {
    buses -= bus
    unanswered -= bus
    removing.keys.toSeq.foreach(old => removedFrom(old, bus))
  }

  // ************** Targeted rebuild (D63, R14)

  private def typesOf(target: RebuildTarget): Seq[AggregateType] = listeners.map(_._1).distinct.filter(t => target match {
    case RebuildTarget.OneType(aggregateType) => aggregateType == t
    case _ => true
  })

  // F192: never refused for work in progress. The active build rebuilds the target in its turn; a build in progress does it
  // too once it caught up, before it switches in. The requester is answered once the build that serves did it: a build
  // that does not serve yet does it before it does.
  private def rebuildTarget(target: RebuildTarget, requester: ActorRef): Unit = {
    val types = typesOf(target)
    if (types.isEmpty || serving.isEmpty) requester ! RebuildRefused(s"$target is not of an aggregate type the projection receives")
    else if (target.isInstanceOf[RebuildTarget.OneSpace] &&
      settings.eventStore.aggregatesOf(types, AggregateId(Long.MinValue), 1, AggregateScope.InSpace(SpaceId(-1))).nonEmpty)
      requester ! RebuildRefused("aggregates of " + types.map(_.simpleName).mkString(", ") + " have no space yet: run backfillSpaces first (F49)")
    else {
      val forBuild = building.filterNot(b => active.exists(_.ref == b.ref)).map(_.record.number)
      if (active.isDefined) addJob(Job(target, None, Vector(requester)))
      forBuild.foreach(n => addJob(Job(target, Some(n), if (active.isDefined) Vector.empty else Vector(requester))))
      saveJobs()
      runNextJob()
    }
  }

  // A job like one waiting (not running) is that one, with one more requester.
  private def addJob(job: Job): Unit = jobs.indexWhere(j => j.target == job.target && j.build == job.build) match {
    case -1 => jobs :+= job
    case i => jobs = jobs.updated(i, jobs(i).copy(requesters = jobs(i).requesters ++ job.requesters))
  }

  private def instanceOfJob(job: Job): Option[Instance] = job.build match {
    case None => active
    case Some(n) => building.filter(_.record.number == n)
  }

  // The next job that can run: the active build's at once, a build's once it caught up. A build's whose build is gone is
  // dropped: a later build reads the store as it is now.
  private def runNextJob(): Unit = if (targeted.isEmpty) {
    jobs = jobs.filter(j => instanceOfJob(j).isDefined || { j.requesters.foreach(_ ! RebuildRefused("the build it was for ended")); false })
    jobs.find(j => j.build.isEmpty || caughtUp && building.exists(_.record.number == j.build.get)).foreach { job =>
      jobs = jobs.filterNot(_ eq job)
      startJob(job)
    }
  }

  private def startJob(job: Job): Unit = instanceOfJob(job).foreach { instance =>
    val types = typesOf(job.target)
    targeted = Some(Targeted(job, types, 0, None))
    log.info("Projection " + spec.name + " rebuilds " + job.target + job.build.fold("")(n => " in build " + n))
    // An active build that serves its tables unchanged while another is built only clears the target: the code may not
    // fit its documents (ServeStale), and the new build has the target rebuilt before it switches in.
    if (job.build.isEmpty && building.isDefined && spec.whileBuilding == ServeStale) instance.ref ! ServeUnchanged
    val (eventStore, scope) = (settings.eventStore, scopeOf(job.target))
    // F112: listed by the projection when it resets, after the deliveries it took before.
    instance.ref ! ResetForRebuild(job.target, () => Iterator.unfold(AggregateId(Long.MinValue)) { after =>
      val page = eventStore.aggregatesOf(types, after, 1000, scope)
      page.lastOption.map(last => (page.map(_.aggregateId), last.aggregateId))
    }.flatten.toVector)
  }

  // The jobs, the running one first, as lines: "aggregate:12", or "build 3 aggregate:12" for a build's.
  private def saveJobs(): Unit = logged("could not save its targeted rebuilds in the registry; one a stop interrupts is lost") {
    val all = targeted.map(_.job).toVector ++ jobs
    registry.setPendingTarget(spec.name, if (all.isEmpty) None else Some(all.map(j => j.build.fold("")(n => "build " + n + " ") + encode(j.target)).mkString("\n")))
  }

  private def decodeJob(line: String): Option[Job] =
    if (line.startsWith("build ")) line.stripPrefix("build ").split(" ", 2) match {
      case Array(n, target) => Try(n.toInt).toOption.flatMap(build => decode(target).map(Job(_, Some(build), Vector.empty)))
      case _ => None
    } else decode(line).map(Job(_, None, Vector.empty))

  private def scopeOf(target: RebuildTarget): AggregateScope = target match {
    case RebuildTarget.OneAggregate(id) => AggregateScope.Only(Set(id))
    case RebuildTarget.OneType(_) => AggregateScope.All
    case RebuildTarget.OneSpace(spaceId) => AggregateScope.InSpace(spaceId)
  }

  private def startTargetedBuilder(): Unit = for (t <- targeted; instance <- instanceOfJob(t.job)) {
    val plan = BuildPlan(listeners.filter { case (aggregateType, _) => t.types.contains(aggregateType) })
    val builder = context.watch(context.actorOf(Props(new ProjectionBuilder(instance.ref, instance.record.subscriber, plan, settings.eventStore,
      settings.subscriptionsState, runtime.sources, NoPositions, self, settings.builder, Some(scopeOf(t.target)))).withDispatcher(BlockingDispatcher.id)))
    targeted = Some(t.copy(builder = Some(builder)))
  }

  // A job left in the registry runs again at the next start, which repeats a finished rebuild harmlessly.
  private def targetRebuilt(): Unit = targeted.foreach { t =>
    log.info("Projection " + spec.name + " rebuilt " + t.target + t.job.build.fold("")(n => " in build " + n))
    targeted = None
    saveJobs()
    t.job.requesters.foreach(_ ! AggregatesRebuilt(t.target, t.aggregates))
    runNextJob()
    trySwitch()
  }

  private def logged(what: String)(work: => Unit): Unit = try work catch {
    case NonFatal(e) => log.error(e, "Projection " + spec.name + " " + what)
  }

  private def rebuild(): Unit = (building, targeted.map(_.job).orElse(jobs.headOption)) match {
    case (Some(b), _) => sender() ! RebuildRefused("build " + b.record.number + " is in progress")
    case (_, Some(j)) => sender() ! RebuildRefused("the rebuild of " + j.target + " is in progress")
    case _ =>
      val b = instanceOf(registry.startBuild(spec.name, spec.version, runtime.contextVersions))
      building = Some(b)
      log.info("Projection " + spec.name + " is built again on request (build " + b.record.number + ")")
      connect()
      if (spec.whileBuilding == ServeStale) serveUnchanged()
      sender() ! RebuildStarted(b.record.number)
  }

  // F153: as when a start finds a build in progress, the active build serves its tables unchanged while the new one is
  // built. It leaves the buses after the new build joined them (connect), so every event has a subscriber that takes it.
  private def serveUnchanged(): Unit = active.filter(_.subscribed).foreach { a =>
    buses.keys.foreach(_ ! RemoveSubscriber(a.ref))
    a.ref ! ServeUnchanged
    active = Some(a.copy(subscribed = false))
  }

  private def terminated(ref: ActorRef): Unit = if (buses.contains(ref) || unanswered.contains(ref)) busEnded(ref) else {
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
        // Its targeted rebuild runs again once the new instance caught up.
        targeted.filter(_.job.build.contains(building.get.record.number)).foreach { t =>
          t.builder.foreach(context.stop)
          jobs = t.job +: jobs
          targeted = None
          runNextJob()
        }
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
        // The registry keeps the jobs: they run again at the start; their requesters are answered now.
        (targeted.map(_.job) ++ jobs).foreach(j => j.requesters.foreach(_ ! RebuildRefused("the projection restarts; the rebuild of " + j.target + " runs again then")))
        targeted = None
        jobs = Vector.empty
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

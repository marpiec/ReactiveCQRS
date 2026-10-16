package io.reactivecqrs.core.commandhandler

import java.time.Instant
import org.apache.pekko.actor.{Actor, ActorRef, PoisonPill, Props, Status, Terminated}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.{GetAggregateRootCurrentMinVersion, GetAggregateRootCurrentVersion, GetEventsCurrentVersion}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.RemoveExpiredResponses
import io.reactivecqrs.core.commandhandler.CommandHandlerActor._
import io.reactivecqrs.core.delivery.RepublishRequest
import io.reactivecqrs.core.eventbus.EventsBusActor.{PublishEventsAck, RegisterPublisher}
import io.reactivecqrs.core.eventstore.EventStoreState
import io.reactivecqrs.core.uid.{NewAggregatesIdsPool, NewCommandsIdsPool, UidGeneratorActor}
import io.reactivecqrs.core.util.{MyActorLogging, RandomUtil}
import org.slf4j.Logger

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Random, Success, Try}
import scala.reflect.runtime.universe._

object AggregateCommandBusActor {

  private case object RemoveExpiredResponses

  def apply[AGGREGATE_ROOT:ClassTag:TypeTag](aggregateContext: AggregateContext[AGGREGATE_ROOT],
                                             uidGenerator: ActorRef, eventStoreState: EventStoreState,
                                             commandResponseState: CommandResponseState,
                                             eventBus: ActorRef, eventsReplayMode: Boolean, maxInactivityMillis: Long = 43200000L, keepAliveLimit: Int = 200,
                                             failedRestoreRetryAfter: FiniteDuration = 5.seconds): Props = { // 43200000 - 12 hours
    Props(new AggregateCommandBusActor[AGGREGATE_ROOT](
      uidGenerator,
      eventStoreState,
      commandResponseState,
      aggregateContext.commandHandlers,
      aggregateContext.eventHandlers,
      aggregateContext.rewriteHistoryCommandHandlers,
      eventBus,
      aggregateContext.eventsVersions,
      eventsReplayMode,
      keepAliveLimit, maxInactivityMillis, None,
      () => aggregateContext.initialAggregateRoot,
      aggregateContext.commandTimeout,
      failedRestoreRetryAfter))
  }


}


class AggregateCommandBusActor[AGGREGATE_ROOT:TypeTag](val uidGenerator: ActorRef,
                                                       eventStoreState: EventStoreState,
                                                       commandResponseState: CommandResponseState,
                                                       val commandsHandlers: AGGREGATE_ROOT => PartialFunction[Any, GenericCommandResult[Any]],
                                                       val eventHandlers: (UserId, Instant, AGGREGATE_ROOT) => PartialFunction[Any, AGGREGATE_ROOT],
                                                       val rewriteHistoryCommandHandlers: (Iterable[EventWithVersion[AGGREGATE_ROOT]], AGGREGATE_ROOT) => PartialFunction[Any, GenericCommandResult[Any]],
                                                       val eventBus: ActorRef,
                                                       val eventsVersions: List[EventVersion[AGGREGATE_ROOT]],
                                                       val eventsReplayMode: Boolean,
                                                       val keepAliveLimit: Int, val maxInactivityMillis: Long,
                                                       val eventsLogger: Option[Logger],
                                                       val initialState: () => AGGREGATE_ROOT,
                                                       val commandTimeout: FiniteDuration,
                                                       val failedRestoreRetryAfter: FiniteDuration)
                                                        (implicit aggregateRootClassTag: ClassTag[AGGREGATE_ROOT])extends Actor with MyActorLogging {

  val minimumInactivityMillis: Long = 120000L // 2 minutes

  val eventsVersionsMap: Map[EventTypeVersion, String] = {
    eventsVersions.flatMap(evs => evs.mapping.map(e => EventTypeVersion(evs.eventBaseType, e.version) -> e.eventType)).toMap
  }

  val eventsVersionsMapReverse: Map[String, EventTypeVersion] = {
    eventsVersions.flatMap(evs => evs.mapping.map(e => e.eventType -> EventTypeVersion(evs.eventBaseType, e.version))).toMap
  }

  val aggregateTypeName: String = aggregateRootClassTag.runtimeClass.getName
  val aggregateTypeSimpleName: String = aggregateRootClassTag.runtimeClass.getSimpleName


  private var nextAggregateId = 0L
  private var remainingAggregateIds = 0L

  private var nextCommandId = 0L
  private var remainingCommandsIds = 0L

  private val commandHandlerActors = mutable.HashMap[Long, ActorRef]()
  private val aggregateRepositoryActors = mutable.HashMap[Long, ActorRef]()
  private val childrenActivity = mutable.HashMap[Long, Long]()

  private var lastClear: Long = 0L

  private var spawnedCount = 0 // used to force acotr name uniqueness if poisonpill not worked (or not yet worked)

  private val idempotency = new Idempotency(commandResponseState)


  override def preStart(): Unit = {
    // The event bus routes re-publish requests for this aggregate type here (D8, D20c).
    eventBus ! RegisterPublisher(AggregateType(aggregateRootClassTag.toString), self)
    if (!eventsReplayMode) {
      // Hourly, at a random minute, so the buses of different aggregate types do not all clean up at once.
      val firstCleanUp = (60 + Random.nextInt(3600)).seconds
      context.system.scheduler.scheduleWithFixedDelay(firstCleanUp, 1.hour, self, RemoveExpiredResponses)(context.dispatcher)
    }
  }

  private def removeExpiredResponses(): Unit = {
    import context.dispatcher
    Future(idempotency.removeExpired()).onComplete {
      case Failure(e) => log.warning("Removing expired idempotent command responses failed: " + e.getMessage)
      case Success(removed) => if (removed > 0) log.info("Removed " + removed + " expired idempotent command responses")
    }
  }

  override def postRestart(reason: Throwable): Unit = {
    // do not call preStart
  }

  private def maintenance(id: AggregateId): Unit = {
    val now = System.nanoTime() / 1_000_000
    this.childrenActivity += id.asLong -> now
    clearOldAggregateRepositories(now)
  }

  override def receive: Receive = logReceive {
    case fce: FirstCommand[_,_] => answeringFailures(fce)(routeFirstCommand(fce.asInstanceOf[FirstCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]]))
    case cce: ConcurrentCommand[_,_] => answeringFailures(cce)(routeConcurrentCommand(cce.asInstanceOf[ConcurrentCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]]))
    case cce: RewriteHistoryCommand[_,_] => answeringFailures(cce)(routeRewriteHistoryCommand(cce.asInstanceOf[RewriteHistoryCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]]))
    case cce: RewriteHistoryConcurrentCommand[_,_] => answeringFailures(cce)(routeRewriteHistoryConcurrentCommand(cce.asInstanceOf[RewriteHistoryConcurrentCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]]))
    case ce: Command[_,_] => answeringFailures(ce)(routeCommand(ce.asInstanceOf[Command[AGGREGATE_ROOT, CustomCommandResponse[_]]]))
    case GetAggregate(id) => routeGetAggregateRoot(id)
    case GetAggregateMinVersion(id, version, maxMillis) => routeGetAggregateRootMinVersion(id, version, maxMillis)
    case GetAggregateForVersion(id, version) => routeGetAggregateRootForVersion(id, version)
    case GetAggregateAtInstant(id, instant) => routeGetAggregateRootAtInstant(id, instant)
    case GetEventsForAggregate(id) => routeGetEvents(id)
    case GetEventsForAggregateForVersion(id, version) => routeGetEventsForVersion(id, version)
    case ack: PublishEventsAck => delivered(ack)
    // Re-publications count as activity, so the owners they start are reaped like others when idle.
    case m: RepublishRequest =>
      getOrCreateAggregateRepositoryActor(m.aggregateId) ! m
      maintenance(m.aggregateId)
    case ReserveAggregateId =>
      val respondTo = sender()
      Try(takeNextAggregateId) match {
        case Success(id) => respondTo ! id
        case Failure(e) => respondTo ! Status.Failure(e)
      }
    case RemoveExpiredResponses => removeExpiredResponses()
    case Terminated(ref) => forgetRepositoryActor(ref)
    case m =>
      // Answered, not thrown: a throw would restart the bus, which stops its children and with them every
      // command in flight for this aggregate type (F70).
      log.warning("Cannot handle this kind of message: " + m.getClass.getName)
      sender() ! Status.Failure(new IllegalArgumentException("Cannot handle this kind of message: " + m + " class: " + m.getClass+". Maybe it should extend Command trait."))
  }

  // A failure while routing one command, such as the uid generator failing, is answered, not thrown: a throw would
  // restart the bus, which stops its children and every command in flight (F75, AR7).
  private def answeringFailures(command: Any)(route: => Unit): Unit = {
    val respondTo = sender()
    try route catch {
      case e: Exception =>
        val errorId = RandomUtil.generateRandomString(16)
        log.error(e, "Cannot route command " + command.getClass.getSimpleName + ", errorId: [" + errorId + "]")
        respondTo ! CommandHandlingError(command.getClass.getSimpleName, errorId, CommandId(0)) // no command id was assigned
    }
  }

  // Owners publish on behalf of this bus, so it gets the acknowledgements: it lives as long as the system, while an
  // owner may be reaped (later passivated) before slow subscribers catch up. Deleting delivered rows needs no aggregate
  // state. A permanent delete waits while a subscription of the aggregate is failed: that event's row stays, and the
  // ack of its next re-publication tries again. A live owner gets the ack to continue re-publishing its outbox.
  private def delivered(ack: PublishEventsAck): Unit = {
    val id = ack.aggregateId
    Try {
      ack.permanentDeleteAt match {
        case Some(_) if eventStoreState.deleteAggregatePermanently(id) =>
          clearOldAggregateRepository(id.asLong) // its state is gone
        case Some(at) =>
          log.warning("Aggregate " + id.asLong + " of type " + aggregateTypeSimpleName + " is not deleted yet: a subscription of it is failed")
          eventStoreState.deletePublished(id, ack.versions.filterNot(_ == at))
        case None =>
          eventStoreState.deletePublished(id, ack.versions)
      }
    }.failed.foreach(e => log.error(e, "Removing delivered events of aggregate " + id.asLong + " from the outbox failed; they are re-published later"))
    aggregateRepositoryActors.get(id.asLong).foreach(_ ! ack)
  }

  // A repository actor stops after a failed restore (F10) or when reaped; its command handler holds the stopped
  // ref, so both are forgotten and the next request creates fresh ones.
  private def forgetRepositoryActor(ref: ActorRef): Unit = {
    aggregateRepositoryActors.collectFirst { case (id, r) if r == ref => id }.foreach { id =>
      aggregateRepositoryActors -= id
      commandHandlerActors.remove(id).foreach(_ ! PoisonPill)
      childrenActivity -= id
    }
  }

  private def routeFirstCommand[RESPONSE <: CustomCommandResponse[_]](firstCommand: FirstCommand[AGGREGATE_ROOT, RESPONSE]): Unit = {
    val commandId = takeNextCommandId
    val newAggregateId = firstCommand.chosenAggregateId.getOrElse(takeNextAggregateId) // Actor construction might be delayed so we need to store current aggregate id
    val respondTo = sender() // with sender this shouldn't be the case, but just to be sure

    val commandHandler = createCommandHandlerActorIfNeeded(newAggregateId)
    commandHandler ! InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo, commandId, firstCommand)
    clearOldAggregateRepositories(System.nanoTime() / 1_000_000)
  }

  private def createCommandHandlerActorIfNeeded(aggregateId: AggregateId): ActorRef = {
    commandHandlerActors.getOrElse(aggregateId.asLong, {
      createCommandHandlerActor(aggregateId)
    })
  }

  private def createCommandHandlerActor(aggregateId: AggregateId): ActorRef = {
    val repositoryActor = getOrCreateAggregateRepositoryActor(aggregateId)
    val commandHandlerActor = context.actorOf(Props(new CommandHandlerActor[AGGREGATE_ROOT](
      aggregateId, repositoryActor, idempotency,
      commandsHandlers.asInstanceOf[AGGREGATE_ROOT => PartialFunction[Any, GenericCommandResult[Any]]],
      rewriteHistoryCommandHandlers.asInstanceOf[(Iterable[EventWithVersion[AGGREGATE_ROOT]], AGGREGATE_ROOT) => PartialFunction[Any, GenericCommandResult[Any]]],
      initialState, commandTimeout)),
      aggregateTypeSimpleName + "_CH_" + aggregateId.asLong+"_"+spawnedCount)
    spawnedCount += 1

    commandHandlerActors += aggregateId.asLong -> commandHandlerActor
    commandHandlerActor
  }

  private def clearOldAggregateRepositories(now: Long): Unit = {
    if(now - lastClear > 300000) { // no more often than every 5 minutes
      lastClear = now
      if (childrenActivity.size > keepAliveLimit) {
        val sorted = childrenActivity.toVector.sortBy(_._2)
        val toKill = sorted.take(sorted.length - keepAliveLimit)
        toKill.foreach(k => {
          if(now - k._2 > minimumInactivityMillis) { // give at least 2 minutes before killing in case of actor prolonged activity
            clearOldAggregateRepository(k._1)
          }
        })
      }

      val toKill = childrenActivity.filter(now - _._2 > maxInactivityMillis)
      toKill.foreach(k => clearOldAggregateRepository(k._1))
    }
  }

  private def clearOldAggregateRepository(aggregateId: Long): Unit = {
    childrenActivity -= aggregateId

    eventsLogger.foreach(_.debug("Terminating aggregate " + aggregateId+" because of inactivity"))

    commandHandlerActors.get(aggregateId).foreach(a => a ! PoisonPill)
    aggregateRepositoryActors.get(aggregateId).foreach(a => a ! PoisonPill)
    commandHandlerActors -= aggregateId
    aggregateRepositoryActors -= aggregateId
  }


  private def getOrCreateAggregateRepositoryActor(aggregateId: AggregateId): ActorRef = {
    childrenActivity += aggregateId.asLong -> System.nanoTime() / 1_000_000
    aggregateRepositoryActors.getOrElse(aggregateId.asLong, {
      val ref = context.actorOf(Props(new AggregateRepositoryActor[AGGREGATE_ROOT](aggregateId, eventStoreState, idempotency, eventBus, self, eventHandlers, initialState, None, eventsVersionsMap, eventsVersionsMapReverse, failedRestoreRetryAfter)),
        aggregateTypeSimpleName + "_AR_" + aggregateId.asLong+"_"+spawnedCount)
      spawnedCount += 1
      context.watch(ref)
      aggregateRepositoryActors += aggregateId.asLong -> ref
      ref
    })
  }

  private def createAggregateRepositoryActorForVersion(aggregateId: AggregateId, aggregateVersion: AggregateVersion): ActorRef = {
    val actor = context.actorOf(Props(new AggregateRepositoryActor[AGGREGATE_ROOT](aggregateId, eventStoreState, idempotency, eventBus, self, eventHandlers, initialState, Some(Left(aggregateVersion)), eventsVersionsMap, eventsVersionsMapReverse, failedRestoreRetryAfter)),
      aggregateTypeSimpleName + "_ARV_" + aggregateId.asLong+"_"+aggregateVersion.asInt+"_"+spawnedCount)
    spawnedCount += 1
    actor
  }

  private def createAggregateRepositoryActorForInstant(aggregateId: AggregateId, instant: Instant): ActorRef = {
    val actor = context.actorOf(Props(new AggregateRepositoryActor[AGGREGATE_ROOT](aggregateId, eventStoreState, idempotency, eventBus, self, eventHandlers, initialState, Some(Right(instant)), eventsVersionsMap, eventsVersionsMapReverse, failedRestoreRetryAfter)),
      aggregateTypeSimpleName + "_ART_" + aggregateId.asLong+"_"+instant.getEpochSecond+"_"+spawnedCount)
    spawnedCount += 1
    actor
  }




  private def routeConcurrentCommand[RESPONSE <: CustomCommandResponse[_]](command: ConcurrentCommand[AGGREGATE_ROOT, RESPONSE]): Unit = {
    val commandId = takeNextCommandId
    val respondTo = sender()

    val commandHandler = createCommandHandlerActorIfNeeded(command.aggregateId)

    commandHandler ! InternalConcurrentCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo, commandId, command)
    maintenance(command.aggregateId)
  }

  private def routeRewriteHistoryCommand[RESPONSE <: CustomCommandResponse[_]](command: RewriteHistoryCommand[AGGREGATE_ROOT, RESPONSE]): Unit = {
    val commandId = takeNextCommandId
    val respondTo = sender()

    val commandHandler = createCommandHandlerActorIfNeeded(command.aggregateId)

    commandHandler ! InternalRewriteHistoryCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo, commandId, command)
    maintenance(command.aggregateId)
  }

  private def routeRewriteHistoryConcurrentCommand[RESPONSE <: CustomCommandResponse[_]](command: RewriteHistoryConcurrentCommand[AGGREGATE_ROOT, RESPONSE]): Unit = {
    val commandId = takeNextCommandId
    val respondTo = sender()

    val commandHandler = createCommandHandlerActorIfNeeded(command.aggregateId)

    commandHandler ! InternalRewriteHistoryConcurrentCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo, commandId, command)
    maintenance(command.aggregateId)
  }

  private def routeCommand[RESPONSE <: CustomCommandResponse[_]](command: Command[AGGREGATE_ROOT, RESPONSE]): Unit = {
    val commandId = takeNextCommandId
    val respondTo = sender()

    val commandHandler = createCommandHandlerActorIfNeeded(command.aggregateId)

    commandHandler ! InternalFollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo, commandId, command)
    maintenance(command.aggregateId)
  }



  private def routeGetAggregateRoot(id: AggregateId): Unit = {
    val respondTo = sender()
    val aggregateRepository = getOrCreateAggregateRepositoryActor(id)

    aggregateRepository ! GetAggregateRootCurrentVersion(respondTo)
    maintenance(id)
  }

  private def routeGetEvents(id: AggregateId): Unit = {
    val respondTo = sender()
    val aggregateRepository = getOrCreateAggregateRepositoryActor(id)

    aggregateRepository ! GetEventsCurrentVersion(respondTo)
    maintenance(id)
  }

  private def routeGetAggregateRootMinVersion(id: AggregateId, version: AggregateVersion, maxMillis: Int): Unit = {
    val respondTo = sender()
    val aggregateRepository = getOrCreateAggregateRepositoryActor(id)

    aggregateRepository ! GetAggregateRootCurrentMinVersion(respondTo, version, maxMillis)
    maintenance(id)
  }

  private def routeGetAggregateRootForVersion(id: AggregateId, version: AggregateVersion): Unit = {
    val respondTo = sender()
    val temporaryAggregateRepositoryForVersion = createAggregateRepositoryActorForVersion(id, version)

    temporaryAggregateRepositoryForVersion ! GetAggregateRootCurrentVersion(respondTo)
  }


  private def routeGetAggregateRootAtInstant(id: AggregateId, instant: Instant): Unit = {
    val respondTo = sender()
    val temporaryAggregateRepositoryForInstant = createAggregateRepositoryActorForInstant(id, instant)

    temporaryAggregateRepositoryForInstant ! GetAggregateRootCurrentVersion(respondTo)
  }


  private def routeGetEventsForVersion(id: AggregateId, version: AggregateVersion): Unit = {
    val respondTo = sender()
    val temporaryAggregateRepositoryForVersion = createAggregateRepositoryActorForVersion(id, version)

    temporaryAggregateRepositoryForVersion ! GetEventsCurrentVersion(respondTo)
  }

  private def takeNextAggregateId: AggregateId = {
    if(remainingAggregateIds == 0) {
      // TODO get rid of ask pattern
      implicit val timeout: Timeout = Timeout(60 seconds)
      val pool: Future[NewAggregatesIdsPool] = (uidGenerator ? UidGeneratorActor.GetNewAggregatesIdsPool).mapTo[NewAggregatesIdsPool]
      val newAggregatesIdsPool: NewAggregatesIdsPool = Await.result(pool, 60 seconds)
      remainingAggregateIds = newAggregatesIdsPool.size
      nextAggregateId = newAggregatesIdsPool.from
    }

    remainingAggregateIds -= 1
    val aggregateId = AggregateId(nextAggregateId)
    nextAggregateId += 1
    aggregateId

  }

  private def takeNextCommandId: CommandId = {
    if(remainingCommandsIds == 0) {
      // TODO get rid of ask pattern
      implicit val timeout: Timeout = Timeout(60 seconds)
      val pool: Future[NewCommandsIdsPool] = (uidGenerator ? UidGeneratorActor.GetNewCommandsIdsPool).mapTo[NewCommandsIdsPool]
      val newCommandsIdsPool: NewCommandsIdsPool = Await.result(pool, 60 seconds)
      remainingCommandsIds = newCommandsIdsPool.size
      nextCommandId = newCommandsIdsPool.from
    }

    remainingCommandsIds -= 1
    val commandId = CommandId(nextCommandId)
    nextCommandId += 1
    commandId


  }
}

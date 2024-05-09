package io.reactivecqrs.core.commandhandler

import java.time.Instant
import org.apache.pekko.actor.{Actor, ActorContext, ActorRef, PoisonPill, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.{GetAggregateRootCurrentMinVersion, GetAggregateRootCurrentVersion, GetEventsCurrentVersion}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.EnsureEventsPublished
import io.reactivecqrs.core.commandhandler.CommandHandlerActor._
import io.reactivecqrs.core.eventstore.EventStoreState
import io.reactivecqrs.core.uid.{NewAggregatesIdsPool, NewCommandsIdsPool, UidGeneratorActor}
import io.reactivecqrs.core.util.MyActorLogging

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object AggregateCommandBusActor {

  private case class EnsureEventsPublished(oldOnly: Boolean)

  def apply[AGGREGATE_ROOT:ClassTag:TypeTag](aggregateContext: AggregateContext[AGGREGATE_ROOT],
                                             uidGenerator: ActorRef, eventStoreState: EventStoreState,
                                             commandResponseState: CommandResponseState,
                                             eventBus: ActorRef, eventsReplayMode: Boolean, maxInactivityMillis: Long = 43200000L, keepAliveLimit: Int = 200): Props = { // 43200000 - 12 hours
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
      keepAliveLimit, maxInactivityMillis,
      aggregateContext.initialAggregateRoot _))
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
                                                       val initialState: () => AGGREGATE_ROOT)
                                                        (implicit aggregateRootClassTag: ClassTag[AGGREGATE_ROOT])extends Actor with MyActorLogging {

  val eventsVersionsMap: Map[EventTypeVersion, String] = {
    eventsVersions.flatMap(evs => evs.mapping.map(e => EventTypeVersion(evs.eventBaseType, e.version) -> e.eventType)).toMap
  }

  val eventsVersionsMapReverse: Map[String, EventTypeVersion] = {
    eventsVersions.flatMap(evs => evs.mapping.map(e => e.eventType -> EventTypeVersion(evs.eventBaseType, e.version))).toMap
  }

  val aggregateTypeName = aggregateRootClassTag.runtimeClass.getName
  val aggregateTypeSimpleName = aggregateRootClassTag.runtimeClass.getSimpleName


  private var nextAggregateId = 0L
  private var remainingAggregateIds = 0L

  private var nextCommandId = 0L
  private var remainingCommandsIds = 0L

  private val commandHandlerActors = mutable.HashMap[Long, ActorRef]()
  private val aggregateRepositoryActors = mutable.HashMap[Long, ActorRef]()
  private val childrenActivity = mutable.HashMap[Long, Long]()

  private var lastClear: Long = 0L

  private var spawnedCount = 0 // used to force acotr name uniqueness if poisonpill not worked (or not yet worked)


  if (!eventsReplayMode) {
    context.system.scheduler.scheduleOnce(1.second, self, EnsureEventsPublished(false))(context.dispatcher)
  }

  override def preStart() {
    if (!eventsReplayMode) {
      context.system.scheduler.schedule(60.seconds, 60.seconds, self, EnsureEventsPublished(true))(context.dispatcher)
    }
  }

  override def postRestart(reason: Throwable) {
    // do not call preStart
  }




  def maintenance(id: AggregateId)(implicit context: ActorContext): Unit = {
    val now = System.currentTimeMillis()
    this.childrenActivity += id.asLong -> now
    clearOldAggregateRepositories(now)
  }

  override def receive: Receive = logReceive {
    case fce: FirstCommand[_,_] => routeFirstCommand(fce.asInstanceOf[FirstCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]])
    case cce: ConcurrentCommand[_,_] => routeConcurrentCommand(cce.asInstanceOf[ConcurrentCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]])
    case cce: RewriteHistoryCommand[_,_] => routeRewriteHistoryCommand(cce.asInstanceOf[RewriteHistoryCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]])
    case cce: RewriteHistoryConcurrentCommand[_,_] => routeRewriteHistoryConcurrentCommand(cce.asInstanceOf[RewriteHistoryConcurrentCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]])
    case ce: Command[_,_] => routeCommand(ce.asInstanceOf[Command[AGGREGATE_ROOT, CustomCommandResponse[_]]])
    case GetAggregate(id) => routeGetAggregateRoot(id)
    case GetAggregateMinVersion(id, version, maxMillis) => routeGetAggregateRootMinVersion(id, version, maxMillis)
    case GetAggregateForVersion(id, version) => routeGetAggregateRootForVersion(id, version)
    case GetEventsForAggregate(id) => routeGetEvents(id)
    case GetEventsForAggregateForVersion(id, version) => routeGetEventsForVersion(id, version)
    case EnsureEventsPublished(oldOnly) => ensureEventsPublished(oldOnly)
    case m => throw new IllegalArgumentException("Cannot handle this kind of message: " + m + " class: " + m.getClass+". Maybe it should extend Command trait.")
  }

  private def ensureEventsPublished(oldOnly: Boolean): Unit = {
    eventStoreState.readAggregatesWithEventsToPublish(aggregateTypeName, oldOnly)(aggregateId => {
      log.info("Initializing Aggregate to resend events (oldOnly="+oldOnly+") " + aggregateId)
      createCommandHandlerActorIfNeeded(aggregateId)
    })
  }

  private def routeFirstCommand[RESPONSE <: CustomCommandResponse[_]](firstCommand: FirstCommand[AGGREGATE_ROOT, RESPONSE]): Unit = {
    val commandId = takeNextCommandId
    val newAggregateId = takeNextAggregateId // Actor construction might be delayed so we need to store current aggregate id
    val respondTo = sender() // with sender this shouldn't be the case, but just to be sure

    val commandHandler = createCommandHandlerActorIfNeeded(newAggregateId)
    commandHandler ! InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo, commandId, firstCommand)
    clearOldAggregateRepositories(System.currentTimeMillis())
  }

  private def createCommandHandlerActorIfNeeded(aggregateId: AggregateId): ActorRef = {
    commandHandlerActors.getOrElse(aggregateId.asLong, {
      createCommandHandlerActor(aggregateId)
    })
  }

  private def createCommandHandlerActor(aggregateId: AggregateId): ActorRef = {
    val repositoryActor = getOrCreateAggregateRepositoryActor(aggregateId)
    val commandHandlerActor = context.actorOf(Props(new CommandHandlerActor[AGGREGATE_ROOT](
      aggregateId, repositoryActor, commandResponseState,
      commandsHandlers.asInstanceOf[AGGREGATE_ROOT => PartialFunction[Any, GenericCommandResult[Any]]],
      rewriteHistoryCommandHandlers.asInstanceOf[(Iterable[EventWithVersion[AGGREGATE_ROOT]], AGGREGATE_ROOT) => PartialFunction[Any, GenericCommandResult[Any]]],
      initialState)),
      aggregateTypeSimpleName + "_CH_" + aggregateId.asLong)

    commandHandlerActors += aggregateId.asLong -> commandHandlerActor
    commandHandlerActor
  }

  private def clearOldAggregateRepositories(now: Long): Unit = {
    if(now - lastClear > 300000) { // no more often than every 5 minutes
      lastClear = now
      val sorted = childrenActivity.toList.sortBy(_._2)
      if (sorted.length > keepAliveLimit) {
        val toKill = sorted.take(sorted.length - keepAliveLimit)
        toKill.foreach(k => clearOldAggregateRepository(k._1))
      }

      val toKill = childrenActivity.filter(now - _._2 > maxInactivityMillis)
      toKill.foreach(k => clearOldAggregateRepository(k._1))
    }
  }

  private def clearOldAggregateRepository(aggregateId: Long): Unit = {
    childrenActivity -= aggregateId

    commandHandlerActors.get(aggregateId).foreach(a => a ! PoisonPill)
    aggregateRepositoryActors.get(aggregateId).foreach(a => a ! PoisonPill)
    commandHandlerActors -= aggregateId
    aggregateRepositoryActors -= aggregateId
  }


  private def getOrCreateAggregateRepositoryActor(aggregateId: AggregateId): ActorRef = {
    childrenActivity += aggregateId.asLong -> System.currentTimeMillis()
    aggregateRepositoryActors.getOrElse(aggregateId.asLong, {
      val ref = context.actorOf(Props(new AggregateRepositoryActor[AGGREGATE_ROOT](aggregateId, eventStoreState, commandResponseState, eventBus, eventHandlers, initialState, None, eventsVersionsMap, eventsVersionsMapReverse)),
        aggregateTypeSimpleName + "_AR_" + aggregateId.asLong+"_"+spawnedCount)
      spawnedCount += 1
      aggregateRepositoryActors += aggregateId.asLong -> ref
      ref
    })
  }

  private def createAggregateRepositoryActorForVersion(aggregateId: AggregateId, aggregateVersion: AggregateVersion): ActorRef = {
    val actor = context.actorOf(Props(new AggregateRepositoryActor[AGGREGATE_ROOT](aggregateId, eventStoreState, commandResponseState, eventBus, eventHandlers, initialState, Some(aggregateVersion), eventsVersionsMap, eventsVersionsMapReverse)),
      aggregateTypeSimpleName + "_ARV_" + aggregateId.asLong+"_"+aggregateVersion.asInt+"_"+spawnedCount)
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


  private def routeGetEventsForVersion(id: AggregateId, version: AggregateVersion): Unit = {
    val respondTo = sender()
    val temporaryAggregateRepositoryForVersion = createAggregateRepositoryActorForVersion(id, version)

    temporaryAggregateRepositoryForVersion ! GetEventsCurrentVersion(respondTo)
  }

  private def takeNextAggregateId: AggregateId = {
    if(remainingAggregateIds == 0) {
      // TODO get rid of ask pattern
      implicit val timeout = Timeout(60 seconds)
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
      implicit val timeout = Timeout(60 seconds)
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

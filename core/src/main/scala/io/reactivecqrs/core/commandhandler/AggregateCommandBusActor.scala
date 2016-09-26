package io.reactivecqrs.core.commandhandler

import java.time.Instant

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.GetAggregateRoot
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.{AggregateActors, EnsureEventsPublished}
import io.reactivecqrs.core.commandhandler.CommandHandlerActor.{InternalConcurrentCommandEnvelope, InternalFirstCommandEnvelope, InternalFollowingCommandEnvelope}
import io.reactivecqrs.core.commandlog.{CommandLogActor, CommandLogState}
import io.reactivecqrs.core.eventstore.EventStoreState
import io.reactivecqrs.core.uid.{NewAggregatesIdsPool, NewCommandsIdsPool, UidGeneratorActor}
import io.reactivecqrs.core.util.ActorLogging

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object AggregateCommandBusActor {

  private case class AggregateActors(commandHandler: ActorRef, repository: ActorRef, commandLog: ActorRef)

  private case class EnsureEventsPublished(oldOnly: Boolean)

  def apply[AGGREGATE_ROOT:ClassTag:TypeTag](aggregateContext: AggregateContext[AGGREGATE_ROOT],
                                             uidGenerator: ActorRef, eventStoreState: EventStoreState, commandLogState: CommandLogState,
                                             commandResponseState: CommandResponseState,
                                             eventBus: ActorRef): Props = {
    Props(new AggregateCommandBusActor[AGGREGATE_ROOT](
      uidGenerator,
      eventStoreState,
      commandLogState,
      commandResponseState,
      aggregateContext.commandHandlers,
      aggregateContext.eventHandlers,
      eventBus,
      aggregateContext.eventsVersions,
      aggregateContext.initialAggregateRoot _))
  }


}


class AggregateCommandBusActor[AGGREGATE_ROOT:TypeTag](val uidGenerator: ActorRef,
                                                       eventStoreState: EventStoreState,
                                                       commandLogState: CommandLogState,
                                                       commandResponseState: CommandResponseState,
                                                       val commandsHandlers: AGGREGATE_ROOT => PartialFunction[Any, CustomCommandResult[Any]],
                                                       val eventHandlers: (UserId, Instant, AGGREGATE_ROOT) => PartialFunction[Any, AGGREGATE_ROOT],
                                                       val eventBus: ActorRef,
                                                       val eventsVersions: List[EventVersion[AGGREGATE_ROOT]],
                                                       val initialState: () => AGGREGATE_ROOT)
                                                        (implicit aggregateRootClassTag: ClassTag[AGGREGATE_ROOT])extends Actor with ActorLogging {

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

  private val aggregatesActors = mutable.HashMap[Long, AggregateActors]()

  context.system.scheduler.scheduleOnce(1.second, self, EnsureEventsPublished(false))(context.dispatcher)
  context.system.scheduler.schedule(60.seconds, 60.seconds, self, EnsureEventsPublished(true))(context.dispatcher)

  override def receive: Receive = logReceive {
    case fce: FirstCommand[_,_] => routeFirstCommand(fce.asInstanceOf[FirstCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]])
    case cce: ConcurrentCommand[_,_] => routeConcurrentCommand(cce.asInstanceOf[ConcurrentCommand[AGGREGATE_ROOT, CustomCommandResponse[_]]])
    case ce: Command[_,_] => routeCommand(ce.asInstanceOf[Command[AGGREGATE_ROOT, CustomCommandResponse[_]]])
    case GetAggregate(id) => routeGetAggregateRoot(id)
    case GetAggregateForVersion(id, version) => routeGetAggregateRootForVersion(id, version)
    case GetEventsForAggregate(id) => ???
    case GetEventsForAggregateForVersion(id, version) => ???
    case EnsureEventsPublished(oldOnly) => ensureEventsPublished(oldOnly)
    case m => throw new IllegalArgumentException("Cannot handle this kind of message: " + m + " class: " + m.getClass)
  }

  private def ensureEventsPublished(oldOnly: Boolean): Unit = {
    eventStoreState.readAggregatesWithEventsToPublish(aggregateTypeName, oldOnly)(aggregateId => {
      log.info("Initializing Aggregate to resend events (oldOnly="+oldOnly+") " + aggregateId)
      createAggregateActorsIfNeeded(aggregateId)
    })
  }

  private def routeFirstCommand[RESPONSE <: CustomCommandResponse[_]](firstCommand: FirstCommand[AGGREGATE_ROOT, RESPONSE]): Unit = {
    val commandId = takeNextCommandId
    val newAggregateId = takeNextAggregateId // Actor construction might be delayed so we need to store current aggregate id
    val respondTo = sender() // with sender this shouldn't be the case, but just to be sure

    val aggregateActors = createAggregateActorsIfNeeded(newAggregateId)
    aggregateActors.commandHandler ! InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo, commandId, firstCommand)
  }

  private def createAggregateActorsIfNeeded(aggregateId: AggregateId): AggregateActors = {
    aggregatesActors.getOrElse(aggregateId.asLong, {
      createAggregateActors(aggregateId)
    })
  }

  private def createAggregateActors(aggregateId: AggregateId): AggregateActors = {
    val repositoryActor = getOrCreateAggregateRepositoryActor(aggregateId)

    val commandLogActor = context.actorOf(Props(new CommandLogActor[AGGREGATE_ROOT](aggregateId, commandLogState)), aggregateTypeSimpleName+"_CommandLog_" + aggregateId.asLong)

    val commandHandlerActor = context.actorOf(Props(new CommandHandlerActor[AGGREGATE_ROOT](
      aggregateId, repositoryActor, commandLogActor, commandResponseState,
      commandsHandlers.asInstanceOf[AGGREGATE_ROOT => PartialFunction[Any, CustomCommandResult[Any]]],
    initialState)),
      aggregateTypeSimpleName + "_CommandHandler_" + aggregateId.asLong)

    val actors = AggregateActors(commandHandlerActor, repositoryActor, commandLogActor)
    aggregatesActors += aggregateId.asLong -> actors
    actors
  }

  private def getOrCreateAggregateRepositoryActor(aggregateId: AggregateId): ActorRef = {
    context.child(aggregateTypeSimpleName + "_AggregateRepository_" + aggregateId.asLong).getOrElse(
      context.actorOf(Props(new AggregateRepositoryActor[AGGREGATE_ROOT](aggregateId, eventStoreState, commandResponseState, eventBus, eventHandlers, initialState, None, eventsVersionsMap, eventsVersionsMapReverse)),
        aggregateTypeSimpleName + "_AggregateRepository_" + aggregateId.asLong)
    )
  }

  private def getOrCreateAggregateRepositoryActorForVersion(aggregateId: AggregateId, aggregateVersion: AggregateVersion): ActorRef = {
    context.child(aggregateTypeSimpleName + "_AggregateRepositoryForVersion_" + aggregateId.asLong+"_"+aggregateVersion.asInt).getOrElse(
      context.actorOf(Props(new AggregateRepositoryActor[AGGREGATE_ROOT](aggregateId, eventStoreState, commandResponseState, eventBus, eventHandlers, initialState, Some(aggregateVersion), eventsVersionsMap, eventsVersionsMapReverse)),
        aggregateTypeSimpleName + "_AggregateRepositoryForVersion_" + aggregateId.asLong+"_"+aggregateVersion.asInt)
    )
  }




  private def routeConcurrentCommand[RESPONSE <: CustomCommandResponse[_]](command: ConcurrentCommand[AGGREGATE_ROOT, RESPONSE]): Unit = {
    val commandId = takeNextCommandId
    val respondTo = sender()

    val aggregateActors = createAggregateActorsIfNeeded(command.aggregateId)

    aggregateActors.commandHandler ! InternalConcurrentCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo, commandId, command)
  }

  private def routeCommand[RESPONSE <: CustomCommandResponse[_]](command: Command[AGGREGATE_ROOT, RESPONSE]): Unit = {
    val commandId = takeNextCommandId
    val respondTo = sender()

    val aggregateActors = createAggregateActorsIfNeeded(command.aggregateId)

    aggregateActors.commandHandler ! InternalFollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo, commandId, command)
  }



  private def routeGetAggregateRoot(id: AggregateId): Unit = {
    val respondTo = sender()
    val aggregateRepository = getOrCreateAggregateRepositoryActor(id)

    aggregateRepository ! GetAggregateRoot(respondTo)
  }

  private def routeGetAggregateRootForVersion(id: AggregateId, version: AggregateVersion): Unit = {
    val respondTo = sender()
    val temporaryAggregateRepositoryForVersion = getOrCreateAggregateRepositoryActorForVersion(id, version)

    temporaryAggregateRepositoryForVersion ! GetAggregateRoot(respondTo)
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

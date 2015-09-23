package io.reactivecqrs.core.commandhandler

import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import akka.pattern.ask
import akka.util.Timeout
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId}
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor
import io.reactivecqrs.core.aggregaterepository.AggregateRepositoryActor.GetAggregateRoot
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.AggregateActors
import io.reactivecqrs.core.commandhandler.CommandHandlerActor.{InternalConcurrentCommandEnvelope, InternalFirstCommandEnvelope, InternalFollowingCommandEnvelope}
import io.reactivecqrs.core.eventstore.EventStoreState
import io.reactivecqrs.core.uid.{NewAggregatesIdsPool, NewCommandsIdsPool, UidGeneratorActor}
import io.reactivecqrs.core.util.ActorLogging

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object AggregateCommandBusActor {

  private case class AggregateActors(commandHandler: ActorRef, repository: ActorRef)


  def apply[AGGREGATE_ROOT:ClassTag:TypeTag](aggregate: AggregateContext[AGGREGATE_ROOT],
                                             uidGenerator: ActorRef, eventStore: EventStoreState, eventBus: ActorRef): Props = {
    Props(new AggregateCommandBusActor[AGGREGATE_ROOT](
      uidGenerator,
      eventStore,
      aggregate.commandHandlers,
      aggregate.eventHandlers,
      eventBus,
    aggregate.initialAggregateRoot _))


  }


}


class AggregateCommandBusActor[AGGREGATE_ROOT:TypeTag](val uidGenerator: ActorRef,
                                              eventStore: EventStoreState,
                                      val commandsHandlers: AGGREGATE_ROOT => PartialFunction[Any, CommandResult[Any]],
                                     val eventHandlers: AGGREGATE_ROOT => PartialFunction[Any, AGGREGATE_ROOT],
                                                val eventBus: ActorRef,
                                                        val initialState: () => AGGREGATE_ROOT)
                                                        (implicit aggregateRootClassTag: ClassTag[AGGREGATE_ROOT])extends Actor with ActorLogging {



  private val aggregateTypeSimpleName = aggregateRootClassTag.runtimeClass.getSimpleName


  private var nextAggregateId = 0L
  private var remainingAggregateIds = 0L

  private var nextCommandId = 0L
  private var remainingCommandsIds = 0L



  private val aggregatesActors = mutable.HashMap[Long, AggregateActors]()


  override def receive: Receive = logReceive {
    case fce: FirstCommand[_,_] => routeFirstCommand(fce.asInstanceOf[FirstCommand[AGGREGATE_ROOT, _]])
    case cce: ConcurrentCommand[_,_] => routeConcurrentCommand(cce.asInstanceOf[ConcurrentCommand[AGGREGATE_ROOT, _]])
    case ce: Command[_,_] => routeCommand(ce.asInstanceOf[Command[AGGREGATE_ROOT, _]])
    case GetAggregate(id) => routeGetAggregateRoot(id)
    case GetAggregateForVersion(id, version) => routeGetAggregateRootForVersion(id, version)
    case m => throw new IllegalArgumentException("Cannot handle this kind of message: " + m + " class: " + m.getClass)
  }

  private def routeFirstCommand[RESPONSE](firstCommand: FirstCommand[AGGREGATE_ROOT, RESPONSE]): Unit = {
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

    val commandHandlerActor = context.actorOf(Props(new CommandHandlerActor[AGGREGATE_ROOT](
      aggregateId, repositoryActor,
      commandsHandlers.asInstanceOf[AGGREGATE_ROOT => PartialFunction[Any, CommandResult[Any]]],
    initialState)),
      aggregateTypeSimpleName + "_CommandHandler_" + aggregateId.asLong)

    val actors = AggregateActors(commandHandlerActor, repositoryActor)
    aggregatesActors += aggregateId.asLong -> actors
    actors
  }

  private def getOrCreateAggregateRepositoryActor(aggregateId: AggregateId): ActorRef = {
    context.child(aggregateTypeSimpleName + "_AggregateRepository_" + aggregateId.asLong).getOrElse(
      context.actorOf(Props(new AggregateRepositoryActor[AGGREGATE_ROOT](aggregateId, eventStore, eventBus, eventHandlers, initialState, None)),
        aggregateTypeSimpleName + "_AggregateRepository_" + aggregateId.asLong)
    )
  }

  private def getOrCreateAggregateRepositoryActorForVersion(aggregateId: AggregateId, aggregateVersion: AggregateVersion): ActorRef = {
    context.child(aggregateTypeSimpleName + "_AggregateRepository_" + aggregateId.asLong+"_"+aggregateVersion.asInt).getOrElse(
      context.actorOf(Props(new AggregateRepositoryActor[AGGREGATE_ROOT](aggregateId, eventStore, eventBus, eventHandlers, initialState, Some(aggregateVersion))),
        aggregateTypeSimpleName + "_AggregateRepository_" + aggregateId.asLong+"_"+aggregateVersion.asInt)
    )
  }


  private def routeConcurrentCommand[RESPONSE](command: ConcurrentCommand[AGGREGATE_ROOT, RESPONSE]): Unit = {
    val commandId = takeNextCommandId
    val respondTo = sender()

    val aggregateActors = createAggregateActorsIfNeeded(command.aggregateId)

    aggregateActors.commandHandler ! InternalConcurrentCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo, commandId, command)
  }

  private def routeCommand[RESPONSE](command: Command[AGGREGATE_ROOT, RESPONSE]): Unit = {
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
      implicit val timeout = Timeout(5 seconds)
      val pool: Future[NewAggregatesIdsPool] = (uidGenerator ? UidGeneratorActor.GetNewAggregatesIdsPool).mapTo[NewAggregatesIdsPool]
      val newAggregatesIdsPool: NewAggregatesIdsPool = Await.result(pool, 5 seconds)
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
      implicit val timeout = Timeout(5 seconds)
      val pool: Future[NewCommandsIdsPool] = (uidGenerator ? UidGeneratorActor.GetNewCommandsIdsPool).mapTo[NewCommandsIdsPool]
      val newCommandsIdsPool: NewCommandsIdsPool = Await.result(pool, 5 seconds)
      remainingCommandsIds = newCommandsIdsPool.size
      nextCommandId = newCommandsIdsPool.from
    }

    remainingCommandsIds -= 1
    val commandId = CommandId(nextCommandId)
    nextCommandId += 1
    commandId


  }
}

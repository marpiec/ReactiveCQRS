package io.reactivecqrs.core

import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import akka.pattern.ask
import akka.util.Timeout
import io.reactivecqrs.api._
import io.reactivecqrs.api.guid.{AggregateId, UserId, CommandId}
import io.reactivecqrs.core.AggregateCommandBusActor._
import io.reactivecqrs.core.AggregateRepositoryActor.ReturnAggregateRoot
import io.reactivecqrs.core.CommandHandlerActor.{InternalFirstCommandEnvelope, InternalFollowingCommandEnvelope}
import io.reactivecqrs.core.uid.{NewAggregatesIdsPool, NewCommandsIdsPool, UidGeneratorActor}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag


object AggregateCommandBusActor {


  object CommandEnvelope {

    def apply[AGGREGATE_ROOT, RESPONSE](userId: UserId, command: FirstCommand[AGGREGATE_ROOT, RESPONSE]) = FirstCommandEnvelope(userId, command)
    def apply[AGGREGATE_ROOT, RESPONSE](userId: UserId, aggregateId: AggregateId,
                                        expectedVersion: AggregateVersion, command: Command[AGGREGATE_ROOT, RESPONSE]) = FollowingCommandEnvelope(userId,aggregateId, expectedVersion, command)

  }
  case class FirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE](userId: UserId,
                                                            command: FirstCommand[AGGREGATE_ROOT, RESPONSE])



  case class FollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE](userId: UserId,
                                                                aggregateId: AggregateId,
                                                                expectedVersion: AggregateVersion,
                                                                command: Command[AGGREGATE_ROOT, RESPONSE])

  private case class AggregateActors(commandHandler: ActorRef, repository: ActorRef)


  def apply[AGGREGATE_ROOT : ClassTag](aggregate: AggregateCommandBus[AGGREGATE_ROOT], uidGenerator: ActorRef): Props = {
    Props(new AggregateCommandBusActor[AGGREGATE_ROOT](
      uidGenerator,
      aggregate.commandsHandlers.asInstanceOf[Seq[CommandHandler[AGGREGATE_ROOT,AbstractCommand[AGGREGATE_ROOT, _],_]]],
      aggregate.eventsHandlers.asInstanceOf[Seq[AbstractEventHandler[AGGREGATE_ROOT, Event[AGGREGATE_ROOT]]]]))

  }


}


class AggregateCommandBusActor[AGGREGATE_ROOT](val uidGenerator: ActorRef,
                                      val commandsHandlersSeq: Seq[CommandHandler[AGGREGATE_ROOT,AbstractCommand[AGGREGATE_ROOT, _],_]],
                                     val eventsHandlersSeq: Seq[AbstractEventHandler[AGGREGATE_ROOT, Event[AGGREGATE_ROOT]]])
                                                        (implicit aggregateRootClassTag: ClassTag[AGGREGATE_ROOT])extends Actor {


  private val commandsHandlers:Map[String, CommandHandler[AGGREGATE_ROOT,AbstractCommand[AGGREGATE_ROOT, Any],Any]] =
    commandsHandlersSeq.map(ch => (ch.commandClassName, ch.asInstanceOf[CommandHandler[AGGREGATE_ROOT,AbstractCommand[AGGREGATE_ROOT, Any],Any]])).toMap
  
  private val eventHandlers = eventsHandlersSeq.map(eh => (eh.eventClassName, eh)).toMap

  private val aggregateTypeSimpleName = aggregateRootClassTag.runtimeClass.getSimpleName


  private var nextAggregateId = 0L
  private var remainingAggregateIds = 0L
  
  private var nextCommandId = 0L
  private var remainingCommandsIds = 0L



  private val aggregatesActors = mutable.HashMap[Long, AggregateActors]()


  override def receive: Receive = LoggingReceive {
    case fce: FirstCommandEnvelope[_,_] => routeFirstCommand(fce.asInstanceOf[FirstCommandEnvelope[AGGREGATE_ROOT, _]])
    case ce: FollowingCommandEnvelope[_,_] => routeCommand(ce.asInstanceOf[FollowingCommandEnvelope[AGGREGATE_ROOT, _]])
    case GetAggregate(id) => routeGetAggregateRoot(id)
    case m => throw new IllegalArgumentException("Cannot handle this kind of message: " + m + " class: " + m.getClass)
  }

  private def routeFirstCommand[RESPONSE](firstCommandEnvelope: FirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE]): Unit = {
    println("Routes first command")
    val commandId = takeNextCommandId
    val newAggregateId = takeNextAggregateId // Actor construction might be delayed so we need to store current aggregate id
    val respondTo = sender() // with sender this shouldn't be the case, but just to be sure

    val aggregateActors = createAggregateActorsIfNeeded(newAggregateId)
    aggregateActors.commandHandler ! InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo, commandId, firstCommandEnvelope)
  }

  private def createAggregateActorsIfNeeded(aggregateId: AggregateId): AggregateActors = {
    aggregatesActors.getOrElse(aggregateId.asLong, {
      createAggregateActors(aggregateId)
    })
  }

  private def createAggregateActors(aggregateId: AggregateId): AggregateActors = {
    val repositoryActor = context.actorOf(Props(new AggregateRepositoryActor[AGGREGATE_ROOT](aggregateId, eventHandlers)),
      aggregateTypeSimpleName + "_AggregateRepository_" + aggregateId.asLong)

    val commandHandlerActor = context.actorOf(Props(new CommandHandlerActor[AGGREGATE_ROOT](aggregateId, repositoryActor, commandsHandlers)),
      aggregateTypeSimpleName + "_CommandHandler_" + aggregateId.asLong)

    val actors = AggregateActors(commandHandlerActor, repositoryActor)
    aggregatesActors += aggregateId.asLong -> actors
    actors
  }

  private def routeCommand[RESPONSE](commandEnvelope: FollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE]): Unit = {
    println("Routes non first command")
    val commandId = takeNextCommandId
    val respondTo = sender()

    val aggregateActors = createAggregateActorsIfNeeded(commandEnvelope.aggregateId)

    aggregateActors.commandHandler ! InternalFollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo, commandId, commandEnvelope)
  }



  private def routeGetAggregateRoot(id: AggregateId): Unit = {
    println(s"Routes routeGetAggregateRoot $id")
    val respondTo = sender()
    val aggregateRepository = context.actorSelection(aggregateTypeSimpleName+"_AggregateRepository_"+id.asLong)
    aggregateRepository ! ReturnAggregateRoot(respondTo)
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

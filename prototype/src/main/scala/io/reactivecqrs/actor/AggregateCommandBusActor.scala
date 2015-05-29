package io.reactivecqrs.actor

import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import akka.pattern.ask
import akka.util.Timeout
import io.reactivecqrs.actor.AggregateCommandBusActor.{FirstCommandEnvelope, FollowingCommandEnvelope}
import io.reactivecqrs.actor.AggregateRepositoryActor.ReturnAggregateRoot
import io.reactivecqrs.actor.CommandHandlerActor.{InternalFirstCommandEnvelope, InternalFollowingCommandEnvelope}
import io.reactivecqrs.api.guid.{AggregateId, CommandId, UserId}
import io.reactivecqrs.core._
import io.reactivecqrs.uid.{NewAggregatesIdsPool, NewCommandsIdsPool, UidGeneratorActor}

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



  override def receive: Receive = LoggingReceive {
    case fce: FirstCommandEnvelope[_,_] => routeFirstCommand(fce.asInstanceOf[FirstCommandEnvelope[AGGREGATE_ROOT, _]])
    case ce: FollowingCommandEnvelope[_,_] => routeCommand(ce.asInstanceOf[FollowingCommandEnvelope[AGGREGATE_ROOT, _]])
    case GetAggregate(id) => routeGetAggregateRoot(id)
    case m => throw new IllegalArgumentException("Cannot handle this kind of message: " + m + " class: " + m.getClass)
  }

  def routeFirstCommand[RESPONSE](firstCommandEnvelope: FirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE]): Unit = {
    println("Routes first command")
    val commandId = takeNextCommandId
    val newAggregateId = takeNextAggregateId // Actor construction might be delayed so we need to store current aggregate id
    val respondTo = sender() // with sender this shouldn't be the case, but just to be sure
    val newRepositoryActor = context.actorOf(Props(new AggregateRepositoryActor[AGGREGATE_ROOT](newAggregateId, eventHandlers)), aggregateTypeSimpleName+"_AggregateRepository_" + newAggregateId.asLong)
    val newCommandHandlerActor = context.actorOf(Props(new CommandHandlerActor[AGGREGATE_ROOT](newAggregateId, newRepositoryActor, commandsHandlers)), aggregateTypeSimpleName+"_CommandHandler_" + newAggregateId.asLong)

    newCommandHandlerActor ! InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo, commandId, firstCommandEnvelope)
  }

  def routeCommand[RESPONSE](commandEnvelope: FollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE]): Unit = {
    println("Routes non first command")
    val commandId = takeNextCommandId
    val respondTo = sender()

    val repositoryActor = context.actorSelection(aggregateTypeSimpleName+"_AggregateRepository_" + commandEnvelope.aggregateId.asLong) // create if not exists?
    val newCommandHandlerActor = context.actorSelection(aggregateTypeSimpleName+"_CommandHandler_" + commandEnvelope.aggregateId.asLong)

    newCommandHandlerActor ! InternalFollowingCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo, commandId, commandEnvelope)
  }



  def routeGetAggregateRoot(id: AggregateId): Unit = {
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

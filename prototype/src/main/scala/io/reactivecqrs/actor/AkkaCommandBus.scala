package io.reactivecqrs.actor

import akka.actor.{ActorRef, Actor, Props}
import akka.event.LoggingReceive
import akka.pattern.ask
import akka.util.Timeout
import io.reactivecqrs.api.guid.{CommandId, AggregateId}
import io.reactivecqrs.core._
import io.reactivecqrs.uid.{NewAggregatesIdsPool, UidGeneratorActor}

import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.concurrent.duration._



class AkkaCommandBus[AGGREGATE_ROOT](val uidGenerator: ActorRef,
                                      val commandsHandlers: Seq[CommandHandler[AGGREGATE_ROOT,AbstractCommand[AGGREGATE_ROOT, _],_]],
                                     val eventsHandlers: Seq[EventHandler[AGGREGATE_ROOT, Event[AGGREGATE_ROOT]]])
                                    (implicit aggregateRootClassTag: ClassTag[AGGREGATE_ROOT]) extends Actor {




  var fromAggregateId = 0L
  var remainingAggregateIds = 0L


  var nextCommandId = 1


  override def receive: Receive = LoggingReceive {
    case ce: CommandEnvelope[_,_] => routeCommand(ce.asInstanceOf[CommandEnvelope[AGGREGATE_ROOT, _]])
    case fce: FirstCommandEnvelope[_,_] => routeFirstCommand(fce.asInstanceOf[FirstCommandEnvelope[AGGREGATE_ROOT, _]])
    case GetAggregateRoot(id) => routeGetAggregateRoot(id)
    case m => throw new IllegalArgumentException("Cannot handle this kind of message: " + m)
  }

  def routeCommand[RESPONSE](command: CommandEnvelope[AGGREGATE_ROOT, RESPONSE]): Unit = {
    println("Routes non first command")
    val commandId = CommandId(nextCommandId)
    nextCommandId += 1
    val existingCommandHandlerActor = context.actorSelection("CommandHandler" + command.aggregateId.asLong)
    val respondTo = sender()
    existingCommandHandlerActor ! InternalCommandEnvelope(respondTo, commandId, command)

  }

  def routeFirstCommand[RESPONSE](firstCommand: FirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE]): Unit = {
    println("Routes first command")
    val commandId = CommandId(nextCommandId)
    nextCommandId += 1
    val newAggregateId = nextAggregateId // Actor construction might be delayed so we need to store current aggregate id
    val respondTo = sender() // with sender this shouldn't be the case, but just to be sure
    val newCommandHandlerActor = context.actorOf(Props(new CommandHandlerActor[AGGREGATE_ROOT](newAggregateId, commandsHandlers, eventsHandlers)), "CommandHandler" + newAggregateId.asLong)
    newCommandHandlerActor ! InternalFirstCommandEnvelope(respondTo, commandId, firstCommand)
  }

  def routeGetAggregateRoot(id: AggregateId): Unit = {
    println(s"Routes routeGetAggregateRoot $id")
    val respondTo = sender()
    val aggregate = context.actorSelection("CommandHandler" + id.asLong+"/" + "AggregateRepository"+id.asLong)
    aggregate ! ReturnAggregateRoot(respondTo)
  }

  private def nextAggregateId: AggregateId = {
    if(remainingAggregateIds == 0) {
      // TODO get rid of ask pattern
      implicit val timeout = Timeout(5 seconds)
      val pool: Future[NewAggregatesIdsPool] = (uidGenerator ? UidGeneratorActor.GetNewAggregatesIdsPool).mapTo[NewAggregatesIdsPool]
      val newAggregatesIdsPool: NewAggregatesIdsPool = Await.result(pool, 5 seconds)
      remainingAggregateIds = newAggregatesIdsPool.size
      fromAggregateId = newAggregatesIdsPool.from
    }

    remainingAggregateIds -= 1
    val aggregateId = AggregateId(fromAggregateId)
    fromAggregateId += 1
    aggregateId


  }
}

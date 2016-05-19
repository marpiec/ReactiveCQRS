package io.reactivecqrs.core.aggregaterepository

import akka.pattern.ask
import akka.actor.{Actor, ActorRef, Props}
import io.reactivecqrs.api.{AggregateType, AggregateVersion, Event}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.aggregaterepository.ReplayAggregateRepositoryActor.ReplayEvent
import io.reactivecqrs.core.util.ActorLogging
import io.reactivecqrs.core.eventbus.EventsBusActor.PublishReplayedEvent
import io.reactivecqrs.core.eventsreplayer.BackPressureActor

import scala.concurrent.Await
import io.reactivecqrs.core.eventsreplayer.BackPressureActor.{AllowMore, AllowedMore}

import scala.collection.mutable
import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe.TypeTag
import scala.concurrent.duration._

object ReplayAggregateRepositoryActor {
  case class ReplayEvent[AGGREGATE_ROOT](backPressureActor: ActorRef, event: IdentifiableEvent[AGGREGATE_ROOT])
}

class ReplayAggregateRepositoryActor[AGGREGATE_ROOT:ClassTag:TypeTag](aggregateId: AggregateId,
                                                                      eventsBus: ActorRef,
                                                                      eventHandlers: AGGREGATE_ROOT => PartialFunction[Any, AGGREGATE_ROOT],
                                                                      initialState: () => AGGREGATE_ROOT) extends Actor with ActorLogging {

  private var version: AggregateVersion = AggregateVersion.ZERO
  private var aggregateRoot: AGGREGATE_ROOT = initialState()
  private val aggregateType = AggregateType(classTag[AGGREGATE_ROOT].toString)
  private val backPressureActor = context.actorOf(Props(new BackPressureActor))

  val maxBufferSize = 10L
  var leftToArrive = 10L

  val buffer: mutable.Queue[PublishReplayedEvent[AGGREGATE_ROOT]] = mutable.Queue.empty
  var messagesAllowed = 1000L
  var messagesRequested = false

  //TODO there's a risk that this actor will need to be recreated in the middle of replaying, so state should be allowed to restore
  private def assureRestoredState(): Unit = {
    version = AggregateVersion.ZERO
    aggregateRoot = initialState()
  }

  assureRestoredState()

  private def handleEvent(event: Event[AGGREGATE_ROOT], aggregateId: AggregateId, noopEvent: Boolean): Unit = {
    if(!noopEvent) {
      try {
        aggregateRoot = eventHandlers(aggregateRoot)(event)
      } catch {
        case e: Exception =>
          log.error("Error while handling event: " + event)
          throw e;
      }
    }

    if(aggregateId == aggregateId) { // otherwise it's event from base aggregate we don't want to count
      version = version.increment
    }
  }

  override def receive: Receive = {
    case ReplayEvent(backPressureActor, event) =>
      replayEvent(event.asInstanceOf[IdentifiableEvent[AGGREGATE_ROOT]])
    case AllowedMore(count) =>
      messagesRequested = false
      messagesAllowed += count
      sendMessagesFromBuffer()
  }

  private def replayEvent(event: IdentifiableEvent[AGGREGATE_ROOT]): Unit = {
    leftToArrive -= 1
    handleEvent(event.event, event.aggregateId, false)
    val messageToSend: PublishReplayedEvent[AGGREGATE_ROOT] = PublishReplayedEvent(backPressureActor, aggregateType, event, aggregateId, version, Option(aggregateRoot))
    if(messagesAllowed > 0) {
      eventsBus ! messageToSend
      messagesAllowed -= 1
    } else {
      buffer.enqueue(messageToSend)
    }
    if(buffer.size > 500 && !messagesRequested) {
      backPressureActor ! AllowMore
      messagesRequested = true
    }
    if(leftToArrive < buffer.size / 2) {

    }
  }

  private def sendMessagesFromBuffer(): Unit = {
    while(messagesAllowed > 0 && buffer.size > 0) {
      eventsBus ! buffer.dequeue()
      messagesAllowed -= 1
    }
  }

}

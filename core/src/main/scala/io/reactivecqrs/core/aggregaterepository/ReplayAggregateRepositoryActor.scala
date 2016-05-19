package io.reactivecqrs.core.aggregaterepository

import akka.actor.{Actor, ActorRef}
import io.reactivecqrs.api.{AggregateType, AggregateVersion, Event}
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.aggregaterepository.ReplayAggregateRepositoryActor.ReplayEvent
import io.reactivecqrs.core.util.ActorLogging
import io.reactivecqrs.core.eventbus.EventsBusActor.PublishReplayedEvent

import scala.reflect._
import scala.reflect.runtime.universe._

object ReplayAggregateRepositoryActor {
  case class ReplayEvent[AGGREGATE_ROOT](event: IdentifiableEvent[AGGREGATE_ROOT])
}

class ReplayAggregateRepositoryActor[AGGREGATE_ROOT:ClassTag:TypeTag](aggregateId: AggregateId,
                                                                      eventsBus: ActorRef,
                                                                      eventHandlers: AGGREGATE_ROOT => PartialFunction[Any, AGGREGATE_ROOT],
                                                                      initialState: () => AGGREGATE_ROOT) extends Actor with ActorLogging {

  private var version: AggregateVersion = AggregateVersion.ZERO
  private var aggregateRoot: AGGREGATE_ROOT = initialState()
  private val aggregateType = AggregateType(classTag[AGGREGATE_ROOT].toString)

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
    case ReplayEvent(event) => replayEvent(event.asInstanceOf[IdentifiableEvent[AGGREGATE_ROOT]])
  }

  private def replayEvent(event: IdentifiableEvent[AGGREGATE_ROOT]): Unit = {
    handleEvent(event.event, event.aggregateId, false)
    eventsBus ! PublishReplayedEvent(aggregateType, event, aggregateId, version, Option(aggregateRoot))
  }

}

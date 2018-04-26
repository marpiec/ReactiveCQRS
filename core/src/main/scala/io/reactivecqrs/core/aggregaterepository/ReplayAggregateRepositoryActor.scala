package io.reactivecqrs.core.aggregaterepository

import java.time.Instant

import akka.actor.{Actor, ActorRef}
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.aggregaterepository.ReplayAggregateRepositoryActor.ReplayEvents
import io.reactivecqrs.core.util.ActorLogging
import io.reactivecqrs.core.eventbus.EventsBusActor.PublishReplayedEvent
import io.reactivecqrs.core.eventstore.EventStoreState

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe.TypeTag

object ReplayAggregateRepositoryActor {
  case class ReplayEvents[AGGREGATE_ROOT](event: IdentifiableEvents[AGGREGATE_ROOT])
}

class ReplayAggregateRepositoryActor[AGGREGATE_ROOT:ClassTag:TypeTag](aggregateId: AggregateId,
                                                                      eventStore: EventStoreState,
                                                                      eventsBus: ActorRef,
                                                                      eventHandlers: (UserId, Instant, AGGREGATE_ROOT) => PartialFunction[Any, AGGREGATE_ROOT],
                                                                      initialState: () => AGGREGATE_ROOT,
                                                                      aggregateVersion: Option[AggregateVersion],
                                                                      eventsVersionsMap: Map[EventTypeVersion, String],
                                                                      eventsVersionsMapReverse: Map[String, EventTypeVersion]) extends Actor with ActorLogging {

  private var version: AggregateVersion = AggregateVersion.ZERO
  private var aggregateRoot: AGGREGATE_ROOT = initialState()
  private val aggregateType = AggregateType(classTag[AGGREGATE_ROOT].toString)

  private def assureRestoredState(): Unit = {
    version = AggregateVersion.ZERO
    aggregateRoot = initialState()

    if(aggregateVersion.isDefined) {
      eventStore.readAndProcessEvents[AGGREGATE_ROOT](eventsVersionsMap, aggregateId, aggregateVersion)(handleEvent)
    }

  }

  assureRestoredState()

  private def handleEvent(userId: UserId, timestamp: Instant, event: Event[AGGREGATE_ROOT], aggId: AggregateId, noopEvent: Boolean): Unit = {
    if(!noopEvent) {
      try {
        aggregateRoot = eventHandlers(userId, timestamp, aggregateRoot)(event)
      } catch {
        case e: Exception =>
          log.error(e, "Error while handling event: " + event +", aggregateRoot: " + aggregateRoot)
          throw e;
      }
    }

    if(aggId == aggregateId) { // otherwise it's event from base aggregate we don't want to count
      version = version.increment
    }
  }

  override def receive: Receive = {
    case ReplayEvents(event) => replayEvent(event.asInstanceOf[IdentifiableEvents[AGGREGATE_ROOT]])
  }

  private def replayEvent(events: IdentifiableEvents[AGGREGATE_ROOT]): Unit = {

    events.events.head.event match {
      case duplicationEvent: DuplicationEvent[_] =>
        // We need state from original aggregate
        version = AggregateVersion.ZERO
        aggregateRoot = initialState()
        eventStore.readAndProcessEvents[AGGREGATE_ROOT](eventsVersionsMap, duplicationEvent.baseAggregateId, Some(duplicationEvent.baseAggregateVersion))(handleEvent)
      case _ => ()
    }

    events.events.foreach(event => {
      handleEvent(event.userId, event.timestamp, event.event, events.aggregateId, noopEvent = false)
    })

    val messageToSend: PublishReplayedEvent[AGGREGATE_ROOT] = PublishReplayedEvent(aggregateType, events.events, aggregateId, Option(aggregateRoot))
    eventsBus ! messageToSend
  }

}

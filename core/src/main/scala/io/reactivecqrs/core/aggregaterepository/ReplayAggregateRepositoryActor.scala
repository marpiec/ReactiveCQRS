package io.reactivecqrs.core.aggregaterepository

import java.time.Instant

import org.apache.pekko.actor.{Actor, ActorRef, PoisonPill}
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.aggregaterepository.ReplayAggregateRepositoryActor.ReplayEvents
import io.reactivecqrs.core.util.MyActorLogging
import io.reactivecqrs.core.eventbus.EventsBusActor.PublishReplayedEvents
import io.reactivecqrs.core.eventstore.EventStoreState

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe.TypeTag
import scala.concurrent.duration._

object ReplayAggregateRepositoryActor {
  case class ReplayEvents[AGGREGATE_ROOT](event: IdentifiableEvents[AGGREGATE_ROOT])
}

class ReplayAggregateRepositoryActor[AGGREGATE_ROOT:ClassTag:TypeTag](aggregateId: AggregateId,
                                                                      eventStore: EventStoreState,
                                                                      eventsBus: ActorRef,
                                                                      eventHandlers: (UserId, Instant, AGGREGATE_ROOT) => PartialFunction[Any, AGGREGATE_ROOT],
                                                                      initialState: () => AGGREGATE_ROOT,
                                                                      eventsVersionsMap: Map[EventTypeVersion, String],
                                                                      maxInactivitySeconds: Int) extends Actor with MyActorLogging {

  private var version: AggregateVersion = AggregateVersion.ZERO
  private var aggregateRoot: AGGREGATE_ROOT = initialState()
  private val aggregateType = AggregateType(classTag[AGGREGATE_ROOT].toString)

  import context.dispatcher

  private val MAX_INACTIVITY_TIME: FiniteDuration = maxInactivitySeconds.seconds
  private var autoKill = context.system.scheduler.scheduleOnce(delay = MAX_INACTIVITY_TIME, self, PoisonPill)

  private def assureRestoredState(): Unit = {
    version = AggregateVersion.ZERO
    aggregateRoot = initialState()
  }

  assureRestoredState()

  private def handleEvent(userId: UserId, timestamp: Instant, event: Event[AGGREGATE_ROOT], aggId: AggregateId, eventVersion: Int, noopEvent: Boolean): Unit = {
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
    case ReplayEvents(events) =>
      autoKill.cancel()
      replayEvents(events.asInstanceOf[IdentifiableEvents[AGGREGATE_ROOT]])
      autoKill = context.system.scheduler.scheduleOnce(delay = MAX_INACTIVITY_TIME, self, PoisonPill)
  }

  private def replayEvents(events: IdentifiableEvents[AGGREGATE_ROOT]): Unit = {

    events.events.head.event match {
      case duplicationEvent: DuplicationEvent[_] =>
        // We need state from original aggregate
        version = AggregateVersion.ZERO
        aggregateRoot = initialState()
        eventStore.readAndProcessEvents[AGGREGATE_ROOT](eventsVersionsMap, duplicationEvent.baseAggregateId, Some(Left(duplicationEvent.baseAggregateVersion)))(handleEvent)
      case _ => ()
    }


    val start = System.currentTimeMillis()

    try {
      events.events.foreach(event => {
        handleEvent(event.userId, event.timestamp, event.event, events.aggregateId, event.version.asInt, noopEvent = false)
      })
    } catch {
      case e: Exception =>
        log.error(e, "Error while replaying events for aggregate " + aggregateId.asLong+" of type "+aggregateType.simpleName)
    }

    if(System.currentTimeMillis() - start > 1000) {
      log.warning("Replaying "+events.events.size+" events for aggregate " + aggregateId.asLong + " of type "+aggregateType.simpleName+" took "+(System.currentTimeMillis() - start)+" ms")
    }

    val messageToSend: PublishReplayedEvents[AGGREGATE_ROOT] = PublishReplayedEvents(aggregateType, events.events, aggregateId, Option(aggregateRoot))
    eventsBus ! messageToSend
  }

}

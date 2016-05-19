package io.reactivecqrs.core.eventsreplayer

import java.time.Instant

import akka.actor.{Actor, ActorContext, ActorRef, Props}
import io.reactivecqrs.api.{AggregateContext, AggregateType, AggregateVersion, Event}
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.aggregaterepository.{AggregateRepositoryActor, IdentifiableEvent, ReplayAggregateRepositoryActor}
import io.reactivecqrs.core.aggregaterepository.ReplayAggregateRepositoryActor.ReplayEvent
import io.reactivecqrs.core.eventsreplayer.EventsReplayerActor.{EventsReplayed, ReplayEvents}
import io.reactivecqrs.core.eventstore.EventStoreState

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

object ReplayerRepositoryActorFactory {
  def apply[AGGREGATE_ROOT: TypeTag:ClassTag](aggregateContext: AggregateContext[AGGREGATE_ROOT]): ReplayerRepositoryActorFactory[AGGREGATE_ROOT] = {
    new ReplayerRepositoryActorFactory[AGGREGATE_ROOT](aggregateContext)
  }
}

class ReplayerRepositoryActorFactory[AGGREGATE_ROOT: TypeTag:ClassTag](aggregateContext: AggregateContext[AGGREGATE_ROOT]) {

  def aggregateRootType = typeOf[AGGREGATE_ROOT]

  def create(context: ActorContext, aggregateId: AggregateId, eventStore: EventStoreState, eventsBus: ActorRef, actorName: String): ActorRef = {
    context.actorOf(Props(new ReplayAggregateRepositoryActor[AGGREGATE_ROOT](aggregateId, eventStore, eventsBus, aggregateContext.eventHandlers,
      None, () => aggregateContext.initialAggregateRoot)), actorName)
  }

}

object EventsReplayerActor {
  case object ReplayEvents
  case class EventsReplayed(eventsCount: Long)
}

class EventsReplayerActor(eventStore: EventStoreState,
                          val eventsBus: ActorRef,
                          actorsFactory: List[ReplayerRepositoryActorFactory[_]]) extends Actor {

  val factories: Map[String, ReplayerRepositoryActorFactory[_]] = actorsFactory.map(f => f.aggregateRootType.toString -> f).toMap

  override def receive: Receive = {
    case ReplayEvents => replayEvents(sender())
  }

  private def replayEvents(respondTo: ActorRef) {
    var count: Long = 0
    eventStore.readAndProcessAllEvents((eventId: Long, event: Event[_], aggregateId: AggregateId, version: AggregateVersion, aggregateType: AggregateType, userId: UserId, timestamp: Instant) => {
      val actor = getOrCreateReplayRepositoryActor(aggregateId, version, aggregateType)
      actor ! ReplayEvent(IdentifiableEvent(eventId, aggregateType, aggregateId, version, event, userId, timestamp))
      count += 1
    })
    respondTo ! EventsReplayed(count)
  }

  // Assumption - we replay events from first event so there is not need to have more than one actor for each event
  // QUESTION - cleanup?
  private def getOrCreateReplayRepositoryActor(aggregateId: AggregateId, aggregateVersion: AggregateVersion, aggregateType: AggregateType): ActorRef = {
    context.child(aggregateType.typeName + "_AggregateRepositorySimulator_" + aggregateId.asLong).getOrElse(
      factories(aggregateType.typeName).create(context,aggregateId, eventStore, eventsBus,
        aggregateType.typeName + "_AggregateRepositorySimulator_" + aggregateId.asLong)
    )
  }
}

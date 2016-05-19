package io.reactivecqrs.core.eventsreplayer

import java.time.Instant

import akka.pattern.ask
import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorContext, ActorRef, Props}
import io.reactivecqrs.api.{AggregateContext, AggregateType, AggregateVersion, Event}
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.aggregaterepository.{AggregateRepositoryActor, IdentifiableEvent, ReplayAggregateRepositoryActor}
import io.reactivecqrs.core.aggregaterepository.ReplayAggregateRepositoryActor.ReplayEvent
import io.reactivecqrs.core.eventsreplayer.BackPressureActor.{AllowMore, AllowedMore, RequestMore}
import io.reactivecqrs.core.eventsreplayer.EventsReplayerActor.{EventsReplayed, ReplayAllEvents}
import io.reactivecqrs.core.eventstore.EventStoreState

import scala.concurrent.Await
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import scala.concurrent.duration._

object ReplayerRepositoryActorFactory {
  def apply[AGGREGATE_ROOT: TypeTag:ClassTag](aggregateContext: AggregateContext[AGGREGATE_ROOT]): ReplayerRepositoryActorFactory[AGGREGATE_ROOT] = {
    new ReplayerRepositoryActorFactory[AGGREGATE_ROOT](aggregateContext)
  }
}

class ReplayerRepositoryActorFactory[AGGREGATE_ROOT: TypeTag:ClassTag](aggregateContext: AggregateContext[AGGREGATE_ROOT]) {

  def aggregateRootType = typeOf[AGGREGATE_ROOT]

  def create(context: ActorContext, aggregateId: AggregateId, eventStore: EventStoreState, eventsBus: ActorRef, actorName: String): ActorRef = {
    context.actorOf(Props(new ReplayAggregateRepositoryActor[AGGREGATE_ROOT](aggregateId, eventsBus, aggregateContext.eventHandlers,
      () => aggregateContext.initialAggregateRoot)), actorName)
  }

}

object EventsReplayerActor {
  case class ReplayAllEvents(count: Long)
  case class ContinueReplayEvents(count: Long)
  case class EventsReplayed(eventsCount: Long)
}



object BackPressureActor {
  case class RequestMore(count: Int)
  case object AllowMore
  case class AllowedMore(count: Int)
}

class BackPressureActor extends Actor {

  var requested: Int = 0

  var producerRequest: Option[ActorRef] = None

  override def receive: Receive = {
    case RequestMore(count) => producerRequest match {
      case Some(producer) =>
        producer ! AllowedMore(count + requested)
        requested = 0
      case None =>
        requested += count
    }
    case AllowMore =>
      if(requested > 0) {
        sender() ! AllowedMore(requested)
        requested = 0
      } else {
        producerRequest = Some(sender())
      }
  }
}


class EventsReplayerActor(eventStore: EventStoreState,
                          val eventsBus: ActorRef,
                          actorsFactory: List[ReplayerRepositoryActorFactory[_]]) extends Actor {

  val factories: Map[String, ReplayerRepositoryActorFactory[_]] = actorsFactory.map(f => f.aggregateRootType.toString -> f).toMap

  var messagesAllowed = 0L

  val backPressureActor = context.actorOf(Props(new BackPressureActor))

  override def receive: Receive = {
    case ReplayAllEvents(count) => replayEvents(sender(), count)
  }

  private def replayEvents(respondTo: ActorRef, count: Long) {
    messagesAllowed = count
    var eventsSent: Long = 0
    eventStore.readAndProcessAllEvents((eventId: Long, event: Event[_], aggregateId: AggregateId, version: AggregateVersion, aggregateType: AggregateType, userId: UserId, timestamp: Instant) => {
      val actor = getOrCreateReplayRepositoryActor(aggregateId, version, aggregateType)
      actor ! ReplayEvent(backPressureActor, IdentifiableEvent(eventId, aggregateType, aggregateId, version, event, userId, timestamp))
      println("Sent event " +eventId)
      messagesAllowed -= 1

      if(messagesAllowed == 0) {
        println("Wait for more")
        messagesAllowed = Await.result((backPressureActor ? AllowMore).mapTo[AllowedMore].map(_.count), 300 seconds) // Ask is only way to bl
        println("Allowed more " + messagesAllowed)
      }

      eventsSent += 1
    })
    respondTo ! EventsReplayed(eventsSent)
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

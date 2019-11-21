package io.reactivecqrs.core.eventsreplayer

import java.time.{Instant, LocalDateTime}
import java.util.Date

import akka.pattern.ask
import akka.actor.{Actor, ActorContext, ActorRef, Props}
import akka.util.Timeout
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.aggregaterepository.ReplayAggregateRepositoryActor
import io.reactivecqrs.core.aggregaterepository.ReplayAggregateRepositoryActor.ReplayEvents
import io.reactivecqrs.core.backpressure.BackPressureActor
import io.reactivecqrs.core.backpressure.BackPressureActor.{Finished, ProducerAllowMore, ProducerAllowedMore, Start, Stop}
import io.reactivecqrs.core.eventsreplayer.EventsReplayerActor.{EventsReplayed, ReplayAllEvents}
import io.reactivecqrs.core.eventstore.EventStoreState
import io.reactivecqrs.core.projection.SubscriptionsState
import io.reactivecqrs.core.util.ActorLogging

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

  val eventsVersionsMap: Map[EventTypeVersion, String] = {
    aggregateContext.eventsVersions.flatMap(evs => evs.mapping.map(e => EventTypeVersion(evs.eventBaseType, e.version) -> e.eventType)).toMap
  }

  val eventsVersionsMapReverse: Map[String, EventTypeVersion] = {
    aggregateContext.eventsVersions.flatMap(evs => evs.mapping.map(e => e.eventType -> EventTypeVersion(evs.eventBaseType, e.version))).toMap
  }

  def aggregateRootType = typeOf[AGGREGATE_ROOT]

  def create(context: ActorContext, aggregateId: AggregateId, aggregateVersion: Option[AggregateVersion], eventStore: EventStoreState, eventsBus: ActorRef, actorName: String): ActorRef = {
    context.actorOf(Props(new ReplayAggregateRepositoryActor[AGGREGATE_ROOT](aggregateId, eventStore, eventsBus, aggregateContext.eventHandlers,
      () => aggregateContext.initialAggregateRoot, aggregateVersion, eventsVersionsMap, eventsVersionsMapReverse)), actorName)
  }

}

object EventsReplayerActor {
  case class ReplayAllEvents(batchPerAggregate: Boolean, aggregatesTypes: Seq[String], delayBetweenAggregateTypes: Long)
  case class EventsReplayed(eventsCount: Long)
}


class EventsReplayerActor(eventStore: EventStoreState,
                          val eventsBus: ActorRef,
                          subscriptionsState: SubscriptionsState,
                          actorsFactory: List[ReplayerRepositoryActorFactory[_]]) extends Actor with ActorLogging {

  import context.dispatcher

  val timeoutDuration: FiniteDuration = 600.seconds
  implicit val timeout = Timeout(timeoutDuration)

  val factories: Map[String, ReplayerRepositoryActorFactory[_]] = actorsFactory.map(f => f.aggregateRootType.toString -> f).toMap

  var eventsToProduceAllowed = 0L

  var backPressureActor: ActorRef = context.actorOf(Props(new BackPressureActor(eventsBus)), "BackPressure")

  val combinedEventsVersionsMap = actorsFactory.map(_.eventsVersionsMap).foldLeft(Map[EventTypeVersion, String]())((acc, m) => acc ++ m)

  override def receive: Receive = {
    case ReplayAllEvents(batchPerAggregate, aggregatesTypes, delayBetweenAggregateTypes) => replayAllEvents(sender, batchPerAggregate, aggregatesTypes, delayBetweenAggregateTypes)
  }

  private def replayAllEvents(respondTo: ActorRef, batchPerAggregate: Boolean, aggregatesTypes: Seq[String], delayBetweenAggregateTypes: Long) {
    backPressureActor ! Start
    val allEvents: Int = eventStore.countAllEvents()
    var allEventsSent: Long = 0
    var lastLogEventsSent: Long = 0
    var lastDumpEventsSent: Long = 0

    log.info("Will replay "+allEvents+" events")

    var lastUpdate = System.currentTimeMillis()

    val notYetPublishedAggregatesVersions = eventStore.readNotYetPublishedEvents()


    aggregatesTypes.foreach(aggregateType => {
      val start = new Date().getTime
      eventStore.readAndProcessAllEvents(combinedEventsVersionsMap, aggregateType, batchPerAggregate, (events: Seq[EventInfo[_]], aggregateId: AggregateId, aggregateType: AggregateType) => {
        if(eventsToProduceAllowed <= 0) {
          // Ask is a way to block during fetching data from db
//          print("Replayer: Waiting more allowed messages, now allowed " + eventsToProduceAllowed)
          eventsToProduceAllowed += Await.result((backPressureActor ? ProducerAllowMore).mapTo[ProducerAllowedMore].map(_.count), timeoutDuration)
//          println("Replayer: Allowed to produce " + eventsToProduceAllowed +" more")
        }

        notYetPublishedAggregatesVersions.get(aggregateId) match {
          case None =>
            val actor = getOrCreateReplayRepositoryActor(aggregateId, events.head.version, aggregateType)
            actor ! ReplayEvents(IdentifiableEvents(aggregateType, aggregateId, events.asInstanceOf[Seq[EventInfo[Any]]]))
          case Some(notPublishedVersion) if events.head.version < notPublishedVersion =>
            val actor = getOrCreateReplayRepositoryActor(aggregateId, events.head.version, aggregateType)
            actor ! ReplayEvents(IdentifiableEvents(aggregateType, aggregateId, events.takeWhile(_.version < notPublishedVersion).asInstanceOf[Seq[EventInfo[Any]]]))
          case Some(notPublishedVersion) => ()
        }
        eventsToProduceAllowed -= events.size

        allEventsSent += events.size
        lastLogEventsSent += events.size
        lastDumpEventsSent += events.size
        val now = System.currentTimeMillis()
        if(allEventsSent < 10 || allEventsSent < 100 && lastLogEventsSent >= 10 || allEventsSent < 1000 && lastLogEventsSent >= 100 || lastLogEventsSent >= 1000 || now - lastUpdate > 10000) {
          println("Replayed " + allEventsSent + "/" + allEvents + " events")
          lastUpdate = System.currentTimeMillis()
          lastLogEventsSent = 0
        }

        if(allEventsSent < 1000 && lastDumpEventsSent >= 100 || allEventsSent < 10000 && lastDumpEventsSent >= 1000 || lastDumpEventsSent >= 10000) {
          subscriptionsState.dump()
          lastDumpEventsSent = 0
        }

      })
      println("Read events for " + aggregateType + " in "+formatMillis(new Date().getTime - start))
      if(delayBetweenAggregateTypes > 0) {
        Thread.sleep(delayBetweenAggregateTypes)
      }
    })



    Await.result(backPressureActor ? Stop, timeoutDuration) match {
      case Finished =>
        println("Replayed "+allEventsSent+"/"+allEvents+" events")
        print("Dumping subscriptions state...")
        subscriptionsState.dump()
        println("DONE")
        respondTo ! EventsReplayed(allEventsSent)
    }
  }

  private def formatMillis(millis: Long): String = {
    if(millis > 60000) {
      (millis/60000)+"m "+((millis%60000)/1000)+"s "+(millis%1000)+"ms"
    } else if(millis > 1000) {
      (millis/1000)+"s "+(millis%1000)+"ms"
    } else {
      millis+"ms"
    }
  }

  // Assumption - we replay events from first event so there is not need to have more than one actor for each event
  // QUESTION - cleanup?
  private def getOrCreateReplayRepositoryActor(aggregateId: AggregateId, aggregateVersion: AggregateVersion, aggregateType: AggregateType): ActorRef = {
    context.child(aggregateType.typeName + "_AggregateRepositorySimulator_" + aggregateId.asLong).getOrElse(
      factories(aggregateType.typeName).create(context,aggregateId, None, eventStore, eventsBus,
        aggregateType.typeName + "_AggregateRepositorySimulator_" + aggregateId.asLong)
    )
  }
}

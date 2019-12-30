package io.reactivecqrs.core.eventsreplayer

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import io.reactivecqrs.api.AggregateContext
import io.reactivecqrs.core.eventbus.EventBusSubscriptionsManagerApi
import io.reactivecqrs.core.eventsreplayer.EventsReplayerActor.{EventsReplayed, ReplayAllEvents}
import io.reactivecqrs.core.projection.{ClearProjectionData, GetSubscribedAggregates, SubscribedAggregates, VersionsState}

import scala.concurrent.Await
import scala.concurrent.duration._

class EventsOrchestrator {


  def replay(eventsReplayerActor: ActorRef, projectionsToRebuild: Iterable[ActorRef],
             versionsState: VersionsState,
             aggregatesTypes: Seq[AggregateContext[AnyRef]],
             timeout: FiniteDuration, subscriptionsManager: EventBusSubscriptionsManagerApi): Unit = {


    implicit val tm: Timeout = timeout
    val start = System.currentTimeMillis

    val subscribedAggregates: Iterable[(ActorRef, SubscribedAggregates)] = projectionsToRebuild.map(projection => {
      (projection, Await.result((projection ? GetSubscribedAggregates).mapTo[SubscribedAggregates], timeout))
    })

    val aggregateTypesToReplay = aggregatesTypes.filter(at => {
      val aggregateChanged = versionsState.versionForAggregate(at.aggregateType.typeName) != at.version
      val projectionChanged = subscribedAggregates.exists(s => {
        s._2.aggregates.contains(at.aggregateType) && versionsState.versionForProjection(s._2.projectionName) != s._2.projectionVersion
      })
      aggregateChanged || projectionChanged
    }).map(_.aggregateType)


    val projectionsToClear = subscribedAggregates.flatMap(s => {
      val commonAggregates = s._2.aggregates.intersect(aggregateTypesToReplay.toSet)
      if(commonAggregates.nonEmpty) {
        Some((s._1, ClearProjectionData))
      } else {
        None
      }
    })

    projectionsToClear.foreach(projectionToClear => {
      projectionToClear._1 ? projectionToClear._2
    })

    val result: EventsReplayed = Await.result((eventsReplayerActor ? ReplayAllEvents(batchPerAggregate = true, aggregateTypesToReplay, 0)).mapTo[EventsReplayed], timeout)
    println(result+" in "+(System.currentTimeMillis - start) +" millis")

  }

}

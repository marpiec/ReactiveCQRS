package io.reactivecqrs.core.eventsreplayer

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import io.reactivecqrs.api.AggregateType
import io.reactivecqrs.core.eventsreplayer.EventsReplayerActor.{EventsReplayed, ReplayAllEvents}
import io.reactivecqrs.core.projection.ClearProjectionData

import scala.concurrent.Await
import scala.concurrent.duration._

class EventsOrchestrator {


  def replay(eventsReplayerActor: ActorRef, projectionsToRebuild: Iterable[ActorRef], aggregatesTypes: Seq[AggregateType],
             timeout: FiniteDuration): Unit = {


    implicit val tm: Timeout = timeout
    val start = System.currentTimeMillis

    projectionsToRebuild.map(projection => {
      projection ? GetSubscribedAggregates()
    })


    projectionsToRebuild.foreach(projection => {
      projection ? ClearProjectionData
    })

    val result: EventsReplayed = Await.result((eventsReplayerActor ? ReplayAllEvents(batchPerAggregate = true, aggregatesTypes, 0)).mapTo[EventsReplayed], timeout)
    println(result+" in "+(System.currentTimeMillis - start) +" millis")

  }

}

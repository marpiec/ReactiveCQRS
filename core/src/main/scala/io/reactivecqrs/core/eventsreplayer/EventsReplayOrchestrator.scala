package io.reactivecqrs.core.eventsreplayer

import java.time.LocalDateTime

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import io.reactivecqrs.api.AggregateContext
import io.reactivecqrs.core.eventsreplayer.EventsReplayerActor.{EventsReplayed, ReplayAllEvents}
import io.reactivecqrs.core.projection.{ClearProjectionData, GetSubscribedAggregates, SubscribedAggregates, VersionsState}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class EventsReplayOrchestrator {


  def replay(eventsReplayerActor: ActorRef,
             projections: Iterable[ActorRef],
             aggregates: Seq[AggregateContext[AnyRef]],
             versionsState: VersionsState,
             timeout: FiniteDuration
            )(implicit ec: ExecutionContext): Unit = {


    implicit val tm: Timeout = timeout
    val start = System.currentTimeMillis

    println("Rebuilding projections started at " + LocalDateTime.now())

    val projectionSubscriptions: Iterable[(ActorRef, SubscribedAggregates)] = Await.result(Future.sequence(projections.map(projection => {
     (projection ? GetSubscribedAggregates).mapTo[SubscribedAggregates].map(s =>  (projection, s))
    })), 60 seconds)

    println("Got list of projections")

    val aggregateToReplay = aggregates.filter(at => {
      val aggregateChanged = versionsState.versionForAggregate(at.aggregateType) != at.version
      val projectionChanged = projectionSubscriptions.exists(s => {
        s._2.aggregates.contains(at.aggregateType) && versionsState.versionForProjection(s._2.projectionName) != s._2.projectionVersion
      })
      aggregateChanged || projectionChanged
    }).map(_.aggregateType)


    val projectionsToClear = projectionSubscriptions.flatMap(s => {
      val commonAggregates = s._2.aggregates.intersect(aggregateToReplay.toSet)
      if(commonAggregates.nonEmpty) {
        Some(s)
      } else {
        None
      }
    })

    println("Will replay events from " + aggregateToReplay.size + " aggregates: " + aggregateToReplay.map(_.simpleName).mkString(", "))
    println("Will rebuild " + projectionsToClear.size + " projections: " + projectionsToClear.map(p => simpleName(p._2.projectionName)).mkString(", "))

    Await.result(Future.sequence(projectionsToClear.map(projectionToClear => {
      (projectionToClear._1 ? ClearProjectionData)
    })), 60 seconds)


    println("Projections cleared")


    val result: EventsReplayed = Await.result((eventsReplayerActor ? ReplayAllEvents(batchPerAggregate = true, aggregateToReplay, 0)).mapTo[EventsReplayed], timeout)
    println(result+" in "+(System.currentTimeMillis - start) +" millis")

    aggregates.foreach(a => versionsState.saveVersionForAggregate(a.aggregateType, a.version))
    projectionSubscriptions.foreach(p => versionsState.saveVersionForProjection(p._2.projectionName, p._2.projectionVersion))
    println("Projection and Aggregates versions updated")

    print("Cool down...")
    waitFor(5)
  }

  private def simpleName(componentName: String): String = {
    val lastDot = componentName.lastIndexOf(".")
    if(lastDot > 0) {
      componentName.substring(lastDot + 1)
    } else {
      componentName
    }
  }

  private def waitFor(seconds: Int) = {
    for (i <- 0 until seconds) {
      print(seconds - i)
      Thread.sleep(250)
      print(".")
      Thread.sleep(250)
      print(".")
      Thread.sleep(250)
      print(".")
      Thread.sleep(250)
    }
    println("\nDONE")
  }
}

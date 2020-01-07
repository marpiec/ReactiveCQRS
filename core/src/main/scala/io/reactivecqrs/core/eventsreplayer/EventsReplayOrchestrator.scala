package io.reactivecqrs.core.eventsreplayer

import java.time.LocalDateTime

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import io.reactivecqrs.api.AggregateContext
import io.reactivecqrs.core.eventsreplayer.EventsReplayerActor.{EventsReplayed, GetStatus, ReplayAllEvents, ReplayerStatus}
import io.reactivecqrs.core.projection.{ClearProjectionData, GetSubscribedAggregates, SubscribedAggregates, VersionsState}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class EventsReplayOrchestrator {


  def replay(eventsReplayerActor: ActorRef,
             projections: Iterable[ActorRef],
             aggregates: Seq[AggregateContext[AnyRef]],
             versionsState: VersionsState,
             timeout: FiniteDuration,
             printStatusInfoOnly: Boolean,
             forceAll: Boolean
            )(implicit ec: ExecutionContext): Unit = {


    implicit val tm: Timeout = timeout
    val start = System.currentTimeMillis

    println("Rebuilding projections started at " + LocalDateTime.now())

    val projectionSubscriptions: Iterable[(ActorRef, SubscribedAggregates)] = Await.result(Future.sequence(projections.map(projection => {
     (projection ? GetSubscribedAggregates).mapTo[SubscribedAggregates].map(s =>  (projection, s))
    })), 60 seconds)

    println("Got list of projections")

    var projectionsToRebuild: Set[(ActorRef, SubscribedAggregates)] = projectionSubscriptions.toSet
    var aggregatesToReplay: Set[AggregateContext[AnyRef]] = aggregates.toSet
    if(!forceAll) {

      val changedAggregates = aggregates.filter(a => versionsState.versionForAggregate(a.aggregateType) != a.version)
      val changedProjections = projectionSubscriptions.filter(p => versionsState.versionForProjection(p._2.projectionName) != p._2.projectionVersion)
      // use all changed projections and aggregates

      // full rebuild of projections with changed aggregates
      projectionsToRebuild = changedProjections.toSet ++ projectionSubscriptions.filter(p =>
        changedAggregates.exists(a => p._2.aggregates.contains(a.aggregateType))
      ).toSet

      // repay all aggregates needed for those projections
      aggregatesToReplay = changedAggregates.toSet ++ aggregates.filter(a =>
        projectionsToRebuild.exists(p => p._2.aggregates.contains(a.aggregateType))
      ).toSet
    }

    if(printStatusInfoOnly) {
      println("Status only (will not rebuild projections)")
    }
    println("Will replay events from " + aggregatesToReplay.size + " of " + aggregates.size+ " aggregates " + aggregatesToReplay.map(_.aggregateType.simpleName).mkString("(", ", ", ")"))
    println("Will rebuild " + projectionsToRebuild.size + " of " + projections.size + " projections " + projectionsToRebuild.map(p => simpleName(p._2.projectionName)).mkString("(", ", ", ")"))

    val orderedAggregatesToReplay = aggregates.filter(a => aggregatesToReplay.contains(a)).map(_.aggregateType)

    if(printStatusInfoOnly) {
      val status: ReplayerStatus = Await.result((eventsReplayerActor ? GetStatus(orderedAggregatesToReplay)).mapTo[ReplayerStatus], timeout)
      println("Will replay " + status.willReplay + " of " + status.allEvents + " events")
    } else {
      Await.result(Future.sequence(projectionsToRebuild.map(projectionToRebuild => {
        (projectionToRebuild._1 ? ClearProjectionData)
      })), 60 seconds)

      println("Projections cleared")

      val result: EventsReplayed = Await.result((eventsReplayerActor ? ReplayAllEvents(batchPerAggregate = true, orderedAggregatesToReplay, 0)).mapTo[EventsReplayed], timeout)
      println(result + " in " + (System.currentTimeMillis - start) + " millis")

      aggregates.foreach(a => versionsState.saveVersionForAggregate(a.aggregateType, a.version))
      projectionSubscriptions.foreach(p => versionsState.saveVersionForProjection(p._2.projectionName, p._2.projectionVersion))
      println("Projection and Aggregates versions updated")

      print("Cool down...")
      waitFor(5)
    }
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

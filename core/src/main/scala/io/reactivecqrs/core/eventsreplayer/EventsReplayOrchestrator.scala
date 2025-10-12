package io.reactivecqrs.core.eventsreplayer

import java.time.LocalDateTime

import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import io.reactivecqrs.api.AggregateContext
import io.reactivecqrs.core.eventsreplayer.EventsReplayerActor.{EventsReplayed, GetStatus, ReplayAllEvents, ReplayerStatus}
import io.reactivecqrs.core.projection.{ClearProjectionData, GetSubscribedAggregates, SubscribedAggregates, VersionsState}
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class EventsReplayOrchestrator {

  private val log = LoggerFactory.getLogger(classOf[EventsReplayOrchestrator])

  private def logMessage(message: String): Unit = {
    println(message)
    log.info(message)
  }

  def replay(eventsReplayerActor: ActorRef,
             projections: Iterable[ActorRef],
             aggregates: Seq[AggregateContext[AnyRef]],
             versionsState: VersionsState,
             timeout: FiniteDuration,
             printStatusInfoOnly: Boolean,
             forceAll: Boolean,
             delayBetweenAggregatesMillis: Long
            )(implicit ec: ExecutionContext): Boolean = {


    implicit val tm: Timeout = timeout
    val start = System.nanoTime() / 1_000_000

    logMessage("Rebuilding projections started at " + LocalDateTime.now())

    val projectionSubscriptions: Iterable[(ActorRef, SubscribedAggregates)] = Await.result(Future.sequence(projections.map(projection => {
     (projection ? GetSubscribedAggregates).mapTo[SubscribedAggregates].map(s =>  (projection, s))
    })), 60.seconds)

    logMessage("Got list of projections")

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
      logMessage("Status only (will not rebuild projections)")
    }
    logMessage("Will replay events from " + aggregatesToReplay.size + " of " + aggregates.size+ " aggregates " + aggregatesToReplay.map(_.aggregateType.simpleName).mkString("(", ", ", ")"))
    logMessage("Will rebuild " + projectionsToRebuild.size + " of " + projections.size + " projections " + projectionsToRebuild.map(p => simpleName(p._2.projectionName)).mkString("(", ", ", ")"))

    val orderedAggregatesToReplay = aggregates.filter(a => aggregatesToReplay.contains(a)).map(_.aggregateType)

    if(printStatusInfoOnly) {
      val status: ReplayerStatus = Await.result((eventsReplayerActor ? GetStatus(orderedAggregatesToReplay)).mapTo[ReplayerStatus], timeout)
      logMessage("Will replay " + status.willReplay + " of " + status.allEvents + " events")
      false
    } else if(aggregatesToReplay.nonEmpty || projectionsToRebuild.nonEmpty) {
      Await.result(Future.sequence(projectionsToRebuild.map(projectionToRebuild => {
        (projectionToRebuild._1 ? ClearProjectionData)
      })), 60.seconds)

      logMessage("Projections cleared")

      val result: EventsReplayed = Await.result((eventsReplayerActor ? ReplayAllEvents(batchPerAggregate = true, orderedAggregatesToReplay, delayBetweenAggregatesMillis)).mapTo[EventsReplayed], timeout)
      logMessage(result + " in " + formatDuration(System.nanoTime() / 1_000_000 - start))

      aggregates.foreach(a => versionsState.saveVersionForAggregate(a.aggregateType, a.version))
      projectionSubscriptions.foreach(p => versionsState.saveVersionForProjection(p._2.projectionName, p._2.projectionVersion))
      logMessage("Projection and Aggregates versions updated")

      log.info("Cooling down")
      print("Cool down...")
      waitFor(5)
      true
    } else {
      logMessage("Nothing to do.")
      false
    }
  }

  private def formatDuration(millis: Long): String = {
    val ms = millis % 1000
    val s = (millis / 1000) % 60
    val m = (millis / (60 * 1000)) % 60
    val h = millis / (60 * 60 * 1000)

    val sb = new StringBuilder
    if(h > 0) {
      sb.append(" ").append(h).append("h")
    }

    if(m > 0) {
      sb.append(" ").append(m).append("m")
    }

    if(s > 0) {
      sb.append(" ").append(s).append("s")
    }

    if(h == 0 && m == 0 && s < 10) {
      sb.append(" ").append(ms).append("ms")
    }

    sb.toString().trim()
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
    logMessage("\nDONE")
  }
}

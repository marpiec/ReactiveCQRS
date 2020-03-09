package io.reactivecqrs.core.eventsreplayer

import java.util.Date
import java.util.concurrent.TimeoutException

import akka.pattern.ask
import akka.actor.{Actor, ActorContext, ActorRef, Props}
import akka.util.Timeout
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.aggregaterepository.ReplayAggregateRepositoryActor
import io.reactivecqrs.core.aggregaterepository.ReplayAggregateRepositoryActor.ReplayEvents
import io.reactivecqrs.core.backpressure.BackPressureActor
import io.reactivecqrs.core.backpressure.BackPressureActor.{Finished, ProducerAllowMore, ProducerAllowedMore, Start, Stop}
import io.reactivecqrs.core.eventbus.EventsBusActor.LogDetailedStatus
import io.reactivecqrs.core.eventsreplayer.EventsReplayerActor.{EventsReplayed, GetStatus, ReplayAllEvents, ReplayerStatus}
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

  def aggregateRootType = aggregateContext.aggregateType.typeName

  def create(context: ActorContext, aggregateId: AggregateId, aggregateVersion: Option[AggregateVersion], eventStore: EventStoreState, eventsBus: ActorRef, actorName: String, maxInactivitySeconds: Int): ActorRef = {
    context.actorOf(Props(new ReplayAggregateRepositoryActor[AGGREGATE_ROOT](aggregateId, eventStore, eventsBus, aggregateContext.eventHandlers,
      () => aggregateContext.initialAggregateRoot, aggregateVersion, eventsVersionsMap, eventsVersionsMapReverse, maxInactivitySeconds)), actorName)
  }

}

object EventsReplayerActor {
  case class ReplayAllEvents(batchPerAggregate: Boolean, aggregatesTypes: Seq[AggregateType], delayBetweenAggregateTypes: Long)
  case class EventsReplayed(eventsCount: Long)
  case class GetStatus(aggregatesTypes: Seq[AggregateType])
  case class ReplayerStatus(willReplay: Int, allEvents: Int)
}


/**
  * @param maxReplayerInactivitySeconds - defaulet30 s
  * @param replayerTimoutSeconds - default 600 s
  */


case class ReplayerConfig(maxReplayerInactivitySeconds: Int = 30,
                          replayerTimoutSeconds: Int = 600)

class EventsReplayerActor(eventStore: EventStoreState,
                          val eventsBus: ActorRef,
                          subscriptionsState: SubscriptionsState,
                          val config: ReplayerConfig,
                          actorsFactory: List[ReplayerRepositoryActorFactory[_]]) extends Actor with ActorLogging {

  import context.dispatcher

  val timeoutDuration: FiniteDuration = config.replayerTimoutSeconds.seconds
  implicit val timeout = Timeout(timeoutDuration)

  val factories: Map[String, ReplayerRepositoryActorFactory[_]] = actorsFactory.map(f => f.aggregateRootType -> f).toMap

  var allowedTotal = 0
  var sendTotal = 0
  var eventsToProduceAllowed = 0L

  var backPressureActor: ActorRef = context.actorOf(Props(new BackPressureActor(eventsBus)), "BackPressure")

  val combinedEventsVersionsMap = actorsFactory.map(_.eventsVersionsMap).foldLeft(Map[EventTypeVersion, String]())((acc, m) => acc ++ m)

  private def logMessage(message: String) {
    println(message)
    log.info(message)
  }

  override def receive: Receive = {
    case GetStatus(aggregatesTypes) => sender ! getStatus(aggregatesTypes)
    case ReplayAllEvents(batchPerAggregate, aggregatesTypes, delayBetweenAggregateTypes) => replayAllEvents(sender, batchPerAggregate, aggregatesTypes, delayBetweenAggregateTypes)
  }

  private def getStatus(aggregatesTypes: Seq[AggregateType]): ReplayerStatus = {
    ReplayerStatus(eventStore.countEventsForAggregateTypes(aggregatesTypes.map(_.typeName)), eventStore.countAllEvents())
  }

  private def replayAllEvents(respondTo: ActorRef, batchPerAggregate: Boolean, aggregatesTypes: Seq[AggregateType], delayBetweenAggregateTypes: Long) {
    backPressureActor ! Start
    val allEvents: Int = eventStore.countAllEvents()
    val allAggregatesEvents = eventStore.countEventsForAggregateTypes(aggregatesTypes.map(_.typeName))
    var allEventsSent: Long = 0
    var lastDumpEventsSent: Long = 0

    logMessage("Will replay "+allAggregatesEvents+" events out of " + allEvents + "(approximately)")

    var lastUpdate = System.currentTimeMillis()

    val notYetPublishedAggregatesVersions = eventStore.readNotYetPublishedEvents()

    var first = true

    aggregatesTypes.foreach(aggregateType => {
      val start = new Date().getTime
      logMessage("Processing events for " + aggregateType.simpleName)
      first = true
      eventStore.readAndProcessAllEvents(combinedEventsVersionsMap, aggregateType.typeName, batchPerAggregate, (events: Seq[EventInfo[_]], aggregateId: AggregateId, aggregateType: AggregateType) => {

        if(first) {
          logMessage("Processing first event for " + aggregateType.simpleName)
          first = false
        }

        while(eventsToProduceAllowed <= 0) {
          // Ask is a way to block during fetching data from db
          //          print("Replayer: Waiting more allowed messages, now allowed " + eventsToProduceAllowed)

          try {
            val allowed = Await.result((backPressureActor ? ProducerAllowMore).mapTo[ProducerAllowedMore].map(_.count), timeoutDuration)
            if(allowed == 0) {
              logMessage("EventsReplayer postponed")
              logMessage("Status: allowedTotal=" + allowedTotal+", sendTotal="+sendTotal+", eventsToProduceAllowed="+eventsToProduceAllowed+", allEventsSent="+allEventsSent+", lastDumpEventsSent="+lastDumpEventsSent)
            }
            eventsToProduceAllowed += allowed
            allowedTotal += allowed
          } catch {
            case e: TimeoutException =>
              logMessage((new Date()) + " Error:\nDid not received confirmation in "+timeoutDuration.toString()+".")
              logMessage("Status: allowedTotal=" + allowedTotal+", sendTotal="+sendTotal+", eventsToProduceAllowed="+eventsToProduceAllowed+", allEventsSent="+allEventsSent+", lastDumpEventsSent="+lastDumpEventsSent)
              logMessage("Threads status:\n" + generateThreadDump)
              eventsBus ! LogDetailedStatus
              Thread.sleep(30)
              println("Please consult main.log to check the cause. Stopping...")
              System.exit(-1)
          }

          //          println("Replayer: Allowed to produce " + eventsToProduceAllowed +" more")
        }

        notYetPublishedAggregatesVersions.get(aggregateId) match {
          case None =>
            val actor = getOrCreateReplayRepositoryActor(aggregateId, events.head.version, aggregateType)
            actor ! ReplayEvents(IdentifiableEvents(aggregateType, aggregateId, events.asInstanceOf[Seq[EventInfo[Any]]]))
            eventsToProduceAllowed -= events.size
            sendTotal += events.size
          case Some(notPublishedVersion) if events.head.version < notPublishedVersion =>
            val actor = getOrCreateReplayRepositoryActor(aggregateId, events.head.version, aggregateType)
            val eventsToSend = events.takeWhile(_.version < notPublishedVersion).asInstanceOf[Seq[EventInfo[Any]]]
            actor ! ReplayEvents(IdentifiableEvents(aggregateType, aggregateId, eventsToSend))
            sendTotal += eventsToSend.size
            eventsToProduceAllowed -= eventsToSend.size
          case Some(notPublishedVersion) => ()
        }


        allEventsSent += events.size
        lastDumpEventsSent += events.size
        val now = System.currentTimeMillis()
        if(now - lastUpdate > 5000) {
          logMessage("Replayed " + allEventsSent + "/" + allAggregatesEvents + " events")
          lastUpdate = System.currentTimeMillis()
        }

        if(allEventsSent < 1000 && lastDumpEventsSent >= 100 || allEventsSent < 10000 && lastDumpEventsSent >= 1000 || lastDumpEventsSent >= 10000) {
          logMessage(subscriptionsState.dump())
          lastDumpEventsSent = 0
        }

      })
      logMessage("Done processing events for " + aggregateType.simpleName + " in "+formatMillis(new Date().getTime - start))
      if(delayBetweenAggregateTypes > 0) {
        Thread.sleep(delayBetweenAggregateTypes)
      }
    })



    Await.result(backPressureActor ? Stop, timeoutDuration) match {
      case Finished =>
        logMessage("Replayed "+allEventsSent+"/"+allAggregatesEvents+" events")
        print("Dumping rest of subscriptions state...")
        log.info("Dumping rest of subscriptions state...")
        logMessage(subscriptionsState.dump())
        logMessage("DONE")
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
    context.child(aggregateType.typeName + "_Simulator_" + aggregateId.asLong).getOrElse(
      factories(aggregateType.typeName).create(context,aggregateId, None, eventStore, eventsBus,
        aggregateType.typeName + "_Simulator_" + aggregateId.asLong, config.maxReplayerInactivitySeconds)
    )
  }

  // based on https://crunchify.com/how-to-generate-java-thread-dump-programmatically/
  def generateThreadDump: StringBuilder = {
    val dump = new StringBuilder
    try {
      val threadMXBean = java.lang.management.ManagementFactory.getThreadMXBean
      val threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds, 100)
      threadInfos.filter(threadInfo => threadInfo != null).foreach(threadInfo => {
        dump.append('"')
        dump.append(threadInfo.getThreadName)
        dump.append("\" ")
        val state = threadInfo.getThreadState
        dump.append("\n   java.lang.Thread.State: ")
        dump.append(state)
        val stackTraceElements = threadInfo.getStackTrace
        for (stackTraceElement <- stackTraceElements) {
          dump.append("\n        at ")
          dump.append(stackTraceElement)
        }
        dump.append("\n\n")
      })
    } catch {
      case e: Exception =>
        dump.append("\nException while generating dump " + e.getClass.getName+" "+e.getMessage)
        e.printStackTrace()
    }
    dump
  }
}

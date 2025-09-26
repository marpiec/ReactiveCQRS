package io.reactivecqrs.core.eventbus

import org.apache.pekko.pattern.ask
import org.apache.pekko.actor.{Actor, ActorRef}
import org.apache.pekko.event.slf4j.Logger
import org.apache.pekko.util.Timeout
import io.reactivecqrs.core.eventbus.EventBusSubscriptionsManager.{GetSubscriptions, Subscribe}
import io.reactivecqrs.core.eventbus.EventsBusActor.SubscribeRequest
import org.slf4j.LoggerFactory

import scala.concurrent.Future


object EventBusSubscriptionsManager {
  case class Subscribe(requests: List[SubscribeRequest])
  case object GetSubscriptions
}

class EventBusSubscriptionsManagerApi(eventBusSubscriptionsManager: ActorRef) {

  def subscribe(requests: List[SubscribeRequest]): Unit = {
    if(requests.nonEmpty) {
      eventBusSubscriptionsManager ! Subscribe(requests)
    }
  }

  def getSubscriptions(implicit timeout: Timeout): Future[List[SubscribeRequest]] = {
    (eventBusSubscriptionsManager ? GetSubscriptions).mapTo[List[SubscribeRequest]]
  }
}

class EventBusSubscriptionsManager(minimumExpectedSubscriptions: Int) extends Actor {

  private var subscriptionsOpen = true
  private var subscriptionsRequests: List[SubscribeRequest] = List.empty

  private var eventBusWaiting: Option[ActorRef] = None

  private val log = LoggerFactory.getLogger(classOf[EventBusSubscriptionsManager])

  override def receive: Receive = {
    case Subscribe(requests) =>
      subscribe(requests)
      eventBusWaiting.foreach(eventBus => {
        if(subscriptionsRequests.size >= minimumExpectedSubscriptions) {
          subscriptionsOpen = false
          eventBus ! subscriptionsRequests
          log.info("All " + subscriptionsRequests.length+ " subscriptions registered")
          eventBusWaiting = None
        }
      })
    case GetSubscriptions =>
      if(subscriptionsRequests.size < minimumExpectedSubscriptions) {
        eventBusWaiting = Some(sender())
      } else {
        subscriptionsOpen = false
        sender() ! subscriptionsRequests
        log.info("All " + subscriptionsRequests.length+ " subscriptions registered")
      }
  }

  private def subscribe(subscribe: List[SubscribeRequest]): Unit = {
    val nonRepeated = subscribe.filterNot(s => subscriptionsRequests.contains(s))
    if(nonRepeated.nonEmpty) {
      if (subscriptionsOpen) {
        subscriptionsRequests :::= subscribe
        log.info("Subscribed " + subscriptionsRequests.length+"/"+minimumExpectedSubscriptions+" "+nonRepeated.map(s => s.summary).mkString(", "))
      } else {
        throw new IllegalStateException("Subscriptions for Event Bus already closed! Got " + subscriptionsRequests.size + " of " + minimumExpectedSubscriptions + " " + subscribe)
      }
    }
  }

}
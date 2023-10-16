package io.reactivecqrs.core.eventbus

import akka.pattern.ask
import akka.actor.{Actor, ActorRef}
import akka.event.slf4j.Logger
import akka.util.Timeout
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
          eventBusWaiting = None
          log.info("Subscribed all: " + subscriptionsRequests.length+"/"+minimumExpectedSubscriptions)
        }
      })
      if(eventBusWaiting.isDefined) {
        log.info("Subscribed: " + subscriptionsRequests.length+"/"+minimumExpectedSubscriptions)
      }
    case GetSubscriptions =>
      if(subscriptionsRequests.size < minimumExpectedSubscriptions) {
        eventBusWaiting = Some(sender())
      } else {
        subscriptionsOpen = false
        sender ! subscriptionsRequests
        log.info("Subscriptions count: " + subscriptionsRequests.length+"/"+minimumExpectedSubscriptions)
      }
  }

  private def subscribe(subscribe: List[SubscribeRequest]): Unit = {
    val nonRepeated = subscribe.filterNot(s => subscriptionsRequests.contains(s))
    if(nonRepeated.nonEmpty) {
      if (subscriptionsOpen) {
        subscriptionsRequests :::= subscribe
      } else {
        throw new IllegalStateException("Subscriptions for Event Bus already closed! Got " + subscriptionsRequests.size + " of " + minimumExpectedSubscriptions + " " + subscribe)
      }
    }
  }

}
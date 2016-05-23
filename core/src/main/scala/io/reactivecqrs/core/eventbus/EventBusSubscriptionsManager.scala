package io.reactivecqrs.core.eventbus

import akka.pattern.ask
import akka.actor.{Actor, ActorRef}
import akka.util.Timeout
import io.reactivecqrs.core.eventbus.EventBusSubscriptionsManager.{GetSubscriptions, Subscribe}
import io.reactivecqrs.core.eventbus.EventsBusActor.SubscribeRequest

import scala.concurrent.Future


object EventBusSubscriptionsManager {
  case class Subscribe(requests: List[SubscribeRequest])
  case object GetSubscriptions
}

class EventBusSubscriptionsManagerApi(eventBusSubscriptionsManager: ActorRef) {
  def subscribe(requests: List[SubscribeRequest]): Unit = {
    eventBusSubscriptionsManager ! Subscribe(requests)
  }

  def getSubscriptions(implicit timeout: Timeout): Future[List[SubscribeRequest]] = {
    (eventBusSubscriptionsManager ? GetSubscriptions).mapTo[List[SubscribeRequest]]
  }
}

class EventBusSubscriptionsManager extends Actor {

  private var subscriptionsOpen = true
  private var subscriptionsRequests: List[SubscribeRequest] = List.empty

  override def receive: Receive = {
    case Subscribe(requests) =>
      subscribe(requests)
    case GetSubscriptions =>
      subscriptionsOpen = false
      sender ! subscriptionsRequests
  }

  private def subscribe(subscribe: List[SubscribeRequest]): Unit = {
    if(subscriptionsOpen) {
      subscriptionsRequests :::= subscribe
    } else {
      throw new IllegalStateException("Subscriptions for Event Bus already closed!")
    }
  }

}
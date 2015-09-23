package io.reactivecqrs.core.projection

import akka.actor.ActorRef
import io.reactivecqrs.core.util.RandomUtil
import io.reactivecqrs.core.projection.Subscribable._

import scala.collection.mutable
import scala.reflect.runtime.universe._

object Subscribable {
  case class SubscribedForProjectionUpdates(subscriptionCode: String, subscriptionId: String)

  case class CancelProjectionSubscriptions(subscriptions: List[String])

  case class ProjectionSubscriptionsCancelled(subscriptions: List[String])

  case class SubscriptionUpdated[UPDATE](subscriptionId: String, data: UPDATE)
}

trait Subscribable extends ProjectionActor {

  protected def receiveSubscriptionRequest: Receive

  protected def receiveSubscription: Receive = receiveSubscriptionRequest orElse {
    case CancelProjectionSubscriptions(subscriptions) => sender ! ProjectionSubscriptionsCancelled(subscriptions.flatMap(handleUnsubscribe))
  }

  abstract override protected def receiveUpdate = super.receiveUpdate orElse receiveSubscription

  private val subscriptionIdToListener = mutable.HashMap[String, ActorRef]()
  private val subscriptionIdToAcceptor = mutable.HashMap[String, _ => Option[_]]()
  private val typeToSubscriptionId = mutable.HashMap[String, List[String]]()


  protected def handleSubscribe[DATA: TypeTag, UPDATE](code: String, listener: ActorRef, filter: (DATA) => Option[UPDATE]) = {
    val subscriptionId = generateNextSubscriptionId
    val typeTagString = typeTag[DATA].toString()

    subscriptionIdToListener += subscriptionId -> listener
    subscriptionIdToAcceptor += subscriptionId -> filter
    typeToSubscriptionId += typeTagString -> (subscriptionId :: typeToSubscriptionId.getOrElse(typeTagString, Nil))

    listener ! SubscribedForProjectionUpdates(code, subscriptionId)
  }



  protected def sendUpdate[DATA: TypeTag](u: DATA) = {
    typeToSubscriptionId.get(typeTag[DATA].toString()) foreach { subscriptions =>
      for {
        subscriptionId: String <- subscriptions
        listener: ActorRef <- subscriptionIdToListener.get(subscriptionId)
        acceptor <- subscriptionIdToAcceptor.get(subscriptionId)
        result <- acceptor.asInstanceOf[DATA => Option[_]](u)
      } yield listener ! SubscriptionUpdated(subscriptionId, result)
    }
  }



  private def handleUnsubscribe(subscriptionId: String): Option[String] = {
    typeToSubscriptionId.find { _._2.contains(subscriptionId) } match {
      case Some((aggregateId, subscriptions)) =>
        val filtered = subscriptions.filterNot(_ == subscriptionId)
        if (filtered.isEmpty) {
          typeToSubscriptionId -= aggregateId
        } else {
          typeToSubscriptionId += aggregateId -> filtered
        }
      case None => ()
    }
    subscriptionIdToAcceptor.get(subscriptionId) match {
      case Some(_) =>
        subscriptionIdToAcceptor -= subscriptionId
      case None => ()
    }
    subscriptionIdToListener.get(subscriptionId) match {
      case Some(subscriber) =>
        subscriptionIdToListener -= subscriptionId
        Some(subscriptionId)
      case None => None
    }
  }

  // utilities

  private val randomUtil = new RandomUtil

  private def generateNextSubscriptionId: String = {
    var subscriptionId: String = null
    do {
      subscriptionId = randomUtil.generateRandomString(32)
    } while (subscriptionIdToListener.contains(subscriptionId))
    subscriptionId
  }
}

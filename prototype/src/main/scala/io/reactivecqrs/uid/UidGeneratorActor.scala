package io.reactivecqrs.uid

import akka.actor.{ActorRef, Actor}
import akka.actor.Actor.Receive
import akka.event.LoggingReceive

import scala.concurrent.Future

case class NewAggregatesIdsPool(from: Long, size: Long)

object UidGeneratorActor {
  case object GetNewAggregatesIdsPool

}



class UidGeneratorActor extends Actor {
  import UidGeneratorActor._

  val uidGenerator = new PostgresUidGenerator

  override def receive: Receive = LoggingReceive {
    case GetNewAggregatesIdsPool => handleGetNewAggregatesIdsPool(sender())
  }

  def handleGetNewAggregatesIdsPool(respondTo: ActorRef): Unit = {
    import context.dispatcher
    Future {
      respondTo ! uidGenerator.nextIdsPool
    }
  }
}

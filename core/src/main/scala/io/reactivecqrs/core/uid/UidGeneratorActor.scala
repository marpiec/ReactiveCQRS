package io.reactivecqrs.core.uid

import akka.actor.{Actor, ActorRef}
import akka.event.LoggingReceive

import scala.concurrent.Future

case class NewAggregatesIdsPool(from: Long, size: Long)
case class NewCommandsIdsPool(from: Long, size: Long)

object UidGeneratorActor {
  case object GetNewAggregatesIdsPool
  case object GetNewCommandsIdsPool

}



class UidGeneratorActor(aggregatesUidGenerator: UidGenerator, commandsUidGenerator: UidGenerator) extends Actor {
  import UidGeneratorActor._


  override def receive: Receive = LoggingReceive {
    case GetNewAggregatesIdsPool => handleGetNewAggregatesIdsPool(sender())
    case GetNewCommandsIdsPool => handleGetNewCommandsIdsPool(sender())
  }

  def handleGetNewAggregatesIdsPool(respondTo: ActorRef): Unit = {
    import context.dispatcher
    Future {
      val pool: IdsPool = aggregatesUidGenerator.nextIdsPool
      respondTo ! NewAggregatesIdsPool(pool.from, pool.size)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }

  def handleGetNewCommandsIdsPool(respondTo: ActorRef): Unit = {
    import context.dispatcher
    Future {
      val pool: IdsPool = aggregatesUidGenerator.nextIdsPool
      respondTo ! NewCommandsIdsPool(pool.from, pool.size)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }
}

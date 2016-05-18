package io.reactivecqrs.core.uid

import akka.actor.{Actor, ActorRef}
import akka.event.LoggingReceive
import io.reactivecqrs.core.util.ActorLogging

import scala.concurrent.Future

case class NewAggregatesIdsPool(from: Long, size: Long)
case class NewCommandsIdsPool(from: Long, size: Long)
case class NewSagasIdsPool(from: Long, size: Long)

object UidGeneratorActor {
  case object GetNewAggregatesIdsPool
  case object GetNewCommandsIdsPool
  case object GetNewSagasIdsPool
}



class UidGeneratorActor(aggregatesUidGenerator: UidGenerator,
                        commandsUidGenerator: UidGenerator,
                        sagasUidGenerator: UidGenerator) extends Actor with ActorLogging {
  import UidGeneratorActor._


  override def receive: Receive = logReceive  {
    case GetNewAggregatesIdsPool => handleGetNewAggregatesIdsPool(sender())
    case GetNewCommandsIdsPool => handleGetNewCommandsIdsPool(sender())
    case GetNewSagasIdsPool => handleGetNewSagsIdsPool(sender())
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
      val pool: IdsPool = commandsUidGenerator.nextIdsPool
      respondTo ! NewCommandsIdsPool(pool.from, pool.size)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }

  def handleGetNewSagsIdsPool(respondTo: ActorRef): Unit = {
    import context.dispatcher
    Future {
      val pool: IdsPool = sagasUidGenerator.nextIdsPool
      respondTo ! NewSagasIdsPool(pool.from, pool.size)
    } onFailure {
      case e: Exception => throw new IllegalStateException(e)
    }
  }
}

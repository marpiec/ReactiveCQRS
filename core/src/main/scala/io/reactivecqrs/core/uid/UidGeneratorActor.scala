package io.reactivecqrs.core.uid

import org.apache.pekko.actor.{Actor, ActorRef}
import io.reactivecqrs.core.util.MyActorLogging

import scala.concurrent.Future
import scala.util.{Failure, Success}

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
                        sagasUidGenerator: UidGenerator) extends Actor with MyActorLogging {
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
    } onComplete {
      case Failure(e) => println("Error while getting new aggregates ids pool", e)
      case _ => ()
    }
  }

  def handleGetNewCommandsIdsPool(respondTo: ActorRef): Unit = {
    import context.dispatcher
    Future {
      val pool: IdsPool = commandsUidGenerator.nextIdsPool
      respondTo ! NewCommandsIdsPool(pool.from, pool.size)
    } onComplete {
      case Failure(e) => println("Error while getting new commands ids pool", e)
      case _ => ()
    }
  }

  def handleGetNewSagsIdsPool(respondTo: ActorRef): Unit = {
    import context.dispatcher
    Future {
      val pool: IdsPool = sagasUidGenerator.nextIdsPool
      respondTo ! NewSagasIdsPool(pool.from, pool.size)
    } onComplete {
      case Failure(e) => println("Error while getting new sagas ids pool", e)
      case _ => ()
    }
  }
}

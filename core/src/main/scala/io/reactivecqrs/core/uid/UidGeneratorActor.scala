package io.reactivecqrs.core.uid

import akka.actor.{ActorRef, Actor}
import akka.actor.Actor.Receive
import akka.event.LoggingReceive

import scala.concurrent.Future

case class NewAggregatesIdsPool(from: Long, size: Long)
case class NewCommandsIdsPool(from: Long, size: Long)

object UidGeneratorActor {
  case object GetNewAggregatesIdsPool
  case object GetNewCommandsIdsPool

}



class UidGeneratorActor extends Actor {
  import UidGeneratorActor._

  val aggregatesUidGenerator = new PostgresUidGenerator("aggregates_uids_seq")
  val commandsUidGenerator = new PostgresUidGenerator("commands_uids_seq")

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

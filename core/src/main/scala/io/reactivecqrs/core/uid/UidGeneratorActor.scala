package io.reactivecqrs.core.uid

import org.apache.pekko.actor.{Actor, ActorRef, Status}
import io.reactivecqrs.core.util.{BlockingDispatcher, MyActorLogging}

import scala.concurrent.{ExecutionContext, Future}
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
    case GetNewAggregatesIdsPool => replyWithPool(sender(), "aggregates", aggregatesUidGenerator)(pool => NewAggregatesIdsPool(pool.from, pool.size))
    case GetNewCommandsIdsPool => replyWithPool(sender(), "commands", commandsUidGenerator)(pool => NewCommandsIdsPool(pool.from, pool.size))
    case GetNewSagasIdsPool => replyWithPool(sender(), "sagas", sagasUidGenerator)(pool => NewSagasIdsPool(pool.from, pool.size))
  }

  // A failure is sent back too, so the requester fails at once instead of waiting out its timeout (F40).
  private def replyWithPool(respondTo: ActorRef, kind: String, generator: UidGenerator)(reply: IdsPool => Any): Unit = {
    implicit val blocking: ExecutionContext = BlockingDispatcher(context.system) // F27
    Future(generator.nextIdsPool).onComplete {
      case Success(pool) => respondTo ! reply(pool)
      case Failure(e) =>
        log.error(e, "Error while getting new " + kind + " ids pool")
        respondTo ! Status.Failure(e)
    }
  }
}

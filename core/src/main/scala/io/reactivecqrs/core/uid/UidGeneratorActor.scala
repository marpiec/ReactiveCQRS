package io.reactivecqrs.core.uid

import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import io.reactivecqrs.core.util.{BlockingDispatcher, MyActorLogging}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** The answer to `GetNewAggregatesIdsPool`. */
case class NewAggregatesIdsPool(from: Long, size: Long)
/** The answer to `GetNewCommandsIdsPool`. */
case class NewCommandsIdsPool(from: Long, size: Long)
/** The answer to `GetNewSagasIdsPool`. */
case class NewSagasIdsPool(from: Long, size: Long)

/** The uid generator's messages, and its `Props` on PostgreSQL. */
object UidGeneratorActor {
  /** Asks for the next pool of aggregate ids. */
  case object GetNewAggregatesIdsPool
  /** Asks for the next pool of command ids. */
  case object GetNewCommandsIdsPool
  /** Asks for the next pool of saga ids. */
  case object GetNewSagasIdsPool

  /** A host's uid generator on PostgreSQL: aggregate ids in the installation's range (D39), read from the same setting the
    * command buses check chosen ids against (`reactivecqrs.installation-number`); command and saga ids from their
    * sequences. The generators are made at once, so a wrong installation number fails the host as it starts. */
  def postgres(system: ActorSystem): Props = {
    val aggregates = new PostgresUidGenerator("aggregates_uids_seq", Some(IdRange.of(system.settings.config)))
    val commands = new PostgresUidGenerator("commands_uids_seq")
    val sagas = new PostgresUidGenerator("sagas_uids_seq")
    Props(new UidGeneratorActor(aggregates, commands, sagas))
  }
}



/** Hands out pools of ids, one generator per kind; one per node (`UidGeneratorActor.postgres`). */
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

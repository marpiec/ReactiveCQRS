package io.reactivecqrs.core.commandhandler

import java.time.Instant
import org.apache.pekko.actor.{Actor, ActorRef, OneForOneStrategy, Props, Status, SupervisorStrategy, Terminated}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId}
import io.reactivecqrs.core.aggregate.AggregateOwner._
import io.reactivecqrs.core.aggregate.{AggregateOwner, OwnerSettings}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor.{PassivateAggregate, RemoveExpiredResponses}
import io.reactivecqrs.core.delivery.RepublishRequest
import io.reactivecqrs.core.eventbus.EventsBusActor.{PublishEventsAck, RegisterPublisher}
import io.reactivecqrs.core.eventstore.EventStoreState
import io.reactivecqrs.core.uid.{NewAggregatesIdsPool, NewCommandsIdsPool, UidGeneratorActor}
import io.reactivecqrs.core.util.{MyActorLogging, RandomUtil}
import org.slf4j.Logger

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Random, Success, Try}

object AggregateCommandBusActor {

  private case object RemoveExpiredResponses

  /** Stops the aggregate's owner once the commands it started are answered; the next message for the aggregate starts a
    * new owner. The bus does the same by itself for owners idle for long; this is for operations and tests. */
  case class PassivateAggregate(id: AggregateId)

  def apply[AGGREGATE_ROOT: ClassTag](aggregateContext: AggregateContext[AGGREGATE_ROOT],
                                             uidGenerator: ActorRef, eventStoreState: EventStoreState,
                                             commandResponseState: CommandResponseState,
                                             eventBus: ActorRef, eventsReplayMode: Boolean, maxInactivityMillis: Long = 43200000L, keepAliveLimit: Int = 200,
                                             failedRestoreRetryAfter: FiniteDuration = 5.seconds): Props = { // 43200000 - 12 hours
    Props(new AggregateCommandBusActor[AGGREGATE_ROOT](aggregateContext, uidGenerator, eventStoreState, commandResponseState, eventBus,
      eventsReplayMode, keepAliveLimit, maxInactivityMillis, None, failedRestoreRetryAfter))
  }

}

/**
  * One per aggregate type: gives commands their ids, routes commands and queries to the owner of their aggregate
  * (one `AggregateOwner` per aggregate, AR1), answers historical reads itself, and deletes delivered outbox rows. It
  * stops owners that were idle for long, or deleted: it keeps the messages that arrive for an owner while it stops and
  * gives them to a new one (Cluster Sharding's passivation, so M5 can put a shard in its place).
  */
class AggregateCommandBusActor[AGGREGATE_ROOT](aggregateContext: AggregateContext[AGGREGATE_ROOT],
                                                       val uidGenerator: ActorRef,
                                                       eventStoreState: EventStoreState,
                                                       commandResponseState: CommandResponseState,
                                                       val eventBus: ActorRef,
                                                       val eventsReplayMode: Boolean,
                                                       val keepAliveLimit: Int, val maxInactivityMillis: Long,
                                                       val eventsLogger: Option[Logger],
                                                       val failedRestoreRetryAfter: FiniteDuration)
                                                        (implicit aggregateRootClassTag: ClassTag[AGGREGATE_ROOT]) extends Actor with MyActorLogging {

  val minimumInactivityMillis: Long = 120000L // 2 minutes

  private val aggregateType = AggregateType.of[AGGREGATE_ROOT]
  val aggregateTypeName: String = aggregateType.typeName
  val aggregateTypeSimpleName: String = aggregateRootClassTag.runtimeClass.getSimpleName

  private var nextAggregateId = 0L
  private var remainingAggregateIds = 0L

  private var nextCommandId = 0L
  private var remainingCommandsIds = 0L

  private val owners = mutable.HashMap[Long, ActorRef]()
  private val ownerIds = mutable.HashMap[ActorRef, Long]()
  private val childrenActivity = mutable.HashMap[Long, Long]()
  // Owners asked to stop, with the messages that arrived for them since; a new owner gets them once the old one stopped.
  private val keptForStopping = mutable.HashMap[Long, Vector[Any]]()

  private var lastClear: Long = 0L

  private var spawnedCount = 0 // keeps actor names unique while a stopped owner's name is still taken

  private val idempotency = new Idempotency(commandResponseState)

  private val settings = new OwnerSettings[AGGREGATE_ROOT](aggregateContext, eventStoreState, idempotency, eventBus, self, failedRestoreRetryAfter)

  // AR7: an owner that fails is stopped, never restarted: a restart would lose its pending callers. It is forgotten
  // when it terminates, and the next request creates a new one.
  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() { case _ => SupervisorStrategy.Stop }

  override def preStart(): Unit = {
    // The event bus routes re-publish requests for this aggregate type here (D8, D20c).
    eventBus ! RegisterPublisher(aggregateType, self)
    if (!eventsReplayMode) {
      // Hourly, at a random minute, so the buses of different aggregate types do not all clean up at once.
      val firstCleanUp = (60 + Random.nextInt(3600)).seconds
      context.system.scheduler.scheduleWithFixedDelay(firstCleanUp, 1.hour, self, RemoveExpiredResponses)(context.dispatcher)
    }
  }

  private def removeExpiredResponses(): Unit = {
    import context.dispatcher
    Future(idempotency.removeExpired()).onComplete {
      case Failure(e) => log.warning("Removing expired idempotent command responses failed: " + e.getMessage)
      case Success(removed) => if (removed > 0) log.info("Removed " + removed + " expired idempotent command responses")
    }
  }

  override def postRestart(reason: Throwable): Unit = {
    // do not call preStart
  }

  override def receive: Receive = logReceive {
    case c: FirstCommand[_,_] => routeCommand(c, () => c.chosenAggregateId.getOrElse(takeNextAggregateId), fresh = c.chosenAggregateId.isEmpty)
    case c: ConcurrentCommand[_,_] => routeCommand(c, () => c.aggregateId)
    case c: RewriteHistoryCommand[_,_] => routeCommand(c, () => c.aggregateId)
    case c: RewriteHistoryConcurrentCommand[_,_] => routeCommand(c, () => c.aggregateId)
    case c: Command[_,_] => routeCommand(c, () => c.aggregateId)
    case GetAggregate(id) => toOwner(id, GetState(sender()))
    case GetAggregateMinVersion(id, version, maxMillis) => toOwner(id, GetStateMinVersion(sender(), version, maxMillis))
    case GetEventsForAggregate(id) => toOwner(id, GetEvents(sender()))
    case GetAggregateForVersion(id, version) => historicalRead(id)(aggregateAt(id, Left(version)))
    case GetAggregateAtInstant(id, instant) => historicalRead(id)(aggregateAt(id, Right(instant)))
    case GetEventsForAggregateForVersion(id, version) => historicalRead(id)(settings.restorer.events(id, Some(Left(version))))
    case ack: PublishEventsAck => delivered(ack)
    // Re-publications count as activity, so the owners they start are stopped like others when idle.
    case m: RepublishRequest => toOwner(m.aggregateId, m)
    case request: SquashAggregate => toOwner(request.id, Squash(sender(), request))
    case ReserveAggregateId =>
      val respondTo = sender()
      Try(takeNextAggregateId) match {
        case Success(id) => respondTo ! id
        case Failure(e) => respondTo ! Status.Failure(e)
      }
    case RemoveExpiredResponses => removeExpiredResponses()
    case PassivateAggregate(id) => stopOwner(id.asLong)
    case OwnerPassivate => ownerIds.get(sender()).foreach(stopOwner)
    case Terminated(ref) => ownerTerminated(ref)
    case m =>
      // Answered, not thrown: a throw would restart the bus, which stops its children and with them every
      // command in flight for this aggregate type (F70).
      log.warning("Cannot handle this kind of message: " + m.getClass.getName)
      sender() ! Status.Failure(new IllegalArgumentException("Cannot handle this kind of message: " + m + " class: " + m.getClass+". Maybe it should extend Command trait."))
  }

  // A failure while routing one command, such as the uid generator failing, is answered, not thrown: a throw would
  // restart the bus, which stops its children and every command in flight (F75, AR7).
  private def routeCommand(command: Any, aggregateIdOf: () => AggregateId, fresh: Boolean = false): Unit = {
    val respondTo = sender()
    try {
      val commandId = takeNextCommandId
      toOwner(aggregateIdOf(), CommandEnvelope(respondTo, commandId, command), fresh)
    } catch {
      case e: Exception =>
        val errorId = RandomUtil.generateRandomString(16)
        log.error(e, "Cannot route command " + command.getClass.getSimpleName + ", errorId: [" + errorId + "]")
        respondTo ! CommandHandlingError(command.getClass.getSimpleName, errorId, CommandId(0)) // no command id was assigned
    }
  }

  private def toOwner(id: AggregateId, message: Any, fresh: Boolean = false): Unit = {
    val key = id.asLong
    val now = System.nanoTime() / 1_000_000
    childrenActivity += key -> now
    keptForStopping.get(key) match {
      case Some(kept) => keptForStopping += key -> (kept :+ message)
      case None => owners.getOrElseUpdate(key, createOwner(id, fresh)) ! message
    }
    clearOldAggregateRepositories(now)
  }

  private def createOwner(id: AggregateId, fresh: Boolean): ActorRef = {
    val owner = context.actorOf(Props(new AggregateOwner[AGGREGATE_ROOT](id, settings, fresh)), aggregateTypeSimpleName + "_" + id.asLong + "_" + spawnedCount)
    spawnedCount += 1
    context.watch(owner)
    ownerIds += owner -> id.asLong
    owner
  }

  private def stopOwner(key: Long): Unit = owners.get(key).foreach { owner =>
    if (!keptForStopping.contains(key)) {
      keptForStopping += key -> Vector.empty
      childrenActivity -= key
      owner ! StopOwner
    }
  }

  // An owner stopped: on request, after a failure, or after a failed restore. Messages kept for it go to a new owner.
  private def ownerTerminated(ref: ActorRef): Unit = ownerIds.remove(ref).foreach { key =>
    if (owners.get(key).contains(ref)) owners -= key
    keptForStopping.remove(key) match {
      case Some(kept) if kept.nonEmpty =>
        val owner = owners.getOrElseUpdate(key, createOwner(AggregateId(key), fresh = false))
        childrenActivity += key -> System.nanoTime() / 1_000_000
        kept.foreach(owner ! _)
      case _ => childrenActivity -= key
    }
  }

  // Historical reads need no owner: the aggregate is read up to the version or instant, outside the actor's thread.
  private def historicalRead(id: AggregateId)(read: => Any): Unit = {
    val respondTo = sender()
    Future(read)(context.dispatcher).onComplete {
      case Success(answer) => respondTo ! Success(answer)
      case Failure(e: AggregateSquashedException) => respondTo ! Failure(e) // D51: an answer, not a failure of the store
      case Failure(e) =>
        log.error(e, "Reading aggregate " + id.asLong + " of type " + aggregateTypeSimpleName + " failed")
        respondTo ! Failure(new AggregateRestoreFailedException(id, aggregateType, e))
    }(context.dispatcher)
  }

  private def aggregateAt(id: AggregateId, at: Either[AggregateVersion, Instant]): Aggregate[AGGREGATE_ROOT] = {
    val restored = settings.restorer.restore(id, Some(at))
    Aggregate[AGGREGATE_ROOT](id, restored.version, if (restored.version == AggregateVersion.ZERO) None else Option(restored.root))
  }

  // Owners publish on behalf of this bus, so it gets the acknowledgements: it lives as long as the system, while an
  // owner may be stopped before slow subscribers catch up (F91). Deleting delivered rows needs no aggregate state. A
  // permanent delete that cannot happen yet (a failed subscription, duplicates made meanwhile) keeps that event's row,
  // and the ack of its next re-publication tries again. A live owner gets the ack to continue re-publishing its outbox.
  private def delivered(ack: PublishEventsAck): Unit = {
    val id = ack.aggregateId
    Try {
      ack.permanentDeleteAt match {
        case Some(at) =>
          def notYet(): Unit = eventStoreState.deletePublished(id, ack.versions.filterNot(_ == at))
          detachingDuplicates(id) match {
            case Right(detach) => eventStoreState.deleteAggregatePermanently(id, detach) match {
              case Right(()) => stopOwner(id.asLong) // its state is gone
              case Left(reason) =>
                log.warning("Aggregate " + id.asLong + " of type " + aggregateTypeSimpleName + " is not deleted yet: " + reason)
                notYet()
            }
            case Left(reason) =>
              log.error("Aggregate " + id.asLong + " of type " + aggregateTypeSimpleName + " is not deleted: " + reason)
              notYet()
          }
        case None =>
          eventStoreState.deletePublished(id, ack.versions)
      }
    }.failed.foreach(e => log.error(e, "Removing delivered events of aggregate " + id.asLong + " from the outbox failed; they are re-published later"))
    if (!keptForStopping.contains(id.asLong)) owners.get(id.asLong).foreach(_ ! ack)
  }

  // F76: duplicates that read the aggregate's history would lose it with the delete, so each is detached first, with a
  // squash event of its state at version 1 (D50). A type without squash events cannot do that: its aggregate with
  // duplicates is not deleted, and the event stays in the outbox.
  private def detachingDuplicates(id: AggregateId): Either[String, Seq[(AggregateId, FirstEvent[_])]] = {
    val duplicates = eventStoreState.duplicatesReading(id, None)
    settings.squash match {
      case _ if duplicates.isEmpty => Right(Nil)
      case Some(policy) => Right(duplicates.map(duplicate =>
        duplicate -> policy.toEvent(settings.restorer.restore(duplicate, Some(Left(AggregateVersion(1)))).root)))
      case None => Left("aggregates " + duplicates.map(_.asLong).mkString(", ") + " were duplicated from it, and the type has no squash " +
        "event to detach them (F76)")
    }
  }

  private def clearOldAggregateRepositories(now: Long): Unit = {
    if(now - lastClear > 300000) { // no more often than every 5 minutes
      lastClear = now
      if (childrenActivity.size > keepAliveLimit) {
        val sorted = childrenActivity.toVector.sortBy(_._2)
        val toStop = sorted.take(sorted.length - keepAliveLimit)
        toStop.foreach { case (key, lastActive) =>
          if(now - lastActive > minimumInactivityMillis) { // give at least 2 minutes before stopping in case of prolonged activity
            stopOwner(key)
          }
        }
      }

      childrenActivity.filter(now - _._2 > maxInactivityMillis).keys.foreach { key =>
        eventsLogger.filter(_.isDebugEnabled).foreach(_.debug("Stopping aggregate " + key + " because of inactivity"))
        stopOwner(key)
      }
    }
  }

  private def takeNextAggregateId: AggregateId = {
    if(remainingAggregateIds == 0) {
      // TODO get rid of ask pattern
      implicit val timeout: Timeout = Timeout(60 seconds)
      val pool: Future[NewAggregatesIdsPool] = (uidGenerator ? UidGeneratorActor.GetNewAggregatesIdsPool).mapTo[NewAggregatesIdsPool]
      val newAggregatesIdsPool: NewAggregatesIdsPool = Await.result(pool, 60 seconds)
      remainingAggregateIds = newAggregatesIdsPool.size
      nextAggregateId = newAggregatesIdsPool.from
    }

    remainingAggregateIds -= 1
    val aggregateId = AggregateId(nextAggregateId)
    nextAggregateId += 1
    aggregateId

  }

  private def takeNextCommandId: CommandId = {
    if(remainingCommandsIds == 0) {
      // TODO get rid of ask pattern
      implicit val timeout: Timeout = Timeout(60 seconds)
      val pool: Future[NewCommandsIdsPool] = (uidGenerator ? UidGeneratorActor.GetNewCommandsIdsPool).mapTo[NewCommandsIdsPool]
      val newCommandsIdsPool: NewCommandsIdsPool = Await.result(pool, 60 seconds)
      remainingCommandsIds = newCommandsIdsPool.size
      nextCommandId = newCommandsIdsPool.from
    }

    remainingCommandsIds -= 1
    val commandId = CommandId(nextCommandId)
    nextCommandId += 1
    commandId


  }
}

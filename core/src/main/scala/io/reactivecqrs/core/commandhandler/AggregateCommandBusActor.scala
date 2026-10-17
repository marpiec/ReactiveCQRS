package io.reactivecqrs.core.commandhandler

import java.time.Instant
import org.apache.pekko.actor.{Actor, ActorRef, OneForOneStrategy, Props, Status, SupervisorStrategy, Terminated}
import org.apache.pekko.pattern.{ask, pipe}
import org.apache.pekko.util.Timeout
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, CommandId}
import io.reactivecqrs.core.aggregate.AggregateOwner._
import io.reactivecqrs.core.aggregate.{AggregateOwner, OwnerSettings}
import io.reactivecqrs.core.cluster.{Members, MessageSize}
import io.reactivecqrs.core.commandhandler.AggregateCommandBusActor._
import io.reactivecqrs.core.delivery.RepublishRequest
import io.reactivecqrs.core.eventbus.EventsBusActor.{PublishEventsAck, RegisterPublisher}
import io.reactivecqrs.core.eventstore.EventStoreState
import io.reactivecqrs.core.uid.{IdPool, IdRange, NewAggregatesIdsPool, NewCommandsIdsPool, UidGeneratorActor}
import io.reactivecqrs.core.util.{BlockingDispatcher, MyActorLogging, RandomUtil}
import org.slf4j.Logger

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Random, Success, Try}

object AggregateCommandBusActor {

  private case object RemoveExpiredResponses
  private case class IdsNotReceived(ofAggregates: Boolean, error: Throwable)
  /** The aggregate is gone; the owners of the duplicates detached with a squash event that may change the state read
    * theirs again (D85). */
  private case class DeletedPermanently(id: AggregateId, changed: Seq[AggregateId])

  /** Stops the aggregate's owner once the commands it started are answered; the next message for the aggregate starts a
    * new owner. The bus does the same by itself for owners idle for long; this is for operations and tests. */
  case class PassivateAggregate(id: AggregateId)

  /** R17, from a purge: detaches these duplicates of `baseId` (aggregates of other organizations that read it), each with
    * a squash event of its state at version 1, as a permanent delete does (D50, F76). Answered `DuplicatesDetached`, or
    * `DetachRefused` with the reason, having changed nothing. */
  case class DetachDuplicates(baseId: AggregateId, duplicates: Seq[AggregateId])
  case class DuplicatesDetached(baseId: AggregateId, duplicates: Seq[AggregateId])
  case class DetachRefused(baseId: AggregateId, reason: String)

  /** R17, from a purge: the aggregates are gone from the store, so their owners stop without writing a snapshot (a
    * sharded one through its shard, which starts an owner only to stop it, without restoring). Answered
    * `AggregatesForgotten`. */
  case class ForgetAggregates(ids: Seq[AggregateId])
  case class AggregatesForgotten(count: Int)

  def apply[AGGREGATE_ROOT: ClassTag](aggregateContext: AggregateContext[AGGREGATE_ROOT],
                                             uidGenerator: ActorRef, eventStoreState: EventStoreState,
                                             commandResponseState: CommandResponseState,
                                             eventBus: ActorRef, maxInactivityMillis: Long = 43200000L, keepAliveLimit: Int = 200,
                                             failedRestoreRetryAfter: FiniteDuration = 5.seconds): Props = { // 43200000 - 12 hours
    Props(new AggregateCommandBusActor[AGGREGATE_ROOT](aggregateContext, uidGenerator, eventStoreState, commandResponseState, eventBus,
      keepAliveLimit, maxInactivityMillis, None, failedRestoreRetryAfter))
  }

}

/**
  * One per aggregate type and node: gives commands their ids, routes commands and queries to the owner of their
  * aggregate (one `AggregateOwner` per aggregate, AR1), answers historical reads itself, and deletes delivered outbox
  * rows. Owners of a `Local` type are its children: it stops those idle for long, or deleted, keeping the messages that
  * arrive for an owner while it stops for a new one. Owners of a `Sharded` type (D43) are the entities of the type's shard
  * region, which this bus starts on its node: the shards place, keep and passivate them, and the bus sends them
  * everything through the region.
  */
class AggregateCommandBusActor[AGGREGATE_ROOT](aggregateContext: AggregateContext[AGGREGATE_ROOT],
                                                       val uidGenerator: ActorRef,
                                                       eventStoreState: EventStoreState,
                                                       commandResponseState: CommandResponseState,
                                                       val eventBus: ActorRef,
                                                       val keepAliveLimit: Int, val maxInactivityMillis: Long,
                                                       val eventsLogger: Option[Logger],
                                                       val failedRestoreRetryAfter: FiniteDuration)
                                                        (implicit aggregateRootClassTag: ClassTag[AGGREGATE_ROOT]) extends Actor with MyActorLogging {

  private val aggregateType = AggregateType.of[AGGREGATE_ROOT]
  val aggregateTypeSimpleName: String = aggregateRootClassTag.runtimeClass.getSimpleName

  // F23: ids come from pools fetched ahead. While a command (or an id reservation) waits for a pool, the ones that arrive
  // after it wait too, in order.
  private val idRange = IdRange.of(context.system.settings.config)
  Members(context.system).track(aggregateContext) // D67: the lowest app-version among the cluster's members
  private val messageSize = aggregateContext.placement match { // F121
    case _: Placement.Sharded => Some(MessageSize(context.system))
    case Placement.Local => None
  }
  private val aggregateIds = new IdPool
  private val commandIds = new IdPool
  private var waitingForIds = Vector[(Any, ActorRef)]()

  private val idempotency = new Idempotency(commandResponseState)

  private val settings = new OwnerSettings[AGGREGATE_ROOT](aggregateContext, eventStoreState, idempotency, eventBus, self, failedRestoreRetryAfter)

  private def ownerProps(id: Option[AggregateId]): Props =
    Props(new AggregateOwner[AGGREGATE_ROOT](settings, id)).withDispatcher(BlockingDispatcher.id)

  // D43: this bus's own children, or the entities of the type's shard region.
  private val owners: Owners = aggregateContext.placement match {
    case Placement.Sharded(numberOfShards) => ShardedOwners.start(context, aggregateType.typeName, ownerProps(None), numberOfShards)
    case Placement.Local => new LocalOwners(context, id => ownerProps(Some(id)), aggregateTypeSimpleName, keepAliveLimit, maxInactivityMillis, eventsLogger)
  }

  // AR7: an owner that fails is stopped, never restarted: a restart would lose its pending callers. It is forgotten
  // when it terminates, and the next request creates a new one.
  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() { case _ => SupervisorStrategy.Stop }

  override def preStart(): Unit = {
    // The event bus routes re-publish requests for this aggregate type here (D8, D20c).
    eventBus ! RegisterPublisher(aggregateType, self)
    askForIds()
    // Hourly, at a random minute, so the buses of different aggregate types do not all clean up at once.
    val firstCleanUp = (60 + Random.nextInt(3600)).seconds
    context.system.scheduler.scheduleWithFixedDelay(firstCleanUp, 1.hour, self, RemoveExpiredResponses)(context.dispatcher)
  }

  private def removeExpiredResponses(): Unit = {
    implicit val blocking: ExecutionContext = BlockingDispatcher(context.system)
    Future(idempotency.removeExpired()).onComplete {
      case Failure(e) => log.warning("Removing expired idempotent command responses failed: " + e.getMessage)
      case Success(removed) => if (removed > 0) log.info("Removed " + removed + " expired idempotent command responses")
    }
  }

  override def postRestart(reason: Throwable): Unit = {
    // do not call preStart
  }

  override def receive: Receive = logReceive {
    case c @ (_: FirstCommand[_, _] | _: ConcurrentCommand[_, _] | _: RewriteHistoryCommand[_, _] | _: RewriteHistoryConcurrentCommand[_, _] |
              _: Command[_, _] | ReserveAggregateId) => withIds(c, sender())
    case NewAggregatesIdsPool(from, size) => aggregateIds.received(from, size); serveWaiting(); askForIds()
    case NewCommandsIdsPool(from, size) => commandIds.received(from, size); serveWaiting(); askForIds()
    case IdsNotReceived(ofAggregates, e) => idsNotReceived(ofAggregates, e)
    case GetAggregate(id) => owners.send(id, GetState(sender()))
    case GetAggregateMinVersion(id, version, maxMillis) => owners.send(id, GetStateMinVersion(sender(), version, maxMillis))
    // F121: read here from the store, not by the owner, whose answer may be too large to come from another node.
    case GetEventsForAggregate(id) => historicalRead(id)(settings.restorer.events(id))
    case GetArchivedEvents(id) => historicalRead(id)(ArchivedEvents(id, eventStoreState.readArchive[AGGREGATE_ROOT](id))) // R11
    case GetAggregateForVersion(id, version) => historicalRead(id)(aggregateAt(id, Left(version)))
    case GetAggregateAtInstant(id, instant) => historicalRead(id)(aggregateAt(id, Right(instant)))
    case GetEventsForAggregateForVersion(id, version) => historicalRead(id)(settings.restorer.events(id, Some(Left(version))))
    case ack: PublishEventsAck => delivered(ack)
    // Re-publications count as activity, so the owners they start are stopped like others when idle.
    case m: RepublishRequest => owners.send(m.aggregateId, m)
    case request: SquashAggregate => owners.send(request.id, Squash(sender(), request))
    case RemoveExpiredResponses => removeExpiredResponses()
    case PassivateAggregate(id) => owners.stop(id, stale = false)
    case DeletedPermanently(id, changed) => (id +: changed).foreach(owners.stop(_, stale = true)) // its state is gone
    case HistoryChanged(changed) => changed.foreach(owners.stop(_, stale = true))
    case DetachDuplicates(baseId, duplicates) => detachForPurge(baseId, duplicates, sender())
    case ForgetAggregates(ids) =>
      ids.foreach(owners.stop(_, stale = true))
      sender() ! AggregatesForgotten(ids.size)
    case m @ (OwnerPassivate | _: OwnerStarted | _: Terminated) => owners.fromOwners(sender()).applyOrElse(m, unhandled)
    case m =>
      // Answered, not thrown: a throw would restart the bus, which stops its children and with them every
      // command in flight for this aggregate type (F70).
      log.warning("Cannot handle this kind of message: " + m.getClass.getName)
      sender() ! Status.Failure(new IllegalArgumentException("Cannot handle this kind of message: " + m + " class: " + m.getClass+". Maybe it should extend Command trait."))
  }

  // ************** Ids (F23)

  private def withIds(message: Any, respondTo: ActorRef): Unit = refused(message) match {
    case Some(reason) => respondTo ! FailureResponse(List(reason))
    case None =>
      if (waitingForIds.nonEmpty || !route(message, respondTo)) waitingForIds :+= (message -> respondTo)
      askForIds()
  }

  // D39: a caller may choose a system or legacy id, or one of this installation's range, never another installation's, a
  // negative one (ephemeral, D77), or one a JavaScript client cannot read.
  private def refused(message: Any): Option[String] = message match {
    case ReserveAggregateId => None
    case c: FirstCommand[_, _] => c.chosenAggregateId.flatMap(idRange.refusedChoice).orElse(tooLarge(c))
    case c => tooLarge(c)
  }

  // F121: a command of a sharded type may go to an owner on another node, so it must fit a message between nodes; it is
  // refused wherever its owner runs, so the outcome does not depend on where that is. The envelope adds under 1 KiB.
  private def tooLarge(command: Any): Option[String] = messageSize.flatMap { sizes =>
    val limit = sizes.limit - 1024
    Try(sizes.of(command.asInstanceOf[AnyRef])) match {
      case Success(size) if size <= limit => None
      case Success(size) => Some("Command " + command.getClass.getSimpleName + " of " + size + " bytes is too large to be sent between " +
        "nodes; the limit is " + limit + " bytes (from pekko.remote.artery.advanced.maximum-frame-size)")
      case Failure(e) => Some("Command " + command.getClass.getSimpleName + " cannot be sent between nodes: " + e)
    }
  }

  private def serveWaiting(): Unit =
    while (waitingForIds.nonEmpty && route(waitingForIds.head._1, waitingForIds.head._2)) waitingForIds = waitingForIds.tail

  private def needsAggregateId(message: Any): Boolean = message match {
    case c: FirstCommand[_, _] => c.chosenAggregateId.isEmpty
    case ReserveAggregateId => true
    case _ => false
  }

  // Routes the message if the ids it needs are at hand, taking them; otherwise takes nothing and says so.
  private def route(message: Any, respondTo: ActorRef): Boolean = {
    val ready = (message == ReserveAggregateId || commandIds.available) && (!needsAggregateId(message) || aggregateIds.available)
    if (ready) message match {
      case ReserveAggregateId => respondTo ! AggregateId(aggregateIds.take())
      case c: FirstCommand[_, _] => routeCommand(c, respondTo, c.chosenAggregateId.getOrElse(AggregateId(aggregateIds.take())), fresh = c.chosenAggregateId.isEmpty)
      case c: ConcurrentCommand[_, _] => routeCommand(c, respondTo, c.aggregateId)
      case c: RewriteHistoryCommand[_, _] => routeCommand(c, respondTo, c.aggregateId)
      case c: RewriteHistoryConcurrentCommand[_, _] => routeCommand(c, respondTo, c.aggregateId)
      case c: Command[_, _] => routeCommand(c, respondTo, c.aggregateId)
      case other => log.error("Not a command: " + other.getClass.getName) // receive passes only commands and reservations
    }
    ready
  }

  private def routeCommand(command: Any, respondTo: ActorRef, aggregateId: AggregateId, fresh: Boolean = false): Unit =
    owners.send(aggregateId, CommandEnvelope(respondTo, CommandId(commandIds.take()), command, fresh))

  private def askForIds(): Unit = {
    import context.dispatcher
    implicit val timeout: Timeout = Timeout(60.seconds)
    if (aggregateIds.shouldAsk) (uidGenerator ? UidGeneratorActor.GetNewAggregatesIdsPool).recover { case e => IdsNotReceived(ofAggregates = true, e) }.pipeTo(self)
    if (commandIds.shouldAsk) (uidGenerator ? UidGeneratorActor.GetNewCommandsIdsPool).recover { case e => IdsNotReceived(ofAggregates = false, e) }.pipeTo(self)
  }

  // The waiting messages that need such ids are answered with the failure (F75: answered, not thrown, AR7). The pool is
  // asked for again when the next message arrives, not at once, so a generator that is down is not asked in a loop.
  private def idsNotReceived(ofAggregates: Boolean, e: Throwable): Unit = {
    val pool = if (ofAggregates) aggregateIds else commandIds
    pool.failed()
    log.error(e, "No pool of " + (if (ofAggregates) "aggregate" else "command") + " ids for " + aggregateTypeSimpleName + " came")
    // A pool asked for ahead that does not come fails nothing while ids of the kind are still at hand.
    val (failing, others) = waitingForIds.partition { case (message, _) =>
      !pool.available && (if (ofAggregates) needsAggregateId(message) else message != ReserveAggregateId)
    }
    waitingForIds = others
    failing.foreach {
      case (ReserveAggregateId, respondTo) => respondTo ! Status.Failure(e)
      case (command, respondTo) =>
        val errorId = RandomUtil.generateRandomString(16)
        log.error("Cannot route command " + command.getClass.getSimpleName + " without ids, errorId: [" + errorId + "]")
        respondTo ! CommandHandlingError(command.getClass.getSimpleName, errorId, CommandId(0)) // no command id was assigned
    }
    serveWaiting()
  }

  // Historical reads need no owner: the aggregate is read up to the version or instant, outside the actor's thread.
  private def historicalRead(id: AggregateId)(read: => Any): Unit = {
    val respondTo = sender()
    implicit val blocking: ExecutionContext = BlockingDispatcher(context.system)
    Future(read).onComplete {
      case Success(answer) => respondTo ! Success(answer)
      case Failure(e: AggregateSquashedException) => respondTo ! Failure(e) // D51: an answer, not a failure of the store
      case Failure(e) =>
        log.error(e, "Reading aggregate " + id.asLong + " of type " + aggregateTypeSimpleName + " failed")
        respondTo ! Failure(new AggregateRestoreFailedException(id, aggregateType, e))
    }
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
    ack.permanentDeleteAt match {
      case Some(at) => deletePermanently(id, ack.versions.filterNot(_ == at))
      case None => Try(eventStoreState.deletePublished(id, ack.versions)).failed.foreach(e =>
        log.error(e, "Removing delivered events of aggregate " + id.asLong + " from the outbox failed; they are re-published later"))
    }
    owners.acknowledged(ack)
  }

  // F27: off this bus's thread, as it restores the duplicates first (F76); the owner is stopped once the state is gone.
  // The duplicates' squash events wait for every node to read their class, as a squash's do (D67, F134).
  private def deletePermanently(id: AggregateId, otherVersions: Seq[AggregateVersion]): Unit = {
    implicit val blocking: ExecutionContext = BlockingDispatcher(context.system)
    Future {
      val deleted = for {
        detach <- detachingDuplicates(id).left.map(reason => (reason, true))
        _ <- settings.notWrittenYet(detach.map(_._2)).map(reason => (reason, false)).toLeft(())
        _ <- eventStoreState.deleteAggregatePermanently(id, detach, settings.squash.fold(Set.empty[String])(_.keep.map(_.getName)))
          .left.map(reason => (reason, false))
      } yield detach.collect { case (duplicate, _: SquashMayChangeState) => duplicate }
      deleted.left.foreach { case (reason, forGood) =>
        if (forGood) log.error("Aggregate " + id.asLong + " of type " + aggregateTypeSimpleName + " is not deleted: " + reason)
        else log.warning("Aggregate " + id.asLong + " of type " + aggregateTypeSimpleName + " is not deleted yet: " + reason)
        eventStoreState.deletePublished(id, otherVersions)
      }
      deleted.toOption
    }.onComplete {
      case Success(Some(changed)) => self ! DeletedPermanently(id, changed)
      case Success(None) => ()
      case Failure(e) => log.error(e, "Deleting aggregate " + id.asLong + " of type " + aggregateTypeSimpleName + " failed; it is tried again when its event is delivered again")
    }
  }

  // R17: the duplicates a purge names are detached as a permanent delete detaches them, off this bus's thread (F27).
  private def detachForPurge(baseId: AggregateId, duplicates: Seq[AggregateId], respondTo: ActorRef): Unit = {
    implicit val blocking: ExecutionContext = BlockingDispatcher(context.system)
    Future {
      for {
        detach <- settings.squash match {
          case Some(policy) => settings.restorer.detachEvents(duplicates, policy)
          case None => Left("aggregates " + duplicates.map(_.asLong).mkString(", ") + " were duplicated from it, and the type has no squash " +
            "event to detach them (F76)")
        }
        _ <- settings.notWrittenYet(detach.map(_._2)).toLeft(())
        _ <- eventStoreState.detachDuplicates(baseId, detach, settings.squash.fold(Set.empty[String])(_.keep.map(_.getName)))
      } yield detach.collect { case (duplicate, _: SquashMayChangeState) => duplicate }
    }.onComplete {
      case Success(Right(changed)) =>
        self ! HistoryChanged(changed)
        respondTo ! DuplicatesDetached(baseId, duplicates)
      case Success(Left(reason)) => respondTo ! DetachRefused(baseId, reason)
      case Failure(e) =>
        log.error(e, "Detaching the duplicates of aggregate " + baseId.asLong + " of type " + aggregateTypeSimpleName + " failed")
        respondTo ! DetachRefused(baseId, "detaching failed: " + e.getMessage)
    }
  }

  // F76: duplicates that read the aggregate's history would lose it with the delete, so each is detached first, with a
  // checked squash event of its state at version 1 (D50, D85). A type without squash events, or one whose squash event
  // does not give a duplicate's state back, cannot do that: the aggregate is not deleted, and the event stays in the outbox.
  private def detachingDuplicates(id: AggregateId): Either[String, Seq[(AggregateId, FirstEvent[_])]] = {
    val duplicates = eventStoreState.duplicatesReading(id, None)
    settings.squash match {
      case _ if duplicates.isEmpty => Right(Nil)
      case Some(policy) => settings.restorer.detachEvents(duplicates, policy)
      case None => Left("aggregates " + duplicates.map(_.asLong).mkString(", ") + " were duplicated from it, and the type has no squash " +
        "event to detach them (F76)")
    }
  }
}

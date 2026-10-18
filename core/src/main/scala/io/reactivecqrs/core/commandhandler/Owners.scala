package io.reactivecqrs.core.commandhandler

import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.aggregate.AggregateOwner._
import io.reactivecqrs.core.eventbus.EventsBusActor.PublishEventsAck
import org.apache.pekko.actor.{ActorContext, ActorRef, Props, Terminated}
import org.apache.pekko.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import org.slf4j.Logger

import scala.collection.mutable

/**
  * The owners of one aggregate type as its command bus on a node sees them (D43): its own children for a `Local` type,
  * the entities of the type's shard region for a `Sharded` one. The bus sends them everything through this, asks it to
  * stop one, and passes it the acknowledgements of publications and the owners' own messages to the bus. Used only on
  * the bus's thread; what it sends to an owner goes with the bus as the sender.
  */
private[commandhandler] trait Owners {

  /** Delivers `message` to the aggregate's owner, starting one if none runs. */
  def send(id: AggregateId, message: Any): Unit

  /** Stops the aggregate's owner once it answered the commands it started; what arrives meanwhile goes to the next
    * owner. `stale`: its history changed under it, so it writes no snapshot. */
  def stop(id: AggregateId, stale: Boolean): Unit

  /** Passes the acknowledgement on to the aggregate's owner when one runs on this node, so an ack never starts one. */
  def acknowledged(ack: PublishEventsAck): Unit

  /** The owners' own messages to the bus, and the ends of the owners it watches. */
  def fromOwners(sender: ActorRef): PartialFunction[Any, Unit]
}

/** The bus's own owners, one per aggregate on this node; idle ones are stopped by a sweep (F46 holds here). */
private[commandhandler] final class LocalOwners(context: ActorContext,
                                                ownerProps: AggregateId => Props,
                                                typeName: String,
                                                keepAliveLimit: Int,
                                                maxInactivityMillis: Long,
                                                eventsLogger: Option[Logger]) extends Owners {

  private val owners = mutable.HashMap[Long, ActorRef]()
  private val ownerIds = mutable.HashMap[ActorRef, Long]()
  private val activity = mutable.HashMap[Long, Long]()
  // Owners asked to stop, with the messages that arrived for them since; a new owner gets them once the old one stopped.
  private val keptForStopping = mutable.HashMap[Long, Vector[Any]]()
  private var nextSweep = 0L
  private var spawnedCount = 0 // keeps actor names unique while a stopped owner's name is still taken

  override def send(id: AggregateId, message: Any): Unit = {
    val key = id.asLong
    val now = System.nanoTime() / 1_000_000
    activity += key -> now
    keptForStopping.get(key) match {
      case Some(kept) => keptForStopping += key -> (kept :+ message)
      case None => owners.getOrElseUpdate(key, create(id)).tell(message, context.self)
    }
    sweep(now)
  }

  // The owner is told a stale history also when it is stopping already.
  override def stop(id: AggregateId, stale: Boolean): Unit = owners.get(id.asLong).foreach { owner =>
    val key = id.asLong
    val stopping = keptForStopping.contains(key)
    if (!stopping) {
      keptForStopping += key -> Vector.empty
      activity -= key
    }
    if (stale) owner.tell(StopOwnerStale, context.self) else if (!stopping) owner.tell(StopOwner, context.self)
  }

  override def acknowledged(ack: PublishEventsAck): Unit =
    if (!keptForStopping.contains(ack.aggregateId.asLong)) owners.get(ack.aggregateId.asLong).foreach(_.tell(ack, context.self))

  override def fromOwners(sender: ActorRef): PartialFunction[Any, Unit] = {
    case OwnerPassivate => ownerIds.get(sender).foreach(key => stop(AggregateId(key), stale = false))
    case Terminated(ref) => terminated(ref)
  }

  private def create(id: AggregateId): ActorRef = {
    val owner = context.actorOf(ownerProps(id), java.net.URLEncoder.encode(typeName, "UTF-8") + "_" + id.asLong + "_" + spawnedCount) // F252
    spawnedCount += 1
    context.watch(owner)
    ownerIds += owner -> id.asLong
    owner
  }

  // An owner stopped: on request, after a failure, or after a failed restore. Messages kept for it go to a new owner.
  private def terminated(ref: ActorRef): Unit = ownerIds.remove(ref).foreach { key =>
    if (owners.get(key).contains(ref)) owners -= key
    keptForStopping.remove(key) match {
      case Some(kept) if kept.nonEmpty =>
        val owner = owners.getOrElseUpdate(key, create(AggregateId(key)))
        activity += key -> System.nanoTime() / 1_000_000
        kept.foreach(owner.tell(_, context.self))
      case _ => activity -= key
    }
  }

  private def sweep(now: Long): Unit = if (now >= nextSweep) {
    val sweep = Passivation.sweep(activity, now, keepAliveLimit, maxInactivityMillis)
    sweep.stop.foreach { key =>
      eventsLogger.filter(_.isDebugEnabled).foreach(_.debug("Stopping aggregate " + key + ", idle"))
      stop(AggregateId(key), stale = false)
    }
    nextSweep = now + sweep.nextInMillis
  }
}

/**
  * Which idle `Local` owners a sweep stops: at most every 5 minutes, beyond `keepAliveLimit` owners the least recently
  * used that were idle for 2 minutes, and any idle for longer than `maxInactivityMillis`. F218: not all at once (each
  * may write a snapshot on the blocking dispatcher every command waits on), the rest a second later. F279: at least
  * `StopsPerSweep` and a tenth of those due, so under sustained traffic the stops keep pace with the new owners and the
  * backlog stays about ten seconds' worth, where a fixed pace let the owners grow without bound.
  */
private[commandhandler] object Passivation {

  val SweepEveryMillis = 300000L
  val AgainAfterMillis = 1000L
  val MinimumInactivityMillis = 120000L // an owner is stopped for room only after 2 minutes without a message
  val StopsPerSweep = 200

  /** The owners to stop now, and when to look again. */
  case class Sweep(stop: Vector[Long], nextInMillis: Long)

  /** `activity`: each owner's last message, in milliseconds. */
  def sweep(activity: collection.Map[Long, Long], now: Long, keepAliveLimit: Int, maxInactivityMillis: Long): Sweep = {
    val sorted = activity.toVector.sortBy(_._2)
    val beyondLimit = sorted.take(math.max(sorted.length - keepAliveLimit, 0)).filter(now - _._2 > MinimumInactivityMillis)
    val inactive = sorted.filter(now - _._2 > maxInactivityMillis)
    val due = (beyondLimit ++ inactive).map(_._1).distinct
    val pace = math.max(StopsPerSweep, due.size / 10)
    Sweep(due.take(pace), if (due.size > pace) AgainAfterMillis else SweepEveryMillis)
  }
}

/** One owner per aggregate in the cluster: the entities of the type's shard region, whose shards place, keep and
  * passivate them (D43, D72). Only an owner that runs on this node gets acknowledgements from here. */
private[commandhandler] final class ShardedOwners(context: ActorContext, region: ActorRef) extends Owners {

  private val running = mutable.HashMap[Long, ActorRef]()
  private val runningIds = mutable.HashMap[ActorRef, Long]()

  override def send(id: AggregateId, message: Any): Unit = region.tell(ToOwner(id, message), context.self)

  // Wherever it runs, started for it if it does not: only the shard can keep what arrives while it stops.
  override def stop(id: AggregateId, stale: Boolean): Unit = region.tell(ToOwner(id, PassivateOwner(stale)), context.self)

  override def acknowledged(ack: PublishEventsAck): Unit = running.get(ack.aggregateId.asLong).foreach(_.tell(ack, context.self))

  override def fromOwners(sender: ActorRef): PartialFunction[Any, Unit] = {
    case OwnerStarted(id) =>
      running += id.asLong -> sender
      runningIds += sender -> id.asLong
      context.watch(sender)
    // The next owner of the aggregate may have started on this node before this one's end is known here.
    case Terminated(ref) => runningIds.remove(ref).foreach(key => if (running.get(key).contains(ref)) running -= key)
  }
}

private[commandhandler] object ShardedOwners {

  /** Starts the type's shard region on this node; its owners are made with `entityProps`, so with this node's settings:
    * they publish to this node's event bus with this node's command bus as the publisher. A rebalance and idle
    * passivation stop an owner with `StopOwner`, after its commands at work (D72); new shards go to the nodes of the
    * newest app-version (D66). */
  def start(context: ActorContext, typeName: String, entityProps: Props, numberOfShards: Int): ShardedOwners = {
    val system = context.system
    val settings = ClusterShardingSettings(system)
    def shardOf(id: Long): String = math.abs(id % numberOfShards).toString
    val region = ClusterSharding(system).start(
      typeName = "owner-" + typeName,
      entityProps = entityProps,
      settings = settings,
      extractEntityId = { case ToOwner(id, message) => (id.asLong.toString, message) },
      extractShardId = {
        case ToOwner(id, _) => shardOf(id.asLong)
        case ShardRegion.StartEntity(entityId) => shardOf(entityId.toLong)
        case other => throw new IllegalArgumentException("Not a message for an owner: " + other.getClass.getName) // the region passes only these
      },
      allocationStrategy = ClusterSharding(system).defaultShardAllocationStrategy(settings),
      handOffStopMessage = StopOwner)
    new ShardedOwners(context, region)
  }
}

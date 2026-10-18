package io.reactivecqrs.core.cluster

import io.reactivecqrs.api.AggregateContext
import org.apache.pekko.actor.{Actor, ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props}
import org.apache.pekko.cluster.{Cluster, ClusterEvent, UniqueAddress}
import org.apache.pekko.util.Version

/**
  * What this node knows of the cluster's members, from cluster events (M5).
  *
  * The lowest app-version (`pekko.cluster.app-version`) among them (D67), kept in the aggregate contexts of this node's
  * command buses, whose owners persist an event class of `writtenFrom` only once every member runs its version. None when
  * the system is not a cluster, or when no member set an app-version (all at Pekko's 0.0.0): then every class is written.
  * Pekko's 0.0.0 while the node knows no member yet, before it joins, so it writes nothing new before it knows who else
  * runs (F138). A member that is leaving still counts: the singletons run on the oldest node, which leaves last.
  *
  * Whether the node is `alone`: not a cluster, or the cluster's only member. While it knows no member it is not: the
  * owners of `Local` aggregates then read the store before they answer (F131).
  */
class Members(system: ExtendedActorSystem) extends Extension {

  private val inCluster = Singletons.isCluster(system)
  private var contexts = Vector[AggregateContext[_]]()
  @volatile private var oldest: Option[Version] = if (inCluster) Some(Version.Zero) else None
  @volatile private var count: Int = if (inCluster) 0 else 1

  if (inCluster) system.systemActorOf(Props(new Members.Listener(this)), "reactivecqrs-members")

  /** The lowest app-version among the members; see the class's description. */
  def oldestAppVersion: Option[Version] = oldest

  /** Whether a member runs an older app-version than this node (a rolling upgrade is under way, D65), or the node knows
    * no member yet; false outside a cluster and when no member set an app-version. */
  def olderVersionsRun: Boolean = oldest.exists(_.compareTo(Cluster(system).selfMember.appVersion) < 0)

  /** Whether this node is the cluster's only member (always, outside a cluster). */
  def alone: Boolean = count == 1

  /** Keeps `context`'s view of the lowest app-version current (a command bus registers its context). */
  def track(context: AggregateContext[_]): Unit = synchronized {
    contexts :+= context
    context.oldestAppVersion = oldest
  }

  private[cluster] def membersChanged(versions: Seq[Version]): Unit = synchronized {
    count = versions.size
    oldest =
      if (versions.isEmpty) Some(Version.Zero)
      else if (versions.forall(_ == Version.Zero)) None
      else Some(versions.reduce((a, b) => if (a.compareTo(b) <= 0) a else b))
    contexts.foreach(_.oldestAppVersion = oldest)
  }
}

/** `Members(system)`: the node's view of the members. */
object Members extends ExtensionId[Members] with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): Members = new Members(system)
  override def lookup: Members.type = Members
  override def get(system: ActorSystem): Members = super.get(system)

  // The members as the events give them: `Cluster.state` may not have taken an event in yet when it arrives here.
  private class Listener(members: Members) extends Actor {
    private val cluster = Cluster(context.system)
    private var known = Map[UniqueAddress, Version]()
    override def preStart(): Unit = cluster.subscribe(self, ClusterEvent.InitialStateAsEvents, classOf[ClusterEvent.MemberEvent])
    override def postStop(): Unit = cluster.unsubscribe(self)
    override def receive: Receive = {
      case ClusterEvent.MemberRemoved(member, _) =>
        known -= member.uniqueAddress
        members.membersChanged(known.values.toSeq)
      case event: ClusterEvent.MemberEvent =>
        known += event.member.uniqueAddress -> event.member.appVersion
        members.membersChanged(known.values.toSeq)
    }
  }
}

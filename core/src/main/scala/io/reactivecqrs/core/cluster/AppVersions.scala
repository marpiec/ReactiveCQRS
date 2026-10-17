package io.reactivecqrs.core.cluster

import io.reactivecqrs.api.AggregateContext
import org.apache.pekko.actor.{Actor, ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props}
import org.apache.pekko.cluster.{Cluster, ClusterEvent, UniqueAddress}
import org.apache.pekko.util.Version

/**
  * D67: the lowest app-version (`pekko.cluster.app-version`) among the cluster's members, kept in the aggregate contexts of
  * this node's command buses, whose owners persist an event class of `writtenFrom` only once every member runs its
  * version. None when the system is not a cluster, or when no member set an app-version (all at Pekko's 0.0.0): then
  * every class is written. A member that is leaving still counts: the singletons run on the oldest node, which leaves last.
  */
class AppVersions(system: ExtendedActorSystem) extends Extension {

  private var contexts = Vector[AggregateContext[_]]()
  @volatile private var oldest: Option[Version] = None

  if (Singletons.isCluster(system)) system.systemActorOf(Props(new AppVersions.Listener(this)), "reactivecqrs-app-versions")

  def oldestAppVersion: Option[Version] = oldest

  def track(context: AggregateContext[_]): Unit = synchronized {
    contexts :+= context
    context.oldestAppVersion = oldest
  }

  private[cluster] def membersChanged(versions: Seq[Version]): Unit = synchronized {
    oldest = if (versions.forall(_ == Version.Zero)) None else Some(versions.reduce((a, b) => if (a.compareTo(b) <= 0) a else b))
    contexts.foreach(_.oldestAppVersion = oldest)
  }
}

object AppVersions extends ExtensionId[AppVersions] with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): AppVersions = new AppVersions(system)
  override def lookup: AppVersions.type = AppVersions
  override def get(system: ActorSystem): AppVersions = super.get(system)

  // The members as the events give them: `Cluster.state` may not have taken an event in yet when it arrives here.
  private class Listener(versions: AppVersions) extends Actor {
    private val cluster = Cluster(context.system)
    private var members = Map[UniqueAddress, Version]()
    override def preStart(): Unit = cluster.subscribe(self, ClusterEvent.InitialStateAsEvents, classOf[ClusterEvent.MemberEvent])
    override def postStop(): Unit = cluster.unsubscribe(self)
    override def receive: Receive = {
      case ClusterEvent.MemberRemoved(member, _) =>
        members -= member.uniqueAddress
        versions.membersChanged(members.values.toSeq)
      case event: ClusterEvent.MemberEvent =>
        members += event.member.uniqueAddress -> event.member.appVersion
        versions.membersChanged(members.values.toSeq)
    }
  }
}

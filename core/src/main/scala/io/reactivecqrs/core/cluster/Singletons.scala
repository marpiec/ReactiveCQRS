package io.reactivecqrs.core.cluster

import org.apache.pekko.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import org.apache.pekko.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}

/**
  * Actors that must run once in the cluster (D46, D53): the managed projections' supervisors, the outbox poller, the
  * squash scheduler, each saga runner, and any subscriber whose effects must happen once. Every node starts each under
  * the same name; it runs on the oldest node and moves once when that node leaves (D66), stopped there with
  * `terminationMessage` before it starts on the next. The `ActorRef` returned is a proxy on this node: it forwards to
  * wherever the singleton runs, with the sender kept, and holds messages while it moves (up to its buffer). An actor
  * system that is not a cluster runs the actor itself under `name`.
  */
object Singletons {

  def start(system: ActorSystem, name: String, props: Props, terminationMessage: Any = PoisonPill): ActorRef =
    if (isCluster(system)) {
      system.actorOf(ClusterSingletonManager.props(props, terminationMessage, ClusterSingletonManagerSettings(system)), name)
      system.actorOf(ClusterSingletonProxy.props("/user/" + name, ClusterSingletonProxySettings(system)), name + "-proxy")
    } else system.actorOf(props, name)

  def isCluster(system: ActorSystem): Boolean = system.settings.ProviderClass == "org.apache.pekko.cluster.ClusterActorRefProvider"
}

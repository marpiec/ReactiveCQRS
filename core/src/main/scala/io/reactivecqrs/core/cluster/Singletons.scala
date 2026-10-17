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

  /** Stops what `start` started on this node: in a cluster its manager, with the singleton when it runs here, and its
    * proxy; otherwise the actor. Stopping the proxy alone would leave the singleton running (found by the suites run
    * on a cluster of one). Another node's manager takes the singleton over only when this node leaves the cluster. */
  def stop(system: ActorSystem, started: ActorRef): Unit = {
    if (isCluster(system) && started.path.name.endsWith("-proxy")) {
      system.actorSelection("/user/" + started.path.name.stripSuffix("-proxy")) ! PoisonPill
    }
    system.stop(started)
  }

  def isCluster(system: ActorSystem): Boolean = system.settings.ProviderClass == "org.apache.pekko.cluster.ClusterActorRefProvider"
}

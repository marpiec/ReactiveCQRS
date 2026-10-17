package io.reactivecqrs.core.cluster

import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}

import scala.concurrent.Future

/**
  * D71: what this node waits for before it may take traffic: each event bus until every subscriber registered on it
  * answered (until then it publishes nothing, D15), which, through the singletons' proxies, also means the projections
  * that run once are up. The schema is initialized before the actors start, and a failure there stops the start.
  */
class Readiness extends Extension {
  private var waiting = Set[String]()

  def waitFor(what: String): Unit = synchronized(waiting += what)
  def done(what: String): Unit = synchronized(waiting -= what)
  def ready: Boolean = synchronized(waiting.isEmpty)
  def waitingFor: Set[String] = synchronized(waiting)
}

object Readiness extends ExtensionId[Readiness] with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): Readiness = new Readiness
  override def lookup: Readiness.type = Readiness
  override def get(system: ActorSystem): Readiness = super.get(system)
}

/** `Readiness` as a Pekko Management readiness check:
  * `pekko.management.health-checks.readiness-checks.reactivecqrs = "io.reactivecqrs.core.cluster.ReadinessCheck"`. */
class ReadinessCheck(system: ActorSystem) extends (() => Future[Boolean]) {
  override def apply(): Future[Boolean] = Future.successful(Readiness(system).ready)
}

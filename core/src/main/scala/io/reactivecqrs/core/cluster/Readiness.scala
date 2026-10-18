package io.reactivecqrs.core.cluster

import io.reactivecqrs.api.AggregateVersion
import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import org.apache.pekko.serialization.SerializationExtension
import org.slf4j.LoggerFactory

import scala.concurrent.Future

/**
  * D71: what this node waits for before it may take traffic: each event bus until every subscriber registered on it
  * answered (until then it publishes nothing, D15), which, through the singletons' proxies, also means the projections
  * that run once are up. The schema is initialized before the actors start, and a failure there stops the start.
  * F178: a node that can talk to others but whose case classes would not go to the library's serializer is never ready.
  */
class Readiness extends Extension {
  private var waiting = Set[String]()

  def waitFor(what: String): Unit = synchronized(waiting += what)
  def done(what: String): Unit = synchronized(waiting -= what)
  def ready: Boolean = synchronized(waiting.isEmpty)
  def waitingFor: Set[String] = synchronized(waiting)
}

object Readiness extends ExtensionId[Readiness] with ExtensionIdProvider {
  private val log = LoggerFactory.getLogger(classOf[Readiness])

  override def createExtension(system: ExtendedActorSystem): Readiness = {
    val readiness = new Readiness
    serializerProblem(system).foreach { problem =>
      log.error(problem)
      readiness.waitFor("the serializer of messages between nodes (F178)")
    }
    readiness
  }
  override def lookup: Readiness.type = Readiness
  override def get(system: ActorSystem): Readiness = super.get(system)

  // F178: the library binds java.io.Serializable to its serializer in its reference.conf, and so does pekko-actor, to
  // Java serialization: the file earlier on the classpath wins, and a classpath's order may be any. A case class of the
  // library tells which won.
  private[cluster] def serializerProblem(system: ExtendedActorSystem): Option[String] =
    if (!system.provider.getDefaultAddress.hasGlobalScope) None // no messages to other nodes
    else {
      val serializer = SerializationExtension(system).serializerFor(classOf[AggregateVersion])
      if (serializer.isInstanceOf[ClusterSerializer]) None
      else Some("Messages between nodes would go through " + serializer.getClass.getName + ", not the library's " +
        "serializer: another reference.conf came first on the classpath. Put in application.conf: " +
        "pekko.actor.serialization-bindings { \"java.io.Serializable\" = reactivecqrs }. The node stays not ready.")
    }
}

/** `Readiness` as a Pekko Management readiness check:
  * `pekko.management.health-checks.readiness-checks.reactivecqrs = "io.reactivecqrs.core.cluster.ReadinessCheck"`. */
class ReadinessCheck(system: ActorSystem) extends (() => Future[Boolean]) {
  override def apply(): Future[Boolean] = Future.successful(Readiness(system).ready)
}

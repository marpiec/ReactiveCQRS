package io.reactivecqrs.core.cluster

import org.apache.pekko.actor.{ActorRef, ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import org.apache.pekko.serialization.SerializationExtension

/**
  * F121: how large a message between the nodes of a cluster may be. Artery drops a message larger than its frame
  * (`pekko.remote.artery.advanced.maximum-frame-size`, 256 KiB by default) and logs it, and its sender is not told.
  * `limit` is that frame, or the large frame when the host routes large messages to destinations of their own
  * (`large-message-destinations`), less 8 KiB for Artery's header. `of` measures a value as the serializer that Pekko
  * uses for it writes it.
  */
class MessageSize(system: ExtendedActorSystem) extends Extension {

  val limit: Int = {
    val artery = system.settings.config.getConfig("pekko.remote.artery")
    val frame = artery.getBytes("advanced.maximum-frame-size").longValue
    val largeFrame = if (artery.getStringList("large-message-destinations").isEmpty) 0L else artery.getBytes("advanced.maximum-large-frame-size").longValue
    (math.max(frame, largeFrame) - 8 * 1024).toInt
  }

  /** What the events of one delivery are packed into: half the limit, the rest left for a state and the envelope. */
  val partBudget: Int = limit / 2

  /** The serialized size of `value`; throws when it cannot be serialized. */
  def of(value: AnyRef): Int = SerializationExtension(system).findSerializerFor(value) match {
    case cluster: ClusterSerializer => cluster.sizeOf(value)
    case other => other.toBinary(value).length
  }

  /** An actor on another node: what it is sent crosses the network. */
  def onAnotherNode(ref: ActorRef): Boolean = ref.path.address.hasGlobalScope
}

object MessageSize extends ExtensionId[MessageSize] with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): MessageSize = new MessageSize(system)
  override def lookup: MessageSize.type = MessageSize
  override def get(system: ActorSystem): MessageSize = super.get(system)
}

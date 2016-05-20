package io.reactivecqrs.core.eventbus

import akka.actor.ActorRef
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.eventbus.EventsBusActor.{MessageAck, MessageToSend}


class MemoryEventBusState extends EventBusState {

  private var events: Map[(AggregateId, Int, String), AnyRef] = Map()

  def persistMessages[MESSAGE <: AnyRef](messages: Seq[MessageToSend]): Unit = {

    messages.foreach { message =>
      events += (message.aggregateId, message.version.asInt, message.subscriber.path.toString) -> message.message
    }

  }

  def deleteSentMessage(messages: Seq[MessageAck]): Unit = {
    messages.foreach(m => {
      events -= ((m.aggregateId, m.version.asInt, m.subscriber.path.toString))
    })

  }

  override def countMessages: Int = events.size
}

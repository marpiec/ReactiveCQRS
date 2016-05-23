package io.reactivecqrs.core.eventbus

import akka.actor.ActorRef
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.eventbus.EventsBusActor.{EventAck, EventToRoute}


class MemoryEventBusState extends EventBusState {

  private var events: Map[(AggregateId, Int, String), EventToRoute] = Map()

  override def persistMessages[MESSAGE <: AnyRef](messages: Seq[EventToRoute]): Unit = {

    messages.foreach { message =>
      val subscriberPath = message.subscriber match {
        case Left(s) => s.path.toString
        case Right(s) => s
      }
      events += (message.aggregateId, message.version.asInt, subscriberPath.toString) -> message
    }

  }

  override def deleteSentMessage(messages: Seq[EventAck]): Unit = {
    messages.foreach(m => {
      events -= ((m.aggregateId, m.version.asInt, m.subscriber.path.toString))
    })

  }

  override def countMessages: Int = events.size

  override def readAllMessages(handler: EventToRoute => Unit) {
    events.values.foreach(handler)
  }
}

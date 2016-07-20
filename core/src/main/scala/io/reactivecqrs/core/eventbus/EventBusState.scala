package io.reactivecqrs.core.eventbus

import io.reactivecqrs.core.eventbus.EventsBusActor.{EventAck, EventToRoute}

abstract class EventBusState {

  def countMessages: Int
  def persistMessages[MESSAGE <: AnyRef](messages: Seq[EventToRoute]): Unit
  def deleteSentMessage(messages: Seq[EventAck]): Unit
  def readAllMessages(handler: (EventToRoute) => Unit)

}

package io.reactivecqrs.core.eventbus

import io.reactivecqrs.core.eventbus.EventsBusActor.{MessageAck, MessageToSend}

abstract class EventBusState {

  def countMessages: Int
  def persistMessages[MESSAGE <: AnyRef](messages: Seq[MessageToSend]): Unit
  def deleteSentMessage(messages: Seq[MessageAck]): Unit

}

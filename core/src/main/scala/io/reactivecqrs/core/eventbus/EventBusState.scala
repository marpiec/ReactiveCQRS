package io.reactivecqrs.core.eventbus

import akka.actor.ActorRef
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.eventbus.EventsBusActor.MessageToSend

abstract class EventBusState {

  def persistMessages[MESSAGE <: AnyRef](messages: Seq[MessageToSend]): Unit
  def deleteSentMessage(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion): Unit

}

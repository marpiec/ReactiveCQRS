package io.reactivecqrs.core.saga

import akka.actor.ActorRef
import io.reactivecqrs.api.id.UserId


abstract class SagaPhase(val name: String)
case object CONTINUES extends SagaPhase("continues")
case object REVERTING extends SagaPhase("reverting")
case object ERROR extends SagaPhase("error")


trait SagaInternalOrder {
  val userId: UserId
}

trait SagaOrder extends SagaInternalOrder

sealed trait SagaInternalRevert

trait SagaResponse
case class SagaFailureResponse(exceptions: List[String]) extends SagaResponse


trait SagaHandlingStatus

case class SagaPersisted(sagaId: Int, respondTo: ActorRef, order: SagaInternalOrder, phase: SagaPhase)


case class SagaContinues(order: SagaInternalOrder) extends SagaHandlingStatus

case class SagaReverts(order: SagaInternalOrder) extends SagaHandlingStatus

case class SagaSucceded(response: SagaResponse) extends SagaHandlingStatus

case class SagaFailed(response: SagaResponse) extends SagaHandlingStatus

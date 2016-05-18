package io.reactivecqrs.core.saga

import io.reactivecqrs.api.id.UserId


abstract class SagaPhase(val name: String)
case object CONTINUES extends SagaPhase("continues")
case object REVERTING extends SagaPhase("reverting")
case object ERROR extends SagaPhase("error")


trait SagaInternalOrder {
  val userId: UserId
}

trait SagaOrder extends SagaInternalOrder

trait SagaResponse
case class SagaFailureResponse(exceptions: List[String]) extends SagaResponse



sealed trait SagaHandlingStatus

case class SagaContinues(order: SagaInternalOrder) extends SagaHandlingStatus

case class SagaSucceded(response: SagaResponse) extends SagaHandlingStatus

case class SagaFailed(response: SagaResponse) extends SagaHandlingStatus





sealed trait SagaRevertHandlingStatus

case class SagaRevertContinues(order: SagaInternalOrder) extends SagaRevertHandlingStatus

case object SagaRevertSucceded extends SagaRevertHandlingStatus

case class SagaRevertFailed(exceptions: List[String]) extends SagaRevertHandlingStatus


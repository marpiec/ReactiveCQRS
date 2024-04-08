package io.reactivecqrs.testdomain.shoppingcart

import akka.pattern.ask
import akka.actor.ActorRef
import io.reactivecqrs.api.{CustomCommandResponse, FailureResponse, SagaStep, SuccessResponse}
import io.reactivecqrs.api.id.{AggregateIdWithVersion, UserId}
import io.reactivecqrs.core.saga._
import io.reactivecqrs.testdomain.shoppingcart.MultipleCartCreatorSaga.{CartsCreated, CartsCreationFailure, CreateMultipleCarts, CreateRemainingCarts}

import scala.concurrent.Future


object MultipleCartCreatorSaga {

  case class CreateMultipleCarts(userId: UserId, cartName: String, cartsCount: Int) extends SagaOrder
  private case class CreateRemainingCarts(userId: UserId, cartName: String, cartsCount: Int, createdCarts: List[AggregateIdWithVersion]) extends SagaInternalOrder


  trait MultipleCartCreatorSagaResponse extends SagaResponse
  case class CartsCreated(carts: List[AggregateIdWithVersion]) extends MultipleCartCreatorSagaResponse
  case class CartsCreationFailure(exceptions: List[String]) extends MultipleCartCreatorSagaResponse

}

class MultipleCartCreatorSaga(val state: SagaState, val uidGenerator: ActorRef, shoppingCartCommandBus: ActorRef) extends SagaActor {

  import context.dispatcher

  override val name = "MultipleCartCreatorSaga"

  override def handleOrder(sagaStep: SagaStep): ReceiveOrder = {
    case order: CreateMultipleCarts => handleCreateMultipleCarts(sagaStep, order)
    case order: CreateRemainingCarts => handleCreateRemainingCarts(sagaStep, order)
  }

  override def handleRevert(sagaStep: SagaStep): ReceiveRevert = {
    case revert: CreateRemainingCarts => revertCreateRemainingCarts(sagaStep, revert.userId, revert.cartName, revert.cartsCount, revert.createdCarts)
  }

  private def handleCreateMultipleCarts(sagaStep: SagaStep, order: CreateMultipleCarts): Future[SagaHandlingStatus] = {
    if(order.cartsCount > 0) {
      createShoppingCart(sagaStep, order.userId, order.cartName, order.cartsCount, List.empty)
    } else {
      Future.successful(SagaSucceded(CartsCreationFailure(List("No carts to create"))))
    }
  }

  private def handleCreateRemainingCarts(sagaStep: SagaStep, order: CreateRemainingCarts): Future[SagaHandlingStatus] = {
    createShoppingCart(sagaStep, order.userId, order.cartName, order.cartsCount, order.createdCarts)
  }

  private def createShoppingCart(sagaStep: SagaStep, userId: UserId, cartName: String, cartsCount: Int, createdCarts: List[AggregateIdWithVersion]) = {

    val currentCartId = createdCarts.size + 1
    (shoppingCartCommandBus ? CreateShoppingCart(Some(sagaStep), userId, cartName + " "+currentCartId))
      .mapTo[CustomCommandResponse[_]]
      .map {
        case c: SuccessResponse if createdCarts.length + 1 < cartsCount =>
          SagaContinues(CreateRemainingCarts(userId, cartName, cartsCount, AggregateIdWithVersion(c.aggregateId, c.aggregateVersion) :: createdCarts))
        case c: SuccessResponse =>
          SagaSucceded(CartsCreated(AggregateIdWithVersion(c.aggregateId, c.aggregateVersion) :: createdCarts))
        case c: FailureResponse =>
          SagaFailed(CartsCreationFailure(c.exceptions))
      }
  }


  private def revertCreateRemainingCarts(sagaStep: SagaStep, userId: UserId, cartName: String, cartsCount: Int, createdCarts: List[AggregateIdWithVersion]): Future[SagaRevertHandlingStatus] = {
    (shoppingCartCommandBus ? DeleteShoppingCart(Some(sagaStep), userId, createdCarts.head.id, createdCarts.head.version))
      .mapTo[CustomCommandResponse[_]]
      .map {
        case c: SuccessResponse if createdCarts.length > 1 =>
          SagaRevertContinues(CreateRemainingCarts(userId, cartName, cartsCount, createdCarts.tail))
        case c: SuccessResponse =>
          SagaRevertSucceded
        case c: FailureResponse =>
          SagaRevertFailed(c.exceptions)
      }
  }


}

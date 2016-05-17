package io.reactivecqrs.testdomain.shoppingcart

import akka.pattern.ask
import akka.actor.ActorRef
import akka.util.Timeout
import io.reactivecqrs.api.{CustomCommandResponse, FailureResponse, SuccessResponse}
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.saga._
import io.reactivecqrs.testdomain.shoppingcart.MultipleCartCreatorSaga.{CartsCreated, CartsCreationFailure, CreateMultipleCarts, CreateRemainingCarts}

import scala.concurrent.Future
import scala.concurrent.duration._


object MultipleCartCreatorSaga {

  case class CreateMultipleCarts(userId: UserId, cartName: String, cartsCount: Int) extends SagaOrder
  private case class CreateRemainingCarts(userId: UserId, cartName: String, cartsCount: Int, createdCarts: List[AggregateId]) extends SagaInternalOrder


  trait MultipleCartCreatorSagaResponse extends SagaResponse
  case class CartsCreated(carts: List[AggregateId]) extends MultipleCartCreatorSagaResponse
  case class CartsCreationFailure(exceptions: List[String]) extends MultipleCartCreatorSagaResponse

}

class MultipleCartCreatorSaga(val state: SagaState, shoppingCartCommandBus: ActorRef) extends SagaActor {

  import context.dispatcher

  implicit val timeout = Timeout(10 seconds)

  override val name = "MultipleCartCreatorSaga"

  override def handleOrder:ReceiveOrder = {
    case order: CreateMultipleCarts =>
      handleCreateMultipleCarts(order)
    case order: CreateRemainingCarts =>
      handleCreateRemainingCarts(order)
  }

  override def handleRevert: ReceiveRevert = {
    case revert => ???
  }

  def handleCreateMultipleCarts(order: CreateMultipleCarts): Future[SagaHandlingStatus] = {
    if(order.cartsCount > 0) {
      createShoppingCart(order.userId, order.cartName, order.cartsCount, List.empty)
    } else {
      Future.successful(SagaSucceded(CartsCreationFailure(List("No carts to create"))))
    }
  }

  def handleCreateRemainingCarts(order: CreateRemainingCarts): Future[SagaHandlingStatus] = {
    createShoppingCart(order.userId, order.cartName, order.cartsCount, order.createdCarts)
  }

  private def createShoppingCart(userId: UserId, cartName: String, cartsCount: Int, createdCarts: List[AggregateId]) = {

    val currentCartId = createdCarts.size + 1
    (shoppingCartCommandBus ? CreateShoppingCart(userId, cartName + " "+currentCartId))
      .mapTo[CustomCommandResponse[_]]
      .map {
        case c: SuccessResponse if createdCarts.length + 1 < cartsCount =>
          SagaContinues(CreateRemainingCarts(userId, cartName, cartsCount, c.aggregateId :: createdCarts))
        case c: SuccessResponse =>
          SagaSucceded(CartsCreated(c.aggregateId :: createdCarts))
        case c: FailureResponse => SagaFailed(CartsCreationFailure(c.exceptions))
      }
  }

}

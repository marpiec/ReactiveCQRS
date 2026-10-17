package io.reactivecqrs.examples.e4

// #saga
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.saga._
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

object CheckoutSaga {
  case class Checkout(userId: UserId, stockId: AggregateId, paymentId: AggregateId, items: Int, amountCents: Long)
    extends SagaOrder
  private case class Charge(userId: UserId, stockId: AggregateId, paymentId: AggregateId, items: Int, amountCents: Long)
    extends SagaInternalOrder

  case class CheckedOut(paymentId: AggregateId) extends SagaResponse
  case class CheckoutRefused(reasons: List[String]) extends SagaResponse
}

class CheckoutSaga(stock: ActorRef, payments: ActorRef)(implicit executionContext: ExecutionContext, timeout: Timeout)
  extends SagaDefinition {
  import CheckoutSaga._

  override val name = "CheckoutSaga"

  override def handleOrder(sagaStep: SagaStep): ReceiveOrder = {
    case o: Checkout => // step 1: reserve the items
      send(stock, ReserveItems(Some(sagaStep), o.userId, o.stockId, o.items)).map {
        case Right(_) => SagaContinues(Charge(o.userId, o.stockId, o.paymentId, o.items, o.amountCents))
        case Left(reasons) => SagaFailed(CheckoutRefused(reasons))
      }
    case o: Charge => // step 2: charge the customer
      send(payments, ChargePayment(Some(sagaStep), o.userId, o.paymentId, o.amountCents)).map {
        case Right(_) => SagaSucceded(CheckedOut(o.paymentId))
        case Left(reasons) => SagaFailed(CheckoutRefused(reasons))
      }
  }

  override def handleRevert(sagaStep: SagaStep): ReceiveRevert = {
    case _: Checkout => Future.successful(SagaRevertSucceded) // step 1 failed: nothing was reserved
    case o: Charge => // step 2 failed: give the reserved items back
      send(stock, ReleaseItems(Some(sagaStep), o.userId, o.stockId, o.items)).map {
        case Right(_) => SagaRevertSucceded
        case Left(reasons) => SagaRevertFailed(reasons)
      }
  }

  // Any answer but a success counts as a refusal, so no answer falls through the match.
  private def send(bus: ActorRef, command: AnyRef): Future[Either[List[String], AggregateId]] =
    (bus ? command).mapTo[CustomCommandResponse[_]].map {
      case SuccessResponse(id, _) => Right(id)
      case FailureResponse(reasons) => Left(reasons)
      case other => Left(List(other.toString))
    }
}
// #saga

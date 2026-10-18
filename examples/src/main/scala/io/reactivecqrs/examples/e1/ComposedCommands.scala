package io.reactivecqrs.examples.e1

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.UserId
import org.apache.pekko.actor.{ActorRef, ActorSystem}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.Future
import scala.util.Try

// E1, the end of chapter 4: the commands of `FirstProgram` composed as futures, the way real code sends them.
object ComposedCommands {

  def composed(system: ActorSystem, carts: ActorRef, alice: UserId)(implicit timeout: Timeout): Future[Aggregate[ShoppingCart]] = {
    // #compose
    import system.dispatcher   // an ExecutionContext for flatMap

    def sendAsync(command: AnyRef): Future[SuccessResponse] =
      (carts ? command).mapTo[CustomCommandResponse[_]].flatMap {
        case success: SuccessResponse => Future.successful(success)
        case other => Future.failed(new IllegalStateException(command.getClass.getSimpleName + " failed: " + other))
      }

    val groceries: Future[Aggregate[ShoppingCart]] = for {
      created <- sendAsync(CreateShoppingCart(alice, "Groceries"))
      apples  <- sendAsync(AddItem(alice, created.aggregateId, created.aggregateVersion, "apples"))
      _       <- sendAsync(AddItem(alice, created.aggregateId, apples.aggregateVersion, "oranges"))
      cart    <- (carts ? GetAggregate(created.aggregateId)).mapTo[Try[Aggregate[ShoppingCart]]].flatMap(Future.fromTry)
    } yield cart
    // #compose
    groceries
  }
}

package io.reactivecqrs.testdomain.spec.utils

import akka.actor.ActorRef
import akka.util.Timeout
import org.scalatest.Assertions
import akka.pattern.ask

import scala.concurrent.Await
import scala.concurrent.duration._


trait ActorAskSupport {
  implicit def toActorRefForTest(actor: ActorRef): ActorAsk = new ActorAsk(actor)
}

class ActorAsk(actor: ActorRef) extends Assertions {

  implicit val timeout = Timeout(1.seconds)

  def ??[R] (message: AnyRef): R = askActor(message)

  private def askActor[R](message: AnyRef): R = {
    val future = actor ? message
    Await.result(future, 1.seconds).asInstanceOf[R]
  }
}

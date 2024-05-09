package io.reactivecqrs.testutils

import org.apache.pekko.pattern.ask
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.util.Timeout
import org.scalatest.Assertions

import scala.concurrent.Await
import scala.concurrent.duration._


trait ActorAskSupport {
  implicit def toActorRefForTest(actor: ActorRef): ActorAsk = new ActorAsk(actor)
}

class ActorAsk(actor: ActorRef) extends Assertions {

  val timeout = Timeout(10.seconds)

  def ??[R] (message: AnyRef): R = askActor(message)(timeout)

  def askActor[R](message: AnyRef)(implicit t: Timeout): R = {
    val future = actor ? message
    Await.result(future, t.duration).asInstanceOf[R]
  }
}

package io.reactivecqrs.testkit

import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag

/** `actor.askFor[T](message)`: asks the actor and waits for its answer, as a `T`. */
trait ActorAskSupport {
  /** Gives every `ActorRef` `askFor`. */
  implicit def toActorAsk(actor: ActorRef): ActorAsk = new ActorAsk(actor)
}

/** An actor to ask, with `askFor`. */
class ActorAsk(actor: ActorRef) {

  /** Asks and waits for the answer, up to `timeout`, as the type named. An answer of another class fails naming both, and
    * an answer `Status.Failure(e)` fails with `e`. The type is named rather than taken from the expected type: Scala 2
    * types such a call with `Nothing` first, which `-Wdead-code` reports at every `val` (F177). */
  def askFor[R](message: Any, timeout: FiniteDuration = 10.seconds)(implicit expected: ClassTag[R]): R = {
    val answer: Any = Await.result(actor.ask(message)(Timeout(timeout)), timeout)
    expected.unapply(answer).getOrElse(throw new ClassCastException(
      if (expected == ClassTag.Nothing) s"Name the type of the answer to $message: askFor[T]($message)"
      else s"${actor.path.name} answered $message with $answer, a ${Option(answer).fold("null")(_.getClass.getName)}, " +
        s"not a ${expected.runtimeClass.getName}"))
  }
}

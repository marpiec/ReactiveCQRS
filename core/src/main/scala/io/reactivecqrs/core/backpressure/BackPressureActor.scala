package io.reactivecqrs.core.backpressure

import org.apache.pekko.actor.{Actor, ActorRef}
import io.reactivecqrs.core.util.MyActorLogging

object BackPressureActor {
  case object Start
  case object Stop

  case object ConsumerStart
  case class ConsumerAllowedMore(count: Int)
  case object ConsumerPostponed
  case object ConsumerStop

  case object ProducerAllowMore
  case class ProducerAllowedMore(count: Int)

  case object ConsumerFinished
  case object Finished
}

class BackPressureActor(consumer: ActorRef) extends Actor with MyActorLogging {

  import BackPressureActor._

  var allowed: Int = 0
  var postponed: Boolean = false
  var producer: Option[ActorRef] = None
  var stopSender: Option[ActorRef] = None

  override def receive: Receive = {
    case Start =>
      allowed = 0
      consumer ! ConsumerStart
    case Stop =>
      consumer ! ConsumerStop
      stopSender = Some(sender())
    case ConsumerAllowedMore(count) =>
      producer match {
        case Some(p) =>
          postponed = false
          p ! ProducerAllowedMore(count + allowed)
          producer = None
          allowed = 0
        case None =>
          allowed += count
      }
    case ConsumerPostponed =>
      producer match {
        case Some(p) =>
          postponed = false
          p ! ProducerAllowedMore(allowed)
          producer = None
          allowed = 0
        case None => postponed = true
      }
    case ProducerAllowMore =>
      if(allowed > 0 || postponed) {
        postponed = false
        sender() ! ProducerAllowedMore(allowed)
        allowed = 0
      } else {
        producer = Some(sender())
      }
    case ConsumerFinished =>
      stopSender.foreach(p => p ! Finished)
  }
}
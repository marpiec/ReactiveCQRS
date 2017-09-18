package io.reactivecqrs.core.backpressure

import akka.actor.{Actor, ActorRef}
import io.reactivecqrs.core.util.ActorLogging

object BackPressureActor {
  case object Start
  case object Stop

  case object ConsumerStart
  case class ConsumerAllowedMore(count: Int)
  case object ConsumerStop

  case object ProducerAllowMore
  case class ProducerAllowedMore(count: Int)

  case object ConsumerFinished
  case object Finished
}

class BackPressureActor(consumer: ActorRef) extends Actor with ActorLogging {

  import BackPressureActor._

  var allowed: Int = 0
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
          p ! ProducerAllowedMore(count + allowed)
          producer = None
          allowed = 0
        case None =>
          allowed += count
      }
    case ProducerAllowMore =>
      if(allowed > 0) {
        sender ! ProducerAllowedMore(allowed)
        allowed = 0
      } else {
        producer = Some(sender())
      }
    case ConsumerFinished =>
      stopSender.foreach(p => p ! Finished)
  }
}
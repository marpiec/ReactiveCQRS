package io.reactivecqrs.core.backpressure

import akka.actor.{Actor, ActorRef}

object BackPressureActor {
  case object Start
  case object Stop

  case object ConsumerAllowMoreStart
  case class ConsumerAllowedMore(count: Int)
  case object ConsumerAllowMoreStop

  case object ProducerAllowMore
  case class ProducerAllowedMore(count: Int)
}

class BackPressureActor(consumer: ActorRef) extends Actor {

  import BackPressureActor._

  var allowed: Int = 0
  var waitingProducer: Option[ActorRef] = None

  override def receive: Receive = {
    case Start =>
      allowed = 0
      consumer ! ConsumerAllowMoreStart
    case Stop =>
      consumer ! ConsumerAllowMoreStop
    case ConsumerAllowedMore(count) =>
      waitingProducer match {
        case Some(producer) =>
          producer ! ProducerAllowedMore(count + allowed)
          waitingProducer = None
          allowed = 0
        case None =>
          allowed += count
      }
    case ProducerAllowMore =>
      if(allowed > 0) {
        sender ! ProducerAllowedMore(allowed)
        allowed = 0
      } else {
        waitingProducer = Some(sender())
      }
  }
}
package io.reactivecqrs.actor

import akka.actor.{Props, ActorSystem, ActorRef}
import io.reactivecqrs.core.Aggregate

import scala.reflect.ClassTag

case class AkkaAggregate[AGGREGATE_ROOT](commandBus: ActorRef)

object AkkaAggregate {

  def create[AGGREGATE_ROOT : ClassTag](aggregate: Aggregate[AGGREGATE_ROOT])(system: ActorSystem): AkkaAggregate[AGGREGATE_ROOT] = {

    val commandBus = system.actorOf(Props(new AkkaCommandBus[AGGREGATE_ROOT](aggregate.commandsHandlers, aggregate.eventsHandlers)), "AkkaCommandBus")

    AkkaAggregate(commandBus)
  }

}

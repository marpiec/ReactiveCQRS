package io.reactivecqrs.actor

import akka.actor.{Props, ActorSystem, ActorRef}
import io.reactivecqrs.core.Aggregate

import scala.reflect.ClassTag

case class AkkaAggregate[AGGREGATE_ROOT](commandBus: ActorRef)

object AkkaAggregate {

  def create[AGGREGATE_ROOT : ClassTag](aggregate: Aggregate[AGGREGATE_ROOT], uidGenerator: ActorRef)(system: ActorSystem): AkkaAggregate[AGGREGATE_ROOT] = {

    val commandBus = system.actorOf(Props(new AkkaCommandBus[AGGREGATE_ROOT](uidGenerator, aggregate.commandsHandlers, aggregate.eventsHandlers)), "AkkaCommandBus")

    AkkaAggregate(commandBus)
  }

}

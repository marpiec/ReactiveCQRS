package io.reactivecqrs.actor

import akka.actor.{Props, ActorSystem, ActorRef}
import io.reactivecqrs.core._

import scala.reflect.ClassTag

case class AkkaAggregate[AGGREGATE_ROOT](commandBus: ActorRef)

object AkkaAggregate {

  def create[AGGREGATE_ROOT : ClassTag](aggregate: AggregateCommandBus[AGGREGATE_ROOT], uidGenerator: ActorRef)(system: ActorSystem): AkkaAggregate[AGGREGATE_ROOT] = {

    val commandBus = system.actorOf(Props(new AggregateCommandBusActor[AGGREGATE_ROOT](
      uidGenerator,
      aggregate.commandsHandlers.asInstanceOf[Seq[CommandHandler[AGGREGATE_ROOT,AbstractCommand[AGGREGATE_ROOT, _],_]]],
      aggregate.eventsHandlers.asInstanceOf[Seq[AbstractEventHandler[AGGREGATE_ROOT, Event[AGGREGATE_ROOT]]]])),
      "AkkaCommandBus")

    AkkaAggregate(commandBus)
  }

}

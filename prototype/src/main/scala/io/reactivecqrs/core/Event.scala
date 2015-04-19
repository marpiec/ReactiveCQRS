package io.reactivecqrs.core

import akka.actor.ActorRef

abstract class Event[AGGREGATE_ROOT]

case class EventEnvelope[AGGREGATE_ROOT](respondTo: ActorRef,
                                         expectedVersion: AggregateVersion,
                                         event: Event[AGGREGATE_ROOT])
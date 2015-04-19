package io.reactivecqrs.core

import akka.actor.ActorRef

abstract class Command[AGGREGATE_ROOT, RESPONSE] {
  val id: AggregateId
  val expectedVersion: AggregateVersion
}

abstract class FirstCommand[AGGREGATE_ROOT, RESPONSE]



case class CommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, command: Command[AGGREGATE_ROOT, RESPONSE])

case class FirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, firstCommand: FirstCommand[AGGREGATE_ROOT, RESPONSE])
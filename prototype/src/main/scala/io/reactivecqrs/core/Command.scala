package io.reactivecqrs.core

import _root_.io.reactivecqrs.api.guid.AggregateId
import akka.actor.ActorRef

import scala.reflect._

sealed trait AbstractCommand[AGGREGATE_ROOT, RESPONSE]

abstract class Command[AGGREGATE_ROOT, RESPONSE] extends AbstractCommand[AGGREGATE_ROOT, RESPONSE] {

  val aggregateId: AggregateId
  val expectedVersion: AggregateVersion
}

abstract class FirstCommand[AGGREGATE_ROOT, RESPONSE] extends AbstractCommand[AGGREGATE_ROOT, RESPONSE]



case class CommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, command: Command[AGGREGATE_ROOT, RESPONSE])

case class FirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, firstCommand: FirstCommand[AGGREGATE_ROOT, RESPONSE])



abstract class CommandHandler[AGGREGATE_ROOT, COMMAND <: AbstractCommand[AGGREGATE_ROOT, RESPONSE], RESPONSE]
          (implicit commandClassTag: ClassTag[COMMAND]) {
  val commandClassName = commandClassTag.runtimeClass.getName
  def handle(aggregateId: AggregateId, command: COMMAND): (Event[AGGREGATE_ROOT], RESPONSE)
}

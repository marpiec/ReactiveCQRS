package io.reactivecqrs.core

import _root_.io.reactivecqrs.api.guid.{CommandId, AggregateId, UserId}
import akka.actor.ActorRef

import scala.reflect._


case class FirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE](userId: UserId,
                                                          command: FirstCommand[AGGREGATE_ROOT, RESPONSE])

case class CommandEnvelope[AGGREGATE_ROOT, RESPONSE](userId: UserId,
                                                     aggregateId: AggregateId,
                                                     expectedVersion: AggregateVersion,
                                                     command: Command[AGGREGATE_ROOT, RESPONSE])


sealed trait AbstractCommand[AGGREGATE_ROOT, RESPONSE]


case class InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, commandId: CommandId, commandEnvelope: FirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE])
case class InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, commandId: CommandId, commandEnvelope: CommandEnvelope[AGGREGATE_ROOT, RESPONSE])


abstract class Command[AGGREGATE_ROOT, RESPONSE] extends AbstractCommand[AGGREGATE_ROOT, RESPONSE]
abstract class FirstCommand[AGGREGATE_ROOT, RESPONSE] extends AbstractCommand[AGGREGATE_ROOT, RESPONSE]




abstract class CommandHandler[AGGREGATE_ROOT, COMMAND <: AbstractCommand[AGGREGATE_ROOT, RESPONSE], RESPONSE]
          (implicit commandClassTag: ClassTag[COMMAND]) {
  val commandClassName = commandClassTag.runtimeClass.getName
  def handle(aggregateId: AggregateId, command: COMMAND): (Seq[Event[AGGREGATE_ROOT]], (AggregateVersion) => RESPONSE)
}

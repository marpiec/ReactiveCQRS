package io.reactivecqrs.core

import io.reactivecqrs.api.guid.{CommandId, AggregateId, UserId}
import akka.actor.ActorRef

import scala.reflect._


sealed trait AbstractCommand[AGGREGATE_ROOT, RESPONSE]


// First Command

abstract class FirstCommand[AGGREGATE_ROOT, RESPONSE] extends AbstractCommand[AGGREGATE_ROOT, RESPONSE]


case class FirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE](userId: UserId,
                                                          command: FirstCommand[AGGREGATE_ROOT, RESPONSE])

case class InternalFirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, commandId: CommandId, commandEnvelope: FirstCommandEnvelope[AGGREGATE_ROOT, RESPONSE])




abstract class Command[AGGREGATE_ROOT, RESPONSE] extends AbstractCommand[AGGREGATE_ROOT, RESPONSE]

case class CommandEnvelope[AGGREGATE_ROOT, RESPONSE](userId: UserId,
                                                     aggregateId: AggregateId,
                                                     expectedVersion: AggregateVersion,
                                                     command: Command[AGGREGATE_ROOT, RESPONSE])

case class InternalCommandEnvelope[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, commandId: CommandId, commandEnvelope: CommandEnvelope[AGGREGATE_ROOT, RESPONSE])




case class CommandResult(aggregateId: AggregateId, aggregateVersion: AggregateVersion)



abstract class CommandHandlingResult[AGGREGATE_ROOT, RESPONSE]

case class Success[AGGREGATE_ROOT, RESPONSE](events: Seq[Event[AGGREGATE_ROOT]], response: (AggregateVersion) => RESPONSE)
  extends CommandHandlingResult[AGGREGATE_ROOT, RESPONSE]

case class Failure[AGGREGATE_ROOT, RESPONSE](response: RESPONSE)
  extends CommandHandlingResult[AGGREGATE_ROOT, RESPONSE]


abstract class CommandHandler[AGGREGATE_ROOT, COMMAND <: AbstractCommand[AGGREGATE_ROOT, RESPONSE] : ClassTag, RESPONSE] {
  val commandClassName = classTag[COMMAND].runtimeClass.getName
  def handle(aggregateId: AggregateId, command: COMMAND): CommandHandlingResult[AGGREGATE_ROOT, RESPONSE]
}



/**
 * Trait used when command have to be transformed before stored in Command Log.
 * E.g. when user registration command contains a password we don't want to store
 * the password for security reasons. Then we'll add this trait to a Command and remove
 * password from command before storing it.
 */
trait CommandLogTransform[AGGREGATE_ROOT, RESPONSE] { self: AbstractCommand[AGGREGATE_ROOT, RESPONSE] =>
  def transform(): Command[AGGREGATE_ROOT, RESPONSE]
}

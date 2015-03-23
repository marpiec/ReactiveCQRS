package io.reactivecqrs.api.command

import io.reactivecqrs.api.event.Event
import io.reactivecqrs.api.guid.{AggregateId, AggregateVersion, UserId}

case class CommandHandlingResult[AGGREGATE, RESPONSE](event: Event[AGGREGATE], response: RESPONSE)

sealed trait AbstractCommandHandler[AGGREGATE, COMMAND <: Command[AGGREGATE, RESPONSE], RESPONSE] {

  def handle(userId: UserId, command: COMMAND): CommandHandlingResult[AGGREGATE, RESPONSE]

  def commandClass: Class[COMMAND] // TODO extract from declaration
}


/**
 * Responsible for handling command send by user.
 * @tparam COMMAND Type of command that can be handled.
 * @tparam RESPONSE Response send to user after attempt to handle command.
 */
trait CommandHandler[AGGREGATE, COMMAND <: Command[AGGREGATE, RESPONSE], RESPONSE] extends AbstractCommandHandler[AGGREGATE, COMMAND, RESPONSE] {

  def validateCommand(userId: UserId, aggregateId: AggregateId, version: AggregateVersion, currentAggregateRoot: AGGREGATE, command: COMMAND): Option[RESPONSE]

  def onConcurrentModification(): OnConcurrentModification

}

trait FirstCommandHandler[AGGREGATE, COMMAND <: Command[AGGREGATE, RESPONSE], RESPONSE] extends AbstractCommandHandler[AGGREGATE, COMMAND, RESPONSE] {

  def validateCommand(userId: UserId, command: COMMAND): Option[RESPONSE]


}
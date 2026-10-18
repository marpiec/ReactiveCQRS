package io.reactivecqrs.core.commandhandler

import io.reactivecqrs.api.{CustomCommandResponse, IdempotentCommand}
import io.reactivecqrs.api.id.AggregateId
import scalikejdbc.DBSession

/**
  * Idempotent commands (D12). A command with an idempotency key takes effect once: its response is stored in the
  * transaction that persists its events, and every later or concurrent copy is answered with that stored response.
  * A copy that loses the race finds the key taken when it persists (its transaction rolls back), or meets a version
  * conflict; in both cases the stored response is its answer. Callers never handle keys themselves (AR2).
  */
class Idempotency(responses: CommandResponseState) {

  /** The response stored for this command, if it is idempotent and one of its copies was already applied. */
  def storedResponse(command: Any): Option[CustomCommandResponse[_]] = keyOf(command).flatMap(responses.responseByKey)

  /** Why the command's key cannot be stored, when it cannot: it is longer than `Idempotency.MaxKeyLength` (F252). */
  def keyRefusal(command: Any): Option[IdempotencyKeyRefused] = keyOf(command).filter(_.length > Idempotency.MaxKeyLength).map { key =>
    new IdempotencyKeyRefused("The idempotency key has " + key.length + " characters; at most " + Idempotency.MaxKeyLength + " are stored")
  }

  /** Whether the command carries an idempotency key. */
  def isIdempotent(command: Any): Boolean = keyOf(command).isDefined

  /** Stores the response of an idempotent command on `aggregateId` inside the persist transaction. Throws
    * `IdempotencyKeyTakenException` when a copy got there first, which must roll the transaction back. */
  def record(command: Any, response: CustomCommandResponse[_], aggregateId: AggregateId)(implicit session: DBSession): Unit = {
    keyOf(command).foreach(responses.storeResponse(_, response, Some(aggregateId)))
  }

  /** Removes the stored responses older than the retention period; later copies of those commands run again. */
  def removeExpired(): Int = responses.removeExpired()

  private def keyOf(command: Any): Option[String] = command match {
    case idempotent: IdempotentCommand[_] => idempotent.idempotencyId.map(_.asDbKey)
    case _ => None
  }
}

object Idempotency {
  /** The longest idempotency key stored: the length of `commands_responses.key`. */
  val MaxKeyLength = 256
}

/** A command whose idempotency key cannot be stored; it is answered with a failure before it runs (F252). */
class IdempotencyKeyRefused(message: String) extends Exception(message)

package io.reactivecqrs.api

import io.reactivecqrs.api.id.{AggregateId, UserId}

//
//// First Command
//
//abstract class FirstCommand[AGGREGATE_ROOT, RESPONSE] extends AbstractCommand[AGGREGATE_ROOT]
//

abstract class FirstCommand[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]] {
  val userId: UserId
}

abstract class ConcurrentCommand[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]] {
  val userId: UserId
  val aggregateId: AggregateId
}

abstract class Command[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]] {
  val userId: UserId
  val aggregateId: AggregateId
  val expectedVersion: AggregateVersion
}




/**
 * Trait used when command have to be transformed before stored in Command Log.
 * E.g. when user registration command contains a password we don't want to store
 * the password for security reasons. Then we'll add this trait to a Command and remove
 * password from command before storing it.
 */
trait CommandLogTransform[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]] { self: Command[AGGREGATE_ROOT, RESPONSE] =>
  def transform(): Command[AGGREGATE_ROOT, RESPONSE]
}

/**
 * Trait used when command have to be transformed before stored in Command Log.
 * E.g. when user registration command contains a password we don't want to store
 * the password for security reasons. Then we'll add this trait to a Command and remove
 * password from command before storing it.
 */
trait FirstCommandLogTransform[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]] { self: FirstCommand[AGGREGATE_ROOT, RESPONSE] =>
  def transform(): FirstCommand[AGGREGATE_ROOT, RESPONSE]
}

/**
  * Trait used when command have to be transformed before stored in Command Log.
  * E.g. when user registration command contains a password we don't want to store
  * the password for security reasons. Then we'll add this trait to a Command and remove
  * password from command before storing it.
  */
trait ConcurrentCommandLogTransform[AGGREGATE_ROOT, RESPONSE <: CustomCommandResponse[_]] { self: ConcurrentCommand[AGGREGATE_ROOT, RESPONSE] =>
  def transform(): ConcurrentCommand[AGGREGATE_ROOT, RESPONSE]
}
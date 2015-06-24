package io.reactivecqrs.api

//
//// First Command
//
//abstract class FirstCommand[AGGREGATE_ROOT, RESPONSE] extends AbstractCommand[AGGREGATE_ROOT]
//


abstract class Command[AGGREGATE_ROOT, RESPONSE]




/**
 * Trait used when command have to be transformed before stored in Command Log.
 * E.g. when user registration command contains a password we don't want to store
 * the password for security reasons. Then we'll add this trait to a Command and remove
 * password from command before storing it.
 */
trait CommandLogTransform[AGGREGATE_ROOT, RESPONSE] { self: Command[AGGREGATE_ROOT, RESPONSE] =>
  def transform(): Command[AGGREGATE_ROOT, RESPONSE]
}

package io.reactivecqrs.testdomain.eventhandler

import io.reactivecqrs.api.event.CreationEventHandler

object UserRegisteredEventHandler extends CreationEventHandler[User, UserRegistered] {

  override def handle(event: UserRegistered): User = User(event.name, None)

}

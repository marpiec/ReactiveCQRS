package io.reactivesqrs.testdomain.eventhandler

import io.reactivecqrs.api.event.CreationEventHandler
import io.reactivesqrs.testdomain.api.{User, UserRegistered}

object UserRegisteredEventHandler extends CreationEventHandler[User, UserRegistered] {

  override def handle(event: UserRegistered): User = User(event.name, None)

}

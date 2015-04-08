package io.reactivesqrs.testdomain.eventhandler

import io.reactivecqrs.api.event.DeletionEventHandler
import io.reactivesqrs.testdomain.api.{User, UserRemoved}

object UserRemovedEventHandler extends DeletionEventHandler[User, UserRemoved] {
  override def eventClass: Class[UserRemoved] = classOf[UserRemoved]
}

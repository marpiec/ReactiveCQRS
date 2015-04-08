package io.reactivesqrs.testdomain.api

import io.reactivecqrs.api.event.{DeleteEvent, UndoEvent, Event}

trait UserEvent extends Event[User] {
  override def aggregateType: Class[User] = classOf[User]
}

case class UserRegistered(name: String) extends UserEvent

case class UserAddressChanged(city: String, street: String, number: String) extends UserEvent

case class UserRemoved() extends DeleteEvent[User] with UserEvent

case class UserChangeUndone(eventsCount: Int) extends UndoEvent[User] with UserEvent

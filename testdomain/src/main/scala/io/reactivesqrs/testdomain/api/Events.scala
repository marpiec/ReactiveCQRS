package io.reactivesqrs.testdomain.api

import io.reactivecqrs.api.event.{UndoEvent, Event}

sealed abstract class UserEvent extends Event[User] {
  override def aggregateType: Class[User] = classOf[User]
}

sealed abstract class UserUndoEvent extends UndoEvent[User] {
  override def aggregateType: Class[User] = classOf[User]
}

case class UserRegistered(name: String) extends UserEvent

case class UserAddressChanged(city: String, street: String, number: String) extends UserEvent

case class UserRemoved() extends UserEvent

case class UserChangeUndone(eventsCount: Int) extends UserUndoEvent

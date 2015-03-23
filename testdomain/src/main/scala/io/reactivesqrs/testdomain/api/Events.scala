package io.reactivesqrs.testdomain.api

import io.reactivecqrs.api.event.{UndoEvent, Event}

case class UserRegistered(name: String) extends Event[User]

case class UserAddressChanged(city: String, street: String, number: String) extends Event[User]

case class UserRemoved() extends Event[User]

case class UserChangeUndone(eventsCount: Int) extends UndoEvent[User]

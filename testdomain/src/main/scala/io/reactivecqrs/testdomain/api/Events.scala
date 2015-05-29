package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api.event.{DeleteEvent, UndoEvent, Event}

sealed trait UserEvent extends Event[User]

case class UserRegistered(name: String) extends UserEvent

case class UserAddressChanged(city: String,
                              street: String,
                              number: String) extends UserEvent

case class UserDeleted() extends DeleteEvent[User] with UserEvent

case class UserChangeUndone(eventsCount: Int) extends UndoEvent[User] with UserEvent

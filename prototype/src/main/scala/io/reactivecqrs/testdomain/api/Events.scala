package io.reactivecqrs.testdomain.api

import io.reactivecqrs.core.Event

sealed trait UserEvent extends Event[User]

case class UserRegistered(name: String) extends UserEvent

case class UserAddressChanged(city: String,
                              street: String,
                              number: String) extends UserEvent

case class UserDeleted() extends UserEvent

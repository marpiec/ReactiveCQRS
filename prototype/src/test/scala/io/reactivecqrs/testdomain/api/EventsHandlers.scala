package io.reactivecqrs.testdomain.api

import io.reactivecqrs.core.EventHandler


object UserAddressChangedEventHandler extends EventHandler[User, UserAddressChanged] {
  override def handle(aggregateRoot: User, event: UserAddressChanged): Unit = {
    aggregateRoot.copy(address = Some(Address(event.city, event.street, event.number)))
  }
}


object UserRegisteredEventHandler extends EventHandler[User, UserRegistered] {
  override def handle(aggregateRoot: User, event: UserRegistered): Unit = {
    User(event.name, None)
  }
}

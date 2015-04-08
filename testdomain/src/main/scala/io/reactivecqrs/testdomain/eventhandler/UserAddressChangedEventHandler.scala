package io.reactivecqrs.testdomain.eventhandler

import io.reactivecqrs.api.event.ModificationEventHandler
import io.reactivecqrs.testdomain.api.{Address, User, UserAddressChanged}

object UserAddressChangedEventHandler extends ModificationEventHandler[User, UserAddressChanged] {

  override def handle(aggregate: User, event: UserAddressChanged): User = {
    aggregate.copy(address = Some(Address(event.city, event.street, event.number)))
  }
}

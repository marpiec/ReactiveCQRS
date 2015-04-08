package io.reactivesqrs.testdomain

import java.time.Clock

import io.reactivecqrs.core.{EventStore, Repository}
import io.reactivesqrs.testdomain.api.User
import io.reactivesqrs.testdomain.eventhandler.{UserAddressChangedEventHandler, UserRegisteredEventHandler}

class UserRepository(clock: Clock, eventStore: EventStore[User])
  extends Repository[User](clock, eventStore,
    Array(UserRegisteredEventHandler,
          UserAddressChangedEventHandler))

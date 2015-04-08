package io.reactivecqrs.testdomain

import java.time.Clock

import io.reactivecqrs.core.{EventStore, Repository}
import io.reactivecqrs.testdomain.api.User
import io.reactivecqrs.testdomain.eventhandler.{UserAddressChangedEventHandler, UserRegisteredEventHandler}

class UserRepository(protected val clock: Clock,
                     protected val eventStore: EventStore[User])
  extends Repository[User](UserRegisteredEventHandler,
                           UserAddressChangedEventHandler)
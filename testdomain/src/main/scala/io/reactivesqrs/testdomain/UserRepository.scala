package io.reactivesqrs.testdomain

import java.time.Clock

import io.reactivecqrs.core.{EventStore, Repository}
import io.reactivesqrs.testdomain.api.User
import io.reactivesqrs.testdomain.eventhandler.{UserAddressChangedEventHandler, UserRegisteredEventHandler}

class UserRepository(protected val clock: Clock,
                     protected val eventStore: EventStore[User])
  extends Repository[User](UserRegisteredEventHandler,
                           UserAddressChangedEventHandler)
package io.reactivesqrs.testdomain

import java.time.Clock

import io.reactivecqrs.core.{EventStore, Repository}
import io.reactivesqrs.testdomain.api.User

class UserRepository(clock: Clock, eventStore: EventStore[User])
  extends Repository[User](clock, eventStore,
    Array())

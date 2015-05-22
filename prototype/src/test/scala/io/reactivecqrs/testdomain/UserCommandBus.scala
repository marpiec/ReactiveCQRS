package io.reactivecqrs.testdomain

import io.reactivecqrs.core._
import io.reactivecqrs.testdomain.api._


class UserCommandBus extends Aggregate[User] {

  override val commandsHandlers = Seq(new RegisterUserHandler(), new ChangeUserAddressHandler(), new DeleteUserHandler()).asInstanceOf[Seq[CommandHandler[User,AbstractCommand[User, _],_]]]
  override val eventsHandlers = Seq(UserAddressChangedEventHandler, UserRegisteredEventHandler).asInstanceOf[Seq[EventHandler[User, Event[User]]]]
}

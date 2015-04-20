package io.reactivecqrs.testdomain

import io.reactivecqrs.core._
import io.reactivecqrs.testdomain.api._


class UserCommandBus extends Aggregate[User] {

  val commandsHandlers: Seq[CommandHandler[User,AbstractCommand[User, _],_]] = Seq(new RegisterUserHandler(), new ChangeUserAddressHandler(), new DeleteUserHandler()).asInstanceOf[Seq[CommandHandler[User,AbstractCommand[User, _],_]]]
  val eventsHandlers:Seq[EventHandler[User, Event[User]]] = Seq(UserAddressChangedEventHandler, UserRegisteredEventHandler).asInstanceOf[Seq[EventHandler[User, Event[User]]]]
}

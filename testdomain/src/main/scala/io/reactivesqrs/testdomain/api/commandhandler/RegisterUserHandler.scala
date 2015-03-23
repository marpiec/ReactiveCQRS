package io.reactivesqrs.testdomain.api.commandhandler


class RegisterUserHandler(eventStore: EventStore) extends CommandHandler[RegisterUser, RegisterUserResult] {

  override def handle(commandId: CommandId, userId: UserId, command: RegisterUser): RegisterUserResult = {
    eventStore.addFirstEvent(commandId, userId, command.userId, UserRegistered(command.name))
    RegisterUserResult(success = true)
  }

}
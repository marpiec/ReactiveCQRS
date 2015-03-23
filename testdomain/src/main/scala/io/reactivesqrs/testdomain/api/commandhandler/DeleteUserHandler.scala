package io.reactivesqrs.testdomain.api.commandhandler

class DeleteUserHandler(eventStore: EventStore) extends CommandHandler[DeleteUser, DeleteUserResult] {

  override def handle(commandId: CommandId, userId: UserId, command: DeleteUser): DeleteUserResult = {
    eventStore.addEvent(commandId, userId, command.userId, command.expectedVersion, UserRemoved())
    DeleteUserResult(success = true)
  }

}
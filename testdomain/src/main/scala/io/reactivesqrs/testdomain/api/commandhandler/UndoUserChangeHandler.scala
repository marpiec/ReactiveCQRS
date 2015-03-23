package io.reactivesqrs.testdomain.api.commandhandler

class UndoUserChangeHandler(eventStore: EventStore) extends CommandHandler[UndoUserChange, UndoUserChangeResult] {
  override def handle(commandId: CommandId, userId: UserId, command: UndoUserChange): UndoUserChangeResult = {
    eventStore.addEvent(commandId, userId, command.userId, command.expectedVersion, new UserChangeUndone(command.stepsToUndo))
    UndoUserChangeResult(success = true)
  }
}
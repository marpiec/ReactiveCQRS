package io.reactivecqrs.testdomain

import akka.actor.{Actor, ActorRef}
import io.reactivecqrs.core.{FirstCommandEnvelope, Command, CommandEnvelope, FirstCommand}
import io.reactivecqrs.testdomain.api.{DeleteUser, ChangeUserAddress, RegisterUser}

class UserCommandHandler extends Actor {

  override def receive: Receive = {
    case ce: CommandEnvelope[_,_] => handleCommand(ce.respondTo, ce.command)
    case ce: FirstCommandEnvelope[_,_] => handleFirstCommand(ce.respondTo, ce.firstCommand)
  }


  def handleCommand[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, command: Command[AGGREGATE_ROOT, RESPONSE]): Unit = {
    command match {
      case c: ChangeUserAddress => handleChangeUserAddress(respondTo, c)
      case c: DeleteUser => handleDeleteUser(respondTo, c)
      case c => throw new IllegalArgumentException("Unsupported command " + c)
    }
    
  }

  def handleFirstCommand[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, command: FirstCommand[AGGREGATE_ROOT, RESPONSE]): Unit = {
    command match {
      case c: RegisterUser => handleRegisterUser(respondTo, c)
      case c => throw new IllegalArgumentException("Unsupported first command " + c)
    }
  }
  
  
  
  def handleRegisterUser(respondTo: ActorRef, registerUser: RegisterUser): Unit = {
    ???
  }

  def handleChangeUserAddress(respondTo: ActorRef, command: ChangeUserAddress): Unit = {
    ???
  }


  def handleDeleteUser(respondTo: ActorRef, command: DeleteUser): Unit = {
    ???
  }
}

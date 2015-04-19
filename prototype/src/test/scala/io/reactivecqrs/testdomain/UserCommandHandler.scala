package io.reactivecqrs.testdomain

import akka.actor.{Props, Actor, ActorRef}
import io.reactivecqrs.core._
import io.reactivecqrs.testdomain.api._

class UserCommandHandler extends Actor {

  override def receive: Receive = {
    case ce: CommandEnvelope[_,_] => handleCommand(ce.respondTo, ce.command)
    case ce: FirstCommandEnvelope[_,_] => handleFirstCommand(ce.respondTo, ce.id, ce.firstCommand)
  }


  def handleCommand[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, command: Command[AGGREGATE_ROOT, RESPONSE]): Unit = {
    command match {
      case c: ChangeUserAddress => handleChangeUserAddress(respondTo, c)
      case c: DeleteUser => handleDeleteUser(respondTo, c)
      case c => throw new IllegalArgumentException("Unsupported command " + c)
    }
    
  }

  def handleFirstCommand[AGGREGATE_ROOT, RESPONSE](respondTo: ActorRef, id: AggregateId, command: FirstCommand[AGGREGATE_ROOT, RESPONSE]): Unit = {
    command match {
      case c: RegisterUser => handleRegisterUser(respondTo, id, c)
      case c => throw new IllegalArgumentException("Unsupported first command " + c)
    }
  }
  
  
  
  def handleRegisterUser(respondTo: ActorRef, id: AggregateId, registerUser: RegisterUser): Unit = {
    if(registerUser.name.isEmpty) {
      throw new IllegalArgumentException("Username cannot be empty!")
    } else {

      println("Sending event to Aggregate")

      val resultAggregator = context.actorOf(Props(new UserCommandResultAggregator(respondTo, RegisterUserResult(id))))


      context.actorOf(Props(new UserAggregate(id))) ! EventEnvelope(resultAggregator, AggregateVersion.ZERO, UserRegistered(registerUser.name))


    }
  }

  def handleChangeUserAddress(respondTo: ActorRef, command: ChangeUserAddress): Unit = {
    ???
  }


  def handleDeleteUser(respondTo: ActorRef, command: DeleteUser): Unit = {
    ???
  }
}

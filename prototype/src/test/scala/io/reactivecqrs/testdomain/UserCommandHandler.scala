package io.reactivecqrs.testdomain

import akka.actor.{Props, Actor, ActorRef}
import akka.event.LoggingReceive
import io.reactivecqrs.core._
import io.reactivecqrs.testdomain.api._

class UserCommandHandler(id: AggregateId) extends Actor {

  println(s"UserCommandHandler with $id created")

  override def receive: Receive = LoggingReceive {
    case CommandEnvelope(respondTo, command) => handleCommand(respondTo, command)
    case FirstCommandEnvelope(respondTo, firstCommand) => println("Received with id " +id);handleFirstCommand(respondTo, firstCommand)
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
    if(registerUser.name.isEmpty) {
      throw new IllegalArgumentException("Username cannot be empty!")
    } else {

      println("Sending event to Aggregate")

      val resultAggregator = context.actorOf(Props(new UserCommandResultAggregator(respondTo, RegisterUserResult(id))), "ResultAggregator")


      context.actorOf(Props(new UserAggregate(id)), "UserAggregate"+id.asLong) ! EventEnvelope(resultAggregator, AggregateVersion.ZERO, UserRegistered(registerUser.name))


    }
  }

  def handleChangeUserAddress(respondTo: ActorRef, command: ChangeUserAddress): Unit = {
    ???
  }


  def handleDeleteUser(respondTo: ActorRef, command: DeleteUser): Unit = {
    ???
  }
}

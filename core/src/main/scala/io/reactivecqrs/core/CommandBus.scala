package io.reactivecqrs.core

import java.time.Clock

import akka.actor.{ActorRef, Actor}
import akka.pattern.ask
import akka.util.Timeout
import io.reactivecqrs.api.command._
import io.reactivecqrs.api.exception._
import io.reactivecqrs.api.guid.{AggregateVersion, UserId}
import io.reactivecqrs.api.{Aggregate, AggregateIdGenerator, CommandIdGenerator}
import io.reactivecqrs.utils.{Failure, Result, Success}

import scala.concurrent.Await
import scala.concurrent.duration._


abstract class CommandBus[AGGREGATE](clock: Clock,
                                     commandIdGenerator: CommandIdGenerator, //TODO make it an actor
                                     aggregateIdGenerator: AggregateIdGenerator,
                                     commandLog: CommandLogActorApi,
                                     aggregateRepositoryActor: ActorRef,
                                     handlers: Array[CommandHandler[AGGREGATE, _ <: Command[AGGREGATE, _], _]]) extends Actor {

  private val repositoryHandler = new CoreRepositoryHandler[AGGREGATE](aggregateRepositoryActor)

  private val commandHandlers = handlers.asInstanceOf[Array[CommandHandler[AGGREGATE, Command[AGGREGATE, AnyRef], AnyRef]]]
    .map(handler => handler.commandClass -> handler).toMap

  implicit val akkaTimeout = Timeout(5 seconds)

  override def receive: Receive = {
    case CommandEnvelope(acknowledgeId, userId, command) => command match {
      case c :FirstCommand[AGGREGATE, AnyRef] => submitFirstCommand(acknowledgeId, userId, c.asInstanceOf[FirstCommand[AGGREGATE, AnyRef]])
      case c :FollowingCommand[AGGREGATE, AnyRef] => submitFollowingCommand(acknowledgeId, userId, c.asInstanceOf[FollowingCommand[AGGREGATE, AnyRef]])
      case c :AnyRef =>
        sender() ! IncorrectCommand(
        s"Received command of type ${command.getClass} but expected instance of ${classOf[Command[AnyRef, AnyRef]]}")
    }
    case envelope: AnyRef =>
      sender() ! IncorrectCommand(
      s"Received commandEnvelope of type ${envelope.getClass} but expected instance of ${classOf[Command[AnyRef, AnyRef]]}")
  }


  def submitFirstCommand(acknowledgeId: String, userId: UserId, command: FirstCommand[AGGREGATE, AnyRef]): Unit = {

    val commandHandler = commandHandlers(command.getClass.asInstanceOf[Class[Command[AGGREGATE, AnyRef]]])
      .asInstanceOf[FirstCommandHandler[AGGREGATE, FirstCommand[AGGREGATE, AnyRef], AnyRef]]

    val result = commandHandler.handle(commandIdGenerator.nextCommandId, userId, command, repositoryHandler)
    sender ! CommandResponseEnvelope(acknowledgeId, result)
  }


  def submitFollowingCommand(acknowledgeId: String, userId: UserId, command: FollowingCommand[AGGREGATE, AnyRef]): Unit = {

    val commandHandler = commandHandlers(command.getClass.asInstanceOf[Class[Command[AGGREGATE, AnyRef]]])
      .asInstanceOf[FollowingCommandHandler[AGGREGATE, FollowingCommand[AGGREGATE, AnyRef], AnyRef]]

    handleCommand(commandHandler)

    // implementation

    def handleCommand(handler: FollowingCommandHandler[AGGREGATE, FollowingCommand[AGGREGATE, AnyRef], AnyRef])= {
      val aggregateState = loadLastAggregateState()
      aggregateState match {
        case Failure(exception) => sender ! exception
        case Success(aggregate) if aggregate.version < command.expectedVersion =>
          sender ! CommandResponseEnvelope(acknowledgeId, Failure(IncorrectAggregateVersionException(s"Expected version ${command.expectedVersion} but was ${aggregate.version}")))
        case Success(aggregate) if aggregate.version > command.expectedVersion =>
          handleConcurrentModification(aggregate.version)
        case Success(aggregate) if aggregate.aggregateRoot.isEmpty =>
          sender ! CommandResponseEnvelope(acknowledgeId, Failure(AggregateWasAlreadyDeletedException(s"Aggregate is already deleted. So no new commands are possible")))
        case Success(aggregate) =>
          val result = handler.handle(commandIdGenerator.nextCommandId, userId, aggregate.aggregateRoot.get, command, repositoryHandler)
          sender ! CommandResponseEnvelope(acknowledgeId, result)
      }
    }



    def loadLastAggregateState(): Result[Aggregate[AGGREGATE], RepositoryException] = {
      val future = aggregateRepositoryActor ? GetAggregate("123", command.aggregateId)
      val result = Await.result(future, 5 seconds)
      result.asInstanceOf[GetAggregateResponse[AGGREGATE]].result
    }
    
    def handleConcurrentModification(currentVersion: AggregateVersion): Unit = {
      ???
    }

  }


}

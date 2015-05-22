package io.reactivecqrs.core

import java.time.Clock

import akka.actor.{ActorRef, Actor}
import akka.event.LoggingReceive
import akka.pattern.ask
import akka.util.Timeout
import io.reactivecqrs.api.command._
import io.reactivecqrs.api.exception._
import io.reactivecqrs.api.guid.{AggregateVersion, UserId}
import io.reactivecqrs.api.{AggregateRoot, AggregateIdGenerator, CommandIdGenerator}
import io.reactivecqrs.utils.{Failure, Result, Success}

import scala.concurrent.Await
import scala.concurrent.duration._


abstract class CommandBus[AGGREGATE_ROOT](handlers: CommandHandler[AGGREGATE_ROOT, _ <: Command[AGGREGATE_ROOT, _], _]*) extends Actor {

  protected val clock: Clock
  protected val commandIdGenerator: CommandIdGenerator //TODO make it an actor
  protected val aggregateIdGenerator: AggregateIdGenerator
  protected val commandLog: CommandLogActorApi
  protected val aggregateRepositoryActor: ActorRef
  private val repositoryHandler = new CoreRepositoryHandler[AGGREGATE_ROOT](aggregateRepositoryActor)

  private val commandHandlers = handlers.toArray.asInstanceOf[Array[CommandHandler[AGGREGATE_ROOT, Command[AGGREGATE_ROOT, AnyRef], AnyRef]]]
    .map(handler => handler.commandClass -> handler).toMap

  implicit val akkaTimeout = Timeout(5 seconds)

  override def receive: Receive = LoggingReceive {
    case CommandEnvelopeOld(acknowledgeId, userId, command) => command match {
      case c :FirstCommandOld[AGGREGATE_ROOT, AnyRef] => submitFirstCommand(acknowledgeId, userId, c.asInstanceOf[FirstCommandOld[AGGREGATE_ROOT, AnyRef]])
      case c :FollowingCommand[AGGREGATE_ROOT, AnyRef] => submitFollowingCommand(acknowledgeId, userId, c.asInstanceOf[FollowingCommand[AGGREGATE_ROOT, AnyRef]])
      case c :AnyRef =>
        sender() ! IncorrectCommand(
        s"Received command of type ${command.getClass} but expected instance of ${classOf[Command[AnyRef, AnyRef]]}")
    }
    case envelope: AnyRef =>
      sender() ! IncorrectCommand(
      s"Received commandEnvelope of type ${envelope.getClass} but expected instance of ${classOf[Command[AnyRef, AnyRef]]}")
  }


  def submitFirstCommand(acknowledgeId: String, userId: UserId, command: FirstCommandOld[AGGREGATE_ROOT, AnyRef]): Unit = {

    val commandHandler = commandHandlers(command.getClass.asInstanceOf[Class[Command[AGGREGATE_ROOT, AnyRef]]])
      .asInstanceOf[FirstCommandHandler[AGGREGATE_ROOT, FirstCommandOld[AGGREGATE_ROOT, AnyRef], AnyRef]]

    val result = commandHandler.handle(commandIdGenerator.nextCommandId, userId, command, repositoryHandler)
    sender ! CommandResponseEnvelope(acknowledgeId, result)
  }


  def submitFollowingCommand(acknowledgeId: String, userId: UserId, command: FollowingCommand[AGGREGATE_ROOT, AnyRef]): Unit = {

    val commandHandler = commandHandlers(command.getClass.asInstanceOf[Class[Command[AGGREGATE_ROOT, AnyRef]]])
      .asInstanceOf[FollowingCommandHandler[AGGREGATE_ROOT, FollowingCommand[AGGREGATE_ROOT, AnyRef], AnyRef]]

    handleCommand(commandHandler)

    // implementation

    def handleCommand(handler: FollowingCommandHandler[AGGREGATE_ROOT, FollowingCommand[AGGREGATE_ROOT, AnyRef], AnyRef])= {
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



    def loadLastAggregateState(): Result[AggregateRoot[AGGREGATE_ROOT], RepositoryException] = {
      val future = aggregateRepositoryActor ? LoadAggregate("123", command.aggregateId)
      val result = Await.result(future, 5 seconds)
      result.asInstanceOf[GetAggregateResponse[AGGREGATE_ROOT]].result
    }
    
    def handleConcurrentModification(currentVersion: AggregateVersion): Unit = {
      ???
    }

  }


}

package io.reactivecqrs.testdomain.spec

import java.util.concurrent.atomic.AtomicReference

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{SpaceId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.eventbus.EventsBusActor
import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.testkit.{CommonSpec, TestDatabase}
import org.apache.pekko.actor.Props

case class ThreadProbe(threads: Vector[String])
case class CreateThreadProbe(userId: UserId) extends FirstCommand[ThreadProbe, CustomCommandResponse[_]]
case class ThreadProbeCreated(spaceId: SpaceId) extends FirstEvent[ThreadProbe]

object ThreadProbeContext {
  /** The thread the last command handler ran on. */
  val handlerThread = new AtomicReference[String]()
}

class ThreadProbeContext extends AggregateContext[ThreadProbe] {
  override val version: Int = 1
  override def initialAggregateRoot: ThreadProbe = ThreadProbe(Vector.empty)
  override def commandHandlers = _ => {
    case _: CreateThreadProbe =>
      ThreadProbeContext.handlerThread.set(Thread.currentThread().getName)
      CommandSuccess(ThreadProbeCreated(SpaceId(0)))
  }
  override def eventHandlers = (_, _, probe) => {
    case _: ThreadProbeCreated => probe
  }
}

// F127: the application's command handlers run on a dispatcher of their own, so one that holds its thread for long
// stalls the commands only, never the buses, projections and sagas on the default dispatcher.
class HandlerDispatcherSpec extends CommonSpec {

  TestDatabase.init()

  private val mpjsons = new MPJsons
  private val typesNamesState = new PostgresTypesNamesState().initSchema()
  private val eventStore = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
  private val responses = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
  private val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
    new PostgresUidGenerator("aggregates_uids_seq"), new PostgresUidGenerator("commands_uids_seq"), new PostgresUidGenerator("sagas_uids_seq"))))

  Feature("Where command handlers run (F127)") {

    Scenario("a command handler runs on the handler dispatcher, not on the default one") {
      val bus = system.actorOf(Props(new EventsBusActor()))
      bus ! RegisterSubscribers(Nil)
      val commands = system.actorOf(AggregateCommandBusActor(new ThreadProbeContext, uidGenerator, eventStore, responses, bus))
      val created = commands.askFor[CustomCommandResponse[_]](CreateThreadProbe(UserId(1)))
      created mustBe a[SuccessResponse]
      ThreadProbeContext.handlerThread.get must include("reactivecqrs.handler-dispatcher")
    }
  }
}

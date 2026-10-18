package io.reactivecqrs.core.projection

/** A projection that answers no queries: a listener whose work is its side effects (sending a mail, say). */
abstract class AggregateListenerActor(options: ProjectionActorOptions = ProjectionActorOptions.DEFAULT)  extends ProjectionActor(options) {

  protected def receiveQuery: Receive = {
    case m => ()
  }

}

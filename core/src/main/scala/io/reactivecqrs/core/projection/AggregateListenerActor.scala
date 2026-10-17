package io.reactivecqrs.core.projection

abstract class AggregateListenerActor(options: ProjectionActorOptions = ProjectionActorOptions.DEFAULT)  extends ProjectionActor(options) {

  protected def receiveQuery: Receive = {
    case m => ()
  }

}

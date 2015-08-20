package io.reactivecqrs.core.projection

abstract class AggregateListenerActor extends ProjectionActor {

  protected def receiveQuery: Receive = {
    case m => throw new IllegalStateException("Unsupported message " + m)
  }

}

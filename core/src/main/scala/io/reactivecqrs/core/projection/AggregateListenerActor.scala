package io.reactivecqrs.core.projection

abstract class AggregateListenerActor extends ProjectionActor {

  protected def receiveQuery: Receive = {
    case m => ()
  }

  protected override def onClearProjectionData(): Unit = {
    // override by child if needed
  }

}

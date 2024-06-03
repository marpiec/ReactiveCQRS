package io.reactivecqrs.core.projection

abstract class AggregateListenerActor(groupUpdatesDelayMillis: Long = 0)  extends ProjectionActor(groupUpdatesDelayMillis) {

  protected def receiveQuery: Receive = {
    case m => ()
  }

  protected override def onClearProjectionData(): Unit = {
    // override by child if needed
  }

}

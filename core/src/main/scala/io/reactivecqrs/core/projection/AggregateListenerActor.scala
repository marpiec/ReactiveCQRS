package io.reactivecqrs.core.projection

abstract class AggregateListenerActor(updateDelayMillis: Long = 0)  extends ProjectionActor(updateDelayMillis) {

  protected def receiveQuery: Receive = {
    case m => ()
  }

  protected override def onClearProjectionData(): Unit = {
    // override by child if needed
  }

}

package io.reactivecqrs.api.event


/**
 * This trait marks class as a business event that occured to aggregate.
 * @tparam AGGREGATE type of aggregate this event is related to.
 */
trait Event[AGGREGATE] {
  def aggregateType:Class[AGGREGATE] // TODO extract from declaration
}

/**
 * Special type of event, for removing effect of previous events.
 * @tparam AGGREGATE type of aggregate this event is related to.
 */
trait UndoEvent[AGGREGATE] extends Event[AGGREGATE] {
  /**
   * How many events should ba canceled.
   */
  val eventsCount: Int
}

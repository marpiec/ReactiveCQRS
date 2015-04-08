package io.reactivecqrs.api.event

import java.lang.reflect.Type

import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl


/**
 * This trait marks class as a business event that occurred to aggregate.
 * @tparam AGGREGATE type of aggregate this event is related to.
 */
trait Event[AGGREGATE] {
  /** TODO TypeTag could be better solution */
  def aggregateType:Class[AGGREGATE] = {

    var clazz = this.getClass.asInstanceOf[Class[_]]
    while(clazz.getGenericSuperclass.isInstanceOf[Class[_]]) {
      clazz = clazz.getGenericSuperclass.asInstanceOf[Class[_]]
    }
    val arguments: Array[Type] = clazz.getGenericSuperclass.asInstanceOf[ParameterizedTypeImpl].getActualTypeArguments
    arguments(0).asInstanceOf[Class[AGGREGATE]]
  }
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

case class NoopEvent[AGGREGATE]() extends Event[AGGREGATE]


case class DeleteEvent[AGGREGATE]() extends Event[AGGREGATE]
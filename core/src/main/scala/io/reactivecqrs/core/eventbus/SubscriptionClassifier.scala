package io.reactivecqrs.core.eventbus

import io.reactivecqrs.api.EventType
import io.reactivecqrs.api.id.AggregateId

abstract class SubscriptionClassifier {
  def accept(aggregateId: AggregateId, eventType: EventType): Boolean
}

abstract class AggregateSubscriptionClassifier extends SubscriptionClassifier {
  final def accept(aggregateId: AggregateId, eventType: EventType): Boolean = {
    accept(aggregateId)
  }

  def accept(aggregateId: AggregateId): Boolean
}


case class AggregateIdClassifier(aggregateId: AggregateId) extends AggregateSubscriptionClassifier {
  override def accept(aggregateId: AggregateId): Boolean = {
    this.aggregateId == aggregateId
  }
}

case object AcceptAllAggregateIdClassifier extends AggregateSubscriptionClassifier {
  override def accept(aggregateId: AggregateId): Boolean = true
}

case class EventTypeClassifier(eventType: EventType) extends SubscriptionClassifier {
  override def accept(aggregateId: AggregateId, eventType: EventType): Boolean = {
    this.eventType == eventType
  }
}

case object AcceptAllClassifier extends SubscriptionClassifier {
  override def accept(aggregateId: AggregateId, eventType: EventType): Boolean = true
}

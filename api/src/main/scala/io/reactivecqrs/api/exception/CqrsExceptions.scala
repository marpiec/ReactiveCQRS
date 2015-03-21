package io.reactivecqrs.api.exception

sealed class CqrsException

case class AggregateAlreadyExistsException(message: String) extends CqrsException

case class AggregateWasAlreadyDeletedException(message: String) extends CqrsException

case class CommandAlreadyExistsException(message: String) extends CqrsException

case class ConcurrentAggregateModificationException(message: String) extends CqrsException

case class NoEventsForAggregateException(message: String) extends CqrsException

case class IncorrectAggregateVersionException(message: String) extends CqrsException
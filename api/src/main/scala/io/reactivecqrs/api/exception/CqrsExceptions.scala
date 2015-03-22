package io.reactivecqrs.api.exception

import io.reactivecqrs.api.guid.AggregateVersion

sealed trait CqrsException

case class AggregateAlreadyExistsException(message: String) extends CqrsException

case class AggregateWasAlreadyDeletedException(message: String) extends CqrsException

case class CommandAlreadyExistsException(message: String) extends CqrsException

case class ConcurrentAggregateModificationException(currentVersion: AggregateVersion, message: String) extends CqrsException

case class IncorrectCommand(message: String) extends CqrsException


sealed trait RepositoryException extends CqrsException

case class AggregateDoesNotExistException(message: String) extends RepositoryException

case class IncorrectAggregateVersionException(message: String) extends RepositoryException

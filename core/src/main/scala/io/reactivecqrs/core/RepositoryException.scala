package io.reactivecqrs.core

sealed class RepositoryException

case class NoEventsForAggregateException(message: String) extends RepositoryException

case class IncorrectAggregateVersionException(message: String) extends RepositoryException

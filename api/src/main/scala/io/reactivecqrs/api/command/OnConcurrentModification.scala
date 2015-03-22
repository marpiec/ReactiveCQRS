package io.reactivecqrs.api.command

sealed trait OnConcurrentModification

case object Retry extends OnConcurrentModification
case object Fail extends OnConcurrentModification
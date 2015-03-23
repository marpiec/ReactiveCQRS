package io.reactivecqrs.api.command



sealed trait ValidationResult[RESPONSE]

case class ValidationSuccess[RESPONSE]() extends ValidationResult[RESPONSE]
case class ValidationFailed[RESPONSE](response: RESPONSE) extends  ValidationResult[RESPONSE]
package io.reactivesqrs.testdomain.api

case class User(name: String, address: Option[Address])

case class Address(city: String, street: String, number: String)

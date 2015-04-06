package io.reactivecqrs.utils

sealed abstract class Result[+A, +B] {
  def isSuccess: Boolean
  def isFailure: Boolean
  def value: A
  def error: B
}


case class Success[+A, +B](value: A) extends Result[A, B] {
  override def isSuccess: Boolean = true
  override def error: B = throw new NoSuchElementException("No error in Success")
  override def isFailure: Boolean = false
}

case class Failure[+A, +B](error: B) extends Result[A, B] {
  override def isSuccess: Boolean = false
  override def value: A = throw new NoSuchElementException("No value in Failure")
  override def isFailure: Boolean = true
}
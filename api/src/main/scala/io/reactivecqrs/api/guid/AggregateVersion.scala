package io.reactivecqrs.api.guid

object AggregateVersion {
  val ZERO = AggregateVersion(0)
}

case class AggregateVersion(version: Int) {
  def < (other: AggregateVersion) = this.version < other.version
  def == (other: AggregateVersion) = this.version == other.version
  def > (other: AggregateVersion) = this.version > other.version
}

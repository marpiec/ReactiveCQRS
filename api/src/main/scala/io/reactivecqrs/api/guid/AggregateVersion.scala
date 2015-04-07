package io.reactivecqrs.api.guid

object AggregateVersion {
  val INITIAL = AggregateVersion(1)
}

case class AggregateVersion(version: Int) {
  def < (other: AggregateVersion) = this.version < other.version
  def == (other: AggregateVersion) = this.version == other.version
  def > (other: AggregateVersion) = this.version > other.version

  def increment = AggregateVersion(version + 1)

}

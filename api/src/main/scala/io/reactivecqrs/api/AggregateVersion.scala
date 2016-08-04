package io.reactivecqrs.api

object AggregateVersion {
  val ZERO = AggregateVersion(0)
  def upTo(version: AggregateVersion, count: Int) = {
    ((version.asInt - count + 1) to version.asInt).toList.map(i => AggregateVersion(i))
  }
}

case class AggregateVersion(asInt: Int) {

  def < (other: AggregateVersion) = this.asInt < other.asInt
  def <= (other: AggregateVersion) = this.asInt <= other.asInt
  def == (other: AggregateVersion) = this.asInt == other.asInt
  def > (other: AggregateVersion) = this.asInt > other.asInt
  def >= (other: AggregateVersion) = this.asInt >= other.asInt

  def increment = AggregateVersion(asInt + 1)
  def incrementBy(count: Int) = AggregateVersion(asInt + count)

  def isJustAfter(version: AggregateVersion): Boolean = asInt - 1 == version.asInt

}

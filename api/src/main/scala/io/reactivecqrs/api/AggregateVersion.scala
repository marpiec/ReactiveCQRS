package io.reactivecqrs.api

object AggregateVersion {

  private final val pool = Range(0, 1000).map(i => new AggregateVersion(i)).toArray

  /** The version of an aggregate that does not exist yet: its first event is stored at version 1. */
  val ZERO: AggregateVersion = pool(0)

  /** The version `asInt`. */
  def apply(asInt: Int): AggregateVersion = {
    require(asInt >= 0, "An aggregate version is not negative: " + asInt) // F252
    if (asInt < 1000) {
      pool(asInt)
    } else {
      new AggregateVersion(asInt)
    }
  }

  def unapply(version: AggregateVersion): Option[Int] = Some(version.asInt)
}

/** The version of an aggregate: its last event's. Each stored event takes the next version; a command that expects a
  * version (`Command.expectedVersion`) is refused when the aggregate is at another. */
class AggregateVersion(val asInt: Int) extends Serializable {

  def < (other: AggregateVersion): Boolean = this.asInt < other.asInt
  def <= (other: AggregateVersion): Boolean = this.asInt <= other.asInt
  def == (other: AggregateVersion): Boolean = this.asInt == other.asInt
  def > (other: AggregateVersion): Boolean = this.asInt > other.asInt
  def >= (other: AggregateVersion): Boolean = this.asInt >= other.asInt

  /** The next version. */
  def increment: AggregateVersion = AggregateVersion(asInt + 1)
  /** The version `count` events later. */
  def incrementBy(count: Int): AggregateVersion = AggregateVersion(asInt + count)

  override def equals(obj: Any): Boolean = obj.isInstanceOf[AggregateVersion] && obj.asInstanceOf[AggregateVersion].asInt == asInt

  override def hashCode(): Int = asInt

  override def toString: String = "Version(" + asInt + ")"

  /** Whether this is the version of an aggregate's first event. */
  def isOne: Boolean = asInt == 1
  /** Whether this is `ZERO`: no event yet. */
  def isZero: Boolean = asInt == 0
}

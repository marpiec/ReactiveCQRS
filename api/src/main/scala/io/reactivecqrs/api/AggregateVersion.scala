package io.reactivecqrs.api

object AggregateVersion {

  def upTo(version: AggregateVersion, count: Int): List[AggregateVersion] = {
    ((version.asInt - count + 1) to version.asInt).toList.map(i => AggregateVersion(i))
  }

  private final val pool = Range(0, 1000).map(i => new AggregateVersion(i)).toArray
  val ZERO = pool(0)

  def apply(asInt: Int): AggregateVersion = {
    if(asInt < 1000) {
      pool(asInt)
    } else {
      new AggregateVersion(asInt)
    }
  }

  def unapply(version: AggregateVersion): Option[Int] = Some(version.asInt)
}

class AggregateVersion(val asInt: Int) extends Serializable {

  def < (other: AggregateVersion): Boolean = this.asInt < other.asInt
  def <= (other: AggregateVersion): Boolean = this.asInt <= other.asInt
  def == (other: AggregateVersion): Boolean = this.asInt == other.asInt
  def > (other: AggregateVersion): Boolean = this.asInt > other.asInt
  def >= (other: AggregateVersion): Boolean = this.asInt >= other.asInt

  def increment = AggregateVersion(asInt + 1)
  def incrementBy(count: Int) = AggregateVersion(asInt + count)

  def isJustAfter(version: AggregateVersion): Boolean = asInt - 1 == version.asInt

  override def equals(obj: Any): Boolean = obj.isInstanceOf[AggregateVersion] && obj.asInstanceOf[AggregateVersion].asInt == asInt

  override def hashCode(): Int = asInt

  override def toString: String = "Version("+asInt+")"

  def isOne = asInt == 1
  def isZero = asInt == 0
}
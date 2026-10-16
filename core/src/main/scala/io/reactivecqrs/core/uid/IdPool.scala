package io.reactivecqrs.core.uid

/**
  * Ids taken from the pools the uid generator gives (F23). The next pool is asked for when a fifth of the current one
  * is left, so while the generator keeps up an id is always at hand and nobody waits for one. It is an actor's state:
  * the actor asks for a pool when `shouldAsk` says so, and gives the answer to `received` or `failed`.
  */
final class IdPool(askWhenLeft: Double = 0.2) {

  private var next = 0L
  private var left = 0L
  private var poolSize = 0L
  private var spare: Option[(Long, Long)] = None
  private var asking = false

  def available: Boolean = left > 0 || spare.nonEmpty

  /** The next id; only when `available`. */
  def take(): Long = {
    if (left == 0) spare match {
      case Some((from, size)) =>
        next = from
        left = size
        spare = None
      case None => throw new IllegalStateException("No id left in the pool")
    }
    left -= 1
    next += 1
    next - 1
  }

  /** True, once, when the next pool should be asked for: a fifth or less of this one is left and none is coming. */
  def shouldAsk: Boolean = {
    val ask = !asking && spare.isEmpty && left <= poolSize * askWhenLeft
    if (ask) asking = true
    ask
  }

  def received(from: Long, size: Long): Unit = {
    asking = false
    poolSize = size
    if (left == 0) {
      next = from
      left = size
    } else spare = Some((from, size))
  }

  /** The pool asked for did not come; the next `shouldAsk` asks again. */
  def failed(): Unit = asking = false
}

package io.reactivecqrs.core.util

object RandomUtil {

  private val alphanumerics = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
  private val alphanumericsLength = alphanumerics.length

  def generateRandomString(length: Int): String = {
    val random = new scala.util.Random(System.nanoTime)
    val sb = new StringBuilder(length)

    for (i <- 0 until length) {
      sb.append(alphanumerics(random.nextInt(alphanumericsLength)))
    }
    sb.toString
  }

}

package io.reactivecqrs.core.util

import java.util.concurrent.ThreadLocalRandom

/** Random strings, such as the ids under which errors are logged (`CommandHandlingError.errorId`). */
object RandomUtil {

  private val alphanumerics = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

  /** `length` random letters and digits. */
  def generateRandomString(length: Int): String = {
    val random = ThreadLocalRandom.current()
    val sb = new StringBuilder(length)
    for (_ <- 0 until length) sb.append(alphanumerics.charAt(random.nextInt(alphanumerics.length)))
    sb.toString
  }
}

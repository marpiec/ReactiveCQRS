package io.reactivecqrs.core.util

import java.sql.{BatchUpdateException, SQLException}
import java.time.format.DateTimeParseException

import io.mpjsons.impl.JsonInnerException
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory

/**
  * F191: what the library logs, stores and answers about a failure. Some exceptions carry the values they failed on: a
  * `MatchError`'s message is the whole unmatched value (an event, a state, an order), PostgreSQL's errors quote rows and
  * statements' parameters, mpjsons quotes the JSON it read, parsers quote their input. Those values are personal data
  * that no erasure (D76) reaches once they are in a log, a reply or `subscription_failures`. Their messages are reduced
  * here to what names the failure: classes, SQLStates, constraints, positions. Any other message is kept, cut to
  * `MaxLength` characters: a host's own exceptions must not carry personal data either. With `LogValues` on, for
  * diagnosis only, the whole exception is logged too, on the logger `io.reactivecqrs.detail`.
  */
object Errors {

  /** The longest message kept. */
  val MaxLength = 1000

  private val MaxCauses = 5
  private val detail = LoggerFactory.getLogger("io.reactivecqrs.detail")

  /** One line: the exception's class and what names the failure, and the same for its causes. */
  def describe(e: Throwable): String = {
    val chain = causes(e)
    if (chain.exists(withholds)) logDetail(e)
    bounded(chain.map(line).mkString("; caused by "))
  }

  /** The message `describe` gives `e` itself, without its class's name and its causes; null when it has none. */
  def message(e: Throwable): String = {
    if (withholds(e)) logDetail(e)
    safeMessage(e).map(bounded).orNull
  }

  /** `e` as the library logs it: the same classes' names and stack traces, with their messages described as
    * `describe` does; `e` itself when nothing in it is withheld or cut. */
  def redacted(e: Throwable): Throwable = {
    val chain = causes(e)
    if (!chain.exists(c => withholds(c) || Option(c.getMessage).exists(_.length > MaxLength))) e
    else {
      logDetail(e)
      rebuilt(chain)
    }
  }

  private def logDetail(e: Throwable): Unit = if (LogValues.enabled) detail.info("A failure described without its values, in full", e)

  private def rebuilt(chain: List[Throwable]): Throwable = chain match {
    case Nil => null
    case head :: rest =>
      val redacted = new RedactedException(bounded(line(head)), rebuilt(rest))
      redacted.setStackTrace(head.getStackTrace)
      redacted
  }

  // The exception and its causes, the next exception of a JDBC batch included; an mpjsons error ends the chain, as its
  // cause is the parser's own.
  private def causes(e: Throwable): List[Throwable] = {
    def from(t: Throwable, depth: Int, seen: Set[Throwable]): List[Throwable] =
      if (t == null || depth >= MaxCauses || seen.contains(t)) Nil
      else t match {
        case _: JsonInnerException => List(t)
        case sql: SQLException if sql.getCause == null => t :: from(sql.getNextException, depth + 1, seen + t)
        case _ => t :: from(t.getCause, depth + 1, seen + t)
      }
    from(e, 0, Set.empty)
  }

  private def withholds(e: Throwable): Boolean = e match {
    case _: MatchError | _: SQLException | _: JsonInnerException | _: NumberFormatException | _: DateTimeParseException => true
    case _ => false
  }

  private def line(e: Throwable): String = e match {
    case redacted: RedactedException => redacted.getMessage
    case _ => e.getClass.getName + safeMessage(e).map(": " + _).getOrElse("")
  }

  private def safeMessage(e: Throwable): Option[String] = e match {
    case m: MatchError => Some(OfClass.findFirstMatchIn(m.getMessage).map("a value of class " + _.group(1)).getOrElse("a null value"))
    case p: PSQLException => Some(postgres(p))
    case b: BatchUpdateException => Some("SQLState " + b.getSQLState + " in a batch")
    case s: SQLException => Some("SQLState " + s.getSQLState)
    case j: JsonInnerException => Some(json(j))
    case _: NumberFormatException | _: DateTimeParseException => Some("the input it could not parse is not shown")
    case other => Option(other.getMessage)
  }

  private val OfClass = """\(of class ([^)]+)\)$""".r

  // A PostgreSQL error by its SQLState and the objects it names; the library's procedures raise their own messages
  // (P0001), which name ids and versions only. A connection's errors carry no server message and no values.
  private def postgres(p: PSQLException): String = Option(p.getServerErrorMessage) match {
    case None => p.getMessage
    case Some(server) if p.getSQLState == "P0001" => "SQLState P0001: " + server.getMessage
    case Some(server) => "SQLState " + p.getSQLState +
      Option(server.getConstraint).map(", constraint " + _).getOrElse("") +
      Option(server.getTable).map(", table " + _).getOrElse("") +
      Option(server.getColumn).map(", column " + _).getOrElse("")
  }

  // mpjsons: "Problem deserializing: <type>", "Problem: <cause>", "At position <n> of <length>:", then the JSON around it.
  private def json(j: JsonInnerException): String = {
    val lines = Option(j.getMessage).getOrElse("").split('\n')
    (lines.headOption.toSeq ++ Option(j.getCause).map("problem: " + _.getClass.getName) ++
      lines.find(_.startsWith("At position")).map(_.stripSuffix(":"))).mkString("; ")
  }

  private def bounded(text: String): String = if (text.length > MaxLength) text.take(MaxLength) + "…" else text
}

/** An exception as the library logs it (`Errors.redacted`): it prints as the original's class and described message,
  * with the original's stack trace and causes. */
final class RedactedException private[util] (line: String, cause: Throwable) extends RuntimeException(line, cause) {
  override def toString: String = line
}

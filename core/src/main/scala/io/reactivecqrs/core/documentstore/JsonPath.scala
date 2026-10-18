package io.reactivecqrs.core.documentstore

/**
  * F210: a path into a document, as the SQL literal of a PostgreSQL text array (`'{a,b}'`). Each element is written as
  * PostgreSQL writes it back (quoted only when it must be: empty, `NULL`, or holding a brace, a comma, a quote, a
  * backslash or a space), so an index's definition reads as the store made it; the literal's quotes are doubled. No
  * element can end the literal or split into two: `a,b` is one key, and `x}' OR '1'='1` is a key too.
  */
private[documentstore] object JsonPath {

  def literal(path: Seq[String]): String = "'" + path.map(element).mkString("{", ",", "}").replace("'", "''") + "'"

  private def element(name: String): String =
    if (name.isEmpty || name.equalsIgnoreCase("NULL") || name.exists(c => "{}\",\\ \t\n\r\u000b\f".indexOf(c.toInt) >= 0))
      "\"" + name.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
    else name
}

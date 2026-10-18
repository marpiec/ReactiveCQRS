package io.reactivecqrs.core.util

import scalikejdbc.SettingsProvider

/**
  * F190: how the library's statements are logged. ScalikeJDBC logs a statement that fails at ERROR, and every statement
  * at DEBUG, with the values of its parameters: events, documents, saga orders, personal data that no erasure (D76)
  * reaches once they are in a log. The library's sessions run with both off; the exception still reaches the code that
  * ran the statement, which logs it without its values (`Errors`). With `LogValues` on, ScalikeJDBC logs as configured.
  * Every file of the library that opens a session imports `SqlLogging.withoutValues` (`LogPrivacySpec` checks it).
  */
object SqlLogging {

  /** The settings of the sessions the library opens. */
  implicit val withoutValues: SettingsProvider =
    if (LogValues.enabled) SettingsProvider.default
    else SettingsProvider.default.copy(loggingSQLErrors = _ => false, loggingSQLAndTime = _.copy(enabled = false))
}

/** F190, F191: whether the library logs the values its failures carry (statements' parameters, the whole exceptions),
  * for diagnosis only: the system property `reactivecqrs.log-values=true`, read at start. Off by default, whatever the
  * log levels: those lines carry personal data. */
object LogValues {
  val enabled: Boolean = sys.props.get("reactivecqrs.log-values").contains("true")
}

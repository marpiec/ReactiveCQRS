package io.reactivecqrs.core.util

import java.util.concurrent.ConcurrentHashMap

import org.slf4j.LoggerFactory
import scalikejdbc.{DB, DBSession}
import io.reactivecqrs.core.util.SqlLogging.withoutValues

/**
 * Keeps in-memory caches from getting ahead of the database.
 *
 * Inside [[AfterCommit.localTx]], actions registered with `AfterCommit(session) { ... }` are queued
 * and run once the transaction has committed; a rollback drops them. For any other session
 * (auto-commit, read-only, a transaction started elsewhere, or no session) they run immediately,
 * because there is nothing to wait for or the commit is out of this module's sight.
 */
object AfterCommit {

  private val log = LoggerFactory.getLogger(AfterCommit.getClass)

  /** A transaction's queued actions and its values (`local`), guarded by the transaction itself. */
  private final class Open {
    var actions = List[() => Unit]()
    val locals = new java.util.IdentityHashMap[AnyRef, Any]()
  }

  // Keyed by session identity: two transactions on the same pooled connection are equal as values. F219: a concurrent
  // map, so transactions take no lock in common.
  private final class Key(val session: DBSession) {
    override def hashCode: Int = System.identityHashCode(session)
    override def equals(other: Any): Boolean = other match {
      case key: Key => key.session eq session
      case _ => false
    }
  }
  private val open = new ConcurrentHashMap[Key, Open]()

  /** A local transaction whose `AfterCommit` actions run after it commits. */
  def localTx[A](block: DBSession => A): A = {
    var key: Key = null
    try {
      val result = DB.localTx { session =>
        key = new Key(session)
        open.put(key, new Open)
        block(session)
      }
      val committed = open.remove(key)
      committed.synchronized(committed.actions).reverse.foreach { action =>
        // The data is committed at this point: a failing cache update must not look like a failed transaction.
        try action() catch {
          case e: Exception => log.error("Action after commit failed; the cache may miss an update until it is reloaded", Errors.redacted(e))
        }
      }
      result
    } finally {
      if (key != null) open.remove(key)
    }
  }

  /** A value of `owner`'s that lives as long as the session's transaction (made by `create` on first use), for what the
    * transaction changed but its caches show only after the commit; `None` when the session is not such a transaction. */
  def local[T](session: DBSession, owner: AnyRef)(create: => T): Option[T] =
    Option(open.get(new Key(session))).map(tx => tx.synchronized {
      Option(tx.locals.get(owner)).getOrElse {
        val value = create
        tx.locals.put(owner, value)
        value
      }.asInstanceOf[T]
    })

  /** Runs `action` after the session's transaction commits, or now if the session is not such a transaction. */
  def apply(session: DBSession)(action: => Unit): Unit = Option(open.get(new Key(session))) match {
    case Some(tx) => tx.synchronized(tx.actions ::= (() => action))
    case None => action
  }
}

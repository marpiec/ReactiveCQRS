package io.reactivecqrs.core.util

import org.slf4j.LoggerFactory
import scalikejdbc.{DB, DBSession}

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

  // Keyed by session identity: two transactions on the same pooled connection are equal as values.
  private val queued = new java.util.IdentityHashMap[DBSession, List[() => Unit]]()
  // Values kept for one transaction while it runs (`local`), by session and then by key.
  private val locals = new java.util.IdentityHashMap[DBSession, java.util.IdentityHashMap[AnyRef, Any]]()

  /** A local transaction whose `AfterCommit` actions run after it commits. */
  def localTx[A](block: DBSession => A): A = {
    var transaction: DBSession = null
    try {
      val result = DB.localTx { session =>
        transaction = session
        queued.synchronized {
          queued.put(session, Nil)
          locals.put(session, new java.util.IdentityHashMap[AnyRef, Any]())
        }
        block(session)
      }
      runQueued(transaction)
      result
    } finally {
      if (transaction != null) {
        queued.synchronized {
          queued.remove(transaction)
          locals.remove(transaction)
        }
      }
    }
  }

  /** A value of `owner`'s that lives as long as the session's transaction (made by `create` on first use), for what the
    * transaction changed but its caches show only after the commit; `None` when the session is not such a transaction. */
  def local[T](session: DBSession, owner: AnyRef)(create: => T): Option[T] = queued.synchronized {
    Option(locals.get(session)).map(values => Option(values.get(owner)).getOrElse {
      val value = create
      values.put(owner, value)
      value
    }.asInstanceOf[T])
  }

  /** Runs `action` after the session's transaction commits, or now if the session is not such a transaction. */
  def apply(session: DBSession)(action: => Unit): Unit = {
    val deferred = queued.synchronized {
      Option(queued.get(session)) match {
        case Some(actions) =>
          queued.put(session, (() => action) :: actions)
          true
        case None => false
      }
    }
    if (!deferred) {
      action
    }
  }

  private def runQueued(transaction: DBSession): Unit = {
    val actions = queued.synchronized(Option(queued.remove(transaction)).getOrElse(Nil)).reverse
    actions.foreach { action =>
      // The data is committed at this point: a failing cache update must not look like a failed transaction.
      try action() catch {
        case e: Exception => log.error("Action after commit failed; the cache may miss an update until it is reloaded", e)
      }
    }
  }
}

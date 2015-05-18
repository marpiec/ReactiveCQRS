package io.reactivecqrs.persistance.postgres

import akka.persistence._
import akka.persistence.journal._
import akka.persistence.snapshot._

import scala.collection.immutable.Seq
import scala.concurrent.Future

import scalikejdbc._

class PostgresSyncJournal extends SyncWriteJournal {

  val journalTableName = "events"

  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs")

  override def writeMessages(messages: Seq[PersistentRepr]): Unit = {

    sql"""INSERT INTO $journalTableName (persistence_id, sequence_number, marker, message, created)
         | VALUES ()""".stripMargin

  }

  override def deleteMessagesTo(persistenceId: String, toSequenceNr: Long, permanent: Boolean): Unit = ???

  @deprecated("deleteMessages will be removed.")
  override def deleteMessages(messageIds: Seq[PersistentId], permanent: Boolean): Unit = ???

  @deprecated("writeConfirmations will be removed, since Channels will be removed.")
  override def writeConfirmations(confirmations: Seq[PersistentConfirmation]): Unit = ???

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = ???

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] = ???
}

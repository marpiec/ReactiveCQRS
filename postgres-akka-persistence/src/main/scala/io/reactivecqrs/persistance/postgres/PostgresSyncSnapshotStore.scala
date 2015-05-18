package io.reactivecqrs.persistance.postgres

import akka.persistence._
import akka.persistence.journal._
import akka.persistence.snapshot._

import scala.concurrent.Future

class PostgresSyncSnapshotStore extends SnapshotStore {
  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = ???

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = ???

  override def saved(metadata: SnapshotMetadata): Unit = ???

  override def delete(metadata: SnapshotMetadata): Unit = ???

  override def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Unit = ???
}

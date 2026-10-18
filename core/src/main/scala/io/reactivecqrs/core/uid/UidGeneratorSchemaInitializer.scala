package io.reactivecqrs.core.uid

import io.reactivecqrs.core.util.SchemaLock

import scalikejdbc._
import io.reactivecqrs.core.util.SqlLogging.withoutValues

/**
  * The sequences behind the id pools: one value per pool, stepping by the pool size. F23: pools of 10 000, fetched ahead.
  * A sequence made with the old step of 100 is changed at start: the next pool then starts past every pool handed out,
  * and a node still running with the old pool size uses only the start of the pools it gets, so no id is given twice.
  */
class UidGeneratorSchemaInitializer {

  private val poolSize = 10000

  /** Creates the sequences, when missing, and sets their step. */
  def initSchema(): Unit = SchemaLock {
    DB.autoCommit { implicit session =>
      // The first 1000 aggregate ids are reserved for system ids.
      initSequence("aggregates_uids_seq", start = 1001)
      initSequence("commands_uids_seq", start = 1)
      initSequence("sagas_uids_seq", start = 1)
    }
  }

  private def initSequence(name: String, start: Long)(implicit session: DBSession): Unit = {
    SQL(s"CREATE SEQUENCE IF NOT EXISTS $name INCREMENT BY $poolSize START $start").execute()
    SQL(s"ALTER SEQUENCE $name INCREMENT BY $poolSize").execute()
  }
}

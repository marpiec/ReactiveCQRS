package io.reactivecqrs.examples.e1

// E1's first program (chapter 2): the connection pool and the tables, and nothing else.

// #check
import io.mpjsons.MPJsons
import io.reactivecqrs.core.commandhandler.PostgresCommandResponseState
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.saga.PostgresSagaState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

object SetupCheck {

  final case class Tables(eventStore: PostgresEventStoreState, responses: PostgresCommandResponseState,
                          sagaState: PostgresSagaState, subscriptions: PostgresSubscriptionsState)

  def main(args: Array[String]): Unit = {
    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs",
      ConnectionPoolSettings(initialSize = 5, maxSize = 32, connectionTimeoutMillis = 3000L))
    try {
      initTables()
      println("The tables are ready.")
    } finally {
      ConnectionPool.closeAll()
    }
  }

  def initTables(): Tables = {
    // #tables
    val mpjsons = new MPJsons

    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()

    val sagaState = new PostgresSagaState(mpjsons, typesNames)
    sagaState.initSchema()   // this one returns nothing, so it takes a line of its own

    val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
    // #tables
    Tables(eventStore, responses, sagaState, subscriptions)
  }
}
// #check

package io.reactivecqrs.core.db.eventbus

import akka.actor.ActorRef
import akka.serialization.Serialization
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.EventsBusActor.MessageToSend
import scalikejdbc._



class EventBus(serialization: Serialization) {



  val settings = ConnectionPoolSettings(
    initialSize = 5,
    maxSize = 20,
    connectionTimeoutMillis = 3000L)

  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs", settings)

  def initSchema(): Unit = {
    (new EventBusSchemaInitializer).initSchema()
  }

  def persistMessages[MESSAGE <: AnyRef](messages: Seq[MessageToSend]): Unit = {
    DB.autoCommit {implicit session =>
      //TODO optimize, make it one query
      messages.foreach { message =>
        sql"""INSERT INTO messages_to_send (id, aggregate_id, version, message_time, subscriber, message)
             |VALUES (NEXTVAL('messages_to_send_seq'), ?, ?, current_timestamp, ?, ?)""".stripMargin
          .bind(message.aggregateId.asLong, message.version.asInt, message.subscriber.path.toString,
            serialization.serialize(message.message).get)
          .executeUpdate().apply()
      }
    }

  }

  def deleteSentMessage(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion): Unit = {
    // TODO optimize SQL query so it will be one query
    DB.autoCommit { implicit session =>
      sql"""DELETE FROM messages_to_send WHERE aggregate_id = ? AND version = ? AND subscriber = ?"""
        .bind(aggregateId.asLong, version.asInt, subscriber.path.toString)
        .executeUpdate().apply()
    }
  }


}

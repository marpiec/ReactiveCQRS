package io.reactivecqrs.core.eventbus

import java.io.ByteArrayInputStream

import akka.actor.ActorRef
import akka.serialization.Serialization
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.eventbus.EventsBusActor.MessageToSend
import scalikejdbc._


class PostgresEventBusState(serialization: Serialization) extends EventBusState {

  val started = System.currentTimeMillis()
  var counter = 0

  def initSchema(): Unit = {
    (new EventBusSchemaInitializer).initSchema()
  }

  def persistMessages[MESSAGE <: AnyRef](messages: Seq[MessageToSend]): Unit = {
    DB.autoCommit {implicit session =>
      //TODO optimize, make it one query
      messages.foreach { message =>
        sql"""INSERT INTO events_to_route (id, aggregate_id, version, message_time, subscriber, message)
             |VALUES (NEXTVAL('events_to_route_seq'), ?, ?, current_timestamp, ?, ?)""".stripMargin
          .bind(message.aggregateId.asLong, message.version.asInt, message.subscriber.path.toString,
            new ByteArrayInputStream(serialization.serialize(message.message).get))
          .executeUpdate().apply()
      }
    }
    counter += messages.size

  }

  def deleteSentMessage(subscriber: ActorRef, aggregateId: AggregateId, version: AggregateVersion): Unit = {
    // TODO optimize SQL query so it will be one query
    DB.autoCommit { implicit session =>
      sql"""DELETE FROM events_to_route WHERE aggregate_id = ? AND version = ? AND subscriber = ?"""
        .bind(aggregateId.asLong, version.asInt, subscriber.path.toString)
        .executeUpdate().apply()
    }
    counter -= 1
    if(counter == 0) {
      println("All messages removed " + (System.currentTimeMillis() - started))
    }

  }


}

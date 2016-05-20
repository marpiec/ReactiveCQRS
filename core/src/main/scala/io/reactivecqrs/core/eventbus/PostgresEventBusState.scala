package io.reactivecqrs.core.eventbus

import java.io.ByteArrayInputStream

import akka.serialization.Serialization
import io.reactivecqrs.core.eventbus.EventsBusActor.{MessageAck, MessageToSend}
import scalikejdbc._


class PostgresEventBusState(serialization: Serialization) extends EventBusState {

  val started = System.currentTimeMillis()
  var counter = 0

  def initSchema(): Unit = {
    (new EventBusSchemaInitializer).initSchema()
  }

  def persistMessages[MESSAGE <: AnyRef](messages: Seq[MessageToSend]): Unit = {
    DB.autoCommit {implicit session =>
      val batchParams = messages.map(m => Seq(m.aggregateId.asLong, m.version.asInt, m.subscriber.path.toString,
        new ByteArrayInputStream(serialization.serialize(m.message).get)))

      sql"""INSERT INTO events_to_route (id, aggregate_id, version, message_time, subscriber, message)
            |VALUES (NEXTVAL('events_to_route_seq'), ?, ?, current_timestamp, ?, ?)""".stripMargin
        .batch(batchParams: _*).apply()
    }
    counter += messages.size

  }

  def deleteSentMessage(messages: Seq[MessageAck]): Unit = {
    // TODO optimize SQL query so it will be one query
    DB.autoCommit { implicit session =>
      val batchParams = messages.map(m => Seq(m.aggregateId.asLong, m.version.asInt, m.subscriber.path.toString))
      sql"""DELETE FROM events_to_route WHERE aggregate_id = ? AND version = ? AND subscriber = ?"""
        .batch(batchParams: _*).apply()
    }
    counter -= messages.length
    if(counter == 0) {
      println(messages.size + " of " + (counter + messages.size) + " messages removed " + (System.currentTimeMillis() - started))
    }

  }

  override def countMessages: Int = {
    DB.readOnly { implicit session =>
      sql"""SELECT COUNT(*) FROM events_to_route""".map(rs => rs.int(1)).single().apply().get
    }
  }

}

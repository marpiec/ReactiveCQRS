package io.reactivecqrs.core.eventbus

import java.io.ByteArrayInputStream

import akka.serialization.Serialization
import io.reactivecqrs.api.AggregateVersion
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.eventbus.EventsBusActor.{EventAck, EventToRoute}
import scalikejdbc._


class PostgresEventBusState(serialization: Serialization) extends EventBusState {

  def initSchema(): Unit = {
    (new EventBusSchemaInitializer).initSchema()
  }

  def persistMessages[MESSAGE <: AnyRef](messages: Seq[EventToRoute]): Unit = {
    DB.autoCommit {implicit session =>

      val batchParams = messages.map(m => {
        val subscriberPath = m.subscriber match {
          case Left(s) => s.path.toString
          case Right(s) => s
        }
        Seq(m.aggregateId.asLong, m.version.asInt, subscriberPath,
          m.message.getClass.getName, new ByteArrayInputStream(serialization.serialize(m.message).get))
      })

      sql"""INSERT INTO events_to_route (id, aggregate_id, version, message_time, subscriber, message_type, message)
            |VALUES (NEXTVAL('events_to_route_seq'), ?, ?, current_timestamp, ?, ?, ?)""".stripMargin
        .batch(batchParams: _*).apply()
    }
  }

  def deleteSentMessage(messages: Seq[EventAck]): Unit = {
    // TODO optimize SQL query so it will be one query
    DB.autoCommit { implicit session =>
      val batchParams = messages.map(m => Seq(m.aggregateId.asLong, m.version.asInt, m.subscriber.path.toString))
      sql"""DELETE FROM events_to_route WHERE aggregate_id = ? AND version = ? AND subscriber = ?"""
        .batch(batchParams: _*).apply()
    }
  }

  override def countMessages: Int = {
    DB.readOnly { implicit session =>
      sql"""SELECT COUNT(*) FROM events_to_route""".map(rs => rs.int(1)).single().apply().get
    }
  }

  override def readAllMessages(handler: EventToRoute => Unit) {
    DB.readOnly { implicit session =>
      sql"""SELECT subscriber, aggregate_id, version, message_type, message FROM events_to_route""".foreach { rs =>
        val message = serialization.deserialize(rs.bytes(5), Class.forName(rs.string(4)))
        handler(EventToRoute(Right(rs.string(1)), AggregateId(rs.long(2)), AggregateVersion(rs.int(3)), message))
      }
    }
  }

}

package io.reactivecqrs.core.serialization

import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import org.scalatest.{MustMatchers, BeforeAndAfter, GivenWhenThen, FeatureSpecLike}

class CommandSerializationSpec extends FeatureSpecLike with GivenWhenThen with BeforeAndAfter with MustMatchers {

  feature("Command responses serialization") {

    scenario("Standard command response serialization") {

      val serializer = new MPJsons

      serializer.markTypedClass[CommandResponse]


      val response = new SuccessResponse(AggregateId(1), AggregateVersion(1))

      val serialized: String = serializer.serialize[CommandResponse](response)


      val deserialized = serializer.deserialize[CommandResponse](serialized)


      response mustBe deserialized

    }

    scenario("Custom command response serialization") {

      val serializer = new MPJsons

      serializer.markTypedClass[CustomCommandResponse[_]]

      val response = new CustomSuccessResponse[String](AggregateId(1), AggregateVersion(1), "hello")

      val serialized: String = serializer.serialize[CustomSuccessResponse[String]](response)

      val deserialized = serializer.deserialize[CustomCommandResponse[String]](serialized)


      response mustBe deserialized

    }

  }
}

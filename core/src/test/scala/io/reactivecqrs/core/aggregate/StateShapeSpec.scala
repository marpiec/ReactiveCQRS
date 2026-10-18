package io.reactivecqrs.core.aggregate

import io.reactivecqrs.testkit.CommonSpec

case class ShapeItem(name: String)
case class ShapeItemWithPrice(name: String, price: Int)
case class ShapeHolder(items: Vector[ShapeItem], owner: Option[ShapeItem])
case class ShapeHolderOfPriced(items: Vector[ShapeItemWithPrice], owner: Option[ShapeItem])
case class ShapeHolderRenamed(things: Vector[ShapeItem], owner: Option[ShapeItem])

// F196: the shape a snapshot is stored with.
class StateShapeSpec extends CommonSpec {

  Feature("The shape of a state class") {

    Scenario("is stable, and changes with a field added, renamed or retyped anywhere in the state, through type arguments too") {
      StateShape.of(classOf[ShapeHolder].getName) mustBe StateShape.of(classOf[ShapeHolder].getName)
      StateShape.of(classOf[ShapeHolder].getName).length mustBe 32
      StateShape.describe(classOf[ShapeHolder]) must include(classOf[ShapeItem].getName + "{name:java.lang.String;}")
      StateShape.describe(classOf[ShapeHolderOfPriced]) must include(classOf[ShapeItemWithPrice].getName + "{name:java.lang.String;price:int;}")
      Set(classOf[ShapeHolder], classOf[ShapeHolderOfPriced], classOf[ShapeHolderRenamed]).map(c => StateShape.of(c.getName)).size mustBe 3
    }

    Scenario("is empty for a class that cannot be loaded") {
      StateShape.of("com.example.Missing") mustBe StateShape.Unknown
    }
  }
}

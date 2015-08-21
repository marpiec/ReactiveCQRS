package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.api.{AggregateVersion, DuplicationEvent, UndoEvent, Event}



case class ShoppingCartCreated(name: String) extends Event[ShoppingCart]

case class ShoppingCartDuplicated(baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion) extends DuplicationEvent[ShoppingCart]

case class ItemAdded(name: String) extends Event[ShoppingCart]

case class ItemRemoved(id: Int) extends Event[ShoppingCart]

case class ShoppingCartDeleted() extends Event[ShoppingCart]

case class ShoppingCartChangesUndone(eventsCount: Int) extends UndoEvent[ShoppingCart]
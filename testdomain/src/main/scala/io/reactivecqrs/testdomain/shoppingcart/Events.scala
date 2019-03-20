package io.reactivecqrs.testdomain.shoppingcart

import io.reactivecqrs.api.id.{AggregateId, SpaceId}
import io.reactivecqrs.api.{AggregateVersion, DuplicationEvent, Event, UndoEvent}



case class ShoppingCartCreated(name: String) extends Event[ShoppingCart]

case class ShoppingCartDuplicated(spaceId: SpaceId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion) extends DuplicationEvent[ShoppingCart]

case class ItemAdded(name: String) extends Event[ShoppingCart]

case class ItemRemoved(id: Int) extends Event[ShoppingCart]

case class ShoppingCartDeleted() extends Event[ShoppingCart]

case class ShoppingCartChangesUndone(eventsCount: Int) extends UndoEvent[ShoppingCart]
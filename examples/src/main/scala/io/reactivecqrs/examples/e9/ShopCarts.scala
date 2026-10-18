package io.reactivecqrs.examples.e9

// E9's aggregate (chapter 16): a cart that belongs to a shop, an organization, and remembers it in its state.

// #domain
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}

case class ShopCart(shop: SpaceId, name: String, items: Vector[String])

case class ShopCartOpened(spaceId: SpaceId, name: String) extends FirstEvent[ShopCart]
case class ShopItemAdded(name: String) extends Event[ShopCart]
case class ShopCartSquashed(spaceId: SpaceId, name: String, items: Vector[String]) extends FirstEvent[ShopCart]

case class OpenShopCart(userId: UserId, shop: SpaceId, name: String)
  extends FirstCommand[ShopCart, CustomCommandResponse[_]]
case class AddShopItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, name: String)
  extends Command[ShopCart, CustomCommandResponse[_]]

class ShopCartContext extends AggregateContext[ShopCart] {
  override val version: Int = 1
  override def initialAggregateRoot: ShopCart = ShopCart(SpaceId.unknown, "", Vector.empty)

  override def commandHandlers = _ => {
    case c: OpenShopCart => CommandSuccess(ShopCartOpened(c.shop, c.name))
    case c: AddShopItem => CommandSuccess(ShopItemAdded(c.name))
  }

  override def eventHandlers = (_, _, cart) => {
    case e: ShopCartOpened => ShopCart(e.spaceId, e.name, Vector.empty)
    case e: ShopItemAdded => cart.copy(items = cart.items :+ e.name)
    case e: ShopCartSquashed => ShopCart(e.spaceId, e.name, e.items)
  }

  // A squash event is a first event too: it names the cart's own shop, which the state remembers.
  override val squash: Option[SquashPolicy[ShopCart]] =
    Some(SquashPolicy(cart => ShopCartSquashed(cart.shop, cart.name, cart.items)))
}
// #domain

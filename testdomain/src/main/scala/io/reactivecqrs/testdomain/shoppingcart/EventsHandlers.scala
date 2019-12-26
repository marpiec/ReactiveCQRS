package io.reactivecqrs.testdomain.shoppingcart


object EventsHandlers {
  def shoppingCartCreated(event: ShoppingCartCreated): ShoppingCart = {
    ShoppingCart(event.name, items = Vector())
  }
  
  def itemAdded(shoppingCart: ShoppingCart, event: ItemAdded): ShoppingCart = {
    val itemId = shoppingCart.items.foldLeft(0)((maxId, item) => math.max(maxId, item.id)) + 1
    val item = Item(itemId, event.name)
    shoppingCart.copy(items = shoppingCart.items :+ item)
  }
  
  def itemRemoved(shoppingCart: ShoppingCart, event: ItemRemoved): ShoppingCart = {
    shoppingCart.copy(items = shoppingCart.items.filterNot(_.id == event.id))
  }

  def cartNameRewritten(shoppingCart: ShoppingCart, event: CartNameRewritten): ShoppingCart = {
    shoppingCart.copy(name = event.name)
  }

}
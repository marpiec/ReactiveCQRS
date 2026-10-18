package io.reactivecqrs.examples.e2

import io.reactivecqrs.api.Placement
import io.reactivecqrs.examples.e1.ShoppingCartContext

// E2's carts (chapter 17): E1's carts, with one owner per cart in the whole cluster.

// #sharded
class ShardedCartContext extends ShoppingCartContext {
  override val placement: Placement = Placement.Sharded(numberOfShards = 30) // about ten per node of three
}
// #sharded

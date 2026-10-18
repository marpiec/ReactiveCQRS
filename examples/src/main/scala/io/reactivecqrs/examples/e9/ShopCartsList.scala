package io.reactivecqrs.examples.e9

// E9's read model (chapter 16): each shop's carts, whose documents carry their shop's space.

// #list
import io.reactivecqrs.api.id.SpaceId
import io.reactivecqrs.core.documentstore.{Document, DocumentStore, ExpectedSingleLongValue}
import io.reactivecqrs.core.projection.{ProjectionActor, RebuildTarget, SubscriptionsState}
import scalikejdbc.DBSession

case class ShopCartSummary(shop: Long, name: String)
case class GetShopCarts(shop: SpaceId)

class ShopCartsList(val subscriptionsState: SubscriptionsState, documentStore: DocumentStore[ShopCartSummary])
  extends ProjectionActor {

  protected val listeners = List(EventsListener[ShopCart] { (aggregateId, events) => implicit session =>
    events.foreach(_.event match {
      case ShopCartOpened(shop, name) =>
        documentStore.insertDocument(shop.asLong, aggregateId.asLong, ShopCartSummary(shop.asLong, name))
      case ShopCartSquashed(shop, name, _) => // what a rebuild meets first for a squashed cart
        documentStore.updateDocument(shop.asLong, aggregateId.asLong, _ => Document(ShopCartSummary(shop.asLong, name)))
      case _ => ()
    })
  })

  // What a targeted rebuild clears first (chapter 10): one organization is one call.
  override protected def clearForRebuild(target: RebuildTarget)(implicit session: DBSession): Unit = target match {
    case RebuildTarget.OneAggregate(id) => documentStore.removeDocument(id.asLong)
    case RebuildTarget.OneSpace(space) => documentStore.clearSpace(space.asLong)
    case RebuildTarget.OneType(_) => documentStore.clearAllData()
  }

  override protected def receiveQuery: Receive = {
    case GetShopCarts(shop) =>
      sender() ! documentStore.findDocumentByPath(ExpectedSingleLongValue(Seq("shop"), shop.asLong)).values.map(_.document).toVector
  }

  override protected val projectionName: String = "ShopCartsList"
}
// #list

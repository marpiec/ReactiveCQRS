# Chapter 16. Organizations (Spaces)

Welcome to Part VI, where your application grows up. Most business applications serve more than
one customer organization from a single installation: shops, firms, schools, clubs. By the end of
this chapter you will know how ReactiveCQRS keeps track of which organization every aggregate
belongs to, how to make your read models carry that knowledge so one organization's data can be
cleared and rebuilt on its own, which one-off steps an installation with older data runs, and how to
remove an organization for good.

---

## Many shops, one building

Think of a shopping mall. Many shops live under one roof: they share the building, the electricity
and the security guards, but each shop's stock, tills and receipts are its own. When one shop
re-arranges its shelves, the others do not notice; when one shop moves out, it empties its own unit,
not the whole mall.

In ReactiveCQRS the building is your installation (your application and its database), and each shop
is an **organization**, called a **space** and written as a `SpaceId`. The mall's own management
office is a space too: **space 0**, the platform, for aggregates that belong to no customer
(settings, the list of organizations, templates everyone may copy).

```scala
import io.reactivecqrs.api.id.SpaceId

val platform = SpaceId(0)
val cornerShop = SpaceId(42)  // cornerShop.asLong == 42
```

There is also `SpaceId.unknown` (-1), which means "no space". You will meet it only in old data.

> **Tip:** if organizations are aggregates in your application, `SpaceId(organizationId)` turns an
> organization's `AggregateId` into its space, so both are the same number.

## Every aggregate belongs to a space

An aggregate gets its space once, from its first event, when it is created. That is why every
`FirstEvent` has a `spaceId` ([chapter 6](06-events.md)). In the test domain every cart belongs to
the platform:

<!-- snippet: shoppingcart/Events.scala#created -->
```scala
case class ShoppingCartCreated(name: String) extends FirstEvent[ShoppingCart] {
  def spaceId: SpaceId = SpaceId(0)
}
```

The library stores the space with the aggregate when the first event is written, and keeps it for
the aggregate's whole life. Later events do not repeat it.

### No space, no aggregate

A new aggregate whose first event gives a negative space, such as `SpaceId.unknown`, is refused
before anything is written: the command is answered with a `FailureResponse` whose message starts
"A new aggregate needs a space", names the event and the space it gave, and suggests `SpaceId(0)`
for the platform. The aggregate is not created.

> **Remember:** there is no default organization. Every first event decides, and for the platform's
> own aggregates it says `SpaceId(0)` out loud.

### Copies name their own space

A duplication event ([chapter 6](06-events.md)) is a first event too, so a copy names its *own*
space, which may differ from its original's. A template cart kept by the platform in space 0 can be
copied into shop 42, and the copy belongs to shop 42. In the test domain, `DuplicateShoppingCart`
makes `ShoppingCartDuplicated(SpaceId(0), baseAggregateId, baseAggregateVersion)`, so its copies
stay on the platform.

A copy keeps reading its original's history up to the version it was copied at, even when the two
live in different organizations. Keep that in mind; when an organization is removed, such copies are
detached first and keep their state (end of this chapter).

### A cart that remembers its shop

Chapter 6 sketched `CartOpened`, a first event whose space comes from the command. Let's take that
idea one step further in E9, the example of this chapter: a cart that belongs to a shop, and
*remembers* it in its state.

<!-- snippet: e9/ShopCarts.scala#domain -->
```scala
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
```

Sending `OpenShopCart(userId, SpaceId(42), "Groceries")` to this type's command bus is answered
`SuccessResponse(cartId, AggregateVersion(1))`, and from then on the cart belongs to shop 42.

Why keep the shop in the state, when the library already knows it? Because two of your own pieces
need the space long after the first event:

- the **squash event** ([chapter 14](14-squash.md)), which becomes the aggregate's first event and
  should name the same space; `toEvent` only gets the state, so the state must know it;
- **projections that listen to states** (`AggregateListener`, [chapter 8](08-projections.md)), which
  never see the first event itself.

> **Remember:** `EventMetadata` has a `spaceId` field too ([chapter 6](06-events.md)). It is an
> optional part of the metadata envelope that your commands may fill, for example to record from
> which organization someone acted. The library does not use it to place aggregates: only the first
> event does that.

> **Under the hood:** the space lives in the `aggregates` table (`space_id`), written by the stored
> procedure that creates the aggregate's row from its first event (or, for a copy, from its
> duplication event). The check that refuses a negative space runs in the aggregate's owner, before
> the transaction starts (decision D38). See
> [ARCHITECTURE.md, sections 2.2 and 4.1](../ARCHITECTURE.md).

## Read models carry the space too

The library knows every aggregate's space. Your read models must know it as well, if you ever want
to clear or rebuild one organization's documents without touching the others. Every document in a
document store has a space, set when the document is inserted ([chapter 8](08-projections.md)):

```scala
def insertDocument(spaceId: Long, key: Long, document: T)(implicit session: DBSession): Unit
def updateDocument(spaceId: Long, key: Long, modify: Option[Document[T]] => Document[T])(implicit session: DBSession): Document[T]
```

Note that the space is a plain `Long` here: pass `space.asLong`. `updateDocument` uses it only when
it has to insert a missing document; `overwriteDocument` and `updateExistingDocument` keep the space
the document already has.

The test domain's projections pass `0` everywhere, because every one of their carts lives in space
0. With shops, pass the shop. Here is a list of each shop's carts:

<!-- snippet: e9/ShopCartsList.scala#list -->
```scala
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
```

Started as a managed projection, the way [chapter 10](10-rebuilding-projections.md) shows, with an
index on the shop:

<!-- snippet: e9/OrganizationsSetup.scala#start-list -->
```scala
val shopCarts: ActorRef = projections.start[ShopCartsList]("shop-carts", 1, "shopCarts") { build =>
  new ShopCartsList(subscriptionsState, new PostgresDocumentStore[ShopCartSummary](build.documentStore("shop_carts"),
    mpjsons, new NoopDocumentStoreCache, Seq(MultipleLongIndex(1, Seq("shop")))))
}
```

Two things to notice:

- **The document's space is for clearing, not for asking.** The document store has no "find by
  space": its space column is what `clearSpace` and an organization's rebuild go by. To list one
  shop's carts, the document carries the shop as well (`ShopCartSummary.shop`), the query looks it up
  by that path, and the index on that path keeps the query fast.
- **The squash event is handled like a creation**, because a projection rebuilt after a squash meets
  it first ([chapter 14](14-squash.md)), and it brings the space along.

> **Warning:** the space you pass is the one `clearSpace` and an organization's rebuild will trust.
> A projection that writes `0` for every document, as the test domain's single-shop projections do,
> works fine until the day you clear or rebuild one organization: then it clears nothing, or, for
> space 0, everything. Pass the aggregate's real space from the start.

> **Remember:** `MemoryDocumentStore`, the in-memory store for tests, keeps each document's space as
> PostgreSQL does: set when the document is first written, kept after, and `clearSpace` removes that
> space's documents. A test of an organization's clean-up can run against it.

## Clearing and rebuilding one organization

With the space in every document, one organization's read models can be put right without touching
anyone else's. The tool is the targeted rebuild of [chapter 10](10-rebuilding-projections.md),
aimed at one space and sent to a managed projection's supervisor. E9 rebuilds the list of the space a
shop moved into (at the end of this chapter):

<!-- snippet: e9/OrganizationsSetup.scala#rebuild -->
```scala
val rebuilt = Await.result(installation.shopCarts.ask(RebuildAggregates(RebuildTarget.OneSpace(target)))(Timeout(10.minutes)),
  10.minutes) // a whole organization can take a while
// AggregatesRebuilt(RebuildTarget.OneSpace(target), aggregates), or RebuildRefused(reason)
```

The projection's `clearForRebuild(OneSpace(space))` clears that organization's documents (above),
the projection forgets how far it got with the organization's aggregates, and it receives their
events again from the store, in the order they were written. The other organizations' documents are
not touched, and the projection keeps serving queries meanwhile. The answer,
`AggregatesRebuilt(target, aggregates)`, says how many aggregates were rebuilt.

A rebuild of one organization is refused (`RebuildRefused`) while any aggregate of the projection's
types has no space yet: aggregates stored by older versions of the library may lack one, and the
reason tells you to run `backfillSpaces` first. That is the next section.

> **Under the hood:** the store finds an organization's events through a narrow table of keys,
> `event_keys`, which records each event's aggregate type and space. Once every older event has its
> key, rebuilding one organization reads that organization's events only, not the whole store
> (issue F124). See [ARCHITECTURE.md, section 8](../ARCHITECTURE.md).

## Upgrading an installation that already has data

An installation that has been running for a while may hold aggregates stored before every first
event had to name a space (their space is -1), documents written before documents had a space (also
-1), and events written before event keys existed. A few one-off jobs put that right. All of them
run online, while your application works, in small transactions, and all of them can be stopped and
run again: they go on from where they left off. [Chapter 18](18-production.md) lists them with the
other production steps; here is what they mean for organizations.

1. **Count what needs fixing.** `StoredDataCheck` reads your stored data on a copy of production
   ([chapter 6](06-events.md)), and its report includes the aggregates without a space:

   <!-- snippet: e9/OrganizationsSpec.scala#withoutSpace -->
   ```scala
   import io.reactivecqrs.core.tools.StoredDataCheck

   val report = StoredDataCheck.run(mpjsons, typesNames)
   println(report.aggregatesWithoutSpace)
   ```

2. **Build the indexes**, without blocking writes:

   <!-- snippet: e9/OrganizationsSpec.scala#buildIndexes -->
   ```scala
   eventStore.buildIndexesConcurrently()
   ```

   It builds the index restores read through, and the one that finds an organization's aggregates,
   with PostgreSQL's `CREATE INDEX CONCURRENTLY`. It does nothing when they exist, and builds again
   an index that an interrupted run left invalid. (A new or small database gets them as its schema is
   created.)

3. **Give old aggregates their space:**

   <!-- snippet: e9/OrganizationsSpec.scala#backfillSpaces -->
   ```scala
   import io.reactivecqrs.core.eventstore.SpaceBackfill

   val spaces: SpaceBackfill = eventStore.backfillSpaces()  // SpaceBackfill(filled, left)
   ```

   Every aggregate stored without a space gets the space of its first event (or of its squash
   event, if it was squashed), in batches of 1 000 aggregates (`batchSize`), one transaction each.
   Some old first events do not know their space: they say `SpaceId.unknown`. For those, pass a
   function that decides from the aggregate's type, its id and its first event:

   <!-- snippet: e9/OrganizationsSpec.scala#backfillByEvent -->
   ```scala
   val spaces = eventStore.backfillSpaces((aggregateType, aggregateId, firstEvent) => firstEvent match {
     case e: LegacyCartCreated => Some(SpaceId(e.shopNumber))
     case _ => None
   })
   ```

   (`LegacyCartCreated` stands for whatever old first event of yours keeps the organization in a
   field of its own; E9's spec has one, to compile and run these steps.) `filled` counts the aggregates that got a space; `left` lists those that
   neither the event nor your function could place: look at them by hand.

4. **Give old events their keys**, once *every* node runs the new version:

   <!-- snippet: e9/OrganizationsSpec.scala#backfillKeys -->
   ```scala
   import io.reactivecqrs.core.eventstore.EventKeysBackfill

   val keys: EventKeysBackfill = eventStore.backfillEventKeys()  // EventKeysBackfill(written, missing)
   ```

   It works through the events in batches of 10 000 (`batchSize`) and records when every event has
   its key; from then on, a rebuild of one organization, or a build of a few types, reads only its
   own events. Until then they read through the whole store, which is correct, just slow. `missing`
   counts events it could not key (those whose aggregate's row is missing).

5. **Rebuild the read models that wrote no real space**: documents written before documents had a
   space, or with `0` for everything. A full rebuild ([chapter 10](10-rebuilding-projections.md))
   writes them again, with the spaces your projection now passes.

> **Warning:** once `backfillEventKeys` has finished, do not go back to an older version of the
> library: older versions write events without keys, and builds would miss those events.

> **Tip:** run these jobs from a maintenance task or a background thread, never on your
> application's start-up path. On a large store they take a while, and none of them needs the
> application to wait.

## Removing an organization: the purge

A shop moves out of the shopping centre. Its shelves, its signs and its receipts in the back office
all go, the shops next door keep trading, and their customers notice nothing. When a customer
organization leaves your platform, or asks for its data to be erased, the **purge** does the same
for its space.

### Before you start

1. **Make the organization quiet.** Stop its users first: the library cannot hold back a command
   sent while the purge runs. (If one slips through and creates an aggregate, the purge notices and
   goes round again, up to three times.)
2. **Check your projections.** Every projection that holds the organization's documents must
   implement `clearForRebuild` for `RebuildTarget.OneSpace` ([chapter 10](10-rebuilding-projections.md)),
   and none may be rebuilding meanwhile. The purge asks each one first and refuses if one cannot
   take part.
3. **Think of your own tables.** Rows the library does not know about (say, receipts an outbox
   writer stores next to the events) need a small `PurgeHook` of yours, shown below.
4. **Tag your sagas.** A saga whose orders belong to an organization says so by overriding
   `spaceOf` ([chapter 12](12-sagas.md)); the purge removes the sagas tagged with the space.

### Running it

<!-- snippet: e9/OrganizationsSetup.scala#purge -->
```scala
val purge = new SpacePurge(installation.eventStore,
  commandBuses = Map(AggregateType.of[ShopCart] -> installation.carts), // this node's bus of every type the shop has
  projections = Seq(installation.shopCarts))                            // every projection with its documents
val purged = Await.result(purge.purge(source), 2.minutes)
```

Run it from one node, once. It answers a `PurgeReport`:

| Field | What it tells you |
|---|---|
| `complete` | true when nothing was refused, nothing failed and nothing of the space is left |
| `aggregates`, `rows` | how many aggregates went, and how many rows per table (a hook's under its name) |
| `detached` | the copies in *other* organizations that now stand on their own (below) |
| `projections`, `failed` | the projections that cleared the space, and whatever did not do its part, with why |
| `refused` | why it did not start; nothing was deleted |
| `passes`, `remaining` | how many rounds it took, and how many aggregates of the space are still stored |

### What it does, step by step

1. **It checks first.** Every type of the space needs a command bus in `commandBuses`, and every
   projection must be able to clear a space. If not, the report says why in `refused`, and nothing
   has changed. The platform's space (0) and space -1 (what was stored before spaces existed) are
   not organizations, so a purge of either is refused too.
2. **It detaches copies in other organizations.** Remember the template cart copied from the
   platform into shop 42? The other way round matters here: a copy that shop 7 made of one of shop
   42's carts still reads shop 42's history. Before that history goes, the copy gets a first event
   of its own, made with the type's squash event ([chapter 14](14-squash.md)) from its own state, so
   it keeps exactly what it had. A type without a squash event cannot do that, and the purge refuses.
3. **It deletes, in small bites.** At most 1 000 aggregates and 100 000 events at a time, each bite
   one transaction: the events, the archive, the outbox rows, the
   snapshots, the projections' bookkeeping, the stored answers of idempotent commands, the aggregate
   rows, and your hooks' rows. After each bite, the owners of those aggregates are stopped and every
   projection is told to forget them.
4. **It removes the space's sagas.**
5. **It clears the projections**: each one's `clearForRebuild(OneSpace(space))` runs, and the
   organization's documents are gone.

> **Tip:** every step can be repeated. A purge that was refused, failed halfway or did not complete
> is simply run again once the reason is fixed; it also clears the projections again, even when no
> aggregate is left to delete.

### Your own tables: `PurgeHook`

<!-- snippet: e9/OrganizationsSpec.scala#purgeHook -->
```scala
import io.reactivecqrs.api.id.{AggregateId, SpaceId}
import io.reactivecqrs.core.space.PurgeHook
import scalikejdbc._

val receiptsHook = new PurgeHook {
  override def name: String = "receipts"
  override def purge(space: SpaceId, aggregateIds: Seq[AggregateId])(implicit session: DBSession): Long =
    sql"DELETE FROM receipts WHERE cart_id IN (${aggregateIds.map(_.asLong)})".update().toLong
}
```

Pass it to the purge with the others (`new SpacePurge(..., hooks = Seq(receiptsHook))`). A hook runs
inside each bite's transaction, so its rows go together with the library's, or not at all.

### Checking that nothing is left

<!-- snippet: e9/OrganizationsSpec.scala#spaceRows -->
```scala
import io.reactivecqrs.core.tools.StoredDataCheck

val left = StoredDataCheck.spaceRows(42L) // Map(aggregates -> 0, event_keys -> 0, sagas -> 0, projection_shop_carts_v1 -> 0)
```

It counts what the space still has in the library's tables and in every read-model table. After a
complete purge, every count is zero.

> **Warning:** a few things are out of the purge's reach. On a cluster, an owner of a purged
> aggregate on *another* node keeps its state in memory until its next use, which finds nothing
> stored, or until it is put to sleep for being idle ([chapter 17](17-cluster.md)). Answers of
> idempotent commands stored by older versions of the library do not say which aggregate they
> belong to, so they wait for their retention to expire (30 days by default). Sagas without
> `spaceOf`, aggregates without a space (`backfillSpaces` first) and your tables without a hook
> stay too.

> **Under the hood:** right after a bite, a projection may still receive an event of a purged cart
> that was already on its way. For half an hour (`purgedDropWindow`) it acknowledges such deliveries
> without applying them, so nothing writes a document again; it keeps 8 bytes per purged aggregate
> meanwhile. See [ARCHITECTURE.md, section 4.7](../ARCHITECTURE.md) and the design in
> [docs/design/M6-organizations.md](../design/M6-organizations.md).

## Moving an organization to another installation

A shop moves to a shopping centre in another town. Its stock, its books and its history pack into
boxes, travel, and are unpacked in an empty unit there, where the new centre's signs are put up
for it. Moving an organization between installations is the same: an **export** packs the boxes,
an **import** unpacks them into an empty space of the other installation, and the read models are
rebuilt there.

### Packing: the export

<!-- snippet: e9/OrganizationsSetup.scala#export -->
```scala
val file = Files.createTempFile("shop-", ".jsonl")
val writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)
val exported = try new SpaceExport(installation.eventStore, contexts = Seq(new ShopCartContext)).write(source, writer)
               finally writer.close()
// ExportReport(space, aggregates, events, archived, detached, waitingInOutbox, refused)
```

The file holds one line of JSON per aggregate and per event, the events in the order they were
stored, plus the archived events ([chapter 14](14-squash.md)); snapshots, read models and sagas stay
behind. Export a quiet organization: `waitingInOutbox` tells you how many of its events were not
delivered yet.

`contexts` are the aggregate contexts of the organization's types. They matter for one case: a
copy that shop 42 made of somebody else's cart reads that other organization's history, which does
not travel. The export gives such a copy a first event of its own, made with its type's squash event
from its own state, just as the purge does, so it keeps exactly what it had. Without a squash event
(or without the context), the export refuses and writes nothing.

### Unpacking: the import

<!-- snippet: e9/OrganizationsSetup.scala#import -->
```scala
val imported = new SpaceImport(installation.eventStore, installation.uidGenerator).read(file, into = target)
// ImportReport(space, aggregates, events, archived, remapped, unknownReferences, someUnknown, refused)
```

It runs on the other installation, into a space that must be empty. It reads the file twice:

1. **It checks first**, writing nothing: every class in the file must exist here, every event must
   fit this installation's limits ([chapter 6](06-events.md)), and the space must be empty. Any
   problem ends up in `refused`.
2. **Then it writes**: the aggregates, then the events in their original order, with their undo
   marks, then the archived events.

Two installations made their ids in ranges of their own (the installation number of chapter 2), so
ids rarely clash; ids from before 1.0 can. An aggregate whose id is taken here gets a new one, and
every reference to it follows: the import reads each event with its own mpjsons instance, which
replaces `AggregateId` and `UserId` values that moved, and turns the old organization's `SpaceId` into
the new one. `remapped` lists the ids that moved.

> **Warning:** the import only sees ids that are typed. An id kept as a plain `Long`, or inside a
> text ("user:5" in a metadata actor), is invisible to it. Give the import an `ImportRewriter` for
> each type that holds such references (`rewriters = Map(AggregateType.of[ShoppingCart] -> ...)`): it
> gets every event with a function that maps old ids to new ones. And if you registered converters
> on your mpjsons, pass `newMPJsons = () => yourConfiguredMPJsons()` so the import reads your classes
> as you do.

`unknownReferences` counts references to aggregates that neither travelled nor exist here, for you
to look at; they are left as they are.

### After the move

1. **Rebuild the read models** of the new space: send each projection
   `RebuildAggregates(RebuildTarget.OneSpace(SpaceId(7)))` ([chapter 10](10-rebuilding-projections.md)).
   The import writes no outbox rows, so nothing is delivered live.
2. **Point the organization's users** at the new installation.
3. **Purge the old copy** when you are sure ([earlier in this chapter](#removing-an-organization-the-purge)).

> **Tip:** an import that stopped halfway, or one you want to redo, is simple to repeat: purge the
> target space and import the same file again.

> **Tip:** if you run more than one installation, give each its own
> `reactivecqrs.installation-number` as early as you can ([chapter 18](18-production.md)). Each
> installation then makes aggregate ids in a range of its own, so organizations can later move
> between installations without two aggregates sharing an id. See
> [ARCHITECTURE.md, section 13.5](../ARCHITECTURE.md).

## What you learned

- An organization is a **space** (`SpaceId`); space 0 is the platform itself.
- Every aggregate's first event names its space, and a new aggregate without one is refused; a copy
  names its own space.
- Keep the space in the state when a squash event or a state-listening projection needs it.
- Documents carry the space (`insertDocument(space.asLong, key, document)`), so one organization's
  documents can be cleared (`clearSpace`) and rebuilt (`RebuildAggregates(RebuildTarget.OneSpace(space))`).
- An installation with older data runs `buildIndexesConcurrently`, `backfillSpaces` and
  `backfillEventKeys` once, online.
- `SpacePurge` removes an organization: it checks first, detaches other organizations' copies,
  deletes in small transactions with your `PurgeHook`s, clears the projections, and reports; run it
  again until `complete`, and verify with `StoredDataCheck.spaceRows`.
- `SpaceExport` packs an organization into a file of JSON lines, and `SpaceImport` unpacks it into
  an empty space of another installation, moving taken ids and the typed references to them; then
  rebuild the space's read models.

Next: [chapter 17](17-cluster.md) spreads all of this over several machines.

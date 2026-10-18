# Rozdział 16. Organizacje (przestrzenie)

Witaj w części VI, w której twoja aplikacja dorasta. Większość aplikacji biznesowych obsługuje z
jednej instalacji więcej niż jedną organizację-klienta: sklepy, firmy, szkoły, kluby. Na końcu tego
rozdziału będziesz wiedzieć, jak ReactiveCQRS pilnuje, do której organizacji należy każdy agregat, jak
sprawić, by twoje modele odczytu też to wiedziały, żeby dane jednej organizacji dało się wyczyścić i
przebudować osobno, jakie jednorazowe kroki wykonuje instalacja ze starszymi danymi i jak usunąć
organizację na dobre.

---

## Wiele sklepów, jeden budynek

Pomyśl o centrum handlowym. Pod jednym dachem mieszka wiele sklepów: dzielą budynek, prąd i
ochroniarzy, ale towar, kasy i paragony każdy sklep ma własne. Kiedy jeden sklep przestawia półki,
inne tego nie zauważają; kiedy jeden sklep się wyprowadza, opróżnia własny lokal, a nie całe centrum.

W ReactiveCQRS budynkiem jest twoja instalacja (twoja aplikacja i jej baza danych), a każdy sklep to
**organizacja**, nazywana **przestrzenią** (space) i zapisywana jako `SpaceId`. Biuro zarządcy
centrum też jest przestrzenią: to **przestrzeń 0**, platforma, dla agregatów, które nie należą do
żadnego klienta (ustawienia, lista organizacji, szablony, które każdy może skopiować).

```scala
import io.reactivecqrs.api.id.SpaceId

val platform = SpaceId(0)
val cornerShop = SpaceId(42)  // cornerShop.asLong == 42
```

Jest też `SpaceId.unknown` (-1), co znaczy „brak przestrzeni”. Tę wartość spotkasz tylko w starych
danych.

> **Wskazówka:** jeśli w twojej aplikacji organizacje są agregatami, `SpaceId(organizationId)` zamienia
> `AggregateId` organizacji w jej przestrzeń, więc jedno i drugie to ta sama liczba.

## Każdy agregat należy do jakiejś przestrzeni

Agregat dostaje przestrzeń raz, od swojego pierwszego zdarzenia, gdy powstaje. Dlatego każde
`FirstEvent` ma `spaceId` ([rozdział 6](06-events.md)). W domenie testowej każdy koszyk należy do
platformy:

<!-- snippet: shoppingcart/Events.scala#created -->
```scala
case class ShoppingCartCreated(name: String) extends FirstEvent[ShoppingCart] {
  def spaceId: SpaceId = SpaceId(0)
}
```

Biblioteka zapisuje przestrzeń razem z agregatem, gdy zapisuje jego pierwsze zdarzenie, i trzyma ją
przez całe życie agregatu. Późniejsze zdarzenia jej nie powtarzają.

### Bez przestrzeni nie ma agregatu

Nowy agregat, którego pierwsze zdarzenie podaje ujemną przestrzeń, na przykład `SpaceId.unknown`,
zostaje odrzucony, zanim cokolwiek się zapisze: polecenie dostaje odpowiedź `FailureResponse`, której
komunikat zaczyna się od „A new aggregate needs a space”, wymienia zdarzenie i podaną w nim przestrzeń
oraz podpowiada `SpaceId(0)` dla platformy. Agregat nie powstaje.

> **Pamiętaj:** nie ma domyślnej organizacji. Decyduje każde pierwsze zdarzenie, a dla własnych
> agregatów platformy mówi to wprost: `SpaceId(0)`.

### Kopie podają własną przestrzeń

Zdarzenie duplikacji ([rozdział 6](06-events.md)) też jest pierwszym zdarzeniem, więc kopia podaje
*własną* przestrzeń, która może się różnić od przestrzeni oryginału. Szablon koszyka, który platforma
trzyma w przestrzeni 0, można skopiować do sklepu 42, a kopia należy wtedy do sklepu 42. W domenie
testowej `DuplicateShoppingCart` tworzy
`ShoppingCartDuplicated(SpaceId(0), baseAggregateId, baseAggregateVersion)`, więc jego kopie zostają
na platformie.

Kopia nadal czyta historię oryginału aż do wersji, przy której ją skopiowano, nawet gdy oba agregaty
żyją w różnych organizacjach. Miej to w pamięci; gdy organizacja jest usuwana, takie kopie są
najpierw odłączane i zachowują swój stan (koniec tego rozdziału).

### Koszyk, który pamięta swój sklep

W rozdziale 6 pojawił się szkic `CartOpened`, pierwszego zdarzenia, którego przestrzeń pochodzi z
polecenia. Pójdźmy z tym pomysłem o krok dalej w E9, przykładzie z tego rozdziału: koszyk należy do
sklepu i *pamięta* go w swoim stanie.

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

Na `OpenShopCart(userId, SpaceId(42), "Groceries")` wysłane do szyny poleceń tego typu przychodzi
odpowiedź `SuccessResponse(cartId, AggregateVersion(1))` i od tej chwili koszyk należy do sklepu 42.

Po co trzymać sklep w stanie, skoro biblioteka już go zna? Bo dwa elementy twojego kodu potrzebują
przestrzeni długo po pierwszym zdarzeniu:

- **zdarzenie squasha** ([rozdział 14](14-squash.md)), które staje się pierwszym zdarzeniem agregatu
  i powinno podawać tę samą przestrzeń; `toEvent` dostaje tylko stan, więc stan musi ją znać;
- **projekcje, które słuchają stanów** (`AggregateListener`, [rozdział 8](08-projections.md)), bo
  samego pierwszego zdarzenia nigdy nie widzą.

> **Pamiętaj:** `EventMetadata` też ma pole `spaceId` ([rozdział 6](06-events.md)). To opcjonalna
> część koperty metadanych, którą mogą wypełniać twoje polecenia, na przykład żeby zapisać, z której
> organizacji ktoś działał. Biblioteka nie używa go, żeby przypisać agregat do przestrzeni: robi to
> tylko pierwsze zdarzenie.

> **Pod maską:** przestrzeń mieszka w tabeli `aggregates` (`space_id`), a zapisuje ją procedura
> składowana, która tworzy wiersz agregatu z jego pierwszego zdarzenia (albo, dla kopii, ze zdarzenia
> duplikacji). Sprawdzenie, które odrzuca ujemną przestrzeń, działa we właścicielu agregatu, zanim
> zacznie się transakcja (decyzja D38). Zobacz
> [ARCHITECTURE.md, sekcje 2.2 i 4.1](../ARCHITECTURE.md).

## Modele odczytu też znają przestrzeń

Biblioteka zna przestrzeń każdego agregatu. Twoje modele odczytu też muszą ją znać, jeśli
kiedykolwiek zechcesz wyczyścić albo przebudować dokumenty jednej organizacji, nie ruszając
pozostałych. Każdy dokument w magazynie dokumentów ma przestrzeń, ustawianą przy wstawianiu dokumentu
([rozdział 8](08-projections.md)):

```scala
def insertDocument(spaceId: Long, key: Long, document: T)(implicit session: DBSession): Unit
def updateDocument(spaceId: Long, key: Long, modify: Option[Document[T]] => Document[T])(implicit session: DBSession): Document[T]
```

Zwróć uwagę, że przestrzeń jest tu zwykłym `Long`: przekazuj `space.asLong`. `updateDocument` używa
jej tylko wtedy, gdy musi wstawić brakujący dokument; `overwriteDocument` i `updateExistingDocument`
zachowują przestrzeń, którą dokument już ma.

Projekcje domeny testowej wszędzie przekazują `0`, bo wszystkie ich koszyki mieszkają w przestrzeni
0. Przy sklepach przekazuj sklep. Oto lista koszyków każdego sklepu:

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

Uruchamiamy ją jako projekcję zarządzaną, tak jak pokazuje [rozdział 10](10-rebuilding-projections.md),
z indeksem na sklepie:

<!-- snippet: e9/OrganizationsSetup.scala#start-list -->
```scala
val shopCarts: ActorRef = projections.start[ShopCartsList]("shop-carts", 1, "shopCarts") { build =>
  new ShopCartsList(subscriptionsState, new PostgresDocumentStore[ShopCartSummary](build.documentStore("shop_carts"),
    mpjsons, new NoopDocumentStoreCache, Seq(MultipleLongIndex(1, Seq("shop")))))
}
```

Dwie rzeczy warte uwagi:

- **Przestrzeń dokumentu służy do czyszczenia, nie do zadawania pytań.** Magazyn dokumentów nie ma
  „szukaj według przestrzeni”: jego kolumną przestrzeni kierują się `clearSpace` i przebudowa
  organizacji. Żeby wypisać koszyki jednego sklepu, dokument niesie też sklep
  (`ShopCartSummary.shop`), zapytanie szuka go po tej ścieżce, a indeks na tej ścieżce sprawia, że
  zapytanie jest szybkie.
- **Zdarzenie squasha jest obsługiwane jak utworzenie**, bo projekcja przebudowywana po squashu
  spotyka je jako pierwsze ([rozdział 14](14-squash.md)), a ono przynosi ze sobą przestrzeń.

> **Ostrzeżenie:** przestrzeni, którą przekażesz, zaufają `clearSpace` i przebudowa organizacji.
> Projekcja, która dla każdego dokumentu zapisuje `0`, tak jak jednosklepowe projekcje domeny
> testowej, działa dobrze aż do dnia, w którym wyczyścisz albo przebudujesz jedną organizację: wtedy
> nie czyści nic albo, dla przestrzeni 0, wszystko. Od początku przekazuj prawdziwą przestrzeń
> agregatu.

> **Pamiętaj:** `MemoryDocumentStore`, magazyn w pamięci do testów, przechowuje przestrzeń każdego
> dokumentu tak jak PostgreSQL: ustawia ją przy pierwszym zapisie dokumentu, potem ją zachowuje, a
> `clearSpace` usuwa dokumenty tej przestrzeni. Test sprzątania po organizacji może więc działać
> właśnie na nim.

## Czyszczenie i przebudowa jednej organizacji

Gdy każdy dokument zna swoją przestrzeń, modele odczytu jednej organizacji można naprawić, nie
ruszając niczyich innych. Narzędziem jest przebudowa celowana z
[rozdziału 10](10-rebuilding-projections.md), wycelowana w jedną przestrzeń i wysłana do nadzorcy
projekcji zarządzanej. E9 przebudowuje listę dla przestrzeni, do której przeprowadził się sklep (pod
koniec tego rozdziału):

<!-- snippet: e9/OrganizationsSetup.scala#rebuild -->
```scala
val rebuilt = Await.result(installation.shopCarts.ask(RebuildAggregates(RebuildTarget.OneSpace(target)))(Timeout(10.minutes)),
  10.minutes) // a whole organization can take a while
// AggregatesRebuilt(RebuildTarget.OneSpace(target), aggregates), or RebuildRefused(reason)
```

`clearForRebuild(OneSpace(space))` projekcji czyści dokumenty tej organizacji (patrz wyżej),
projekcja zapomina, dokąd doszła z agregatami organizacji, i dostaje ich zdarzenia jeszcze raz z
magazynu, w kolejności zapisu. Dokumenty innych organizacji zostają nietknięte, a projekcja w tym
czasie dalej odpowiada na zapytania. Odpowiedź, `AggregatesRebuilt(target, aggregates)`, mówi, ile
agregatów przebudowano.

Przebudowa jednej organizacji jest odrzucana (`RebuildRefused`), dopóki któryś agregat z typów
projekcji nie ma jeszcze przestrzeni: agregatom zapisanym przez starsze wersje biblioteki może jej
brakować, a powód odmowy podpowiada, żeby najpierw uruchomić `backfillSpaces`. O tym jest następna
sekcja.

> **Pod maską:** magazyn znajduje zdarzenia organizacji przez wąską tabelę kluczy, `event_keys`,
> która zapisuje typ agregatu i przestrzeń każdego zdarzenia. Gdy każde starsze zdarzenie ma już swój
> klucz, przebudowa jednej organizacji czyta tylko zdarzenia tej organizacji, a nie cały magazyn
> (problem F124). Zobacz [ARCHITECTURE.md, sekcję 8](../ARCHITECTURE.md).

## Aktualizacja instalacji, która ma już dane

Instalacja, która działa już od jakiegoś czasu, może przechowywać agregaty zapisane, zanim każde
pierwsze zdarzenie musiało podawać przestrzeń (ich przestrzeń to -1), dokumenty zapisane, zanim
dokumenty dostały przestrzeń (też -1), i zdarzenia zapisane, zanim pojawiły się klucze zdarzeń.
Naprawia to kilka jednorazowych zadań. Wszystkie działają online, w trakcie pracy aplikacji, w małych
transakcjach, i wszystkie można zatrzymać i uruchomić ponownie: ruszają dalej od miejsca, w którym
skończyły. [Rozdział 18](18-production.md) wymienia je razem z innymi krokami produkcyjnymi; tutaj
zobaczysz, co znaczą dla organizacji.

1. **Policz, co trzeba naprawić.** `StoredDataCheck` czyta twoje zapisane dane na kopii produkcji
   ([rozdział 6](06-events.md)), a jego raport obejmuje też agregaty bez przestrzeni:

   <!-- snippet: e9/OrganizationsSpec.scala#withoutSpace -->
   ```scala
   import io.reactivecqrs.core.tools.StoredDataCheck

   val report = StoredDataCheck.run(mpjsons, typesNames)
   println(report.aggregatesWithoutSpace)
   ```

2. **Zbuduj indeksy**, nie blokując zapisów:

   <!-- snippet: e9/OrganizationsSpec.scala#buildIndexes -->
   ```scala
   eventStore.buildIndexesConcurrently()
   ```

   Buduje indeks, przez który czyta odtwarzanie agregatów, i indeks, który znajduje agregaty
   organizacji, poleceniem PostgreSQL `CREATE INDEX CONCURRENTLY`. Gdy indeksy już istnieją, nic nie
   robi, a indeks, który przerwane uruchomienie zostawiło jako nieprawidłowy, buduje od nowa. (Nowa
   albo mała baza danych dostaje je przy tworzeniu schematu.)

3. **Nadaj starym agregatom przestrzeń:**

   <!-- snippet: e9/OrganizationsSpec.scala#backfillSpaces -->
   ```scala
   import io.reactivecqrs.core.eventstore.SpaceBackfill

   val spaces: SpaceBackfill = eventStore.backfillSpaces()  // SpaceBackfill(filled, left)
   ```

   Każdy agregat zapisany bez przestrzeni dostaje przestrzeń swojego pierwszego zdarzenia (albo
   zdarzenia squasha, jeśli jego historię zwinięto), w partiach po 1 000 agregatów (`batchSize`),
   każda w osobnej transakcji. Niektóre stare pierwsze zdarzenia nie znają swojej przestrzeni: mówią
   `SpaceId.unknown`. Dla nich przekaż funkcję, która zdecyduje na podstawie typu agregatu, jego
   identyfikatora i pierwszego zdarzenia:

   <!-- snippet: e9/OrganizationsSpec.scala#backfillByEvent -->
   ```scala
   val spaces = eventStore.backfillSpaces((aggregateType, aggregateId, firstEvent) => firstEvent match {
     case e: LegacyCartCreated => Some(SpaceId(e.shopNumber))
     case _ => None
   })
   ```

   (`LegacyCartCreated` zastępuje tu dowolne twoje stare pierwsze zdarzenie, które trzyma
   organizację we własnym polu; spec E9 ma takie zdarzenie, żeby te kroki dało się skompilować i
   uruchomić.) `filled` liczy agregaty, które dostały przestrzeń; `left` wymienia te, których
   przestrzeni nie ustaliło ani zdarzenie, ani twoja funkcja: przejrzyj je ręcznie.

4. **Nadaj starym zdarzeniom klucze**, gdy już *każdy* węzeł działa na nowej wersji:

   <!-- snippet: e9/OrganizationsSpec.scala#backfillKeys -->
   ```scala
   import io.reactivecqrs.core.eventstore.EventKeysBackfill

   val keys: EventKeysBackfill = eventStore.backfillEventKeys()  // EventKeysBackfill(written, missing)
   ```

   Przechodzi przez zdarzenia w partiach po 10 000 (`batchSize`) i zapisuje moment, w którym każde
   zdarzenie ma już klucz; od tej pory przebudowa jednej organizacji albo budowa kilku typów czyta
   tylko własne zdarzenia. Do tego czasu czytają cały magazyn, co jest poprawne, tylko wolne.
   `missing` liczy zdarzenia, którym nie udało się nadać klucza (te, których agregat nie ma swojego
   wiersza).

5. **Przebuduj modele odczytu, które nie zapisywały prawdziwej przestrzeni**: dokumenty zapisane,
   zanim dokumenty dostały przestrzeń, albo z `0` dla wszystkiego. Pełna przebudowa
   ([rozdział 10](10-rebuilding-projections.md)) zapisze je od nowa, z przestrzeniami, które twoja
   projekcja teraz przekazuje.

> **Ostrzeżenie:** gdy `backfillEventKeys` skończy pracę, nie wracaj do starszej wersji biblioteki:
> starsze wersje zapisują zdarzenia bez kluczy, a budowy pominęłyby te zdarzenia.

> **Wskazówka:** uruchamiaj te zadania z zadania serwisowego albo z wątku w tle, nigdy na ścieżce
> startu aplikacji. Na dużym magazynie trwają dłuższą chwilę, a żadne z nich nie wymaga, żeby
> aplikacja czekała.

## Usuwanie organizacji (purge)

Sklep wyprowadza się z centrum handlowego. Znikają jego półki, szyldy i paragony z zaplecza,
sąsiednie sklepy handlują dalej, a ich klienci niczego nie zauważają. Gdy organizacja-klient odchodzi
z twojej platformy albo prosi o wymazanie swoich danych, **usunięcie organizacji** (purge) robi to
samo z jej przestrzenią.

### Zanim zaczniesz

1. **Wycisz organizację.** Najpierw zatrzymaj jej użytkowników: biblioteka nie wstrzyma polecenia
   wysłanego w trakcie usuwania. (Jeśli jakieś się prześlizgnie i utworzy agregat, usuwanie to
   zauważy i zrobi kolejne okrążenie, najwyżej trzy razy.)
2. **Sprawdź projekcje.** Każda projekcja, która trzyma dokumenty organizacji, musi implementować
   `clearForRebuild` dla `RebuildTarget.OneSpace` ([rozdział 10](10-rebuilding-projections.md)) i
   żadna nie może być w tym czasie przebudowywana. Usuwanie najpierw pyta każdą z nich i odmawia,
   jeśli któraś nie może wziąć udziału.
3. **Pomyśl o własnych tabelach.** Wiersze, o których biblioteka nie wie (na przykład paragony, które
   writer outboxu zapisuje obok zdarzeń), potrzebują twojego małego `PurgeHook`, pokazanego niżej.
4. **Oznacz swoje sagi.** Saga, której zlecenia należą do organizacji, mówi o tym, nadpisując
   `spaceOf` ([rozdział 12](12-sagas.md)); usuwanie kasuje sagi oznaczone tą przestrzenią.

### Uruchomienie

<!-- snippet: e9/OrganizationsSetup.scala#purge -->
```scala
val purge = new SpacePurge(installation.eventStore,
  commandBuses = Map(AggregateType.of[ShopCart] -> installation.carts), // this node's bus of every type the shop has
  projections = Seq(installation.shopCarts))                            // every projection with its documents
val purged = Await.result(purge.purge(source), 2.minutes)
```

Uruchom je z jednego węzła, raz. W odpowiedzi dostaniesz `PurgeReport`:

| Pole | Co mówi |
|---|---|
| `complete` | true, gdy nic nie zostało odrzucone, nic nie zawiodło i z przestrzeni nic nie zostało |
| `aggregates`, `rows` | ile agregatów usunięto i ile wierszy z każdej tabeli (wiersze hooka pod jego nazwą) |
| `detached` | kopie w *innych* organizacjach, które teraz stoją na własnych nogach (niżej) |
| `projections`, `failed` | projekcje, które wyczyściły przestrzeń, i wszystko, co nie zrobiło swojej części, z powodem |
| `refused` | dlaczego się nie zaczęło; nic nie zostało usunięte |
| `passes`, `remaining` | ile okrążeń to zajęło i ile agregatów przestrzeni wciąż jest zapisanych |

### Co robi, krok po kroku

1. **Najpierw sprawdza.** Każdy typ przestrzeni potrzebuje szyny poleceń w `commandBuses`, a każda
   projekcja musi umieć wyczyścić przestrzeń. Jeśli nie, raport podaje powód w `refused` i nic się
   nie zmienia. Przestrzeń platformy (0) i przestrzeń -1 (to, co zapisano, zanim istniały
   przestrzenie) nie są organizacjami, więc usuwanie którejkolwiek z nich też jest odrzucane.
2. **Odłącza kopie w innych organizacjach.** Pamiętasz szablon koszyka skopiowany z platformy do
   sklepu 42? Tutaj liczy się kierunek odwrotny: kopia, którą sklep 7 zrobił z jednego z koszyków
   sklepu 42, wciąż czyta historię sklepu 42. Zanim ta historia zniknie, kopia dostaje własne
   pierwsze zdarzenie, zrobione z jej własnego stanu za pomocą zdarzenia squasha jej typu
   ([rozdział 14](14-squash.md)), więc zachowuje dokładnie to, co miała. Typ bez zdarzenia squasha
   tego nie potrafi i wtedy usuwanie odmawia.
3. **Usuwa małymi kęsami.** Najwyżej 1 000 agregatów i 100 000 zdarzeń naraz, każdy kęs w jednej
   transakcji: zdarzenia, archiwum, wiersze outboxu, migawki, zapiski projekcji, zapisane odpowiedzi
   poleceń idempotentnych, wiersze agregatów i wiersze twoich hooków. Po każdym kęsie właściciele tych
   agregatów są zatrzymywani, a każda projekcja dowiaduje się, że ma o nich zapomnieć.
4. **Usuwa sagi przestrzeni.**
5. **Czyści projekcje**: w każdej uruchamia się `clearForRebuild(OneSpace(space))` i dokumenty
   organizacji znikają.

> **Wskazówka:** każdy krok można powtórzyć. Usuwanie, które zostało odrzucone, przerwało się w
> połowie albo nie dobiegło końca, po prostu uruchamiasz jeszcze raz, gdy uporasz się z przyczyną;
> ono też jeszcze raz czyści projekcje, nawet gdy nie został już żaden agregat do usunięcia.

### Twoje własne tabele: `PurgeHook`

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

Przekaż go do `SpacePurge` razem z resztą (`new SpacePurge(..., hooks = Seq(receiptsHook))`). Hook
działa w transakcji każdego kęsa, więc jego wiersze znikają razem z wierszami biblioteki albo wcale.

### Sprawdzamy, czy nic nie zostało

<!-- snippet: e9/OrganizationsSpec.scala#spaceRows -->
```scala
import io.reactivecqrs.core.tools.StoredDataCheck

val left = StoredDataCheck.spaceRows(42L) // Map(aggregates -> 0, event_keys -> 0, sagas -> 0, projection_shop_carts_v1 -> 0)
```

Liczy, co przestrzeń wciąż ma w tabelach biblioteki i w każdej tabeli modelu odczytu. Po kompletnym
usunięciu każda liczba wynosi zero.

> **Ostrzeżenie:** kilka rzeczy jest poza zasięgiem usuwania. W klastrze właściciel usuniętego
> agregatu na *innym* węźle trzyma swój stan w pamięci do następnego użycia, które nie znajdzie już nic
> zapisanego, albo dopóki nie zostanie uśpiony z powodu bezczynności ([rozdział 17](17-cluster.md)).
> Odpowiedzi poleceń idempotentnych zapisane przez starsze wersje biblioteki nie mówią, do którego
> agregatu należą, więc czekają, aż minie ich okres przechowywania (domyślnie 30 dni). Zostają też
> sagi bez `spaceOf`, agregaty bez przestrzeni (najpierw `backfillSpaces`) i twoje tabele bez hooka.

> **Pod maską:** tuż po kęsie projekcja może jeszcze dostać zdarzenie usuniętego koszyka, które było
> już w drodze. Przez pół godziny (`purgedDropWindow`) potwierdza takie dostarczenia, nie stosując
> ich, więc nic nie zapisze dokumentu z powrotem; w tym czasie trzyma 8 bajtów na każdy usunięty
> agregat. Zobacz [ARCHITECTURE.md, sekcję 4.7](../ARCHITECTURE.md) i projekt w
> [docs/design/M6-organizations.md](../design/M6-organizations.md).

## Przeprowadzka organizacji do innej instalacji

Sklep przenosi się do centrum handlowego w innym mieście. Jego towar, księgi i historia trafiają do
pudeł, jadą i zostają rozpakowane w pustym lokalu na miejscu, gdzie nowe centrum wiesza dla niego
swoje szyldy. Przenoszenie organizacji między instalacjami wygląda tak samo: **eksport** pakuje pudła,
**import** rozpakowuje je do pustej przestrzeni drugiej instalacji, a tam przebudowuje się modele
odczytu.

### Pakowanie: eksport

<!-- snippet: e9/OrganizationsSetup.scala#export -->
```scala
val file = Files.createTempFile("shop-", ".jsonl")
val writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)
val exported = try new SpaceExport(installation.eventStore, contexts = Seq(new ShopCartContext)).write(source, writer)
               finally writer.close()
// ExportReport(space, aggregates, events, archived, detached, waitingInOutbox, refused)
```

Plik zawiera jedną linię JSON na każdy agregat i każde zdarzenie, zdarzenia w kolejności zapisu, a do
tego zarchiwizowane zdarzenia ([rozdział 14](14-squash.md)); migawki, modele odczytu i sagi zostają na
miejscu. Eksportuj wyciszoną organizację: `waitingInOutbox` mówi, ile jej zdarzeń nie zostało jeszcze
dostarczonych.

`contexts` to konteksty agregatów typów organizacji. Mają znaczenie w jednym przypadku: kopia, którą
sklep 42 zrobił z czyjegoś koszyka, czyta historię tamtej organizacji, a ta nie wyjeżdża razem ze
sklepem. Eksport daje takiej kopii własne pierwsze zdarzenie, zrobione z jej stanu za pomocą
zdarzenia squasha jej typu, tak samo jak przy usuwaniu organizacji, więc kopia zachowuje dokładnie
to, co miała. Bez zdarzenia squasha (albo bez kontekstu) eksport odmawia i niczego nie zapisuje.

### Rozpakowywanie: import

<!-- snippet: e9/OrganizationsSetup.scala#import -->
```scala
val imported = new SpaceImport(installation.eventStore, installation.uidGenerator).read(file, into = target)
// ImportReport(space, aggregates, events, archived, remapped, unknownReferences, someUnknown, refused)
```

Działa na drugiej instalacji i wczytuje dane do przestrzeni, która musi być pusta. Czyta plik dwa
razy:

1. **Najpierw sprawdza**, niczego nie zapisując: każda klasa z pliku musi tu istnieć, każde zdarzenie
   musi mieścić się w limitach tej instalacji ([rozdział 6](06-events.md)), a przestrzeń musi być
   pusta. Każdy problem trafia do `refused`.
2. **Potem zapisuje**: agregaty, następnie zdarzenia w ich pierwotnej kolejności, razem ze
   znacznikami cofnięć, a na końcu zarchiwizowane zdarzenia.

Dwie instalacje tworzyły identyfikatory we własnych zakresach (numer instalacji z rozdziału 2), więc
identyfikatory rzadko się zderzają; te sprzed 1.0 mogą. Agregat, którego identyfikator jest tu już
zajęty, dostaje nowy, a za nim idą wszystkie odwołania do niego: import czyta każde zdarzenie własną
instancją mpjsons, która podmienia przeniesione wartości `AggregateId` i `UserId`, a `SpaceId` starej
organizacji zamienia na nowy. `remapped` wymienia identyfikatory, które się zmieniły.

> **Ostrzeżenie:** import widzi tylko typowane identyfikatory. Identyfikator trzymany jako zwykły
> `Long` albo wewnątrz tekstu („user:5” w aktorze metadanych) jest dla niego niewidoczny. Daj
> importowi `ImportRewriter` dla każdego typu, który ma takie odwołania
> (`rewriters = Map(AggregateType.of[ShoppingCart] -> ...)`): dostaje on każde zdarzenie razem z
> funkcją, która zamienia stare identyfikatory na nowe. A jeśli w swoim mpjsons masz zarejestrowane
> konwertery, przekaż `newMPJsons = () => yourConfiguredMPJsons()`, żeby import czytał twoje klasy tak
> samo jak ty.

`unknownReferences` liczy odwołania do agregatów, które ani nie przyjechały, ani tu nie istnieją.
Przejrzyj je; import zostawia je takie, jakie są.

### Po przeprowadzce

1. **Przebuduj modele odczytu** nowej przestrzeni: wyślij do każdej projekcji
   `RebuildAggregates(RebuildTarget.OneSpace(SpaceId(7)))` ([rozdział 10](10-rebuilding-projections.md)).
   Import nie zapisuje wierszy outboxu, więc nic nie jest dostarczane na żywo.
2. **Skieruj użytkowników organizacji** do nowej instalacji.
3. **Usuń starą kopię**, gdy będziesz mieć pewność
   ([wcześniej w tym rozdziale](#usuwanie-organizacji-purge)).

> **Wskazówka:** import, który zatrzymał się w połowie albo który chcesz powtórzyć, łatwo zrobić
> jeszcze raz: usuń przestrzeń docelową i zaimportuj ten sam plik ponownie.

> **Wskazówka:** jeśli masz więcej niż jedną instalację, jak najwcześniej nadaj każdej własny
> `reactivecqrs.installation-number` ([rozdział 18](18-production.md)). Każda instalacja tworzy wtedy
> identyfikatory agregatów we własnym zakresie, więc organizacje mogą później przenosić się między
> instalacjami bez ryzyka, że dwa agregaty dostaną ten sam identyfikator. Zobacz
> [ARCHITECTURE.md, sekcję 13.5](../ARCHITECTURE.md).

## Co już umiesz

- Organizacja to **przestrzeń** (`SpaceId`); przestrzeń 0 to sama platforma.
- Pierwsze zdarzenie każdego agregatu podaje jego przestrzeń, a nowy agregat bez niej jest
  odrzucany; kopia podaje własną przestrzeń.
- Trzymaj przestrzeń w stanie, gdy potrzebuje jej zdarzenie squasha albo projekcja słuchająca
  stanów.
- Dokumenty niosą przestrzeń (`insertDocument(space.asLong, key, document)`), więc dokumenty jednej
  organizacji można wyczyścić (`clearSpace`) i przebudować
  (`RebuildAggregates(RebuildTarget.OneSpace(space))`).
- Instalacja ze starszymi danymi uruchamia raz, online, `buildIndexesConcurrently`, `backfillSpaces`
  i `backfillEventKeys`.
- `SpacePurge` usuwa organizację: najpierw sprawdza, odłącza kopie należące do innych organizacji,
  usuwa w małych transakcjach razem z twoimi `PurgeHook`, czyści projekcje i składa raport;
  uruchamiaj je ponownie aż do `complete` i sprawdź wynik przez `StoredDataCheck.spaceRows`.
- `SpaceExport` pakuje organizację do pliku z liniami JSON, a `SpaceImport` rozpakowuje go do pustej
  przestrzeni innej instalacji, zmieniając zajęte identyfikatory i typowane odwołania do nich; potem
  przebuduj modele odczytu tej przestrzeni.

Dalej: [rozdział 17](17-cluster.md) rozkłada to wszystko na kilka maszyn.

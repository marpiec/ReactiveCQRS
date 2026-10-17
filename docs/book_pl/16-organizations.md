# Rozdział 16. Organizacje (przestrzenie)

Witamy w części VI, gdzie Twoja aplikacja się rozwija. Większość aplikacji biznesowych obsługuje więcej niż
jedna organizacja klienta z jednej instalacji: sklepy, firmy, szkoły, kluby. Do końca
w tym rozdziale dowiesz się, jak ReactiveCQRS śledzi, która organizacja jest każdym agregatem
należy do, w jaki sposób sprawić, by Twoje modele odczytu zawierały tę wiedzę, aby dane jednej organizacji mogły być
wyczyszczone i odbudowane samodzielnie, które jednorazowe kroki instalacji ze starszymi danymi i jak to zrobić
usunąć organizację na dobre.

---

## Wiele sklepów, jeden budynek

Wyobraź sobie centrum handlowe. Wiele sklepów mieści się pod jednym dachem: dzielą budynek, prąd i…
i ochroniarzy, ale każdy sklep ma swój własny asortyment, kasy i paragony. Kiedy jeden sklep
przestawia swoje półki, inni tego nie zauważają; gdy jeden sklep się wyprowadza, opróżnia swój lokal,
nie całe centrum handlowe.

W ReactiveCQRS budynkiem jest Twoja instalacja (Twoja aplikacja i jej baza danych), a każdy sklep
jest **organizacją**, nazywaną **przestrzenią** i zapisywaną jako `SpaceId`. Własny zarząd centrum handlowego
Biuro to także przestrzeń: **przestrzeń 0**, platforma, dla agregatów, które nie należą do żadnego klienta
(ustawienia, lista organizacji, szablony, które każdy może skopiować).

```scala
importuj io.reactivecqrs.api.id.SpaceId

val platform = SpaceId(0)
val cornerShop = SpaceId(42) // cornerShop.asLong == 42
```

Istnieje również `SpaceId.unknown` (-1), co oznacza „brak spacji”. Spotkasz się z tym tylko w starych danych.

> **Wskazówka:** jeśli organizacje są agregatami w Twojej aplikacji, `SpaceId(organizationId)` zmienia
> `AggregateId` organizacji do swojej przestrzeni, więc oba będą miały tę samą liczbę.

## Każdy agregat należy do przestrzeni

Agregat otrzymuje swoją przestrzeń raz, od pierwszego zdarzenia, w momencie utworzenia. Dlatego każdy
`FirstEvent` ma `spaceId` ([rozdział 6](06-events.md)). W domenie testowej każdy koszyk należy do
platforma:

```scala
klasa przypadku ShoppingCartCreated(nazwa: String) rozszerza FirstEvent[ShoppingCart] {
  def spaceId: SpaceId = SpaceId(0)
}
```

Biblioteka przechowuje przestrzeń z agregatem w momencie zapisania pierwszego zdarzenia i zachowuje ją na później.
całe życie całości. Późniejsze wydarzenia tego nie powtórzą.

### Brak spacji, brak agregatu

Nowy agregat, którego pierwsze zdarzenie daje ujemną przestrzeń, np. `SpaceId.unknown`, jest odrzucany
przed napisaniem czegokolwiek: na polecenie odpowiadamy komunikatem `FailureResponse`, którego wiadomość rozpoczyna się
„Nowy agregat potrzebuje miejsca”, nazywa zdarzenie i przestrzeń, którą ono zapewniło, i sugeruje „SpaceId(0)”
dla platformy. Agregat nie jest tworzony.

> **Pamiętaj:** nie ma domyślnej organizacji. Każde pierwsze zdarzenie decyduje, a dla platformy
> własnych agregatów mówi na głos `SpaceId(0)`.

### Kopie nazywają własną przestrzeń

Zdarzenie duplikacji ([rozdział 6](06-events.md)) jest również pierwszym zdarzeniem, więc kopia nadaje *własną* nazwę
przestrzeń, która może różnić się od oryginału. Szablon koszyka przechowywany przez platformę w przestrzeni 0 może być
skopiowano do sklepu 42, a kopia należy do sklepu 42. W domenie testowej `DuplicateShoppingCart`
tworzy `ShoppingCartDuplicated(SpaceId(0), baseAggregateId, baseAggregateVersion)`, więc jego kopie
Zostań na platformie.

Kopia odczytuje historię oryginału aż do wersji, w której została skopiowana, nawet gdy dwie wersje są niekompletne.
mieszkają w różnych organizacjach. Pamiętaj o tym; gdy organizacja zostanie usunięta, takie kopie zostaną
najpierw odłączyć i zachować ich stan (koniec tego rozdziału).

### Wózek, który pamięta swój sklep

W rozdziale 6 naszkicowano „CartOpened”, pierwsze zdarzenie, którego przestrzeń pochodzi z polecenia. Weźmy to
pomysł o krok dalej, w małym przykładzie naszego własnego (nie jest to domena testowa): wózek, który
należy do sklepu i *pamięta* go w jego stanie.

```scala
importuj io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}

klasa przypadku ShopCart(sklep: SpaceId, nazwa: String, elementy: Vector[String])

klasa przypadku ShopCartOpened(spaceId: SpaceId, nazwa: String) rozszerza FirstEvent[ShopCart]
klasa przypadku ShopItemAdded(nazwa: String) rozszerza Event[ShopCart]
klasa przypadku ShopCartSquashed(spaceId: SpaceId, nazwa: String, elementy: Vector[String]) rozszerza FirstEvent[ShopCart]

klasa przypadku OpenShopCart(userId: UserId, shop: SpaceId, name: String)
  rozszerza FirstCommand[ShopCart, CustomCommandResponse[_]]
klasa przypadku AddShopItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, name: String)
  rozszerza polecenie [ShopCart, CustomCommandResponse [_]]

klasa ShopCartContext rozszerza AggregateContext[ShopCart] {
  nadpisz wersję val: Int = 1
  nadpisanie def initialAggregateRoot: ShopCart = ShopCart(SpaceId.unknown, "", Vector.empty)

  nadpisz def commandHandlers = _ => {
    przypadek c: OpenShopCart => CommandSuccess(ShopCartOpened(c.shop, c.name))
    przypadek c: AddShopItem => CommandSuccess(ShopItemAdded(c.name))
  }

  nadpisz def eventHandlers = (_, _, cart) => {
    przypadek e: ShopCartOpened => ShopCart(e.spaceId, e.name, Vector.empty)
    przypadek e: ShopItemAdded => cart.copy(items = cart.items :+ e.name)
    przypadek e: ShopCartSquashed => ShopCart(e.spaceId, e.name, e.items)
  }

  // Zawody w squasha są również pierwszymi wydarzeniami: nadają nazwę sklepowi, w którym znajduje się wózek, o czym pamięta stan.
  nadpisz val squash: Option[SquashPolicy[ShopCart]] =
    Some(SquashPolicy(cart => ShopCartSquashed(cart.shop, cart.name, cart.items)))
}
```

Wysyłanie `OpenShopCart(userId, SpaceId(42), "Groceries")` do magistrali poleceń tego typu jest odpowiedzią
`SuccessResponse(cartId, AggregateVersion(1))`, a od tej pory koszyk należy do sklepu 42.

Po co utrzymywać sklep w takim stanie, skoro biblioteka już o tym wie? Bo dwa twoje własne dzieła…
potrzeba przestrzeni długo po pierwszym wydarzeniu:

- zdarzenie **squash** ([rozdział 14](14-squash.md)), które staje się pierwszym zdarzeniem agregatu i
  powinien nazwać tę samą przestrzeń; `toEvent` pobiera tylko stan, więc stan musi go znać;
- **projekcje nasłuchujące stanów** (`AggregateListener`, [rozdział 8](08-projections.md)), które
  nigdy nie zobaczyć pierwszego wydarzenia.

> **Pamiętaj:** `EventMetadata` ma również pole `spaceId` ([rozdział 6](06-events.md)). To jest
> opcjonalna część koperty metadanych, którą mogą wypełniać Twoje polecenia, na przykład w celu nagrywania z
> w jakiej organizacji ktoś działał. Biblioteka nie używa jej do umieszczania agregatów: tylko pierwszy
> wydarzenie to powoduje.

> **Pod maską:** przestrzeń znajduje się w tabeli `aggregates` (`space_id`), zapisanej przez przechowywany
> procedura, która tworzy wiersz agregatu z jego pierwszego zdarzenia (lub w przypadku kopii z jego
> zdarzenie duplikacji). Sprawdzanie, które odrzuca spację ujemną, odbywa się u właściciela agregatu, przed
> transakcja się rozpoczyna (decyzja D38). Zobacz
> [ARCHITECTURE.md, sekcje 2.2 i 4.1](../ARCHITECTURE.md).

## Modele do czytania również zajmują dużo miejsca

Biblioteka zna przestrzeń każdego agregatu. Twoje modele odczytu również muszą ją znać, jeśli kiedykolwiek zechcesz.
wyczyścić lub odbudować dokumenty jednej organizacji bez naruszania pozostałych. Każdy dokument w
magazyn dokumentów ma spację ustawianą podczas wstawiania dokumentu ([rozdział 8](08-projections.md)):

```scala
def insertDocument(spaceId: Long, key: Long, document: T)(implicit session: DBSession): Unit
def updateDocument(spaceId: Long, key: Long, modify: Option[Document[T]] => Document[T])(implicit session: DBSession): Document[T]
```

Należy pamiętać, że spacja jest tutaj zwykłym `Long`: należy przekazać `space.asLong`. `updateDocument` używa jej tylko wtedy, gdy
musi wstawić brakujący dokument; `overwriteDocument` i `updateExistingDocument` zachowują przestrzeń
dokument już ma.

Projekcje domeny testowej wszędzie przechodzą przez `0`, ponieważ każdy z ich wózków znajduje się w przestrzeni
0. W sklepach, przejdź obok sklepu. Oto lista wózków w każdym sklepie:

```scala
importuj io.reactivecqrs.api.id.SpaceId
import io.reactivecqrs.core.documentstore.{Dokument, Magazyn dokumentów, Oczekiwana pojedyncza długa wartość}
import io.reactivecqrs.core.projection.{ProjectionActor, RebuildTarget, SubscriptionsState}
import scalikejdbc.DBSession

klasa przypadku ShopCartSummary(sklep: Long, nazwa: String)
klasa przypadku GetShopCarts(sklep: SpaceId)

klasa ShopCartsList(val subscriptionsState: SubscriptionsState, documentStore: DocumentStore[ShopCartSummary])
  rozszerza ProjectionActor {

  chronione val listeners = List(EventsListener[ShopCart] { (aggregateId, events) => niejawna sesja =>
    events.foreach(_.event match {
      przypadek ShopCartOpened(sklep, nazwa) =>
        documentStore.insertDocument(shop.asLong, aggregateId.asLong, ShopCartSummary(shop.asLong, name))
      case ShopCartSquashed(shop, name, _) => // co najpierw spełnia odbudowa w przypadku zgniecionego koszyka
        documentStore.updateDocument(shop.asLong, aggregateId.asLong, _ => Document(ShopCartSummary(shop.asLong, name)))
      przypadek _ => ()
    })
  })

  // Co najpierw rozwiązuje celowa przebudowa (rozdział 10): jedna organizacja to jedno połączenie.
  nadpisz chronione def clearForRebuild(target: RebuildTarget)(implicit session: DBSession): Unit = target match {
    przypadek RebuildTarget.OneAggregate(id) => documentStore.removeDocument(id.asLong)
    przypadek RebuildTarget.OneSpace(space) => documentStore.clearSpace(space.asLong)
    przypadek RebuildTarget.OneType(_) => documentStore.clearAllData()
  }

  nadpisz chronione def receiveQuery: Receive = {
    przypadek GetShopCarts(sklep) =>
      sender() ! documentStore.findDocumentByPath(ExpectedSingleLongValue(Seq("shop"), shop.asLong)).values.map(_.document).toVector
  }

  nadpisz chronioną wartość projectionName: String = "ShopCartsList"
}
```

Rozpoczęto jako projekcję zarządzaną, jak pokazano w [rozdziale 10](10-rebuilding-projections.md), z
indeks sklepu:

```scala
import io.reactivecqrs.core.documentstore.{MultipleLongIndex, NoopDocumentStoreCache, PostgresDocumentStore}
import org.apache.pekko.actor.ActorRef

val shopCarts: ActorRef = projections.start[ShopCartsList]("shop-carts", 1, "shopCarts") { build =>
  nowy ShopCartsList(subscriptionsState, nowy PostgresDocumentStore[ShopCartSummary](build.documentStore("shop_carts"),
    mpjsons, nowy NoopDocumentStoreCache, Seq(MultipleLongIndex(1, Seq("sklep")))))
}
```

Dwie rzeczy, na które należy zwrócić uwagę:

- **Przestrzeń dokumentu służy do czyszczenia, a nie do zadawania pytań.** Magazyn dokumentów nie ma funkcji „znajdź według”
  „Space”: kolumna „space” to to, co „clearSpace” i odbudowa organizacji. Aby wymienić jedną
  koszyki sklepowe, dokument zawiera również sklep (`ShopCartSummary.shop`), zapytanie go wyszukuje
  tą ścieżką, a indeks na tej ścieżce sprawia, że ​​zapytanie jest szybkie.
- **Wydarzenie squasha jest traktowane jak stworzenie**, ponieważ projekcja jest odbudowywana po spotkaniu squasha
  najpierw to ([rozdział 14](14-squash.md)), a ono zabiera ze sobą przestrzeń.

> **Ostrzeżenie:** przekazana przestrzeń jest przestrzenią, której `clearSpace` i odbudowa organizacji będzie zaufana.
> Projekcja, która zapisuje `0` dla każdego dokumentu, tak jak robią to projekcje pojedynczego sklepu domeny testowej,
> działa dobrze do dnia, w którym wyczyścisz lub odbudujesz jedną organizację: wtedy nie wyczyści niczego lub, na przykład,
> przestrzeń 0, wszystko. Przekaż rzeczywistą przestrzeń agregatu od początku.

> **Pamiętaj:** `MemoryDocumentStore`, magazyn pamięci dla testów, ignoruje spacje: jego
> `clearSpace` nic nie robi. Przetestuj czyszczenie organizacji w `PostgresDocumentStore`.

## Oczyszczanie i odbudowa jednej organizacji

Dzięki wolnej przestrzeni w każdym dokumencie można poprawić modele odczytu danej organizacji bez konieczności ich dotykania
czyjegoś innego. Narzędziem jest celowa przebudowa [rozdziału 10](10-rebuilding-projections.md),
skierowane do jednej przestrzeni i wysłane do kierownika zarządzanej projekcji:

```scala
import io.reactivecqrs.core.projection.RebuildTarget
import io.reactivecqrs.core.rebuild.ProjectionSupervisor.{AgregatyOdbudowane, OdbudowaneAgregaty, OdbudowaneOdrzucone}
zaimportuj org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._

niejawna wartość limitu czasu: Limit czasu = Limit czasu (10 minut) // cała organizacja może potrzebować trochę czasu

val odbudowany: Future[Any] = shopCarts ? RebuildAggregates(RebuildTarget.OneSpace(SpaceId(42)))
// AggregatesRebuilt(RebuildTarget.OneSpace(SpaceId(42)), agregaty), lub RebuildRefused(powód)
```

Projekcja `clearForRebuild(OneSpace(space))` czyści dokumenty tej organizacji (powyżej),
projekcja zapomina, jak daleko zaszła z agregatami organizacji i otrzymuje ich
wydarzenia ponownie ze sklepu, w kolejności ich zapisu. Dokumenty pozostałych organizacji są
nietknięte, a projekcja w międzyczasie nadal obsługuje zapytania. Odpowiedź,
`AggregatesRebuilt(target, aggregates)`, informuje, ile agregatów zostało odbudowanych.

Odbudowa jednej organizacji jest odrzucana („RebuildRefused”), podczas gdy jakakolwiek zbiorcza część projekcji
typy nie mają jeszcze wolnej przestrzeni: agregaty przechowywane w starszych wersjach biblioteki mogą jej nie mieć, a
Rozum podpowiada, żeby najpierw uruchomić `backfillSpaces`. To jest następna sekcja.

> **Pod maską:** sklep wyszukuje wydarzenia organizacji za pomocą wąskiej tabeli kluczy,
> `event_keys`, który rejestruje łączny typ i przestrzeń każdego zdarzenia. Gdy każde starsze zdarzenie ma swój
> klucz, odbudowa jednej organizacji odczytuje tylko wydarzenia tej organizacji, a nie cały magazyn
> (wydanie F124). Zobacz [ARCHITECTURE.md, sekcja 8](../ARCHITECTURE.md).

## Aktualizacja instalacji, która już zawiera dane

Instalacja działająca przez pewien czas może gromadzić agregaty zgromadzone przed każdym pierwszym
wydarzenie musiało nazwać spację (ich spacja wynosi -1), dokumenty napisane przed dokumentami miały spację (również
-1) i zdarzenia zapisane przed pojawieniem się kluczy zdarzeń. Kilka pojedynczych prac to naprawiło. Wszystkie
uruchamiać online, podczas gdy Twoja aplikacja działa, w małych transakcjach, a wszystkie z nich można zatrzymać i
ponownie uruchomić: kontynuują od miejsca, w którym skończyły. [Rozdział 18](18-production.md) wymienia je z
inne etapy produkcji. Oto, co oznaczają one dla organizacji.

1. **Policz, co wymaga naprawy.** `StoredDataCheck` odczytuje zapisane dane na kopii produkcyjnej
   ([rozdział 6](06-events.md)), a jego raport zawiera agregaty bez spacji:

   ```scala
   import io.reactivecqrs.core.tools.StoredDataCheck

   val report = StoredDataCheck.run(mpjsons, typesNames)
   println(report.aggregatesWithoutSpace)
   ```

2. **Budowanie indeksów** bez blokowania zapisu:

   ```scala
   eventStore.buildIndexesConcurrently()
   ```

   Buduje indeks, który przywraca odczyty i znajduje agregaty organizacji,
   z funkcją `CREATE INDEX CONCURRENTLY` w PostgreSQL. Nie robi nic, gdy istnieją, i kompiluje się ponownie.
   indeks, który po przerwaniu działania stał się nieprawidłowy. (Nowa lub mała baza danych otrzymuje je w ramach swojego schematu)
   stworzony.)

3. **Daj starym agregatom przestrzeń:**

   ```scala
   import io.reactivecqrs.core.eventstore.SpaceBackfill

   przestrzenie val: SpaceBackfill = eventStore.backfillSpaces() // SpaceBackfill(wypełniony, lewy)
   ```

   Każdy agregat przechowywany bez spacji otrzymuje przestrzeń swojego pierwszego zdarzenia (lub swojej spacji)
   zdarzenie, jeśli zostało zmiażdżone), w partiach po 1 000 agregatów (`batchSize`), po jednej transakcji.
   Niektóre stare, pierwsze zdarzenia nie znają swojej przestrzeni: wyświetlają się jako „SpaceId.unknown”. W takim przypadku należy przekazać
   funkcja decydująca na podstawie typu agregatu, jego identyfikatora i pierwszego zdarzenia:

   ```scala
   val spaces = eventStore.backfillSpaces((aggregateType, aggregateId, firstEvent) => firstEvent match {
     przypadek e: LegacyCartCreated => Some(SpaceId(e.shopNumber))
     przypadek _ => Brak
   })
   ```

   (`LegacyCartCreated` oznacza pierwsze wydarzenie, które utrzymuje organizację w
   pole własne.) `filled` zlicza agregaty, które otrzymały spację; `left` wyświetla te, które
   ani wydarzenie ani twoja funkcja nie mogły zostać umiejscowione: spójrz na nie ręcznie.

4. **Przypisz starym wydarzeniom ich klucze**, gdy *każdy* węzeł uruchomi nową wersję:

   ```scala
   import io.reactivecqrs.core.eventstore.EventKeysBackfill

   klucze val: EventKeysBackfill = eventStore.backfillEventKeys() // EventKeysBackfill(zapisane, brakujące)
   ```

   Działa na podstawie zdarzeń w partiach po 10 000 (`batchSize`) i rejestruje moment wystąpienia każdego zdarzenia.
   jego klucz; od tego momentu odbudowa jednej organizacji lub kompilacja kilku typów odczytuje tylko jej
   własne wydarzenia. Do tego czasu czytają cały sklep, co jest poprawne, tylko powolne. `brakuje`
   zlicza zdarzenia, których nie udało się zidentyfikować (te, których wiersz agregatu jest nieobecny).

5. **Przebuduj modele odczytu, które nie zapisały żadnej rzeczywistej przestrzeni**: dokumenty napisane przed dokumentami, które miały
   spacja lub `0` dla wszystkiego. Pełna przebudowa ([rozdział 10](10-rebuilding-projections.md))
   zapisuje je ponownie, z przestrzeniami, które teraz przekracza twoja projekcja.

> **Ostrzeżenie:** po zakończeniu `backfillEventKeys` nie należy powracać do starszej wersji
> biblioteka: starsze wersje zapisują zdarzenia bez kluczy, a kompilacje pomijały te zdarzenia.

> **Wskazówka:** uruchamiaj te zadania z zadania konserwacyjnego lub wątku w tle, nigdy na swoim komputerze.
> ścieżka startowa aplikacji. W dużym sklepie zajmuje to trochę czasu i żadna z nich nie wymaga
> wniosek o czekanie.

## Usuwanie organizacji: czyszczenie

Sklep wyprowadza się z centrum handlowego. Jego półki, szyldy i paragony w zapleczu.
Wszystko jest w porządku, sąsiednie sklepy nadal handlują, a ich klienci niczego nie zauważają. Kiedy klient
jeśli organizacja opuści Twoją platformę lub poprosi o usunięcie swoich danych, **oczyszczenie** zrobi to samo
ze względu na przestrzeń.

### Zanim zaczniesz

1. **Ucisz organizację.** Najpierw zatrzymaj jej użytkowników: biblioteka nie może wstrzymać polecenia
   wysłane, gdy trwa oczyszczanie. (Jeśli ktoś się przedostanie i utworzy agregat, oczyszczanie to zauważy i
   (okrąża jeszcze raz, nawet trzy razy.)
2. **Sprawdź swoje prognozy.** Każda prognoza zawierająca dokumenty organizacji musi
   zaimplementuj `clearForRebuild` dla `RebuildTarget.OneSpace` ([rozdział 10](10-rebuilding-projections.md)),
   i nikt nie może się w tym czasie odbudowywać. Czystka pyta najpierw każdego i odmawia, jeśli ktoś nie może
   wziąć udział.
3. **Pomyśl o swoich własnych tabelach.** Wiersze, o których biblioteka nie wie (np. paragony i skrzynka nadawcza)
   (sklepy Writer obok wydarzeń) potrzebują Twojego małego `PurgeHook`, pokazanego poniżej.
4. **Oznacz swoje sagi.** Saga, której rozkazy należą do organizacji, oznacza to poprzez nadpisanie
   `spaceOf` ([rozdział 12](12-sagas.md)); czyszczenie usuwa sagi oznaczone etykietą spacja.

### Uruchomienie

```scala
importuj io.reactivecqrs.api.AggregateType
importuj io.reactivecqrs.api.id.SpaceId
importuj io.reactivecqrs.core.space.SpacePurge

val purge = new SpacePurge(eventStore,
  commandBuses = Map(AggregateType.of[ShoppingCart] -> carts), // autobus tego węzła każdego typu, jaki ma organizacja
  projekcje = Seq(cartsList), // każda projekcja z jej dokumentami
  haki = Seq(hak paragonów))(system)

purge.purge(SpaceId(42)).foreach { raport =>
  jeśli (report.complete) println("Sklep 42 zniknął: " + report.aggregates + " aggregates")
  else println("Nie wykonano jeszcze: " + zgłoś)
}
```

Uruchom go z jednego węzła, raz. Odpowiada na `PurgeReport`:

| Pole | Co ci mówi |
|---|---|
| `complete` | prawda, gdy nic nie zostało odrzucone, nic nie zawiodło i nic z przestrzeni nie pozostało |
| `agregaty`, `wiersze` | ile agregatów zostało utworzonych i ile wierszy na tabelę (hak pod nazwą) |
| `odłączone` | kopie w *innych* organizacjach, które teraz działają samodzielnie (poniżej) |
| `projekcje`, `nieudane` | projekcje, które oczyściły przestrzeń i wszystko, co nie spełniło swojej roli, z wyjaśnieniem dlaczego |
| `odmówiono` | dlaczego się nie uruchomiło; nic nie zostało usunięte |
| `przepustki`, `pozostałe` | ile rund to zajęło i ile agregatów przestrzeni jest nadal przechowywanych |

### Co robi, krok po kroku

1. **Najpierw następuje sprawdzenie.** Każdy typ przestrzeni wymaga magistrali poleceń w `commandBuses`, a każda
   Projekcja musi być w stanie oczyścić przestrzeń. Jeśli nie, raport podaje powód w formie „odmowy” i nic
   uległo zmianie.
2. **Oddziela kopie w innych organizacjach.** Pamiętaj o szablonie koszyka skopiowanym z
   platforma do sklepu 42? Tutaj liczy się odwrotna sytuacja: kopia, którą sklep 7 wykonał z jednego ze sklepów
   Wózki 42 nadal odczytują historię sklepu 42. Zanim historia zniknie, kopia otrzymuje pierwsze zdarzenie.
   własnego, utworzonego za pomocą zdarzenia squasha typu ([rozdział 14](14-squash.md)) z własnego stanu, więc
   zachowuje dokładnie to, co miał. Typ bez zdarzenia squash nie może tego zrobić, a czyszczenie odmawia.
3. **Usuwa małymi porcjami.** Maksymalnie 1 000 agregatów i 100 000 zdarzeń na raz, w każdym kęsie
   jedna transakcja: zdarzenia, archiwum, wiersze skrzynki nadawczej,
   migawki, księgowość projekcji, przechowywane odpowiedzi poleceń idempotentnych, agregat
   rzędy i rzędy twoich haczyków. Po każdym ugryzieniu właściciele tych agregatów są zatrzymywani i każdy
   projekcja ma o nich zapomnieć.
4. **Usuwa sagi kosmiczne.**
5. **Czyści projekcje**: uruchamia się `clearForRebuild(OneSpace(space))` każdego z nich, a
   dokumenty organizacji zniknęły.

> **Wskazówka:** każdy krok można powtórzyć. Czyszczenie, które zostało odrzucone, nie powiodło się w połowie lub nie zostało ukończone
> jest po prostu uruchamiany ponownie, gdy przyczyna zostanie ustalona; czyści również projekcje, nawet gdy nie ma
> Agregat jest pozostawiony do usunięcia.

### Twoje własne tabele: `PurgeHook`

```scala
import io.reactivecqrs.api.id.{AggregateId, SpaceId}
import io.reactivecqrs.core.space.PurgeHook
import scalikeydbc._

val receiptsHook = nowy PurgeHook {
  nadpisz def name: String = "paragons"
  nadpisz def purge(space: SpaceId, aggregateIds: Seq[AggregateId])(niejawna sesja: DBSession): Long =
    sql"USUŃ Z paragonów GDZIE cart_id W (${aggregateIds.map(_.asLong)})".update().toLong
}
```

Hak jest uruchamiany w transakcji każdego bitu, więc jego wiersze są połączone z wierszami biblioteki lub nie.
Wszystko.

### Sprawdzanie, czy nic nie zostało

```scala
import io.reactivecqrs.core.tools.StoredDataCheck

StoredDataCheck.spaceRows(42L) // Mapa(agregaty -> 0, klucze_zdarzeń -> 0, sagi -> 0, lista_wózków_projekcyjnych -> 0)
```

Liczy, ile miejsca pozostało w tabelach biblioteki i w każdej tabeli modelu odczytu. Po
całkowita czystka, wszystkie wyniki są zerowe.

> **Ostrzeżenie:** kilka rzeczy jest poza zasięgiem czyszczenia. W klastrze właściciel czyszczonego
> agregat na *innym* węźle zachowuje swój stan w pamięci do następnego użycia, które nie znajduje niczego
> przechowywane lub do momentu uśpienia z powodu bezczynności ([rozdział 17](17-cluster.md)). Odpowiedzi na pytania
> polecenia idempotentne przechowywane w starszych wersjach biblioteki nie określają, który agregat
> należą, więc czekają, aż ich retencja wygaśnie (domyślnie 30 dni). Sagi bez
> `spaceOf`, agregaty bez spacji (najpierw `backfillSpaces`) i tabele bez haka
> zostań też.

> **Pod maską:** zaraz po ugryzieniu projekcja może nadal otrzymać zdarzenie oczyszczonego koszyka
> to już było w drodze. Przez pół godziny (`purgedDropWindow`) potwierdza takie dostawy
> bez ich stosowania, więc nic nie zapisuje dokumentu ponownie; zachowuje 8 bajtów na wyczyszczony agregat
> tymczasem. Zobacz [ARCHITECTURE.md, sekcja 4.7](../ARCHITECTURE.md) i projekt w
> [docs/design/M6-organizations.md](../design/M6-organizations.md).

## Przenoszenie organizacji do innej instalacji

Sklep przenosi się do centrum handlowego w innym mieście. Jego asortyment, książki i historia pakują się w
pudełka, podróżują i są rozpakowywane w pustym pomieszczeniu, gdzie umieszczane są znaki nowego centrum
dla niego. Przenoszenie organizacji między instalacjami wygląda tak samo: **eksport** pakuje pudła,
**import** rozpakowuje je do pustej przestrzeni innej instalacji, a modele odczytu są
odbudowany tam.

### Pakowanie: eksport

```scala
importuj java.nio.charset.StandardCharsets
import java.nio.file.{Pliki, Ścieżki}
import io.reactivecqrs.core.space.SpaceExport

val writer = Files.newBufferedWriter(Paths.get("shop-42.jsonl"), StandardCharsets.UTF_8)
val exported = wypróbuj nowy SpaceExport(eventStore, contexts = Seq(new ShoppingCartAggregateContext)).write(SpaceId(42), writer)
               na koniec writer.close()
// ExportReport(przestrzeń, agregaty, zdarzenia, zarchiwizowane, odłączone, waitingInOutbox, odrzucone)
```

Plik zawiera jedną linię JSON na agregat i na zdarzenie, zdarzenia w kolejności, w jakiej wystąpiły.
przechowywane, a także zarchiwizowane wydarzenia ([rozdział 14](14-squash.md)); migawki, modele odczytu i sagi pozostają
z tyłu. Eksportuj cichą organizację: `waitingInOutbox` informuje, ile jej zdarzeń nie zostało
jeszcze nie dostarczono.

„Konteksty” to zbiorcze konteksty typów organizacji. Mają one znaczenie w jednym przypadku:
Skopiuj ten sklep 42 z czyjegoś koszyka, aby odczytać historię tej innej organizacji, która
nie podróżuje. Eksport daje takiej kopii pierwsze własne zdarzenie, utworzone za pomocą zdarzenia squash tego typu
z własnego stanu, tak jak robi to czystka, więc zachowuje dokładnie to, co miał. Bez wydarzenia squash
(lub bez kontekstu), eksport odmawia i nic nie zapisuje.

### Rozpakowywanie: import

```scala
importuj io.reactivecqrs.core.space.SpaceImport

val imported = new SpaceImport(eventStore, uidGenerator).read(Paths.get("shop-42.jsonl"), into = SpaceId(7))
// ImportReport(przestrzeń, agregaty, zdarzenia, zarchiwizowane, ponownie zmapowane, nieznane odniesienia, niektóre nieznane, odrzucone)
```

Działa na innej instalacji, w przestrzeni, która musi być pusta. Odczytuje plik dwukrotnie:

1. **Najpierw sprawdza**, nic nie zapisując: każda klasa w pliku musi tu istnieć, każde zdarzenie musi
   musi mieścić się w granicach tej instalacji ([rozdział 6](06-events.md)), a przestrzeń musi być pusta. Każdy
   problem kończy się na `odmówiono`.
2. **Następnie zapisuje**: agregaty, następnie zdarzenia w ich oryginalnej kolejności, z ich cofnięciem
   znaki, a następnie zarchiwizowane wydarzenia.

Dwie instalacje utworzyły swoje identyfikatory w swoich własnych zakresach (numer instalacji z rozdziału 2), więc
Identyfikatory rzadko kolidują; identyfikatory sprzed wersji 1.0 mogą. Agregat, którego identyfikator jest tu pobierany, otrzymuje nowy, a
każde odniesienie do niego następuje: import odczytuje każde zdarzenie z własną instancją mpjsons, która
zastępuje przeniesione wartości `AggregateId` i `UserId` i zmienia `SpaceId` starej organizacji na
nowy. `remapped` wyświetla identyfikatory, które zostały przeniesione.

> **Ostrzeżenie:** import widzi tylko identyfikatory wpisane. Identyfikator przechowywany jako zwykły `Long` lub w
> tekst („user:5” w aktorze metadanych) jest dla niego niewidoczny. Nadaj importowi wartość `ImportRewriter` dla
> każdy typ, który zawiera takie odniesienia (`rewriters = Map(AggregateType.of[ShoppingCart] -> ...)`): to
> pobiera każde zdarzenie z funkcją, która mapuje stare identyfikatory na nowe. A jeśli zarejestrowałeś konwertery
> w swoich mpjsonach przekaż `newMPJsons = () => yourConfiguredMPJsons()`, aby import odczytał Twoje klasy
> tak jak ty.

`unknownReferences` zlicza dla Ciebie odniesienia do agregatów, które nie podróżowały ani tu nie istnieją
patrzeć; pozostawia się je takimi, jakie są.

### Po przeprowadzce

1. **Przebuduj modele odczytu** nowej przestrzeni: wyślij każdą projekcję
   `RebuildAggregates(RebuildTarget.OneSpace(SpaceId(7)))` ([rozdział 10](10-rebuilding-projections.md)).
   Import nie zapisuje żadnych wierszy skrzynki nadawczej, więc nic nie jest dostarczane na bieżąco.
2. **Wskaż użytkownikom organizacji** nową instalację.
3. **Usuń starą kopię**, gdy będziesz mieć pewność ([wcześniej w tym rozdziale](#usuwanie-organizacji-czyszczenie)).

> **Wskazówka:** import, który zatrzymał się w połowie lub chcesz go powtórzyć, można łatwo powtórzyć: wyczyść
> przestrzeń docelowa i ponownie zaimportuj ten sam plik.

> **Wskazówka:** jeśli uruchamiasz więcej niż jedną instalację, nadaj każdej z nich własny
> `reactivecqrs.installation-number` tak wcześnie, jak to możliwe ([rozdział 18](18-production.md)). Każdy
> instalacja następnie tworzy identyfikatory zbiorcze w swoim własnym zakresie, dzięki czemu organizacje mogą później przenosić
> między instalacjami bez dwóch agregatów współdzielących identyfikator. Zobacz
> [ARCHITECTURE.md, sekcja 13.5](../ARCHITECTURE.md).

## Czego się nauczyłeś

- Organizacja to **przestrzeń** (`SpaceId`); przestrzeń 0 to sama platforma.
- Pierwsze zdarzenie każdego agregatu nadaje nazwę jego przestrzeni, a nowy agregat bez takiego zdarzenia jest odrzucany; kopia
  nadaje nazwę własnej przestrzeni.
- Utrzymuj przestrzeń w stanie, w którym jest to wymagane podczas zawodów squasha lub projekcji na żywo.
- Dokumenty zawierają przestrzeń (`insertDocument(space.asLong, key, document)`), więc jedna organizacja
  dokumenty mogą być czyszczone (`clearSpace`) i odbudowywane (`RebuildAggregates(RebuildTarget.OneSpace(space))`).
- Instalacja ze starszymi danymi uruchamia `buildIndexesConcurrently`, `backfillSpaces` i
  `backfillEventKeys` raz, online.
- `SpacePurge` usuwa organizację: najpierw sprawdza, odłącza kopie innych organizacji,
  usuwa w małych transakcjach za pomocą `PurgeHook`, czyści projekcje i raporty; uruchom go
  ponownie, aż do `zakończenia` i sprawdź za pomocą `StoredDataCheck.spaceRows`.
- `SpaceExport` pakuje organizację do pliku zawierającego linie JSON, a `SpaceImport` rozpakowuje ją do
  pusta przestrzeń innej instalacji, przenosząca pobrane identyfikatory i odniesienia do nich; następnie
  odbudować modele odczytu przestrzeni.

Następnie: [rozdział 17](17-cluster.md) rozkłada to wszystko na kilka maszyn.
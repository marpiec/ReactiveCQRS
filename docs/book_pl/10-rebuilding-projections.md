# Rozdział 10. Przebudowa projekcji bez przestojów

Pod koniec tego rozdziału będziesz umieć wypuścić nową wersję projekcji, poprawić w niej błąd albo
dodać zupełnie nowy model odczytu, zbudowany z wieloletniej historii zdarzeń, a stary model odczytu
przez cały ten czas będzie odpowiadał na zapytania. Naprawisz też dokument jednego koszyka, nie
ruszając reszty. Ta sztuczka to *projekcja zarządzana*, a kosztuje cię mniej więcej dziesięć linijek
kodu, który wszystko ze sobą spina.

---

## Po co w ogóle przebudowywać?

Pamiętasz bank z rozdziału 1? Księga zapisuje każdą transakcję, a podsumowanie w twojej aplikacji
bankowej jest jej projekcją. Pewnego dnia bank postanawia, że podsumowanie ma pokazywać także sumy
miesięczne. Nikt nie prosi klientów, żeby jeszcze raz wpisali swoje transakcje. Bank czyta księgę od
pierwszej linijki i pisze nowe podsumowanie.

Tego samego zechcesz dla swoich modeli odczytu, i to z całkiem codziennych powodów:

- **Nowa wersja.** Lista koszyków ma pokazywać nazwy wielkimi literami albo liczbę pozycji obok
  każdej nazwy.
- **Poprawka błędu.** Listener przez miesiące zapisywał błędne dane. Poprawiasz kod, a błędne wiersze
  trzeba zapisać od nowa.
- **Nowy model odczytu.** Projekcja dodana dzisiaj musi wiedzieć o koszykach utworzonych w zeszłym
  roku. Zwykła projekcja (rozdział 8) usłyszy o starym koszyku dopiero wtedy, gdy ten znów się
  zmieni.
- **Zmieniony agregat.** Zmieniasz sposób, w jaki zdarzenia koszyka budują jego stan, i podnosisz
  `version` kontekstu. Modele odczytu zrobione ze stanów trzeba zrobić od nowa.
- **Naprawa.** Jeden dokument się zepsuł (powiedzmy, że ręczna poprawka w SQL-u poszła nie tak) i
  chcesz zapisać od nowa tylko jego, z jego zdarzeń.

## Sklep obok

Przebudowa modelu odczytu w miejscu ma brzydką stronę: gdy opróżniasz jego tabele i wypełniasz je od
nowa, każdy, kto akurat zapyta, dostaje pół odpowiedzi. Dlatego ReactiveCQRS nie przebudowuje w
miejscu.

Wyobraź sobie sklep, który potrzebuje nowych półek. Zamiast zamykać go na tydzień, urządzasz pusty
lokal obok i zaopatrujesz go według ewidencji magazynowej, a stary sklep w tym czasie dalej sprzedaje.
Gdy nowy jest gotowy, przewieszasz szyld nad jego drzwi. Klienci nigdy nie trafiają na zamknięte
drzwi.

Tak właśnie działa **projekcja zarządzana**:

```
                                ┌── build 1 (version 1), tables *_v1 ── answers queries, takes live events
you ── queries ──▶ supervisor ──┤
                                └── build 2 (version 2), tables *_v2 ── filled from the event store
```

- **Budowa** (*build*) to jedna kopia projekcji, z własnymi tabelami. Budowy są numerowane: 1, 2,
  3...
- **Aktywna budowa** odpowiada na zapytania. **Budowa w toku** jest właśnie wypełniana z magazynu
  zdarzeń.
- **Nadzorca** (*supervisor*) to aktor, z którym rozmawiasz. Prowadzi budowy, przekazuje twoje
  zapytania aktywnej i przełącza na nową, gdy ta nadrobi zaległości: jedna aktualizacja w bazie
  danych i szyld przewieszony.
- **Rejestr** to kilka tabel, w których biblioteka pamięta dla każdej projekcji jej aktywną budowę i
  budowę w toku.

Po przełączeniu tabele starej budowy zostają usunięte. Nie piszesz ani linijki tej księgowości.

## Przygotowanie projekcji zarządzanych

Pod zarząd weźmiemy projekcję nazw koszyków z przykładu E10, opartą na koszykach przykładu E1 z
rozdziału 2. Trzyma ona jeden dokument na koszyk, a w nim nazwę koszyka. Umie też zapisywać nazwy
wielkimi literami, czym zajmie się jej wersja 2, a `clearForRebuild` na samym dole przyda się przy
przebudowach celowanych pod koniec rozdziału:

<!-- snippet: e10/CartNames.scala#projection -->
```scala
case class GetCartName(id: AggregateId) // answered with Option[String], once the cart's document is there

class CartNames(val subscriptionsState: SubscriptionsState, store: DocumentStore[String], capitals: Boolean) extends ProjectionActor {

  override protected val projectionName: String = "CartNames"

  protected val listeners = List(AggregateListener[ShoppingCart] { (id, _, created, cart) => implicit session =>
    cart match {
      case Some(c) if created => store.insertDocument(0, id.asLong, written(c.name))
      case Some(c)            => store.overwriteDocument(id.asLong, written(c.name))
      case None               => store.removeDocument(id.asLong) // the cart was deleted
    }
  })

  private def written(name: String): String = if (capitals) name.toUpperCase else name

  override protected def receiveQuery: Receive = {
    case GetCartName(id) => delayIfNotAvailable(sender(), () => store.getDocument(id.asLong).map(_.document), 2000)
  }

  override protected def clearForRebuild(target: RebuildTarget)(implicit session: DBSession): Unit = target match {
    case RebuildTarget.OneAggregate(id) => store.removeDocument(id.asLong)
    case RebuildTarget.OneSpace(space)  => store.clearSpace(space.asLong)
    case RebuildTarget.OneType(_)       => store.clearAllData() // CartNames receives carts only
  }
}
```

W listenerze nie ma nic, czego nie znasz z rozdziału 8. Zmienia się sposób uruchomienia. Cztery
kroki, wszystkie tutaj:

<!-- snippet: e10/RebuildSetup.scala#start -->
```scala
val registry = new PostgresProjectionRegistry().initSchema()
val projections = new Projections(system, ProjectionsSettings(eventStore, subscriptions, registry,
  contexts = Seq(new ShoppingCartContext)))

val cartNames = projections.start[CartNames]("e10-cart-names", version, "cartNames") { build =>
  new CartNames(subscriptions,
    new PostgresDocumentStore[String](build.documentStore("e10_cart_names"), mpjsons, new NoopDocumentStoreCache),
    capitals = build.version >= 2)
}
eventBus ! EventsBusActor.RegisterSubscribers(List(cartNames)) // the supervisor, where the projection would be
```

1. **Rejestr**, tworzony raz, przy starcie aplikacji: kilka tabel, w których biblioteka pamięta
   budowy każdej projekcji.
2. **Jeden obiekt `Projections`** na system aktorów. To on prowadzi wszystkie projekcje zarządzane.
   `eventStore` to miejsce, z którego budowy czytają zdarzenia, a `subscriptions` to stan
   subskrypcji, którego twoje projekcje już używają (rozdział 8). `contexts` wymienia konteksty
   agregatów, których *stany* dostają twoje projekcje zarządzane: `CartNames` słucha stanów koszyków,
   więc trafia tu kontekst koszyka. Trzy kolejne parametry mają wartości domyślne: `builder` (jak
   budowa czyta, `BuilderSettings()`), `buildsAtOnce = 2` oraz `restartAfter = 10.seconds` (ile
   nadzorca czeka, zanim spróbuje ponownie, gdy coś zawiodło).
3. **Projekcja, uruchamiana przez niego.** Czytaj od lewej do prawej: klasa projekcji, jej **nazwa**
   (`"e10-cart-names"`), jej **wersja** (`version`: 1 w pierwszym wydaniu), nazwa aktora jej
   nadzorcy (`"cartNames"`) i funkcja, która tworzy jedną instancję projekcji dla danej budowy.
   Funkcja pyta budowę o nazwę tabeli: `build.documentStore("e10_cart_names")` daje
   `e10_cart_names_v1` dla pierwszej budowy wersji 1, a magazyn dokumentów robi z tego tabelę
   `projection_e10_cart_names_v1`. Funkcja mówi też instancji, na podstawie wersji budowy, czy ma
   pisać wielkimi literami.
4. **Nadzorca wszędzie tam, gdzie byłaby projekcja**: rejestrujesz go w szynie zdarzeń (razem z
   pozostałymi subskrybentami, w jednym komunikacie) i to do niego wysyłasz zapytania:

   ```scala
   val name: Future[Option[String]] = (cartNames ? GetCartName(cartId)).mapTo[Option[String]]
   ```

Przy pierwszym uruchomieniu rejestr nie zna jeszcze `e10-cart-names`, więc nadzorca zaczyna budowę 1
i wypełnia ją wszystkimi koszykami, które już są w magazynie. Innej budowy nie ma, więc budowa 1 od
początku odpowiada na zapytania tym, co już ma. Gdy nadrobi zaległości, po prostu pracuje dalej na
zdarzeniach na żywo. Od tej chwili rejestr mówi: `e10-cart-names`, aktywna budowa 1, wersja 1.

> **Pamiętaj:** nazwa to tożsamość projekcji w bazie danych. Żeby zmienić projekcję, zmień jej
> wersję, a nazwę zostaw.

> **Ostrzeżenie:** każdy magazyn twórz wewnątrz funkcji, a każdą tabelę nazywaj przez budowę:
> `build.documentStore(table)` dla magazynu dokumentów, `build.table(name)` i `build.sequence(name)`
> dla własnych tabel i sekwencji. Wtedy dwie budowy nigdy nie dzielą tabeli, a usuwana budowa zabiera
> ze sobą wszystkie swoje tabele. Magazyn czy pamięć podręczna, utworzone raz, poza funkcją, byłyby
> wspólne dla dwóch budów i pomieszałyby ich dokumenty.

> **Wskazówka:** nazwy tabel trzymaj krótkie. Mogą zawierać tylko litery, cyfry i podkreślenia;
> budowa dokleja przyrostek w rodzaju `_v3_b4`, magazyn dokumentów dodaje z przodu `projection_`, a
> PostgreSQL pozwala na 63 znaki. Jeśli pełna nazwa magazynu byłaby dłuższa, jego tworzenie kończy się
> błędem.

Przy okazji: projekcje zarządzane nie potrzebują `.withDispatcher(BlockingDispatcher.id)`. Nadzorca
sam tworzy za ciebie każdą instancję na dispatcherze blokującym.

## Wypuszczamy nową wersję

Powiedzmy, że sklep chce mieć nazwy koszyków wielkimi literami. E10 jest na to gotowy: `CartNames`
pisze wielkimi literami, gdy `capitals` jest prawdą, a funkcja tworząca instancję ustawia tę flagę na
podstawie wersji budowy (`capitals = build.version >= 2`). Całe wydanie wygląda więc tak:

1. Podnieś wersję w `start` z 1 na 2. (Bez flagi z E10 trzeba by zmienić listener tak, żeby
   zapisywał `c.name.toUpperCase`, i podnieść wersję.)
2. Wdróż i uruchom aplikację.

To wszystko, co robisz. A oto, co dzieje się dalej:

- Nadzorca czyta rejestr: aktywna budowa jest w wersji 1, a kod w wersji 2. Zaczyna więc budowę 2, z
  własnymi tabelami (`projection_e10_cart_names_v2`).
- Budowa 1 dalej odpowiada na zapytania.
- Budowniczy (*builder*) czyta koszyki z magazynu zdarzeń i podaje je budowie 2, po wiele naraz,
  jedna transakcja na porcję.
- Gdy budowa 2 ma już każde zdarzenie, rejestr przełącza się w jednej aktualizacji. Od tej chwili
  odpowiada budowa 2.
- Budowa 1 odłącza się od szyny zdarzeń i zatrzymuje; jej tabele, wersje subskrypcji i zapisy o
  błędach zostają usunięte.

E10 uruchamia oba wydania, każde we własnym systemie aktorów, bo ponowne wdrożenie też uruchamia
aplikację od nowa; `RebuildSpec` sprawdza, że wersja 1 podawała nazwy tak, jak je wpisano, a wersja 2
podaje je wielkimi literami.

Wszystko widać w logu. Dla naszej projekcji wygląda to tak (pozostałe linie pominięte):

```
Projection e10-cart-names serves build 1 (version 1), builds 2 (version 2)
Projection projection:e10-cart-names_v2 is live; reconciling it with the store
Projection e10-cart-names switched to build 2 (version 2)
Projection e10-cart-names: build 1 is dropped
```

Możesz też zapytać nadzorcę. Na `GetBuilds` odpowiada on `Builds(name, active, building, caughtUp)`:

<!-- snippet: e10/RebuildSpec.scala#getBuilds -->
```scala
import io.reactivecqrs.core.rebuild.ProjectionSupervisor._

val builds = (cartNames ? GetBuilds).mapTo[Builds]
// while it builds:  active = build 1, building = build 2
// after the switch: active = build 2, building = None
```

`active` i `building` to rekordy `BuildRecord`: `number` budowy, jej `version`, przyrostek jej tabel
`suffix` (`"_v2"`) oraz nazwa `subscriber`, pod którą zapisywane są wersje jej subskrypcji
(`"projection:e10-cart-names_v2"`). `caughtUp` jest prawdą, gdy budowa w toku ma już każde zdarzenie;
zastępuje ona aktywną budowę, gdy tylko nie ma żadnej nieudanej subskrypcji (więcej o tym niżej).
`position` w rekordzie budowy w toku to miejsce, w którym budowa ostatnio zapisała swoje postępy. E10
czeka na przełączenie, pytając o budowy, aż odpowiada już tylko nowa wersja:

<!-- snippet: e10/RebuildSetup.scala#builds -->
```scala
/** The builds once the projection's `version` serves alone, the switch to it done; asked every 200 ms. */
@tailrec
def switchedTo(shop: Shop, version: Int, deadline: Deadline)(implicit timeout: Timeout): Builds = {
  val builds = Await.result((shop.cartNames ? GetBuilds).mapTo[Builds], timeout.duration)
  if (builds.active.exists(_.version == version) && builds.building.isEmpty) builds
  else if (deadline.isOverdue()) throw new IllegalStateException(s"Version $version does not serve yet: $builds")
  else {
    Thread.sleep(200)
    switchedTo(shop, version, deadline)
  }
}
```

Żeby podejrzeć budowę przy pracy, wyślij temu samemu nadzorcy `GetBuildStatus`:

<!-- snippet: e10/RebuildSpec.scala#buildStatus -->
```scala
import io.reactivecqrs.core.rebuild.ProjectionBuilder.{BuildStatus, GetBuildStatus}

val buildStatus = (cartNames ? GetBuildStatus).mapTo[BuildStatus]
// BuildStatus("Bulk", 182044, 91000, 0, 0, 0): reading the store, 91 000 events sent so far
```

Faza (`phase`) przechodzi przez `Bulk` (czytanie magazynu), `GoingLive` i `Reconciling` (dołączanie
do zdarzeń na żywo), a na końcu `CaughtUp`. Gdy żaden budowniczy nie pracuje, nadzorca odpowiada
`Waiting` (budowa czeka na swoją kolej albo na restart po błędzie) lub `NoBuild`. Pozostałe liczby
to: wysłane zdarzenia, wypełnione luki, agregaty przekazane do dostarczania na żywo i zdarzenia,
których nie udało się odczytać.

> **Pamiętaj:** przerwana budowa (gdy zatrzymujesz aplikację albo ta pada) przy następnym starcie
> rusza dalej od miejsca, w którym była; nie zaczyna od nowa. Ale jeśli wdrożysz wersję 3, gdy wersja
> 2 wciąż się buduje, w połowie zbudowana wersja 2 trafia do kosza: zamiast niej buduje się wersja 3.

## Co w tym czasie robi stara budowa

Gdy buduje się wersja 2, na zapytania odpowiadają tabele wersji 1. Ale *kodu* wersji 1 już nie ma:
wdrożona jest wersja 2. Co więc ze zdarzeniami, które przychodzą w międzyczasie? Wybór należy do
ciebie, a decyduje o nim parametr `whileBuilding` metody `start`:

- **`ServeLive`** (domyślnie): aktywna budowa dalej nanosi zdarzenia na żywo na swoje stare tabele,
  już nowym kodem. Zapytania widzą świeże dane, a zapytania czekające, aż pojawi się dokument
  (rozdział 8), dostają odpowiedź. Wybierz tę opcję, gdy nowy kod potrafi pracować na starych
  dokumentach, tak jak wersja z wielkimi literami: obie wersje przechowują `String`.
- **`ServeStale`**: aktywna budowa odpowiada ze swoich tabel w takim stanie, w jakim są, i aż do
  przełączenia nie przyjmuje żadnych zdarzeń. Wybierz tę opcję, gdy nowy kod nie umie pracować na
  starych dokumentach. Powiedzmy, że wersja 3 przechowuje `CartSummary(name, itemsCount)` zamiast
  `String`: kod wersji 3 nie przeczyta wierszy wersji 2. Obowiązuje to przy każdej budowie: tej,
  którą zaczyna nowa wersja, i tej, o którą prosisz przez `RebuildProjection`, gdy aplikacja działa.

```scala
projections.start[CartNames]("e10-cart-names", 3, "cartNames", whileBuilding = ServeStale) { build => ... }
```

Obie wartości znajdziesz w `io.reactivecqrs.core.rebuild.ProjectionSupervisor`.

> **Ostrzeżenie:** przy `ServeStale` aktywna budowa nie przyjmuje zdarzeń, dopóki buduje się nowa. Jeśli
> przed przełączeniem wrócisz do starszego kodu, nowa budowa zostanie porzucona, a aktywna znów obsłuży
> zdarzenia na żywo, ale bez tego, co zatwierdzono w międzyczasie: agregat nadrobi to dopiero przy
> swoim następnym zdarzeniu. Po takim powrocie wyślij `RebuildAggregates(RebuildTarget.OneType(...))`
> dla każdego typu projekcji.

> **Wskazówka:** funkcja dostaje budowę, a budowa zna własną wersję (`build.version`). Instancja
> zrobiona dla aktywnej budowy wersji 1 zachowuje się jak wersja 1, dopóki buduje się wersja 2.
> Właśnie to robi `capitals = build.version >= 2` z E10: przy `ServeLive` aktywna budowa wersji 1
> dalej zapisuje nazwy tak, jak je wpisano, w tabelach wersji 1.

> **Uwaga:** magazyny dokumentów aktywnej budowy zachowują indeksy, które mają ich tabele. Gdy wersja 2
> deklaruje nowy indeks, w tym unikalny, który dokumenty wersji 1 by naruszyły, dostają go tylko
> własne tabele wersji 2; wersja 1 obsługuje zapytania jak dotąd. Dla instancji starszej budowy
> `build.current` jest fałszywe.

## Co widzi projekcja w trakcie budowy

Budowa podaje twojej projekcji te same zdarzenia, w tej samej kolejności w obrębie agregatu, co
dostarczanie na żywo, razem ze zdarzeniami cofnięcia. Dlatego przebudowane tabele wychodzą takie same
jak tabele utrzymywane na żywo; `RebuildEquivalenceSpec` sprawdza to na losowych historiach. Trzy
rzeczy się jednak różnią, a projekcja potrafi je rozpoznać:

- **`rebuilding`** ma wartość `true`, gdy instancja jest budowana i jeszcze nie obsługuje zapytań, a
  także wtedy, gdy przebudowa celowana (niżej) podaje jej ponownie stare zdarzenia. Wszystko, co ma
  efekt uboczny, na przykład komunikat do innego aktora albo aktualizacja wypychana na ekran
  (rozdział 11), najpierw to sprawdza. `ManagedNotes` w `ProjectionsSpec` wypycha swoje aktualizacje
  tak:

  ```scala
  if (!rebuilding) sendUpdate[NoteLines](lines)
  ```

- **`onActivated()`** jest wywoływana raz, gdy zbudowana instancja zaczyna obsługiwać zapytania.
  Nadpisz ją, jeśli twoja projekcja ma wtedy coś do dokończenia.
- **Porcje.** Budowa zatwierdza wiele agregatów w jednej transakcji, a po zatrzymaniu wznawia pracę
  od ostatniej zapisanej porcji. Listener, który między wywołaniami trzyma stan w pamięci, poza
  swoimi tabelami, nie może na tym stanie polegać w trakcie budowy.

## Kiedy zaczyna się budowa?

Budowę uruchamiają cztery rzeczy:

1. **Nowa projekcja.** Pierwsze uruchomienie projekcji o nazwie, której rejestr nie zna, zaczyna jej
   budowę.
2. **Nowa wersja.** Wersja w kodzie różni się od wersji aktywnej budowy.
3. **Zmieniony kontekst.** Wersja (`version`) kontekstu agregatu, którego stany dostaje projekcja,
   różni się od tej, z którą zrobiono aktywną budowę. Podnieś `ShoppingCartContext.version`, a
   `CartNames` zbuduje się od nowa, w swojej własnej wersji. Projekcja, która słucha tylko zdarzeń
   koszyka (`EventsListener`), zachowuje swoją budowę.
4. **Prosisz o nią** przez `RebuildProjection` (następna sekcja).

Naraz działają najwyżej dwie budowy (`buildsAtOnce`); pozostałe czekają na swoją kolej.

## Prośba o przebudowę

Poprawka błędu w listenerze, bez żadnych innych zmian? Poproś o świeżą budowę tej samej wersji:

<!-- snippet: e10/RebuildSpec.scala#rebuild -->
```scala
val started = cartNames ? RebuildProjection
// answers RebuildStarted(2): build 2 of version 1, in tables with the suffix "_v1_b2"
```

Dalej ten sam taniec: budowa 2 wypełnia się z magazynu, a budowa 1 w tym czasie obsługuje zapytania;
potem przełączenie i usunięcie. Jeśli jakaś budowa albo przebudowa celowana już trwa, nadzorca
odpowiada zamiast tego `RebuildRefused(reason)`.

> **Wskazówka:** przebudowa bywa też najszybszym lekarstwem na projekcję, która mocno została w tyle,
> na przykład po długiej awarii: budowa czyta magazyn po tysiąc zdarzeń naraz, a nadrabianie przez
> szynę zdarzeń i outbox idzie agregat po agregacie (`docs/SCALING.md`, sekcje 4 i 5).

## Budowa, która nie chce się przełączyć

Budowa zastępuje aktywną tylko wtedy, gdy nie ma żadnej nieudanej subskrypcji. Jeśli podczas budowy
listener zawiedzie na jakimś koszyku (o błędach mówi rozdział 9), budowa czeka: nadzorca zapisuje w
logu ostrzeżenie i sprawdza ponownie co 30 sekund. Aktywna budowa w tym czasie normalnie obsługuje
zapytania.

Żeby dotrzeć do budowy w toku, opakuj swój komunikat w `ToBuildInProgress`. Nadzorca przekaże go
wtedy budowie w toku zamiast aktywnej:

<!-- snippet: e10/RebuildSpec.scala#toBuild -->
```scala
import io.reactivecqrs.core.projection.{GetProjectionStatus, RetryAllFailedSubscriptions, SkipFailedVersion}

cartNames ? ToBuildInProgress(GetProjectionStatus)          // the build's ProjectionStatus
cartNames ? ToBuildInProgress(RetryAllFailedSubscriptions)  // FailedSubscriptionsRetried(count)
cartNames ? ToBuildInProgress(SkipFailedVersion(cartId))    // FailedSubscriptionsHandled(cartId, count)
```

Gdy ponowisz albo pominiesz to, co się nie udało, budowa przełączy się przy najbliższym sprawdzeniu.
Gdy żadna budowa nie trwa, odpowiedzią jest `RebuildRefused("no build is running")`.

## Naprawa kilku agregatów: przebudowy celowane

Czasem na jednej stronie katalogu jest literówka. Drukujesz od nowa tę jedną stronę, a nie cały
katalog. **Przebudowa celowana** robi to samo z modelem odczytu: przebudowuje w miejscu to, co
aktywna budowa zrobiła z jednego agregatu, jednego typu agregatu albo jednej organizacji, a wszystko
inne zostawia w spokoju.

Najpierw projekcja musi wiedzieć, jak wyczyścić to, co zrobiła z danego celu. `CartNames` nadpisuje
`clearForRebuild`:

<!-- snippet: e10/CartNames.scala#clear -->
```scala
override protected def clearForRebuild(target: RebuildTarget)(implicit session: DBSession): Unit = target match {
  case RebuildTarget.OneAggregate(id) => store.removeDocument(id.asLong)
  case RebuildTarget.OneSpace(space)  => store.clearSpace(space.asLong)
  case RebuildTarget.OneType(_)       => store.clearAllData() // CartNames receives carts only
}
```

Potem poproś nadzorcę o przebudowę jednego koszyka,

<!-- snippet: e10/RebuildSpec.scala#oneCart -->
```scala
val one = cartNames ? RebuildAggregates(RebuildTarget.OneAggregate(cartId))
// answers AggregatesRebuilt(RebuildTarget.OneAggregate(cartId), 1)
```

wszystkich koszyków,

<!-- snippet: e10/RebuildSpec.scala#everyCart -->
```scala
import io.reactivecqrs.api.AggregateType

val everyCart = cartNames ? RebuildAggregates(RebuildTarget.OneType(AggregateType.of[ShoppingCart]))
```

albo wszystkich koszyków jednej organizacji (rozdział 16); naraz trwa zawsze tylko jedna przebudowa:

<!-- snippet: e10/RebuildSpec.scala#organization -->
```scala
import io.reactivecqrs.api.id.SpaceId

val organization = cartNames ? RebuildAggregates(RebuildTarget.OneSpace(SpaceId(7)))
```

Co się dzieje:

1. Nadzorca zapisuje cel w rejestrze, więc przebudowa przerwana zatrzymaniem rusza ponownie przy
   następnym starcie.
2. Aktywna budowa nanosi dostarczenia, które już przyjęła, wypisuje agregaty celu i w jednej
   transakcji wywołuje twoje `clearForRebuild` oraz zapomina, jak daleko doszła z tymi agregatami.
3. Budowniczy czyta ich zdarzenia z magazynu, w kolejności, w jakiej zostały zatwierdzone, i podaje
   je projekcji jeszcze raz. Projekcja w tym czasie dalej obsługuje zapytania i dalej przyjmuje
   zdarzenia na żywo.
4. Odpowiedzią jest `AggregatesRebuilt(target, count)`, z liczbą przebudowanych agregatów.

Między krokami 2 i 4 zapytania o cel nie znajdują jego dokumentów albo znajdują je przebudowane w
połowie.

E10 naprawia w ten sposób jeden koszyk, którego nazwę w tabeli aktywnej budowy nadpisała nieudana
ręczna poprawka w SQL-u:

<!-- snippet: e10/RebuildSetup.scala#repair -->
```scala
def repair(shop: Shop, cartId: AggregateId)(implicit timeout: Timeout): AggregatesRebuilt =
  Await.result(shop.cartNames ? RebuildAggregates(RebuildTarget.OneAggregate(cartId)), timeout.duration) match {
    case rebuilt: AggregatesRebuilt => rebuilt // AggregatesRebuilt(OneAggregate(cartId), 1)
    case refused => throw new IllegalStateException("Not rebuilt: " + refused)
  }
```

Praca w toku jej nie odrzuca. Gdy trwa inna przebudowa celowana, ta czeka na swoją kolej. Trwająca
budowa też ją wykonuje, gdy już nadrobi zaległości, a zanim zastąpi aktywną budowę: jej przebieg
masowy mógł odczytać zdarzenia celu, zanim się zmieniły. Odpowiedź dostajesz, gdy skończy aktywna
budowa; trwająca budowa robi swoją część, zanim zacznie obsługiwać zapytania. Nadzorca trzyma
czekające przebudowy w rejestrze, więc zatrzymanie żadnej nie gubi: uruchomią się ponownie przy
następnym starcie.

Nadzorca odpowiada zamiast tego `RebuildRefused(reason)`, gdy:

- projekcja nie implementuje `clearForRebuild` (mówi o tym powód);
- cel nie dotyczy typu agregatu, który dostaje projekcja;
- cel to organizacja, a niektóre agregaty typów tej projekcji nie znają jeszcze swojej organizacji.
  Agregatom zapisanym przez starsze wersje biblioteki może jej brakować; `backfillSpaces` naprawia
  to, jednorazowo (rozdział 16).

> **Ostrzeżenie:** czyść dokładnie to, co wytworzyły agregaty celu. Wyczyścisz mniej, a zostaną
> nieaktualne dane. Wyczyścisz więcej, a stracisz to na dobre: przebudowa oddaje tylko zdarzenia
> celu. `clearAllData()` dla `OneType` jest w `CartNames` w porządku tylko dlatego, że ta projekcja
> dostaje jeden typ.

> **Wskazówka:** ask ma swój limit czasu, a przebudowa wszystkich koszyków trwa o wiele dłużej niż
> jednego. Daj askowi pasujący limit albo wyślij komunikat z aktora, który poczeka na odpowiedź. Ask,
> któremu minął czas, nie zatrzymuje przebudowy.

## Co zostaje niezarządzane: efekty uboczne

Niektórzy subskrybenci w ogóle nie są modelami odczytu: choćby ten, który wysyła powitalnego e-maila,
gdy powstaje koszyk, albo ten, który wysyła polecenie do innego agregatu. Przebudowa takich
subskrybentów byłaby małą katastrofą: każdy kiedykolwiek wysłany e-mail poszedłby jeszcze raz.

Niech zostaną zwykłymi projekcjami, tworzonymi i rejestrowanymi tak, jak pokazuje rozdział 8, a nie
przez `Projections`. Biblioteka nigdy ich nie przebudowuje: budowa dostarcza zdarzenia tylko tej instancji
projekcji, którą wypełnia. Gdzie postawić granicę:

- efekt uboczny **jest** zadaniem subskrybenta: zostaw go niezarządzanym;
- efekt uboczny to mały dodatek do modelu odczytu (aktualizacja wypychana na ekran): niech model
  odczytu będzie zarządzany, a dodatek osłoń warunkiem `if (!rebuilding)`.

## Wycofanie projekcji z użytku

Projekcja zniknęła z kodu? Tabele jej budów i cała księgowość zostają w bazie danych, dopóki nie
wycofasz jej z użytku, łącznie z zapisami o błędach, które inaczej wstrzymywałyby trwałe usuwanie
agregatów, o których mówią. Wywołaj to przy starcie aplikacji:

<!-- snippet: e10/RebuildSpec.scala#retire -->
```scala
projections.retire("e10-cart-names")
```

To usuwa każdą budowę projekcji i zapomina jej nazwę. W klastrze w trakcie aktualizacji kroczącej
czeka, aż na każdym węźle będzie działać nowa wersja (rozdział 17).

## Ile to trwa

Budowa czyta każde zdarzenie typów, których słucha projekcja. Pomiar na jednej maszynie (przebieg
`baseline-M4` z benchmarku, 17/10/2026: Windows 10, 12 wątków procesora, OpenJDK 17 ze stertą 2 GB,
PostgreSQL 17.4 na tej samej maszynie): dwie nowe projekcje zarządzane, obejmujące 112 211 zdarzeń,
były obie zbudowane i przełączone około 16,6 sekundy po starcie systemu. Ta oparta na zdarzeniach
przetwarzała 6 769 zdarzeń na sekundę, a szczytowe zużycie sterty wyniosło 150 MB. To jeden przebieg
na jednej maszynie: dobry do orientacji, ale nie obietnica dla twojej.

Na podstawie tego tempa `docs/SCALING.md` szacuje, że przebudowa projekcji opartej na zdarzeniach,
obejmującej 100 milionów zdarzeń, zajmie około 4,1 godziny (oszacowanie, nie pomiar). Przebudowa
kosztuje czas i obciążenie bazy danych, ale nie przestój: aktywna budowa przez cały czas obsługuje
zapytania.

Co pomaga:

- **Projekcje tylko na stanach mają mniej pracy.** Gdy każdy listener projekcji jest
  `AggregateListener`, jak w `CartNames`, budowa pomija historię: przekazuje końcowy stan każdego
  agregatu, jeden raz.
- **Migawki pomagają też budowom.** Budowa, która odtwarza zdarzenia w kolejności zatwierdzeń, trzyma
  otwartych najwyżej 10 000 stanów agregatów; stan, który porzuciła, odczytuje ponownie z jego
  migawki (rozdział 13) i zdarzeń po niej.
- **Dwie budowy naraz** (`buildsAtOnce = 2`) to wartość domyślna i zalecana: każda budowa czyta
  magazyn i zapisuje własne tabele.
- **Duże zdarzenia?**
  `ProjectionsSettings(..., builder = BuilderSettings(pageSize = ..., chunkEvents = ...))` ustala,
  ile zdarzeń budowa czyta i nanosi naraz: domyślnie po 1 000, przy czym najwyżej 64 MB na stronę i
  16 MB na porcję. `BuilderSettings` znajdziesz w `io.reactivecqrs.core.rebuild.ProjectionBuilder`.
- **Instalacje po aktualizacji:** budowa jednego typu albo jednej organizacji czyta tylko swoje
  zdarzenia, gdy każde zapisane zdarzenie ma klucz. Instalacja, która miała już zdarzenia, gdy
  wprowadzono klucze, uruchamia raz `backfillEventKeys()` (rozdział 18); do tego czasu takie budowy
  przechodzą przez cały magazyn.

> **Pod maską:** budowa ma trzy fazy. Przebieg masowy czyta zdarzenia projekcji w kolejności
> zatwierdzeń (projekcja tylko na stanach dostaje zamiast nich stany końcowe) i nanosi je porcjami do
> 1 000 zdarzeń, każdą w osobnej transakcji, po dwie porcje w locie; w tym czasie nowa budowa
> potwierdza dostarczenia z szyny, nie nanosząc ich, bo ich zdarzenia są w magazynie. Potem przechodzi
> na żywo i nanosi dostarczenia z szyny. Na koniec uzgadnianie porównuje wersję każdego agregatu z
> wersją w budowie i dostarcza to, czego brakuje: zdarzenia zapisane w trakcie przebiegu i wiersze
> zatwierdzone z opóźnieniem. Pozycja jest zapisywana po każdej porcji, więc wznowiona budowa powtarza
> najwyżej porcje, które były w locie, a wersje subskrypcji sprawiają, że to nieszkodliwe (D57–D63,
> D70). Pełną historię znajdziesz w [ARCHITECTURE.md, sekcja 8](../ARCHITECTURE.md) i w
> [dokumencie projektowym M4](../design/M4-rebuild.md).

## Co już umiesz

- Model odczytu uruchamiasz przez `projections.start[P](name, version, actorName) { build => ... }`,
  a zwróconego nadzorcy używasz do zapytań i w `RegisterSubscribers`.
- Każda budowa ma własne tabele, nazwane przez budowę (`build.documentStore(table)`).
- Podniesienie wersji buduje nową wersję z magazynu zdarzeń, gdy stara obsługuje zapytania, potem
  przełącza w jednej aktualizacji i usuwa stare tabele; `whileBuilding` mówi, co w tym czasie robi
  stara budowa.
- `RebuildProjection`, `GetBuilds`, `GetBuildStatus` i `ToBuildInProgress` służą do zaczynania,
  obserwowania i odblokowywania budów.
- `RebuildAggregates` naprawia w miejscu jeden agregat, typ albo organizację, przez twoje
  `clearForRebuild`.
- Subskrybenci, których zadaniem jest efekt uboczny, zostają niezarządzani i nigdy nie są
  przebudowywani.

W [rozdziale 11](11-subscribable-projections.md) twoje projekcje zaczną wypychać aktualizacje na żywo
na ekrany, które je obserwują.

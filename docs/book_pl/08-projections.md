# Rozdział 8. Projekcje: widoki, o które można pytać

Pod koniec tego rozdziału będziesz mieć w PostgreSQL listę koszyków zakupowych, która sama dba o swoją
aktualność, i będziesz móc zadawać jej pytania w rodzaju „co jest w koszyku 42?” albo „które koszyki
mają więcej niż trzy produkty?”. Poznasz trzy rodzaje listenerów, magazyn dokumentów, w którym leżą
dane, i sztuczkę, dzięki której ludzie widzą swoją zmianę w tej samej chwili, w której ją wprowadzają.

---

## Tablica ogłoszeń w przedpokoju

Wróćmy do rodzinnego pamiętnika z rozdziału 1. Czytanie całego pamiętnika za każdym razem, gdy ktoś
pyta „co jest na liście?”, szybko się nudzi. Ktoś więc wiesza w przedpokoju tablicę ogłoszeń i za
każdym razem, gdy w pamiętniku pojawia się nowa linijka, aktualizuje tablicę. Nikt nie pisze na
tablicy bezpośrednio: ona zawsze tylko podąża za pamiętnikiem. Gdy tablica się zniszczy, zdejmujesz ją
i odtwarzasz z pamiętnika.

Tablica to **model odczytu**: dane ułożone tak, żeby wygodnie się je czytało. Osoba, która dba o jej
aktualność, to **projekcja**. W ReactiveCQRS projekcja jest aktorem, podklasą `ProjectionActor`,
który:

- słucha zdarzeń jednego lub kilku typów agregatów,
- zapisuje to, czego się dowie, w swoim modelu odczytu, zwykle w **magazynie dokumentów** (tabeli
  PostgreSQL z dokumentami JSON),
- odpowiada na pytania o ten model jak każdy aktor: wysyłasz mu wiadomość, a on odpowiada.

```
cart owner ──events──▶ event bus ──▶ CartsList projection ──writes──▶ table projection_carts_list
                                         ▲           │
                       GetCartSummary(42)│           │Some(CartSummary(...))
                                         │           ▼
                                      your code (a web page, a report)
```

Domena testowa ma dwie małe projekcje koszyków, w `ShoppingCartsListProjection.scala`: jedną, która
słucha zdarzeń, i drugą, która słucha stanów. W tym rozdziale krok po kroku zbudujemy nieco bogatszą
kuzynkę tej drugiej.

## Cztery pola do wypełnienia

Każda projekcja wypełnia te same cztery pola:

```scala
class MyProjection(val subscriptionsState: SubscriptionsState) extends ProjectionActor {
  override protected val projectionName: String = "MyProjection"
  protected val listeners = List(...)                      // what it follows, and what it does
  override protected def receiveQuery: Receive = { ... }   // the questions it answers
}
```

- **`subscriptionsState`** to notatnik projekcji: dla każdego agregatu ostatnia wersja, którą
  zastosowała. Dzięki niemu projekcja po restarcie wie, gdzie skończyła, i rozpoznaje zdarzenie, które
  już kiedyś widziała (o tym opowiada rozdział 9). Tworzysz jeden `PostgresSubscriptionsState` na cały
  system i przekazujesz go każdej projekcji.
- **`projectionName`** to etykieta w liniach logu i raportach o stanie.
- **`listeners`** mówią, za którymi typami agregatów projekcja podąża i co robi z wieściami o nich.
- **`receiveQuery`** odpowiada na pytania.

## Trzy sposoby słuchania

Wyobraź sobie mecz piłkarski, na który nie możesz pójść. Możesz śledzić go w radiu, gdzie komentator
relacjonuje każdego gola na bieżąco; możesz zerknąć na tablicę wyników, która pokazuje tylko aktualny
wynik; albo możesz oglądać go w telewizji, z komentarzem i wynikiem w rogu ekranu. Projekcja ma te same
trzy możliwości:

| Listener | Twoja funkcja dostaje | Jak |
|---|---|---|
| `EventsListener[R]` | `(AggregateId, Seq[EventInfo[R]])` | radio: każde nowe zdarzenie |
| `AggregateListener[R]` | `(AggregateId, AggregateVersion, Boolean, Option[R])` | tablica wyników: stan w tej chwili |
| `AggregateWithEventsListener[R]` | `(AggregateId, AggregateVersion, Option[R], Seq[EventInfo[R]])` | telewizja: jedno i drugie |

`R` to klasa korzenia agregatu: dla koszyków to `ShoppingCart`. Każda z tych funkcji zwraca kolejną
funkcję, `DBSession => Unit`, czyli pracę do wykonania w transakcji bazy danych (więcej o tym za
chwilę). Parametry po kolei:

- **`AggregateId`**: który koszyk.
- **`Seq[EventInfo[R]]`**: nowe zdarzenia, od najstarszego. `EventInfo` zawiera wersję (`version`),
  samo zdarzenie (`event`), `userId` osoby, która je wywołała, jego `timestamp` i `metadata`
  (rozdział 6). Może ich być kilka naraz (polecenie, które zapisało trzy zdarzenia, albo dostarczenia,
  które przyszły razem), więc zawsze przechodź po nich w pętli.
- **`AggregateVersion`**: wersja koszyka po tych zdarzeniach.
- **`Boolean`**, flaga `created` listenera stanu: prawda, gdy projekcja słyszy o koszyku po raz
  pierwszy, zwykle przy wersji 1. To sygnał, żeby wstawić nowy dokument, a nie aktualizować
  istniejący.
- **`Option[R]`**: stan koszyka po tych zdarzeniach. `None` oznacza, że koszyka już nie ma: handler
  zdarzenia `ShoppingCartDeleted` w koszyku zwraca `null`, a projekcja dostaje `None`.

Listener stanu widzi stan po całej paczce zdarzeń, a nie każdy krok po drodze. Dokładnie tak działa
tablica wyników.

Który wybrać?

- Wybierz **stan** (`AggregateListener`), gdy twój model odczytu jest obrazem bieżącego stanu, na
  przykład listą koszyków z ich nazwami i rozmiarami. Cofnięcia, duplikowanie, przepisywanie historii i
  squash (rozdziały 6, 14 i 15) masz wtedy za darmo, bo stan, który dostajesz, jest już poprawny.
- Wybierz **zdarzenia** (`EventsListener`), gdy obchodzi cię to, co się wydarzyło, a nie to, jak się
  skończyło: dziennik aktywności, jak często ktoś dodaje mleko, kto co usunął.
- Wybierz **jedno i drugie** (`AggregateWithEventsListener`), gdy potrzebujesz i obrazu, i opowieści.

> **Pamiętaj:** listener zdarzeń dostaje każdą klasę zdarzenia swojego typu: zdarzenia cofnięcia,
> zdarzenie squasha z rozdziału 14 i wszystkie klasy, które dodasz w przyszłym roku. `match` bez
> `case _ =>`, który natrafi na nieznane zdarzenie, rzuca `MatchError`, a listener, który rzuca
> wyjątek, to awaria (rozdział 9). Oparta na zdarzeniach lista koszyków z domeny testowej, gdy widzi
> cofnięcie, tylko wypisuje przeprosiny: przy zdarzeniach ustalenie, co zostało cofnięte, to twoje
> zadanie, a przy stanach niczyje.

Dwie drobne zasady: jeden listener danego rodzaju na typ agregatu, a typ musi być prawdziwą klasą.
Wpisz na listę dwa `EventsListener[ShoppingCart]` albo jeden dla `Any` lub `Nothing`, a projekcja
odmówi startu: szyna nigdy nie dostanie jej subskrypcji i będzie czekać bez końca, a w logu
przeczytasz dlaczego („Projection ... declares 2 EventsListeners of ...”).

## Gdzie mieszkają modele odczytu: magazyny dokumentów

Wyobraź sobie szafkę na dokumenty z jedną szufladą na każdy rodzaj dokumentów i jedną teczką na każdy
koszyk, opisaną jego numerem. To jest **magazyn dokumentów**: `DocumentStore[T]` trzyma dokumenty
typu `T` pod kluczami typu `Long`, zwykle identyfikatorami agregatów (`cartId.asLong`).

- **`PostgresDocumentStore[T]`** trzyma je w tabeli `projection_<name>` z kolumnami `space_id`, `id`,
  `version` i `document` (dokument jako `JSONB`, zapisany przez mpjsons). Tabelę i indeksy, o które
  prosisz, zakłada w chwili tworzenia magazynu.
- **`MemoryDocumentStore[T]`** trzyma je w mapie w pamięci. Jest przeznaczony do testów (rozdział 19);
  ignoruje przestrzenie, więc jego `clearSpace` nic nie robi.

Oto magazyn listy koszyków, którą budujemy w tym rozdziale. Jej dokumenty to
`CartSummary(name: String, itemCount: Int, version: Int)`:

<!-- snippet: cart/ReadModelsSpec.scala#store -->
```scala
import io.reactivecqrs.core.documentstore.{MultipleTextIndex, NoopDocumentStoreCache, PostgresDocumentStore}

val cartsStore = new PostgresDocumentStore[CartSummary](
  "carts_list",                           // the table: projection_carts_list
  mpjsons,                                // the MPJsons you made in chapter 4
  new NoopDocumentStoreCache,             // a cache that keeps nothing
  Seq(MultipleTextIndex(1, Seq("name")))) // an index on the JSON field "name"
```

- **Nazwa** może zawierać tylko litery, cyfry i podkreślenia, a najdłuższa nazwa, jaką tworzy
  magazyn (tabeli albo indeksu, takiego jak `projection_carts_list_idx_1`), musi się zmieścić w 63
  znakach, na które pozwala PostgreSQL. Inaczej magazyn odmówi startu, rzucając
  `IllegalArgumentException`.
- **Pamięć podręczna**: `NoopDocumentStoreCache` niczego nie przechowuje, więc każdy odczyt idzie do
  bazy danych. Własna pamięć podręczna implementuje niewielki trait `DocumentStoreCache`.
- **Indeksy** przyspieszają wyszukiwanie po polach JSON: `MultipleTextIndex`, `UniqueTextIndex`,
  `MultipleIntIndex`, `UniqueIntIndex`, `MultipleLongIndex`, `UniqueLongIndex`,
  `MultipleTextArrayIndex`, a także `MultipleCombinedIndex` lub `UniqueCombinedIndex` na kilku
  ścieżkach naraz. Numer (tutaj `1`) jest nazwą indeksu. Przy starcie magazyn tworzy indeksy, których
  jeszcze nie ma, usuwa te, których nie ma już na liście, i buduje od nowa taki, którego definicja się
  zmieniła (inna ścieżka albo inny rodzaj pod tym samym numerem), zostawiając w logu ostrzeżenie.

> **Ostrzeżenie:** ponowne budowanie indeksu blokuje zapisy projekcji na cały czas swojego trwania, a
> projekcja startuje dopiero, gdy się zakończy: w tabeli z milionami dokumentów może to trwać
> minutami. Przy dużej tabeli zmieniaj indeks razem z nową wersją projekcji zarządzanej (rozdział 10):
> nowa budowa dostaje nowe tabele, które powstają, podczas gdy stare wciąż obsługują zapytania.

Operacje, których będziesz używać najczęściej:

| Wywołanie | Co robi |
|---|---|
| `insertDocument(spaceId, key, document)` | dodaje nowy dokument; kończy się błędem, jeśli klucz jest zajęty |
| `overwriteDocument(key, document)` | zastępuje istniejący dokument; kończy się błędem, jeśli go nie ma |
| `updateDocument(spaceId, key, modify)` | czyta, zmienia i zapisuje z powrotem; `modify` dostaje `Option[Document[T]]` (`None`: dokumentu jeszcze nie ma) i zwraca nowy `Document[T]` |
| `updateExistingDocument(key, modify)` | to samo dla dokumentu, który musi istnieć |
| `removeDocument(key)` | usuwa dokument |
| `getDocument(key)` | `Option[Document[T]]` |
| `getDocuments(keys)` | `Map[Long, Document[T]]` z tymi, które udało się znaleźć |
| `findAll()` | wszystkie dokumenty jako `Map[Long, Document[T]]` |
| `countAll()` | ile ich jest |
| `findDocumentByPath(path, value)` | dokumenty, w których tekst pod ścieżką `path` jest równy `value` |
| `findDocumentByPaths(conditions*)` | dokumenty, które spełniają wszystkie warunki, na przykład `ExpectedSingleLongValue(Seq("shop"), 7)` |
| `findDocument(query)` | wyszukiwanie z warunkami, sortowaniem i stronicowaniem (dalej w tym rozdziale) |
| `clearSpace(spaceId)`, `clearAllData()` | usuwa dokumenty jednej organizacji albo wszystkie |

`Document[T]` to malutkie opakowanie: `Document(summary)` opakowuje, `doc.document` wyjmuje z
opakowania. `spaceId` to organizacja, do której należy dokument (rozdział 16). Wszystkie nasze koszyki
mieszkają w przestrzeni 0, bo tak mówi `ShoppingCartCreated` (`spaceId = SpaceId(0)`).

> **Pamiętaj:** `updateDocument` może wywołać twoją funkcję `modify` więcej niż raz: jeśli w
> międzyczasie dokument zmienił ktoś inny, czyta nową wersję i próbuje jeszcze raz, maksymalnie 10
> razy. Niech `modify` będzie czysta: tylko oblicza, niczego nie robi.

> **Wskazówka:** kolumna `version` w samej tabeli liczy zapisy dokumentu; to nie jest wersja koszyka.
> Jeśli potrzebujesz wersji koszyka w modelu odczytu (a dalej w tym rozdziale będzie potrzebna),
> trzymaj ją w samym dokumencie, tak jak robi to `CartSummary`.

## Transakcja, w której działa listener

Każdy zapis przyjmuje niejawny parametr `DBSession`. Wewnątrz listenera ta sesja to transakcja, którą
projekcja otworzyła za ciebie, i ta sama transakcja zapisuje postęp projekcji: „koszyk 42 jest
zastosowany do wersji 3”. Model odczytu i zakładka są więc zatwierdzane razem albo wcale. Jeśli
listener w połowie rzuci wyjątek, nie ruszy się ani dokument, ani zakładka, a projekcja spróbuje
jeszcze raz (rozdział 9).

Dlatego listener jest funkcją, która przyjmuje sesję. Oznacz ją jako `implicit`, a wywołania magazynu
w środku same ją podchwycą:

```scala
AggregateListener[ShoppingCart] { (cartId, version, created, cart) => implicit session =>
  // every store call here runs in `session`
}
```

> **Ostrzeżenie:** nigdy nie zatwierdzaj ani nie wycofuj sesji, którą dostajesz, i nigdy nie otwieraj
> dla modelu odczytu własnej transakcji. Gdy aktualizacje napływają szybko, projekcja stosuje w jednej
> transakcji aktualizacje nawet 100 koszyków; jeśli któraś zawiedzie, wycofuje je wszystkie i stosuje
> każdy koszyk osobno, tak żeby ponawiany był tylko ten wadliwy. Twoje własne zatwierdzenie by to
> zepsuło.

Poza listenerem (w skrypcie, teście albo zadaniu naprawczym) sesję otwierasz samodzielnie, na przykład
przez `DB.localTx { implicit session => ... }` ze ScalikeJDBC. Odczyty takie jak `getDocument` i
`findAll` nie potrzebują sesji: bez niej otwierają własną sesję tylko do odczytu. Wewnątrz listenera
każdy odczyt też działa w sesji listenera, więc widzi to, co listener zapisał chwilę wcześniej, i nie
zabiera z puli drugiego połączenia.

> **Pod maską:** transakcja pochodzi z `SubscriptionsState.localTx`, a wersja projekcji przesuwa się
> operacją compare-and-set, więc z dwóch piszących przesunąć ją może tylko jeden. Grupowanie i jego
> wariant awaryjny opisuje [ARCHITECTURE.md, sekcja 6.3](../ARCHITECTURE.md), magazyn dokumentów
> sekcja 6.8, a notatnik (`SubscriptionsState`) sekcja 7.

## Budujemy listę koszyków krok po kroku

Nasz model odczytu ma po jednym dokumencie na koszyk, z jego nazwą, liczbą produktów i wersją.
Korzystamy z tego, co powstało w rozdziale 4: systemu aktorów `system`, `mpjsons`, `typesNames`, szyny
zdarzeń `eventBus`, użytkownika `alice` i szyny poleceń koszyków `carts`.

1. **Spisz dokument i pytania.**

   <!-- snippet: cart/CartsList.scala#documents -->
   ```scala
   import io.reactivecqrs.api.id.AggregateId

   case class CartSummary(name: String, itemCount: Int, version: Int)

   case class GetCartSummary(cartId: AggregateId)   // answered with Option[CartSummary]
   case object GetAllCarts                          // answered with Map[AggregateId, CartSummary]
   case class FindCartsByName(name: String)         // answered with Vector[CartSummary]
   ```

2. **Napisz projekcję.**

   <!-- snippet: cart/CartsList.scala#projection -->
   ```scala
   import io.reactivecqrs.core.documentstore.DocumentStore
   import io.reactivecqrs.core.projection.{ProjectionActor, ProjectionActorOptions, SubscriptionsState}
   import io.reactivecqrs.testdomain.shoppingcart.ShoppingCart

   class CartsListProjection(val subscriptionsState: SubscriptionsState,
                             store: DocumentStore[CartSummary],
                             options: ProjectionActorOptions = ProjectionActorOptions.DEFAULT) extends ProjectionActor(options) {

     override protected val projectionName: String = "CartsList"

     protected val listeners = List(
       AggregateListener[ShoppingCart] { (cartId, version, created, cart) => implicit session =>
         cart match {
           case Some(c) =>
             val summary = CartSummary(c.name, c.items.size, version.asInt)
             if (created) store.insertDocument(0, cartId.asLong, summary)
             else store.overwriteDocument(cartId.asLong, summary)
           case None =>
             store.removeDocument(cartId.asLong)
         }
       })

     override protected def receiveQuery: Receive = {
       case GetCartSummary(cartId) =>
         sender() ! store.getDocument(cartId.asLong).map(_.document)
       case GetAllCarts =>
         sender() ! store.findAll().map { case (id, doc) => AggregateId(id) -> doc.document }
       case FindCartsByName(name) =>
         sender() ! store.findDocumentByPath(Seq("name"), name).values.map(_.document).toVector
       case FindBigCarts(moreThan) =>
         val query = DocumentStoreQuery(
           where = Seq(ExpectedGreaterThanIntValue(Seq("itemCount"), moreThan)),
           sortBy = Seq(SortDescInt("itemCount")),
           offset = 0,
           limit = 20)
         sender() ! store.findDocument(query).map { case (id, doc) => AggregateId(id) -> doc.document }
       case GetCartSummaryAtLeast(cartId, version) =>
         delayIfNotAvailable(sender(), () =>
           store.getDocument(cartId.asLong).map(_.document).filter(_.version >= version.asInt),
           2000)
     }
   }
   ```

   Listener czyta się jak tablica wyników, bo nią właśnie jest: nowy koszyk dostaje dokument,
   zmieniony dostaje nowy w miejsce starego, a usunięty swój traci. To projekcja
   `ShoppingCartsListProjectionAggregatesBased` z domeny testowej, uzupełniona o liczbę produktów i
   wersję. Projekcja odpowiada jeszcze na dwa pytania, `FindBigCarts` i `GetCartSummaryAtLeast`, które
   pojawią się dalej w tym rozdziale, a jej trzeci parametr, czyli opcje, poznasz na końcu rozdziału.

3. **Utwórz notatnik**, raz na system; magazyn to ten z poprzedniej sekcji:

   <!-- snippet: cart/ReadModelsSpec.scala#notebook -->
   ```scala
   import io.reactivecqrs.core.projection.PostgresSubscriptionsState

   val subscriptionsState = new PostgresSubscriptionsState(typesNames).initSchema()
   ```

   `initSchema()` tworzy tabelę `subscriptions` (notatnik) i `subscription_failures` (rozdział 9),
   jeśli jeszcze ich nie ma.

4. **Uruchom projekcję na dispatcherze blokującym.**

   <!-- snippet: cart/ReadModelsSpec.scala#start -->
   ```scala
   import org.apache.pekko.actor.Props
   import io.reactivecqrs.core.util.BlockingDispatcher

   val cartsList = system.actorOf(
     Props(new CartsListProjection(subscriptionsState, cartsStore)).withDispatcher(BlockingDispatcher.id),
     "CartsList")
   ```

   Projekcja rozmawia z PostgreSQL na własnym wątku. `BlockingDispatcher.id`, czyli
   `reactivecqrs.blocking-dispatcher` (32 wątki, chyba że skonfigurujesz inaczej), to pula, którą
   biblioteka trzyma właśnie na taką pracę, dzięki czemu reszta systemu nigdy nie czeka w kolejce za
   wolnym zapytaniem.

5. **Dopisz ją do listy subskrybentów szyny zdarzeń.** W rozdziale 4 aplikacja powiedziała szynie przez
   `RegisterSubscribers`, kim są jej subskrybenci. Projekcja trafia na tę listę razem ze wszystkimi
   innymi projekcjami, sagami i listenerami systemu:

   <!-- snippet: cart/ReadModelsSpec.scala#register -->
   ```scala
   import io.reactivecqrs.core.eventbus.EventsBusActor

   eventBus ! EventsBusActor.RegisterSubscribers(List(cartsList))
   ```

   Szyna pyta każdego subskrybenta, czego chce (projekcja odpowiada sama, po jednej subskrypcji na
   listener), i niczego nie publikuje, dopóki nie odpowie każdy subskrybent z listy. Czekając, co kilka
   sekund zapisuje w logu `Event bus waits for the subscriptions of: ...`, co bardzo się przydaje, gdy
   zapomnisz uruchomić któregoś z nich.

> **Ostrzeżenie:** projekcja uruchomiona na domyślnym dispatcherze też działa, ale przy starcie
> zapisuje w logu ostrzeżenie („runs its listener transactions on the default dispatcher; create it
> with Props(...).withDispatcher(BlockingDispatcher.id)”), a każde jej zapytanie i każda transakcja
> zajmują wątki potrzebne reszcie Pekko. Zawsze dodawaj `.withDispatcher(BlockingDispatcher.id)`.

> **Ostrzeżenie:** projekcja uruchomiona w ten sposób trzyma swój notatnik pod **nazwą klasy** (na
> przykład `com.example.CartsListProjection`), a nie pod `projectionName`. Gdy zmienisz nazwę klasy
> albo ją przeniesiesz, projekcja zapomni, co już zastosowała: przy następnym zdarzeniu każdego
> koszyka zacznie go od nowa, od wersji 1, wprost na dokumentach, które już istnieją. Projekcje
> zarządzane z rozdziału 10 trzymają notatnik pod nazwą wybraną przez ciebie.

## Wypróbujmy

Utwórz koszyk, dodaj mleko i zapytaj. Bloki kodu z tego rozdziału działają w `ReadModelsSpec`
(`examples/src/test/scala/io/reactivecqrs/examples/cart/`), który sprawdza ich odpowiedzi:

<!-- snippet: cart/ReadModelsSpec.scala#tryIt -->
```scala
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import io.reactivecqrs.api._
import io.reactivecqrs.testdomain.shoppingcart._
import scala.concurrent.Await
import scala.concurrent.duration._

implicit val timeout: Timeout = Timeout(5.seconds)

def send(command: AnyRef): CustomCommandResponse[_] =
  Await.result((carts ? command).mapTo[CustomCommandResponse[_]], 5.seconds)

val cartId = send(CreateShoppingCart(None, alice, "Groceries")) match {
  case SuccessResponse(id, _) => id
  case refused => throw new IllegalStateException("No cart: " + refused)
}
send(AddItem(alice, cartId, AggregateVersion(1), "milk"))       // SuccessResponse(Id(42),Version(2))

Thread.sleep(500) // give the projection a moment; a better way comes later in this chapter

val summary = Await.result((cartsList ? GetCartSummary(cartId)).mapTo[Option[CartSummary]], 5.seconds)
// Some(CartSummary(Groceries,1,2))
```

Zajrzyj do PostgreSQL, a znajdziesz wiersz koszyka w `projection_carts_list`, z
`{"name": "Groceries", "version": 2, "itemCount": 1}` w kolumnie `document` (PostgreSQL układa klucze
wartości `JSONB` po swojemu).

> **Pamiętaj:** projekcja uruchomiona w ten sposób słyszy tylko o zdarzeniach opublikowanych po tym, jak
> dołączyła. Koszyk, który istniał wcześniej, pojawi się przy swojej następnej zmianie: projekcja
> zauważy, że przegapiła wersje od 1 do 3, poprosi o nie właściciela koszyka, a potem zastosuje po
> kolei wersje od 1 do 4 (dokładnie to pokazuje `DeliveryEndToEndSpec` z biblioteki). Koszyk, który już
> nigdy się nie zmieni, nigdy się nie pojawi. Żeby wypełnić nową projekcję całą historią, uruchom ją
> jako projekcję zarządzaną (rozdział 10).

## Bardziej precyzyjne pytania

`findDocument` przyjmuje `DocumentStoreQuery`: warunki na ścieżkach JSON, kolejność sortowania,
przesunięcie (offset) i limit. Ścieżka to lista nazw pól prowadzących do wartości: `Seq("name")` albo
`Seq("address", "city")` dla pola zagnieżdżonego. Oto największe koszyki, od największego. Pytanie:

<!-- snippet: cart/CartsList.scala#findBigCarts -->
```scala
case class FindBigCarts(moreThan: Int) // answered with Seq[(AggregateId, CartSummary)]
```

a oto jego obsługa w projekcji (`DocumentStoreQuery`, `ExpectedGreaterThanIntValue` i `SortDescInt`
pochodzą z `io.reactivecqrs.core.documentstore`):

<!-- snippet: cart/CartsList.scala#bigCartsCase -->
```scala
case FindBigCarts(moreThan) =>
  val query = DocumentStoreQuery(
    where = Seq(ExpectedGreaterThanIntValue(Seq("itemCount"), moreThan)),
    sortBy = Seq(SortDescInt("itemCount")),
    offset = 0,
    limit = 20)
  sender() ! store.findDocument(query).map { case (id, doc) => AggregateId(id) -> doc.document }
```

Wszystkie warunki muszą być spełnione naraz. Oprócz `ExpectedGreaterThanIntValue` są jeszcze
`ExpectedSingleTextValue`, `ExpectedSingleTextValueLike` (wzorzec SQL `ILIKE`, na przykład `"%groc%"`),
`ExpectedMultipleTextValues` (jedna z kilku wartości), `ExpectedSingleIntValue`, `ExpectedSingleLongValue`,
`ExpectedSingleBooleanValue`, `ExpectedLessThanIntValue`, `ExpectedNoValue` i kilka innych dla tablic.
Do sortowania służą `SortAscText`, `SortDescText`, `SortAscInt`, `SortDescInt`, `SortAscLong` i
`SortDescLong`. `limit` równy `-1` oznacza brak limitu, a `DocumentStoreQuery.basic(where)` to skrót od
„te warunki, bez sortowania, bez limitu”.

> **Wskazówka:** zapytanie z pustym `where` znajduje wszystkie dokumenty, posortowane i podzielone na
> strony tak, jak sobie życzysz; jeśli chcesz dostać wszystkie naraz, prościej użyć `findAll()`.

A co, gdy zapytanie się nie uda? Na zapytanie, które rzuci wyjątek, projekcja odpowiada
`Status.Failure`, więc twój `ask` kończy się tym wyjątkiem, a projekcja spokojnie pracuje dalej.

> **Pamiętaj:** zapytania i aktualizacje czekają na swoją kolej w tej samej skrzynce odbiorczej. Zanim
> projekcja odpowie na zapytanie, stosuje wszystkie dostarczenia, które już otrzymała, więc odpowiedzi
> są spójne; ale dopóki trwa zapytanie, żadna aktualizacja nie jest stosowana. Pilnuj, żeby zapytania
> były szybkie: szukaj po kluczu, korzystaj z indeksów i dziel wyniki na strony za pomocą `limit`.

## Ten sam pomysł ze zdarzeniami: dziennik aktywności

Niektóre modele odczytu to opowieści, a nie obrazki. Oto dziennik wszystkiego, co działo się z każdym
koszykiem, zbudowany ze zdarzeń:

<!-- snippet: cart/CartActivity.scala#activity -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.core.documentstore.Document

case class CartActivity(lines: Vector[String])
case class GetCartActivity(cartId: AggregateId) // answered with Option[CartActivity]

class CartActivityProjection(val subscriptionsState: SubscriptionsState,
                             store: DocumentStore[CartActivity]) extends ProjectionActor {

  override protected val projectionName: String = "CartActivity"

  protected val listeners = List(
    EventsListener[ShoppingCart] { (cartId, events) => implicit session =>
      val lines = events.map(e => "v" + e.version.asInt + ", user " + e.userId.asLong + ": " + describe(e.event))
      store.updateDocument(0, cartId.asLong, existing =>
        Document(CartActivity(existing.map(_.document.lines).getOrElse(Vector.empty) ++ lines)))
    })

  private def describe(event: Event[ShoppingCart]): String = event match {
    case ShoppingCartCreated(name) => "created \"" + name + "\""
    case ShoppingCartDuplicated(_, baseId, baseVersion) => "copied from cart " + baseId.asLong + " at version " + baseVersion.asInt
    case ItemAdded(name) => "added " + name
    case ItemRemoved(itemId) => "removed item " + itemId
    case ShoppingCartChangesUndone(count) => "undid the last " + count + " change(s)"
    case ShoppingCartDeleted() => "deleted the cart"
    case other => other.getClass.getSimpleName // anything else, the squash event included
  }

  override protected def receiveQuery: Receive = {
    case GetCartActivity(cartId) => sender() ! store.getDocument(cartId.asLong).map(_.document)
  }
}
```

Zwróć uwagę na dwie rzeczy. `updateDocument` wstawia dokument, gdy jeszcze go nie ma (`existing` to
`None`), więc ten listener nie potrzebuje flagi `created`. A cofnięcie to po prostu kolejna linijka w
dzienniku: do opowieści zdarzenia pasują najlepiej. Tę projekcję uruchamiasz i rejestrujesz dokładnie
tak samo jak listę koszyków.

## Własna zmiana widoczna od razu

Alicja dodaje chleb, a jej strona od razu przeładowuje koszyk. Polecenie się powiodło, ale jego
zdarzenie może jeszcze być w drodze do projekcji, więc strona pokazuje koszyk bez chleba. Irytujące!

W szatni zdarza się to przez cały dzień: oddajesz płaszcz, a sekundę później prosisz o niego z
powrotem. Dobry szatniarz nie powie „nie mamy takiego płaszcza”, tylko „chwileczkę”, i spojrzy
jeszcze raz, gdy płaszcz trafi na wieszak. **Opóźnione zapytanie** to właśnie taki szatniarz: gdy odpowiedzi
jeszcze nie ma, projekcja czeka na swoje kolejne aktualizacje, zanim powie „nie”.

Wiesz, na co czekać: odpowiedź na polecenie, `SuccessResponse(cartId, version)`, podaje wersję, którą
utworzyła twoja zmiana. Pytanie brzmi więc: „koszyk 42, gdy tylko będzie w wersji 3 albo późniejszej”:

<!-- snippet: cart/CartsList.scala#atLeast -->
```scala
case class GetCartSummaryAtLeast(cartId: AggregateId, version: AggregateVersion) // answered with Option[CartSummary]
```

a oto jego obsługa w projekcji:

<!-- snippet: cart/CartsList.scala#atLeastCase -->
```scala
case GetCartSummaryAtLeast(cartId, version) =>
  delayIfNotAvailable(sender(), () =>
    store.getDocument(cartId.asLong).map(_.document).filter(_.version >= version.asInt),
    2000)
```

`delayIfNotAvailable(respondTo, search, forMaximumMillis)` od razu uruchamia `search`. `Some` trafia
natychmiast do `respondTo`. `None` odkłada zapytanie na bok: projekcja powtarza wyszukiwanie, gdy
zastosuje aktualizacje, i odpowiada, gdy tylko coś znajdzie. Gdy minie 2 000 milisekund bez
powodzenia, szuka ostatni raz i odpowiada tym, co znajdzie, czyli `None`, jeśli wciąż nic nie ma. Po
stronie wywołującej:

<!-- snippet: cart/ReadModelsSpec.scala#readYourWrite -->
```scala
val withBread = send(AddItem(alice, cartId, AggregateVersion(2), "bread")) match {
  case SuccessResponse(_, version) =>
    Await.result((cartsList ? GetCartSummaryAtLeast(cartId, version)).mapTo[Option[CartSummary]], 5.seconds)
    // Some(CartSummary(Groceries,2,3)), as soon as the projection has it
  case other => println("Not added: " + other)
}
```

> **Wskazówka:** ustaw limit czasu `ask` (tutaj 5 sekund) dłuższy niż czas czekania (2 sekundy), żeby
> dostać od projekcji `None`, a nie `AskTimeoutException`.

> **Wskazówka:** to działa, bo dokument przechowuje wersję koszyka. Wkładaj wersję agregatu do każdego
> dokumentu, z którego chcesz odczytywać własne zapisy. Gdy potrzebujesz samego koszyka, a nie modelu
> odczytu, zapytaj jego szynę poleceń przez `GetAggregateMinVersion` (rozdział 7).

Zwykła wersja szuka na wątku projekcji, co przy wyszukiwaniu po kluczu w zupełności wystarcza.
Cięższe wyszukiwanie możesz przekazać do `Future`:

| Metoda | `search` zwraca | Odpowiedź |
|---|---|---|
| `delayIfNotAvailable(respondTo, search, maxMillis)` | `Option[T]` | `Some(t)` albo, po upływie terminu, wynik ostatniego wyszukiwania |
| `delayIfNotAvailableAsync(respondTo, search, maxMillis)` | `Future[Option[T]]` | `Some(t)` albo, po upływie terminu, wynik ostatniego wyszukiwania |
| `delayIfNotAvailableAsyncCustom(respondTo, search, found, maxMillis)` | `Future[T]` oraz `found: T => Boolean` | `T`, gdy tylko `found` powie „tak”, albo, po upływie terminu, `T` z ostatniego wyszukiwania |

Każda z nich ma też wariant, który przyjmuje termin jako `Instant` zamiast milisekund. Na
wyszukiwanie, które rzuci wyjątek, albo `Future`, który się nie powiedzie, odpowiedzią jest
`Status.Failure`.

> **Pod maską:** odłożone zapytanie czeka wewnątrz projekcji (jako prywatny `SyncDelayedQuery`,
> `AsyncDelayedQuery` albo `AsyncDelayedQueryCustom`; tych klas nigdy nie tworzysz samodzielnie).
> Wyszukiwanie powtarza się raz na serię zastosowanych aktualizacji, nie przy każdej z osobna; gdy mija
> termin, zapytanie dostaje jeszcze jedno, ostatnie wyszukiwanie. Asynchroniczne
> wyszukiwanie, które wciąż trwa w chwili zastosowania aktualizacji, po swojej odpowiedzi rusza jeszcze
> raz, więc nigdy tej aktualizacji nie przegapi. Zobacz [ARCHITECTURE.md, sekcja 6.5](../ARCHITECTURE.md).

## Opcje, które warto znać

Projekcja dostaje opcje w konstruktorze, jako `ProjectionActorOptions`: to trzeci parametr listy
koszyków, który przekazuje ona dalej przez `extends ProjectionActor(options)`. Zacznij od
`ProjectionActorOptions.DEFAULT` i zmień to, czego potrzebujesz:

<!-- snippet: cart/ReadModelsSpec.scala#options -->
```scala
import io.reactivecqrs.core.projection.ProjectionActorOptions

val options = ProjectionActorOptions.DEFAULT.withGroupUpdatesDelayMillis(200).withMinimumDelayVersion(2)
```

| Opcja (domyślnie) | Co robi | Jak ją ustawić |
|---|---|---|
| `groupUpdatesDelayMillis` (0) | czeka tyle milisekund, zanim zastosuje dostarczenia koszyka, żeby kilka aktualizacji jednego koszyka zastosować razem | `withGroupUpdatesDelayMillis(200)` |
| `minimumDelayVersion` (1) | dostarczenia zaczynające się poniżej tej wersji nigdy nie czekają; przy 2 nowy koszyk pojawia się od razu | `withMinimumDelayVersion(2)` |
| `eventsToProcessImmediately` (brak) | klasy zdarzeń, które nigdy nie czekają (dla listenerów, które dostają zdarzenia) | `withEventsToProcessImmediately(classOf[ShoppingCartDeleted])` |
| `maxBatchesPerTransaction` (100) | gdy dostarczenia się piętrzą: aktualizacje ilu koszyków może zastosować jedna transakcja; 1 stosuje każdy koszyk osobno | `.copy(maxBatchesPerTransaction = 1)` |
| `parallelUpdateProcessing` (0) | powyżej 1: tyle wątków stosuje aktualizacje; każdy koszyk wciąż zachowuje swoją kolejność | `withParallelUpdateProcessing(4)` |
| `eventsLogger` (brak) | logger, który dostaje linię na poziomie debug przy każdym dostarczeniu i każdej zastosowanej paczce | `withEventsLogger(LoggerFactory.getLogger("carts-list"))` |
| `retries`, `retryFailuresOnStart`, `gapRequestRepeat`, `gapGiveUpAfter`, `failureRetryInterval` | co się dzieje, gdy coś pójdzie nie tak | rozdział 9 |

Pierwsze trzy mają znaczenie tylko razem: gdy `groupUpdatesDelayMillis` wynosi 0 (domyślnie), i tak
nic nie czeka. `withEventsLogger` przyjmuje `Logger` z SLF4J.

> **Ostrzeżenie:** z `parallelUpdateProcessing` twoje listenery działają na kilku wątkach naraz, na
> tych samych obiektach: muszą być bezpieczne wątkowo, a pula JDBC potrzebuje co najmniej tylu
> połączeń. Ta opcja wyłącza też grupowanie z `maxBatchesPerTransaction`. Większość projekcji świetnie
> radzi sobie bez niej.

> **Wskazówka:** dla projekcji bez żadnych zapytań, na przykład takiej, która tylko zasila inny system,
> rozszerzaj `AggregateListenerActor` zamiast `ProjectionActor`: to to samo, tylko z `receiveQuery`,
> które ignoruje każdą wiadomość. Rozdział 9 korzysta z takiej projekcji.

## Co już umiesz

- **Projekcja** to `ProjectionActor` z listenerami, modelem odczytu i zapytaniami; jej notatnik,
  `SubscriptionsState`, zapisuje dla każdego agregatu ostatnią zastosowaną wersję.
- Są trzy **listenery**: `AggregateListener` do stanu (obrazki), `EventsListener` do zdarzeń
  (opowieści) i `AggregateWithEventsListener` do jednego i drugiego.
- **Magazyn dokumentów** trzyma dokumenty JSON pod kluczami: `PostgresDocumentStore` w tabeli
  `projection_<name>` z kolumną `space_id`, `MemoryDocumentStore` w testach.
- Listener działa w **transakcji**, która zapisuje też postęp projekcji; przekazuj ją magazynowi i
  nigdy nie zatwierdzaj jej samodzielnie.
- Projekcję uruchamiasz przez `Props(...).withDispatcher(BlockingDispatcher.id)` i dopisujesz do listy
  `RegisterSubscribers` szyny.
- **Opóźnione zapytania** (`delayIfNotAvailable` i spółka) pozwalają ludziom odczytać własne zapisy.

Dalej: [rozdział 9](09-projection-failures.md) celowo coś psuje i pokazuje, jak projekcje dochodzą do
siebie.

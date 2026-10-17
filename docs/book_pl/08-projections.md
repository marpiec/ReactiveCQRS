# Rozdział 8. Projekcje: widoki, które ludzie mogą kwerendować

Pod koniec tego rozdziału będziesz mieć listę koszyków zakupowych w PostgreSQL, która sama się przechowuje
na bieżąco i będziesz mógł zadać mu pytania takie jak „co jest w koszyku 42?” lub „które koszyki
Czy można przechowywać więcej niż trzy elementy? Spotkasz trzy rodzaje słuchaczy: magazyn dokumentów, który
przechowuje dane i jest to trik pozwalający ludziom zobaczyć resztę od razu po jej dokonaniu.

---

## Tablica ogłoszeń na korytarzu

Pamiętasz dziennik rodzinny z rozdziału 1? Czytasz cały dziennik za każdym razem, gdy ktoś pyta: „Co się dzieje?”
„Na liście?” szybko się nudzi. Ktoś więc wiesza tablicę ogłoszeń na korytarzu i za każdym razem, gdy pojawia się nowa linia,
Wchodzi do dziennika, aktualizują tablicę. Nikt nie pisze bezpośrednio na tablicy: ona zawsze…
Zgodnie z dziennikiem. Jeśli tablica się zniszczy, zdejmujesz ją i układasz od nowa z dziennika.

Płytka to **model do odczytu**: dane ukształtowane do odczytu. Osoba, która ją aktualizuje, to
**projekcja**. W ReactiveCQRS projekcja to aktor, podklasa `ProjectionActor`, która

- nasłuchuje zdarzeń jednego lub więcej typów agregatów,
- zapisuje to, czego się uczy, do swojego modelu odczytu, zwykle **magazynu dokumentów** (tabela PostgreSQL
  dokumenty JSON),
- odpowiada na zadawane mu pytania, jak każdy aktor: wysyłasz mu wiadomość, on odpowiada.

```
właściciel koszyka ──wydarzenia──▶ magistrala zdarzeń ──▶ projekcja CartsList ──zapisy──▶ tabela projekcja_listy_wózków
                                         ▲ │
                       GetCartSummary(42)│ │Some(CartSummary(...))
                                         │ ▼
                                      Twój kod (strona internetowa, raport)
```

Domena testowa ma dwie małe projekcje koszyków w `ShoppingCartsListProjection.scala`: jedną
który słucha zdarzeń i taki, który słucha stanów. W tym rozdziale budujemy nieco bogatszą
kuzyn drugiego, krok po kroku.

## Cztery puste pola projekcji

Każda projekcja wypełnia te same cztery luki:

```scala
klasa MyProjection(val subscriptionsState: SubscriptionsState) rozszerza ProjectionActor {
  nadpisz chronioną wartość projectionName: String = "MyProjection"
  protected val listeners = List(...) // co następuje i co robi
  nadpisz chronione def receiveQuery: Receive = { ... } // pytania, na które odpowiada
}
```

- **`subscriptionsState`** to notatnik projekcji: dla każdego agregatu ostatnia wersja, którą
  Zastosowano. Dzięki temu projekcja wie, gdzie zatrzymała się po restarcie i rozpoznaje
  wydarzenie, które już widziałeś (rozdział 9 opowiada tę historię). Tworzysz jeden `PostgresSubscriptionsState`
  na system i podaj go każdej projekcji.
- **`projectionName`** to etykieta dla wierszy dziennika i raportów stanu.
- **`słuchacze`** określają, jakie typy agregatów uwzględnia projekcja i co robi z ich wiadomościami.
- **`receiveQuery`** odpowiada na pytania.

## Trzy sposoby słuchania

Wyobraź sobie mecz piłkarski, na który nie możesz pójść. Możesz go śledzić w radiu, gdzie komentator
informuje Cię o każdym golu w momencie jego zdobycia; możesz spojrzeć na tablicę wyników, która pokazuje tylko wynik
już teraz; albo możesz obejrzeć w telewizji, z komentarzem i wynikiem w rogu. Projekcja
ma te same trzy możliwości:

| Słuchacz | Twoja funkcja otrzymuje | Lubię to |
|---|---|---|
| `EventsListener[R]` | `(AggregateId, Seq[EventInfo[R]])` | radio: każde nowe zdarzenie |
| `AggregateListener[R]` | `(AggregateId, AggregateVersion, Boolean, Option[R])` | tablica wyników: stan obecny |
| `AggregateWithEventsListener[R]` | `(AggregateId, AggregateVersion, Option[R], Seq[EventInfo[R]])` | telewizor: oba |

`R` to klasa nadrzędna agregatu: `ShoppingCart` dla koszyków. Każda funkcja zwraca inną funkcję,
`DBSession => Unit`, czyli praca, którą należy wykonać w ramach transakcji bazy danych (więcej na ten temat wkrótce).
Parametry w kolejności:

- **`AggregateId`**: który koszyk.
- **`Seq[EventInfo[R]]`**: nowe zdarzenia, najstarsze na początku. `EventInfo` zawiera `wersję`,
  samo `wydarzenie`, `userId` osoby, która je spowodowała, jego `znacznik czasu` i jego `metadane` (rozdział 6).
  Może być ich kilka naraz (polecenie, które zapisało trzy zdarzenia lub dostawy, które dotarły)
  razem), więc zawsze należy je powtarzać.
- **`AggregateVersion`**: wersja koszyka po tych zdarzeniach.
- **`Boolean`**, flaga `created` nasłuchiwacza stanu: prawda, gdy jest to pierwszy raz
  projekcja słyszy o wózku, zwykle w wersji 1. To jest sygnał, aby wstawić nowy
  dokument zamiast go aktualizować.
- **`Opcja[R]`**: stan wózka po tych zdarzeniach. `Brak` oznacza, że ​​wózek zniknął: stan wózka
  Obsługujący zdarzenie dla `ShoppingCartDeleted` zwraca `null`, a projekcja otrzymuje `None`.

Słuchacz stanu widzi stan po całym pakiecie, a nie po każdym kroku. To jest dokładnie to,
co robi tablica wyników.

Którą opcję wybrać?

- Wybierz **stan** (`AggregateListener`), gdy model odczytu jest obrazem bieżącego stanu
  stan, na przykład lista wózków z ich nazwami i rozmiarami. Cofanie, duplikowanie, przepisywanie historii
  i squash (rozdziały 6, 14 i 15) są darmowe, bo stan, który otrzymujesz, jest już odpowiedni.
- Wybierz **wydarzenia** (`EventsListener`), jeśli zależy Ci na tym, co się wydarzyło, a nie gdzie to nastąpiło
  Zakończono: dziennik aktywności, jak często dolewane jest mleko, kto co usunął.
- Wybierz **oba** (`AggregateWithEventsListener`), jeśli potrzebujesz obrazu i historii.

> **Pamiętaj:** obiekt nasłuchujący zdarzeń otrzymuje każdą klasę zdarzeń swojego typu: zdarzenia cofania, zdarzenia squasha
> wydarzenie z rozdziału 14 i wszystkie klasy, które dodasz w przyszłym roku. Dopasowanie bez `case _ =>`, które
> napotyka zdarzenie, o którym nie wie, zgłasza błąd `MatchError`, a obiekt nasłuchujący, który zgłasza błąd, jest błędem
> (rozdział 9). Lista koszyków oparta na zdarzeniach domeny testowej drukuje przeprosiny tylko wtedy, gdy widzi
> cofnij: ustalenie, co zostało cofnięte, to twoje zadanie w przypadku zdarzeń, a niczyje zadanie w przypadku stanów.

Dwie małe zasady: jeden nasłuchiwacz na rodzaj na typ agregatu (wymień dwa `EventsListener[ShoppingCart]`
i tylko ostatnia opcja się liczy), a typ musi być rzeczywistą klasą, ponieważ `Any` i `Nothing` są odrzucane.

## Gdzie znajdują się modele odczytu: magazyny dokumentów

Wyobraź sobie szafkę na dokumenty z jedną szufladą na każdy rodzaj dokumentu i jednym segregatorem na każdy wózek.
oznaczonych numerem koszyka. To jest **magazyn dokumentów**: `DocumentStore[T]` przechowuje dokumenty
typu `T` pod kluczami typu `Long`, zwykle identyfikator agregatu (`cartId.asLong`).

- **`PostgresDocumentStore[T]`** przechowuje je w tabeli o nazwie `projection_<name>`, z kolumnami
  `space_id`, `id`, `version` i `document` (dokument w formacie `JSONB`, zapisany przez mpjsons).
  tworzy tabelę i indeksy, o które prosisz podczas tworzenia sklepu.
- **`MemoryDocumentStore[T]`** przechowuje je na mapie w pamięci. Jest to przeznaczone do testów (rozdział 19);
  ignoruje spacje, więc polecenie `clearSpace` nic nie robi.

```scala
import io.reactivecqrs.core.documentstore.{MultipleTextIndex, NoopDocumentStoreCache, PostgresDocumentStore}

klasa przypadku CartSummary(nazwa: String, liczba elementów: Int, wersja: Int)

val cartsStore = nowy PostgresDocumentStore[CartSummary](
  "carts_list", // tabela: projection_carts_list
  mpjsons, // pliki MPJsons, które utworzyłeś w rozdziale 4
  nowy NoopDocumentStoreCache, // pamięć podręczna, która niczego nie przechowuje
  Seq(MultipleTextIndex(1, Seq("name")))) // indeks w polu JSON "name"
```

- **Nazwa** może zawierać wyłącznie litery, cyfry i podkreślenia, a także najdłuższą nazwę utworzoną przez sklep
  (tabela lub indeks, taki jak `projection_carts_list_idx_1`) musi być zgodny z 63-bitowym standardem PostgreSQL
  znaków. W przeciwnym razie sklep odmawia uruchomienia z wyjątkiem `IllegalArgumentException`.
- **Pamięć podręczna**: `NoopDocumentStoreCache` niczego nie przechowuje, więc każdy odczyt trafia do bazy danych. Pamięć podręczna
  implementuje własną cechę `DocumentStoreCache`.
- **Indeksy** przyspieszają wyszukiwanie w polach JSON: `MultipleTextIndex`, `UniqueTextIndex`,
  `MultipleIntIndex`, `UniqueIntIndex`, `MultipleLongIndex`, `UniqueLongIndex`,
  `MultipleTextArrayIndex` i `MultipleCombinedIndex` lub `UniqueCombinedIndex` na kilku
  Ścieżki. Liczba (tutaj `1`) określa indeks. Na początku sklep tworzy indeksy, których nie
  jeszcze nie ma i usuwa te, których już nie ma na liście.

> **Ostrzeżenie:** sklep rozpoznaje indeks wyłącznie na podstawie jego numeru. Zmień ścieżkę indeksu 1, ale
> Zachowaj numer, a stary indeks pozostanie bez zmian. Zmienionemu indeksowi nadaj nowy numer.

Operacje, z których będziesz korzystać najczęściej:

| Zadzwoń | Co to robi |
|---|---|
| `insertDocument(spaceId, key, document)` | dodaje nowy dokument; kończy się niepowodzeniem, jeśli klucz jest zajęty |
| `overwriteDocument(klucz, dokument)` | zastępuje istniejący dokument; kończy się niepowodzeniem, jeśli nie ma żadnego dokumentu |
| `updateDocument(spaceId, key, modify)` | odczytuje, zmienia i zapisuje z powrotem; `modify` pobiera `Option[Document[T]]` (`None`: jeszcze nie ma dokumentu) i zwraca nowy `Document[T]` |
| `updateExistingDocument(klucz, modyfikuj)` | to samo, dla dokumentu, który musi istnieć |
| `removeDocument(key)` | usuwa go |
| `getDocument(klucz)` | `Opcja[Dokument[T]]` |
| `getDocuments(keys)` | `Map[Long, Document[T]]` znalezionych |
| `findAll()` | każdy dokument, jako `Map[Long, Document[T]]` |
| `countAll()` | ile ich jest |
| `findDocumentByPath(ścieżka, wartość)` | dokumenty, których tekst w ścieżce `ścieżka` jest równy `wartość` |
| `findDocument(query)` | wyszukiwanie z warunkami, sortowaniem i stronicowaniem (dalej w tym rozdziale) |
| `clearSpace(spaceId)`, `clearAllData()` | usuwa dokumenty jednej organizacji lub wszystkie z nich |

`Document[T]` to maleńki wrapper: `Document(summary)` zawija, `doc.document` rozwija. `spaceId`
to organizacja, do której należy dokument (rozdział 16). Wszystkie nasze koszyki znajdują się w przestrzeni 0, ponieważ
to jest to co mówi `ShoppingCartCreated` (`spaceId = SpaceId(0)`).

> **Pamiętaj:** `updateDocument` może wywołać funkcję `modify` więcej niż raz: jeśli inny autor
> zmienił dokument w międzyczasie, odczytuje nowy i próbuje ponownie, nawet 10 razy. Zachowaj
> `modyfikuj` czysto: obliczaj, nie działaj.

> **Wskazówka:** kolumna `wersja` tabeli zlicza zapisy dokumentu, a nie jest kolumną koszyka
> wersja. Jeśli potrzebujesz wersji koszyka w modelu do odczytu (a będziesz jej potrzebować później w tym rozdziale),
> zachowaj je w dokumencie, tak jak robi to `CartSummary`.

## Transakcja, w której działa Twój słuchacz

Każdy zapis wymaga niejawnej sesji DBS. W programie nasłuchującym ta sesja jest transakcją.
projekcja otwarta dla Ciebie, a ta sama transakcja rejestruje postęp projekcji: „koszyk
42 jest stosowany do wersji 3". Tak więc model odczytu i zakładka są zatwierdzane razem lub wcale.
Jeśli słuchacz rzuci w połowie, ani dokument, ani zakładka nie poruszą się, a projekcja
próbuje ponownie (rozdział 9).

Dlatego nasłuchiwacz to funkcja, która przyjmuje sesję. Oznacz ją jako „niejawną”, a następnie zapisz
połączenia wewnątrz odbierają się same:

```scala
AggregateListener[ShoppingCart] { (cartId, wersja, utworzony, koszyk) => niejawna sesja =>
  // każde wywołanie sklepu tutaj jest uruchamiane w `session`
}
```

> **Ostrzeżenie:** nigdy nie zatwierdzaj ani nie wycofuj sesji, którą Ci udostępniono, i nigdy nie otwieraj transakcji
> własne dla modelu do odczytu. Gdy aktualizacje pojawiają się szybko, prognoza dotyczy do 100 kart.
> aktualizacje w jednej transakcji; jeśli jedna z nich się nie powiedzie, wycofuje je wszystkie i stosuje każdy koszyk na raz
> swój własny, tak aby ponowna próba dotyczyła tylko wadliwego koszyka. Twoje zatwierdzenie mogłoby to zepsuć.

Poza słuchaczem (w skrypcie, teście lub zadaniu naprawczym) samodzielnie otwierasz sesję, np.
z `DB.localTx { implicit session => ... }` ScalikeJDBC. Odczyty takie jak `getDocument` i
`findAll` nie wymaga sesji: bez niej otwiera własną sesję tylko do odczytu.

> **Pod maską:** transakcja pochodzi z `SubscriptionsState.localTx`, a projekcja
> wersja porusza się z porównaniem i ustawieniem, więc dwóch autorów nigdy nie może jej jednocześnie przenosić. Grupowanie i jego
> awaryjne są opisane w [ARCHITECTURE.md, sekcja 6.3](../ARCHITECTURE.md); magazyn dokumentów
> w sekcji 6.8; notatnik (`SubscriptionsState`) w sekcji 7.

## Tworzenie listy koszyka krok po kroku

Nasz model odczytu przechowuje jeden dokument na koszyk: jego nazwę, liczbę zawartych w nim pozycji i numer koszyka.
Wersja. Wykorzystujemy ponownie to, co zbudowaliśmy w rozdziale 4: system aktorów „system”, „mpjsons”, „typesNames”,
szyna zdarzeń `eventBus`, użytkownik `alice` i szyna poleceń koszyka `carts`.

1. **Zapisz dokument i pytania.**

   ```scala
   importuj io.reactivecqrs.api.id.AggregateId

   klasa przypadku CartSummary(nazwa: String, liczba elementów: Int, wersja: Int)

   klasa przypadku GetCartSummary(cartId: AggregateId) // odpowiedź udzielona za pomocą opcji [CartSummary]
   obiekt przypadku GetAllCarts // odpowiedź udzielona za pomocą Map[AggregateId, CartSummary]
   klasa przypadku FindCartsByName(name: String) // odpowiedź udzielona za pomocą Vector[CartSummary]
   ```

2. **Napisz projekcję.**

   ```scala
   import io.reactivecqrs.core.documentstore.DocumentStore
   import io.reactivecqrs.core.projection.{ProjectionActor, SubscriptionsState}
   import io.reactivecqrs.testdomain.shoppingcart.ShoppingCart

   klasa CartsListProjection(val subscriptionsState: SubscriptionsState,
                             sklep: DocumentStore[CartSummary]) rozszerza ProjectionActor {

     nadpisz chronioną wartość projectionName: String = "CartsList"

     chronionych słuchaczy wartości = Lista(
       AggregateListener[ShoppingCart] { (cartId, wersja, utworzony, koszyk) => niejawna sesja =>
         dopasowanie koszyka {
           przypadek Some(c) =>
             val summary = CartSummary(c.name, c.items.size, version.asInt)
             jeśli (utworzono) store.insertDocument(0, cartId.asLong, summary)
             w przeciwnym razie przechowuj.overwriteDocument(cartId.asLong, summary)
           przypadek Żaden =>
             sklep.usuńDokument(cartId.asLong)
         }
       })

     nadpisz chronione def receiveQuery: Receive = {
       przypadek GetCartSummary(cartId) =>
         nadawca() ! store.getDocument(cartId.asLong).map(_.document)
       przypadek GetAllCarts =>
         sender() ! store.findAll().map { case (id, doc) => AggregateId(id) -> doc.document }
       przypadek FindCartsByName(nazwa) =>
         sender() ! store.findDocumentByPath(Seq("name"), name).values.map(_.document).toVector
     }
   }
   ```

   Słuchacz czyta jak tablicę wyników: nowy wózek otrzymuje dokument, zmieniony wózek otrzymuje swój
   Dokument zastąpiony, usunięty koszyk go utraci. To jest domena testowa
   `ShoppingCartsListProjectionAggregatesBased`, a także liczba pozycji i wersja.

3. **Utwórz notatnik i sklep**, raz dla każdego systemu i raz dla każdego sklepu:

   ```scala
   import io.reactivecqrs.core.projection.PostgresSubscriptionsState

   val subscriptionsState = new PostgresSubscriptionsState(typesNames).initSchema()
   val cartsStore = nowy PostgresDocumentStore[CartSummary]("carts_list", mpjsons,
     nowy NoopDocumentStoreCache, Seq(MultipleTextIndex(1, Seq("name"))))
   ```

   `initSchema()` tworzy tabelę `subscriptions` (notatnik) i `subscription_failures`
   (rozdział 9) jeśli jeszcze tam nie są.

4. **Rozpocznij projekcję na blokującym dyspozytorze.**

   ```scala
   importuj org.apache.pekko.actor.Props
   importuj io.reactivecqrs.core.util.BlockingDispatcher

   val cartsList = system.actorOf(
     Rekwizyty (nowa projekcja listy koszyków (stan subskrypcji, sklep z koszykami)).withDispatcher (BlockingDispatcher.id),
     „Lista koszyków”)
   ```

   Projekcja komunikuje się z PostgreSQL w ramach własnego wątku. `BlockingDispatcher.id`, czyli
   `reactivecqrs.blocking-dispatcher` (32 wątki, chyba że skonfigurujesz inaczej), to pula
   Biblioteka jest przeznaczona do tego typu zadań, więc reszta systemu nigdy nie czeka w kolejce po wolne zapytanie.

5. **Umieść go na liście subskrybentów magistrali zdarzeń.** W rozdziale 4 gospodarz poinformował magistralę, kto jest jego
   Subskrybenci są zarejestrowani w sekcji „RegisterSubscribers”. Projekcja trafia na tę listę wraz z
   każda inna projekcja, saga i słuchacz systemu:

   ```scala
   import io.reactivecqrs.core.eventbus.EventsBusActor

   eventBus ! EventsBusActor.RegisterSubscribers(List(cartsList))
   ```

   Autobus pyta każdego abonenta, czego chce (projekcja odpowiada sama, jedna subskrypcja
   na słuchacza) i nie publikuje niczego, dopóki wszyscy subskrybenci na liście nie odpowiedzą. Podczas gdy
   czeka, rejestruje co kilka sekund komunikat „Magistrala zdarzeń czeka na subskrypcje: ...”, co jest przydatne
   gdy zapomniałeś zacząć któregoś z nich.

> **Ostrzeżenie:** uruchom projekcję na domyślnym dyspozytorze i nadal działa, ale rejestruje
> ostrzeżenie na starcie („uruchamia transakcje nasłuchujące na domyślnym dyspozytorze; utwórz go za pomocą
> Props(...).withDispatcher(BlockingDispatcher.id)"), a każde zapytanie i każda wykonywana przez niego transakcja są zachowywane
> wątki potrzebne reszcie Pekko. Zawsze dodawaj `.withDispatcher(BlockingDispatcher.id)`.

> **Ostrzeżenie:** projekcja rozpoczęta w ten sposób przechowuje swój notatnik pod **nazwą klasy** (na przykład
> przykład `com.example.CartsListProjection`), a nie pod `projectionName`. Zmień nazwę lub przenieś klasę
> a projekcja zapomina, co zastosowała: następne zdarzenie każdego wózka powoduje, że uruchamia ten wózek
> ponownie z wersji 1, bezpośrednio do dokumentów, które już istnieją. Zarządzane projekcje
> rozdział 10 zachowaj swój notatnik pod nazwą, którą wybierzesz.

## Wypróbowuję

Utwórz koszyk, dodaj mleko i zapytaj:

```scala
zaimportuj org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
importuj io.reactivecqrs.api._
import io.reactivecqrs.testdomain.shoppingcart._
import scala.concurrent.Await
import scala.concurrent.duration._

niejawny limit czasu wartości: Limit czasu = Limit czasu (5 sekund)

def send(polecenie: AnyRef): CustomCommandResponse[_] =
  Await.result((carts ? command).mapTo[CustomCommandResponse[_]], 5 sekund)

val SuccessResponse(cartId, _) = send(CreateShoppingCart(None, alice, "Groceries")) // szkic: prawdziwe kontrole kodu
send(AddItem(alicja, cartId, AggregateVersion(1), "mleko")) // SuccessResponse(Id(42),Version(2))

Thread.sleep(500) // daj projekcji chwilę; lepszy sposób pojawi się później w tym rozdziale

Await.result((cartsList ? GetCartSummary(cartId)).mapTo[Option[CartSummary]], 5.sekund)
// Some(CartSummary(Spożywcze,1,2))
```

Sprawdź w PostgreSQL, a znajdziesz wiersz koszyka w `projection_carts_list`, z
`{"name": "Artykuły spożywcze", "version": 2, "itemCount": 1}` w swojej kolumnie `document` (PostgreSQL zachowuje
klucze wartości `JSONB` w określonej kolejności).

> **Pamiętaj:** projekcja rozpoczęta w ten sposób odbiera tylko zdarzenia opublikowane po jej dołączeniu.
> Wózek, który istniał wcześniej, pojawia się przy następnej zmianie: projekcja zauważa, że
> pominięte wersje od 1 do 3, pyta o nie właściciela wózka, a następnie stosuje wersje od 1 do 4 w kolejności (
> `DeliveryEndToEndSpec` biblioteki pokazuje dokładnie to). Koszyk, który nigdy się nie zmienia, nigdy nie jest wyświetlany
> w górę. Aby wypełnić nową projekcję całą historią, uruchom ją jako projekcję zarządzaną (rozdział 10).

## Zadawanie bardziej precyzyjnych pytań

`findDocument` przyjmuje `DocumentStoreQuery`: warunki dotyczące ścieżek JSON, kolejność sortowania, przesunięcie i
limit. Ścieżka to lista nazw pól prowadzących do wartości: `Seq("name")` lub
`Seq("adres", "miasto")` dla zagnieżdżonego. Oto największe koszyki, od największych na początku:

```scala
import io.reactivecqrs.core.documentstore.{DocumentStoreQuery, ExpectedGreaterThanIntValue, SortDescInt}

klasa przypadku FindBigCarts(moreThan: Int) // odpowiedź udzielona za pomocą Seq[(AggregateId, CartSummary)]

// jeszcze jeden przypadek w receiveQuery:
przypadek FindBigCarts(moreThan) =>
  zapytanie val = DocumentStoreQuery(
    gdzie = Seq(Oczekiwana Wartość Większa Niż Int(Seq("liczba elementów"), więcej niż)),
    sortBy = Seq(SortDescInt("itemCount")),
    przesunięcie = 0,
    granica = 20)
  sender() ! store.findDocument(query).map { case (id, doc) => AggregateId(id) -> doc.document }
```

Wszystkie warunki muszą być spełnione. Oprócz `ExpectedGreaterThanIntValue` istnieją
`ExpectedSingleTextValue`, `ExpectedSingleTextValueLike` (wzorzec SQL `ILIKE` taki jak `"%groc%"`),
`ExpectedMultipleTextValues` (jeden z kilku), `ExpectedSingleIntValue`, `ExpectedSingleLongValue`,
`ExpectedSingleBooleanValue`, `ExpectedLessThanIntValue`, `ExpectedNoValue` i kilka innych dla
tablice. Sortowanie odbywa się jako `SortAscText`, `SortDescText`, `SortAscInt`, `SortDescInt`, `SortAscLong` i
`SortDescLong`. `Limit` równy `-1` oznacza brak limitu, a `DocumentStoreQuery.basic(where)` to
skrót od „te warunki, brak sortowania, brak ograniczeń”.

> **Wskazówka:** zapytanie z pustym polem `where` znajdzie każdy dokument posortowany i podzielony na strony zgodnie z Twoim zapytaniem; dla wszystkich
> z nich naraz, `findAll()` jest prostsze.

Co się dzieje, gdy zapytanie się nie powiedzie? Na zapytanie, które powoduje błąd, odpowiadamy `Status.Failure`, więc
Twoje `zapytanie` kończy się niepowodzeniem z powodu tego wyjątku, a projekcja kontynuuje swoją pracę.

> **Pamiętaj:** zapytania i aktualizacje następują na zmianę w tej samej skrzynce pocztowej. Zanim odpowie na zapytanie,
> projekcja dotyczy każdej dostawy, którą już otrzymała, więc odpowiedzi są spójne; ale podczas gdy
> zapytanie jest uruchamiane, aktualizacja nie jest stosowana. Zadbaj o szybkość zapytań: wyszukiwanie według klucza, przeszukiwanie indeksów,
> i strona z `limit`.

## Ten sam pomysł z wydarzeniami: dziennik aktywności

Niektóre modele to historie, a nie obrazy. Oto dziennik wszystkiego, co się z nimi działo.
wózek, zbudowany ze zdarzeń:

```scala
importuj io.reactivecqrs.api._
importuj io.reactivecqrs.core.documentstore.Document

klasa przypadku CartActivity(linie: Vector[String])
klasa przypadku GetCartActivity(cartId: AggregateId) // odpowiedź udzielona za pomocą opcji [CartActivity]

klasa CartActivityProjection(val subscriptionsState: SubscriptionsState,
                             sklep: DocumentStore[CartActivity]) rozszerza ProjectionActor {

  nadpisz chronioną wartość projectionName: String = "CartActivity"

  chronionych słuchaczy wartości = Lista(
    EventsListener[ShoppingCart] { (cartId, events) => niejawna sesja =>
      val lines = events.map(e => "v" + e.version.asInt + ", user " + e.userId.asLong + ": " + describe(e.event))
      store.updateDocument(0, cartId.asLong, existing =>
        Dokument(CartActivity(existing.map(_.document.lines).getOrElse(Vector.empty) ++ lines)))
    })

  prywatna def describe(event: Event[ShoppingCart]): String = event match {
    przypadek ShoppingCartCreated(name) => "utworzono \"" + nazwa + "\""
    case ShoppingCartDuplicated(_, baseId, baseVersion) => "skopiowano z koszyka" + baseId.asLong + " w wersji" + baseVersion.asInt
    przypadek ItemAdded(nazwa) => "dodano" + nazwa
    przypadek ItemRemoved(itemId) => "usunięty element" + itemId
    case ShoppingCartChangesUndone(count) => "cofnięto ostatnią " + count + " zmianę(y)"
    case ShoppingCartDeleted() => "usunięto koszyk"
    przypadek inny => other.getClass.getSimpleName // cokolwiek innego, zdarzenie squash uwzględnione
  }

  nadpisz chronione def receiveQuery: Receive = {
    przypadek GetCartActivity(cartId) => sender() ! store.getDocument(cartId.asLong).map(_.document)
  }
}
```

Należy zwrócić uwagę na dwie rzeczy. `updateDocument` wstawia dokument, gdy jeszcze go nie ma (`existing` to
`None`), więc ten nasłuchujący nie potrzebuje flagi `created`. A cofnięcie to po prostu kolejny wiersz w logu:
W przypadku historii wydarzenia są naturalnym wyborem. Zaczynasz i rejestrujesz je dokładnie tak samo, jak listę w koszyku.

## Od razu widać zmiany

Alicja dodaje chleb, a jej strona natychmiast przeładowuje koszyk. Polecenie powiodło się, ale jego zdarzenie
może być jeszcze w drodze do projekcji, więc strona pokazuje wózek bez chleba. Denerwujące!

Pracownik szatni ma z tym do czynienia przez cały dzień: oddaje płaszcz i prosi o jego zwrot następnego dnia.
Po drugie. Dobry pracownik nie mówi „nie mamy twojego płaszcza”, tylko „chwileczkę” i patrzy
ponownie, gdy płaszcz dotrze na wieszak. **Opóźnione zapytanie** to ten, który go obsługuje: kiedy odpowiedź nie brzmi
na razie projekcja czeka na kolejne aktualizacje zanim powie „nie”.

Wiesz, na co czekać: odpowiedź polecenia „SuccessResponse(cartId, version)” informuje Cię,
Wersja, którą utworzyłeś/aś. Pytanie brzmi więc: „koszyk 42, gdy tylko osiągnie wersję 3 lub
później":

```scala
klasa przypadku GetCartSummaryAtLeast(cartId: AggregateId, version: AggregateVersion) // odpowiedź udzielona za pomocą opcji [CartSummary]

// jeszcze jeden przypadek w receiveQuery:
przypadek GetCartSummaryAtLeast(cartId, wersja) =>
  delayIfNotAvailable(sender(), () =>
    store.getDocument(cartId.asLong).map(_.document).filter(_.version >= version.asInt),
    (2000)
```

`delayIfNotAvailable(respondTo, search, forMaximumMillis)` uruchamia `search` natychmiast. `Some` przechodzi do
`respondTo` od razu. `None` parkuje zapytanie: projekcja uruchamia wyszukiwanie ponownie po nim.
stosuje aktualizacje i odpowiada natychmiast po znalezieniu czegoś. Gdy minie 2000 milisekund bez
szczęście, odpowiada „Brak”. Po stronie wywołującej:

```scala
wyślij(AddItem(alicja, cartId, AggregateVersion(2), "chleb")) dopasuj {
  przypadek SuccessResponse(_, wersja) =>
    Await.result((cartsList ? GetCartSummaryAtLeast(cartId, version)).mapTo[Option[CartSummary]], 5.seconds)
    // Some(CartSummary(Groceries,2,3)), jak tylko projekcja to ma
  case other => println("Nie dodano: " + other)
}
```

> **Wskazówka:** ustaw czas oczekiwania na odpowiedź (tutaj 5 sekund) na dłuższy niż czas oczekiwania (2 sekundy), aby uzyskać
> projekcja `None` zamiast `AskTimeoutException`.

> **Wskazówka:** to działa, ponieważ dokument zawiera wersję koszyka. Umieść wersję agregatu
> do każdego dokumentu, z którego chcesz odczytać własne zapisy. Kiedy potrzebujesz samego koszyka, a nie
> niż model odczytu, zapytaj jego magistralę poleceń za pomocą `GetAggregateMinVersion` (rozdział 7).

Zwykła wersja uruchamia wyszukiwanie w wątku projekcji, co jest wystarczające w przypadku wyszukiwania według klucza.
Jeśli chcesz wykonać bardziej szczegółowe wyszukiwanie, możesz zlecić to zadanie „Przyszłości”:

| Metoda | `search` zwraca | Odpowiedź |
|---|---|---|
| `delayIfNotAvailable(respondTo, search, maxMillis)` | `Option[T]` | `Some(t)` lub `None` w terminie ostatecznym |
| `delayIfNotAvailableAsync(respondTo, search, maxMillis)` | `Future[Option[T]]` | `Some(t)` lub `None` w terminie ostatecznym |
| `delayIfNotAvailableAsyncCustom(respondTo, search, found, maxMillis)` | `Future[T]`, plus `found: T => Boolean` | `T` gdy `found` oznacza tak, lub `None` w momencie ostatecznym |

Każdy z nich występuje również w formie, w której termin końcowy jest „natychmiastowy”, a nie milisekundowy.
wyszukiwanie, które powoduje błąd lub `Future`, którego wynik się nie powiedzie, jest obsługiwane za pomocą `Status.Failure`.

> **Ostrzeżenie:** wariant niestandardowy odpowiada Twoim `T`, gdy się powiedzie, ale `None`, gdy czas
> się kończy. Wywołujący musi być gotowy na obie rzeczy: proste `mapTo[T]` nie powiedzie się w przypadku `None`.

> **Pod maską:** zaparkowane zapytanie czeka wewnątrz projekcji (jako prywatne `SyncDelayedQuery`,
> `AsyncDelayedQuery` lub `AsyncDelayedQueryCustom`; nigdy nie tworzysz ich samodzielnie). Jest to wyszukiwane
> ponownie raz na serię zastosowanych aktualizacji, a nie raz na aktualizację, a po upływie terminu zostanie udzielona odpowiedź
> „Brak” bez kolejnego wyszukiwania. Warianty asynchroniczne parkują zapytanie tylko wtedy, gdy jego „Przyszłość” zostanie
> odpowiedział, więc aktualizacja zastosowana podczas działania `Future` jest zauważana dopiero przy następnej aktualizacji; dla
> wyszukiwanie według klucza, prosta wersja jest bezpiecznym wyborem. Zobacz
> [ARCHITECTURE.md, sekcja 6.5](../ARCHITECTURE.md).

## Opcje, które warto znać

Projekcja przyjmuje swoje opcje w konstruktorze, jako `ProjectionActorOptions`. Zacznij od
`ProjectionActorOptions.DEFAULT` i zmień to, czego potrzebujesz:

```scala
import io.reactivecqrs.core.projection.ProjectionActorOptions

klasa CartsListProjection(val subscriptionsState: SubscriptionsState, store: DocumentStore[CartSummary])
  rozszerza ProjectionActor(ProjectionActorOptions.DEFAULT.withGroupUpdatesDelayMillis(200).withMinimumDelayVersion(2)) {
  // ... jak poprzednio
}
```

| Opcja (domyślna) | Co robi | Jak ją ustawić |
|---|---|---|
| `groupUpdatesDelayMillis` (0) | czeka tak długo przed zastosowaniem dostaw z koszyka, tak aby kilka aktualizacji jednego koszyka zostało zastosowanych jednocześnie | `withGroupUpdatesDelayMillis(200)` |
| `minimumDelayVersion` (1) | dostawy rozpoczynające się poniżej tej wersji nigdy nie czekają; przy wersji 2 nowy koszyk pojawia się od razu | `withMinimumDelayVersion(2)` |
| `eventsToProcessImmediately` (brak) | klasy zdarzeń, które nigdy nie czekają (na słuchaczy odbierających zdarzenia) | `withEventsToProcessImmediately(classOf[ShoppingCartDeleted])` |
| `maxBatchesPerTransaction` (100) | gdy dostawy się kumulują, do ilu aktualizacji koszyków można zastosować jedną transakcję; 1 dotyczy każdego koszyka osobno | `.copy(maxBatchesPerTransaction = 1)` |
| `parallelUpdateProcessing` (0) | powyżej 1, tyle wątków stosuje aktualizacje; każdy koszyk nadal zachowuje swoją kolejność | `withParallelUpdateProcessing(4)` |
| `eventsLogger` (brak) | rejestrator, który pobiera linię debugowania dla każdej dostawy i każdej zastosowanej partii | `withEventsLogger(LoggerFactory.getLogger("carts-list"))` |
| `retries`, `retryFailuresOnStart`, `gapRequestRepeat`, `gapGiveUpAfter`, `failureRetryInterval` | Co się dzieje, gdy coś pójdzie nie tak | rozdział 9 |

Pierwsze trzy mają znaczenie tylko razem: przy `groupUpdatesDelayMillis` na poziomie 0 (domyślnie) nic się nie dzieje
czeka w każdym razie. `withEventsLogger` przyjmuje `Logger` SLF4J.

> **Ostrzeżenie:** w przypadku `parallelUpdateProcessing` Twoje programy nasłuchujące działają na kilku wątkach jednocześnie, na
> te same obiekty: muszą być bezpieczne pod kątem wątków, a pula JDBC wymaga co najmniej takiej samej liczby połączeń.
> Wyłącza również grupowanie `maxBatchesPerTransaction`. Większość projekcji działa bez niego.

> **Wskazówka:** w przypadku projekcji bez żadnych zapytań, np. takiej, która jedynie zasila inny system,
> rozszerz `AggregateListenerActor` zamiast `ProjectionActor`: to jest to samo, co
> `receiveQuery`, które ignoruje każdą wiadomość. Rozdział 9 używa takiego zapytania.

## Czego się nauczyłeś

- **Projekcja** to `ProjectionActor` z nasłuchiwaczami, modelem odczytu i zapytaniami; jego notatnikiem,
  `SubscriptionsState` rejestruje zbiorczo ostatnią zastosowaną wersję.
- Istnieją trzy **listenery**: `AggregateListener` dla stanu (obrazy),
  `EventsListener` dla zdarzeń (historii) i `AggregateWithEventsListener` dla obu.
- **Magazyn dokumentów** przechowuje dokumenty JSON według klucza: `PostgresDocumentStore` w tabeli
  `projection_<name>` z kolumną `space_id`, `MemoryDocumentStore` do testów.
- Słuchacz działa w **transakcji**, która również rejestruje postęp projekcji; przekazuje ją do
  przechowuj i nigdy nie twórz go samodzielnie.
- Rozpocznij projekcję za pomocą `Props(...).withDispatcher(BlockingDispatcher.id)` i umieść ją na
  lista `RegisterSubscribers` serwisu bus.
- **Opóźnione zapytania** (`delayIfNotAvailable` i podobne) pozwalają innym użytkownikom odczytywać własne zapisy.

Dalej: [rozdział 9](09-projection-failures.md) celowo psuje rzeczy i pokazuje, jak projekcje
odzyskiwać.
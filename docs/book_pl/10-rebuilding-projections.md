# Rozdział 10. Odbudowa projekcji bez przestoju

Do końca tego rozdziału będziesz w stanie dostarczyć nową wersję projekcji, naprawić błąd w
jeden lub dodać zupełnie nowy model odczytu na podstawie lat starych zdarzeń, podczas gdy stary model odczytu zachowuje
Odpowiadając na pytania przez cały czas. Naprawisz również dokument pojedynczego koszyka bez
dotykając reszty. Ta sztuczka nazywa się *zarządzaną projekcją* i kosztuje około dziesięciu linii
okablowanie.

---

## Po co w ogóle odbudowywać?

Pamiętasz bank z rozdziału 1: księga rejestruje każdą transakcję, a podsumowanie w Twoim
Aplikacja bankowa jest jego projekcją. Pewnego dnia bank decyduje, że podsumowanie powinno pokazywać miesięczne sumy
też. Nikt nie prosi klientów o ponowne wpisywanie transakcji. Bank odczytuje księgę rachunkową z
pierwszy wiersz i pisze nowe podsumowanie.

To samo należy stosować w przypadku modeli do czytania, z powodów codziennych:

- **Nowa wersja.** Lista w koszyku powinna zawierać nazwy pisane wielkimi literami lub liczbę pozycji obok każdej pozycji.
  nazwa.
- **Poprawka błędu.** Słuchacz przez miesiące zapisywał błędne dane. Poprawiasz kod, a błędne wiersze muszą zostać usunięte.
  zostanie napisane ponownie.
- **Nowy model odczytu.** Projekcja, którą dodajesz dzisiaj, musi uwzględniać koszyki utworzone w zeszłym roku. Zwykły
  projekcja (rozdział 8) słyszy o starym wozie dopiero, gdy ten wóz znów się zmienia.
- **Zmieniony agregat.** Zmieniłeś sposób, w jaki zdarzenia koszyka budują jego stan i podniosłeś kontekst
  `wersja`. Modele odczytu utworzone na podstawie stanów muszą zostać utworzone ponownie.
- **Naprawa.** Jeden dokument uległ uszkodzeniu (np. błędnie napisano poprawkę SQL) i chcesz go po prostu
  ten napisany jeszcze raz na podstawie wydarzeń.

## Sklep obok

Ponowne budowanie modelu odczytu w miejscu ma swoją brzydką stronę: podczas opróżniania jego tabel i ponownego ich wypełniania,
Każdy, kto pyta, otrzymuje pół odpowiedzi. Dlatego ReactiveCQRS nie odbudowuje się w miejscu.

Wyobraź sobie sklep, który potrzebuje nowych półek. Zamiast zamykać go na tydzień, wyposażasz pusty sklep.
obok i uzupełniać zapasy z ewidencji magazynowej, podczas gdy stary sklep nadal sprzedaje. Kiedy nowy
Gdy już wszystko jest gotowe, wieszasz znak nad drzwiami. Klienci nigdy nie zastają drzwi zamkniętych.

Oto jak działa **zarządzana projekcja**:

```
                                ┌── kompilacja 1 (wersja 1), tabele *_v1 ── odpowiada na zapytania, obsługuje zdarzenia na żywo
ty ── zapytania ──▶ kierownik ──┤
                                └── kompilacja 2 (wersja 2), tabele *_v2 ── wypełnione z magazynu zdarzeń
```

- **Build** to jedna kopia projekcji z własnymi tabelami. Kompilacje są numerowane 1, 2, 3...
- **Aktywna kompilacja** odpowiada na zapytania. **Kompilacja w toku** jest wypełniana na podstawie zdarzenia.
  sklep.
- **Supervisor** to aktor, z którym rozmawiasz. Uruchamia kompilacje, przekazuje zapytania do aktywnego
  jeden i przełącza się na nowy, gdy już nadrobi zaległości: jedna aktualizacja w bazie danych, znak
  wzruszony.
- **Rejestr** to kilka tabel, w których biblioteka zapamiętuje, na podstawie projekcji, aktywną kompilację i
  budowa w toku.

Po przełączeniu tabele starej kompilacji zostaną usunięte. Nigdy nie zapiszesz żadnej z tych księgowości.

## Konfigurowanie zarządzanych projekcji

Będziemy zarządzać małą projekcją nazw koszyków, wzorowaną na `ShoppingCartsListProjectionAggregatesBased`
w domenie testowej. Przechowuje jeden dokument na koszyk, zawierający jego nazwę:

```scala
importuj io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.documentstore.DocumentStore
import io.reactivecqrs.core.projection.{ProjectionActor, SubscriptionsState}
import io.reactivecqrs.testdomain.shoppingcart.ShoppingCart

klasa przypadku GetCartName(id: AggregateId)

klasa CartNames(val subscriptionsState: SubscriptionsState, store: DocumentStore[String]) rozszerza ProjectionActor {

  chronione val listeners = List(AggregateListener[ShoppingCart] { (id, _, created, cart) => niejawna sesja =>
    dopasowanie koszyka {
      przypadek Some(c) jeśli utworzony => store.insertDocument(0, id.asLong, c.name)
      przypadek Some(c) => store.overwriteDocument(id.asLong, c.name)
      case None => store.removeDocument(id.asLong) // koszyk został usunięty
    }
  })

  nadpisz chronione def receiveQuery: Receive = {
    przypadek GetCartName(id) => sender() ! store.getDocument(id.asLong).map(_.document)
  }

  nadpisz chronioną wartość projectionName: String = "CartNames"
}
```

Nic nowego od rozdziału 8. Zmienia się tylko sposób rozpoczęcia. Cztery kroki:

1. **Utwórz rejestr** raz, podczas uruchamiania aplikacji:

   ```scala
   import io.reactivecqrs.core.rebuild.PostgresProjectionRegistry

   val registry = new PostgresProjectionRegistry().initSchema()
   ```

2. **Utwórz jedną `Projekcję`** dla systemu aktorów. Uruchamia ona każdą zarządzaną projekcję:

   ```scala
   import io.reactivecqrs.core.rebuild.{Projekcje, Ustawienia projekcji}

   projekcje val = nowe projekcje (system, ustawienia projekcji (magazyn zdarzeń, stan subskrypcji, rejestr,
     konteksty = Seq(nowy ShoppingCartAggregateContext)))
   ```

   `eventStore` to miejsce, z którego kompilacje odczytują zdarzenia, a `subscriptionsState` to miejsce, w którym
   projekcje już korzystają (rozdział 8). `konteksty` wymieniają zbiorcze konteksty, których *stany* są twoimi
   zarządzane projekcje otrzymują: `CartNames` nasłuchuje stanów koszyka, więc kontekst koszyka jest wprowadzany. Trzy
   więcej parametrów ma wartości domyślne: `builder` (jak odczytuje się kompilację, `BuilderSettings()`),
   `buildsAtOnce = 2` i `restartAfter = 10.sekund` (jak długo nadzorca czeka przed próbą
   ponownie po tym, jak coś zawiedzie).

3. **Rozpocznij projekcję** za jej pomocą:

   ```scala
   import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}

   val cartNames: ActorRef = projections.start[CartNames]("cart-names", 1, "cartNames") { build =>
     nowe nazwy koszyków (stan subskrypcji,
       nowy PostgresDocumentStore[String](build.documentStore("cart_names"), mpjsons, nowy NoopDocumentStoreCache))
   }
   ```

   Przeczytaj od lewej do prawej: klasa projekcji, jej **nazwa** (`"cart-names"`), jej
   **wersja** (`1`), nazwa aktora jego nadzorcy (`"cartNames"`) i funkcja, która tworzy jedną
   wystąpienie projekcji dla danej kompilacji. Funkcja pyta kompilację o nazwę tabeli:
   `build.documentStore("cart_names")` zwraca `cart_names_v1` dla pierwszej kompilacji wersji 1, a
   magazyn dokumentów tworzy z niego tabelę `projection_cart_names_v1`.

4. **Użyj funkcji Supervisor wszędzie tam, gdzie użyłbyś projekcji**: zarejestruj ją w wydarzeniu
   autobus i wyślij do niego swoje zapytania.

   ```scala
   import io.reactivecqrs.core.eventbus.EventsBusActor.RegisterSubscribers
   zaimportuj org.apache.pekko.pattern.ask

   eventBus ! RegisterSubscribers(Seq(cartNames)) // razem z innymi subskrybentami, w jednej wiadomości

   val name: Future[Opcja[Ciąg]] = (cartNames ? GetCartName(cartId)).mapTo[Opcja[Ciąg]]
   ```

Podczas pierwszego uruchomienia rejestr nigdy nie słyszał o `cart-names`, więc nadzorca uruchamia
Kompilacja 1 i wypełnia ją wszystkimi koszykami, które już są w sklepie. Kompilacja 1 to wszystko, co jest dostępne, więc odpowiada
zapytania od samego początku, z tym, co do tej pory. Kiedy już nadrobi zaległości, po prostu kontynuuje
z wydarzeniami na żywo. Od teraz w rejestrze widnieje: `cart-names`, active build 1, version 1.

> **Pamiętaj:** nazwa to identyfikator projekcji w bazie danych. Aby zmienić projekcję,
> zmienić jego wersję i zachować nazwę.

> **Ostrzeżenie:** utwórz każdy sklep wewnątrz funkcji i nazwij każdą tabelę poprzez kompilację:
> `build.documentStore(table)` dla magazynu dokumentów, `build.table(name)` i `build.sequence(name)`
> dla własnych tabel i sekwencji. Wtedy dwie kompilacje nigdy nie będą współdzielić tabeli, a porzucona kompilacja
> zabiera ze sobą wszystkie swoje tabele. Magazyn (lub pamięć podręczna) utworzony raz, poza funkcją, byłby
> współdzielone przez dwie kompilacje i mieszałyby ich dokumenty.

> **Wskazówka:** nazwy tabel powinny być krótkie. Mogą zawierać tylko litery, cyfry i znaki podkreślenia.
> dodaje sufiks taki jak `_v3_b4`, magazyn dokumentów dodaje `projection_` na początku, a PostgreSQL
> pozwala na 63 znaki: sklep, którego pełna nazwa byłaby dłuższa, zostanie niepoprawny podczas tworzenia.

Nawiasem mówiąc, nie potrzebujesz `.withDispatcher(BlockingDispatcher.id)` w przypadku zarządzanych projekcji:
nadzorca tworzy dla Ciebie każdą instancję w blokującym dyspozytorze.

## Wysyłka nowej wersji

Załóżmy, że sklep chce, aby nazwy koszyków były pisane wielkimi literami. Oto całe wydanie:

1. Zmień obiekt nasłuchujący, aby zapisał `c.name.toUpperCase` zarówno w `insertDocument`, jak i
   `nadpiszDokument`.
2. Podnieś wersję w `start`: `projections.start[CartNames]("cart-names", 2, "cartNames") { ... }`.
3. Wdróż i uruchom aplikację.

To wszystko, co musisz zrobić. Co dzieje się dalej:

- Nadzorca odczytuje rejestr: aktywna kompilacja jest w wersji 1, kod w wersji 2.
  rozpoczyna kompilację 2 z własnymi tabelami (`projection_cart_names_v2`).
- Wersja 1 nadal odpowiada na zapytania.
- Budowniczy odczytuje wózki ze sklepu z wydarzeniami i przekazuje je do zbudowania 2, wielu na raz, jednego
  transakcji na blok.
- Gdy kompilacja 2 zawiera wszystkie zdarzenia, rejestr przełącza się w jednej aktualizacji. Od tego momentu kompilacja 2 odpowiada.
- Kompilacja 1 opuszcza magistralę zdarzeń i zatrzymuje się; jej tabele, wersje subskrypcji i rekordy błędów są
  upuszczony.

Dziennik opowiada historię. Z naszą projekcją wygląda to tak (pominięto inne wiersze):

```
Nazwy koszyków projekcji obsługują kompilację 1 (wersja 1), kompilacje 2 (wersja 2)
Projekcja projection:cart-names_v2 jest już dostępna; uzgadnianie jej ze sklepem
Nazwy koszyków projekcji przełączono na kompilację 2 (wersja 2)
Nazwy koszyków projekcji: kompilacja 1 została porzucona
```

Możesz również zapytać kierownika. Odpowiedź na `GetBuilds` brzmi `Builds(nazwa, aktywny, budynek,
złapany)`:

```scala
import io.reactivecqrs.core.rebuild.ProjectionSupervisor._

(cartNames ? GetBuilds).mapTo[Builds]
// podczas kompilacji: aktywny = kompilacja 1, kompilacja = kompilacja 2
// po przełączeniu: aktywny = kompilacja 2, kompilacja = Brak
```

`aktywny` i `budynek` to `BuildRecord`: numer` kompilacji, jej `wersja`, jej `sufiks` tabeli
(`"_v2"`) i nazwa `subskrybenta`, pod którą przechowywane są jego wersje subskrypcji
(`"projekcja:cart-names_v2"`). `caughtUp` jest prawdą, gdy kompilacja w toku zawiera wszystkie zdarzenia;
zastępuje aktywną kompilację, gdy tylko nie wystąpią żadne nieudane subskrypcje (więcej informacji poniżej).

> **Pamiętaj:** kompilacja, która zostanie przerwana (zatrzymasz aplikację lub ulegnie ona awarii), będzie kontynuowana
> z miejsca, w którym znajdowała się przy następnym uruchomieniu; nie zaczyna się od nowa. Ale wdrażaj wersję 3, podczas gdy wersja 2
> jest nadal budowana, a częściowo ukończona wersja 2 zostaje odrzucona: zamiast niej powstaje wersja 3.

## Co stara wersja robi w międzyczasie

Podczas kompilacji wersji 2 tabele wersji 1 odpowiadają na zapytania. Ale *kod* wersji 1 zniknął:
wdrożona wersja 2. A co z wydarzeniami, które nadejdą w międzyczasie? Wybierasz, korzystając z
Parametr `whileBuilding` metody `start`:

- **`ServeLive`** (domyślnie): aktywna kompilacja ciągle stosuje zdarzenia na żywo do swoich starych tabel,
  nowy kod. Zapytania widzą nowe dane, a zapytania są parkowane do momentu pojawienia się dokumentu (rozdział 8)
  są odpowiedzi. Wybierz tę opcję, jeśli nowy kod może działać na starych dokumentach, ponieważ wielkie litery
  wersja może: obie wersje przechowują `String`.
- **`ServeStale`**: aktywna kompilacja odpowiada na podstawie swoich tabel w takiej formie, w jakiej są, i nie przyjmuje żadnych zdarzeń do momentu
  Przełącznik. Wybierz go, gdy nowy kod nie może działać na starych dokumentach. Załóżmy, że wersja 3 przechowuje
  `CartSummary(name, itemsCount)` zamiast `String`: kod wersji 3 nie może odczytać kodu wersji 2
  wydziwianie.

```scala
projekcje.start[NazwyKoszyków]("nazwy-koszyków", 3, "NazwyKoszyków", whileBuilding = ServeStale) { build => ... }
```

Oba znajdują się w `io.reactivecqrs.core.rebuild.ProjectionSupervisor`.

> **Wskazówka:** funkcja otrzymuje kompilację, a kompilacja zna swoją własną wersję (`build.version`).
> Instancja utworzona dla aktywnej kompilacji wersji 1 może zachować zachowanie wersji 1, podczas gdy wersja 2
> kompilacje. Gdyby `CartNames` przyjął flagę `wielkimi literami`, napisałbyś
> `nowe CartNames(subscriptionsState, store, capitals = build.version >= 2)`. `ProjectionsSpec` działa
> dokładnie to.

## Co widzi Twoja projekcja podczas jej tworzenia

Kompilacja przekazuje projekcji te same wydarzenia, w tej samej kolejności, w jakiej odbywa się transmisja na żywo.
Zmieniono, wliczając w to zdarzenia cofania. Dzięki temu odbudowane tabele będą takie same jak tabele utrzymywane na żywo;
`RebuildEquivalenceSpec` sprawdza to w przypadku losowych historii. Trzy rzeczy się różnią i
projekcja może powiedzieć:

- **`rebuilding`** ma wartość `true`, gdy instancja jest budowana i jeszcze nie obsługuje, a także gdy
  Celowana przebudowa (poniżej) przywraca stare zdarzenia. Wszystko, co ma efekt uboczny, np. komunikat
  do innego aktora lub aktualizacji push na ekranie (rozdział 11), najpierw ją sprawdza. `ManagedNotes` w
  `ProjectionsSpec` wysyła swoje aktualizacje w następujący sposób:

  ```scala
  jeśli (!rebuilding) sendUpdate[NoteLines](linie)
  ```

- **`onActivated()`** jest wywoływane jednorazowo, gdy zbudowana instancja zaczyna działać. Zastąp to, jeśli
  projekcja musi coś dokończyć.
- **Kawałki.** Kompilacja zatwierdza wiele agregatów na transakcję i wznawia się po zatrzymaniu od ostatniej
  zapisany fragment. Słuchacz, który przechowuje stan w pamięci pomiędzy wywołaniami, poza swoimi tabelami, nie może polegać
  w tym stanie, podczas gdy jest budowany.

## Kiedy rozpoczyna się kompilacja?

Cztery rzeczy zaczynają się od jednego:

1. **Nowa projekcja.** Pierwsze wystąpienie nazwy, której rejestr nie zna, powoduje jej utworzenie.
2. **Nowa wersja.** Wersja kodu różni się od wersji aktywnej kompilacji.
3. **Zmieniony kontekst.** `Wersja` kontekstu zbiorczego, którego stany określają projekcję
   różni się od tego, z którego wykonano aktywną kompilację. Podnieś
   `ShoppingCartAggregateContext.version` i `CartNames` kompilują się ponownie w swojej własnej wersji.
   projekcja, która nasłuchuje tylko zdarzeń koszyka (`EventsListener`), zachowuje swoją kompilację.
4. **Prosisz o to**, za pomocą `RebuildProjection` (następna sekcja).

Maksymalnie dwie kompilacje są uruchamiane jednocześnie (`buildsAtOnce`); pozostałe czekają na swoją kolej.

## Prośba o odbudowę

Naprawiłeś błąd w programie nasłuchującym bez wprowadzania zmian? Poproś o nową wersję tego samego
wersja:

```scala
cartNames ? RebuildProjection
// odpowiedzi RebuildStarted(2): kompilacja 2 wersji 1, w tabelach z sufiksem „_v1_b2”
```

Następuje ten sam taniec: zbuduj 2 wypełnienia ze sklepu, podczas gdy kompilacja 1 serwuje, następnie przełącz i
upuścić. Podczas gdy kompilacja lub docelowa kompilacja jest już uruchomiona, nadzorca odpowiada
Zamiast tego `RebuildRefused(reason)`.

> **Wskazówka:** przebudowa może być również najszybszym lekarstwem na prognozę, która znacznie odbiegała od rzeczywistości, np. po
> długa przerwa: kompilacja odczytuje ze sklepu tysiąc zdarzeń na raz, jednocześnie nadrabiając zaległości
> magistrala zdarzeń i skrzynka nadawcza są przesyłane zbiorczo (`docs/SCALING.md`, sekcje 4 i 5).

## Kompilacja, która nie będzie się przełączać

Kompilacja zastępuje aktywną tylko wtedy, gdy nie ma nieudanej subskrypcji. Jeśli nasłuchiwacz ulegnie awarii,
jakiś wózek podczas kompilacji (rozdział 9 wyjaśnia błędy), kompilacja czeka: nadzorca rejestruje
ostrzeżenie i sprawdza ponownie co 30 sekund. Tymczasem aktywna kompilacja działa normalnie.

Aby dotrzeć do trwającej kompilacji, opakowuj wiadomość w tag „ToBuildInProgress”. Nadzorca przekaże ją dalej.
do kompilacji zamiast do aktywnej:

```scala
import io.reactivecqrs.core.projection.{GetProjectionStatus, RetryAllFailedSubscriptions, SkipFailedVersion}

cartNames ? ToBuildInProgress(GetProjectionStatus) // ProjectionStatus kompilacji
cartNames ? ToBuildInProgress(RetryAllFailedSubscriptions) // FailedSubscriptionsRetried(count)
cartNames ? ToBuildInProgress(SkipFailedVersion(cartId)) // FailedSubscriptionsHandled(cartId, count)
```

Po ponownym próbie lub pominięciu błędów kompilacja przełącza się przy następnym podejściu. Bez kompilacji
uruchomiono, odpowiedź brzmi `RebuildRefused("żadna kompilacja nie jest uruchomiona")`.

## Naprawa kilku agregatów: ukierunkowane przebudowy

Czasami na jednej stronie katalogu jest literówka. Przedrukowujesz tę stronę, a nie cały katalog.
**celowana odbudowa** robi to samo w przypadku modelu odczytu: odbudowuje na miejscu to, co zostało aktywnie zbudowane
utworzony z jednego agregatu, jednego typu agregatu lub jednej organizacji i pozostawia wszystko inne bez zmian.

Po pierwsze, projekcja musi wiedzieć, jak wyczyścić to, co utworzyła z celu. Nadpisz
`clearForRebuild` w `CartNames`:

```scala
import io.reactivecqrs.core.projection.RebuildTarget
import scalikejdbc.DBSession

nadpisz chronione def clearForRebuild(target: RebuildTarget)(implicit session: DBSession): Unit = target match {
  przypadek RebuildTarget.OneAggregate(id) => store.removeDocument(id.asLong)
  przypadek RebuildTarget.OneSpace(space) => store.clearSpace(space.asLong)
  case RebuildTarget.OneType(_) => store.clearAllData() // CartNames otrzymuje tylko koszyki
}
```

To jest wzór „ManagedNotes” w „ProjectionsSpec”. Następnie zapytaj przełożonego:

```scala
importuj io.reactivecqrs.api.AggregateType
importuj io.reactivecqrs.api.id.SpaceId

cartNames ? RebuildAggregates(RebuildTarget.OneAggregate(cartId))
// odpowiedzi AggregatesRebuilt(RebuildTarget.OneAggregate(cartId), 1)

cartNames ? RebuildAggregates(RebuildTarget.OneType(AggregateType.of[ShoppingCart])) // każdy koszyk
cartNames ? RebuildAggregates(RebuildTarget.OneSpace(SpaceId(7))) // organizacja 7
```

Co się dzieje:

1. Nadzorca zapisuje cel w rejestrze, więc odbudowa przerwana przez zatrzymanie jest wykonywana
   ponownie przy następnym starcie.
2. Aktywna kompilacja stosuje dostawy, które już przyjęła, tworzy listę agregatów docelowych i w
   jedna transakcja, wywołuje `clearForRebuild` i zapomina, jak daleko zaszła z tymi agregatami.
3. Twórca odczytuje swoje wydarzenia ze sklepu w kolejności, w jakiej zostały zatwierdzone, i przekazuje je
   projekcja ponownie. Projekcja nadal działa i w międzyczasie nadal transmituje wydarzenia na żywo.
4. Odpowiedź brzmi `AggregatesRebuilt(target, count)` wraz z liczbą odbudowanych agregatów.

Pomiędzy krokami 2 i 4 zapytania dotyczące obiektu docelowego wykazują, że jego dokumenty zaginęły lub zostały w połowie odtworzone.

Zamiast tego nadzorca odpowiada `RebuildRefused(reason)`, gdy:

- projekcja nie implementuje `clearForRebuild` (powód tak mówi);
- trwa budowa (i tak zastępuje aktywną budowę w całości) lub inna docelowa budowa
  Jest;
- cel nie jest typem agregatu, jaki otrzymuje projekcja;
- celem jest organizacja, a niektóre agregaty typów projekcji nie znają swoich
  organizacja jeszcze. Agregaty przechowywane w starszych wersjach biblioteki mogą jej nie zawierać;
  `backfillSpaces` rozwiązuje ten problem (rozdział 16).

> **Ostrzeżenie:** wyczyść dokładnie to, co wygenerowały agregaty docelowe. Wyczyść mniej, a nieaktualne dane pozostaną
> z tyłu. Wyczyść więcej, a zniknie na zawsze: odbudowa zwraca tylko wydarzenia celu.
> `clearAllData()` dla `OneType` jest poprawne w `CartNames` tylko dlatego, że otrzymuje pojedynczy typ.

> **Wskazówka:** zapytanie ma limit czasu, a odbudowa każdego koszyka zajmuje znacznie więcej czasu niż odbudowa jednego. Daj
> poproś o dopasowanie limitu czasu lub wyślij wiadomość od aktora, który czeka na odpowiedź.
> przekroczenie limitu czasu zapytania nie zatrzymuje odbudowy.

## Co pozostaje niezarządzane: skutki uboczne

Niektórzy subskrybenci w ogóle nie czytają modeli: takiego, który wysyła powitalny e-mail po utworzeniu koszyka,
taki, który wysyła polecenie do innego agregatu. Odbudowa tych agregatów byłaby małą katastrofą: każdy
Wszystkie wysłane wiadomości e-mail zostaną ponownie wysłane.

Zachowaj je jako proste projekcje, stworzone i zarejestrowane w sposób pokazany w rozdziale 8, a nie poprzez
`Projekcje`. Biblioteka nigdy ich nie przebudowuje: kompilacja dostarcza zdarzenia tylko do kompilacji.
wypełnienia. Gdzie postawić granicę:

- efektem ubocznym **jest** zadanie abonenta: pozostawienie go bez zarządzania;
- efektem ubocznym jest niewielki dodatek do modelu odczytu (aktualizacja push na ekranie): zarządzanie odczytem
  model i zabezpiecz dodatkowy za pomocą `if (!rebuilding)`.

## Wycofywanie projekcji

Usunąłeś projekcję z kodu? Tabele kompilacji i księgowość pozostają w bazie danych do momentu
wycofujesz go, w tym rekordy błędów, które w przeciwnym razie uniemożliwiałyby trwałe usunięcie
agregaty, które nazywają. Wywołaj to podczas uruchamiania aplikacji:

```scala
projekcje.wycofanie("nazwy-koszyków")
```

Porzuca każdą kompilację projekcji i zapomina nazwy. W skupisku pośrodku toczącego się
aktualizacja, czeka, aż każdy węzeł uruchomi nową wersję (rozdział 17).

## Ile to trwa

Kompilacja odczytuje każde zdarzenie typu, którego nasłuchuje projekcja. Pomiary przeprowadzane są na jednej maszynie (
test `baseline-M4` z 17.10.2026 r.: Windows 10, 12 wątków procesora, OpenJDK 17 z pamięcią masową 2 GB,
PostgreSQL 17.4 na tej samej maszynie): dwie nowe zarządzane projekcje obejmujące 112 211 zdarzeń zostały
zbudowany i przełączony około 16,6 sekundy po uruchomieniu systemu. Przełączenie oparte na zdarzeniach zajęło 6769 sekund.
zdarzeń na sekundę, a sterta osiągnęła szczyt 150 MB. To jest jedno uruchomienie na jednej maszynie: dobre dla
orientacja, a nie obietnica dla ciebie.

Na podstawie tego tempa, jak szacuje `docs/SCALING.md`, odbudowanie projekcji opartej na zdarzeniach zajmie około 4,1 godziny
ponad 100 milionów zdarzeń (szacunkowo, nie mierzono). Odbudowa kosztuje czas i obciążenie bazy danych, a nie
przestój: aktywna kompilacja działa przez cały czas.

Rzeczy, które pomagają:

- **Projekcje obejmujące wyłącznie stan wykonują mniej pracy.** Gdy każdy słuchacz projekcji jest
  `AggregateListener`, podobnie jak w `CartNames`, kompilacja pomija historię: przekazuje każdą
  stan końcowy agregatu, raz.
- **Migawki pomagają również w kompilacjach.** Kompilacja, która odtwarza zdarzenia w kolejności zatwierdzania, przechowuje maksymalnie 10 000
  stany agregatu są otwarte; stan, który został przez niego usunięty, jest odczytywany ponownie z jego migawki (rozdział 13) i
  wydarzenia po nim.
- **Dwie kompilacje na raz** (`buildsAtOnce = 2`) to ustawienie domyślne, a zalecenie: każda kompilacja
  odczytuje dane ze sklepu i zapisuje własne tabele.
- **Duże wydarzenia?** `ProjectionsSettings(..., builder = BuilderSettings(pageSize = ..., chunkEvents
  = ...))` ustawia liczbę zdarzeń, które kompilacja odczytuje i stosuje jednocześnie: domyślnie 1000 zdarzeń, a przy
  maksymalnie 64 MB na stronę i 16 MB na fragment. `BuilderSettings` znajduje się w
  `io.reactivecqrs.core.rebuild.ProjectionBuilder`.
- **Ulepszone instalacje:** kompilacja jednego typu lub jednej organizacji odczytuje tylko własne zdarzenia
  kiedyś każde zapisane zdarzenie miało klucz. Instalacja, która miała już zdarzenia, gdy klucze były
  wprowadzono uruchomienie `backfillEventKeys()` raz (rozdział 18); do tego czasu takie kompilacje przechodzą całą
  sklep.

> **Pod maską:** kompilacja składa się z trzech faz. Przebieg zbiorczy odczytuje zdarzenia projekcji w
> kolejność zatwierdzania (projekcja obejmująca tylko stan otrzymuje stany końcowe) i stosuje je w częściach
> do 1000 zdarzeń, po jednej transakcji na każde, dwa fragmenty w trakcie realizacji; tymczasem nowa kompilacja potwierdza
> dostawy autobusu bez ich stosowania, ponieważ ich wydarzenia są w sklepie. Następnie zostaje uruchomione
> i stosuje dostawy autobusu. Na koniec, uzgadnianie porównuje wersję każdego agregatu z
> kompilacja i dostarcza to, czego brakuje: zdarzenia zapisane podczas wykonywania przebiegu i wiersze, które
> zatwierdzono późno. Pozycja jest zapisywana po każdym fragmencie, więc wznowiona kompilacja powtarza się co najwyżej
> fragmenty w locie, które wersje subskrypcyjne unieszkodliwiają (D57–D63, D70). Pełna historia jest
> w [ARCHITECTURE.md, sekcja 8](../ARCHITECTURE.md) i [projekt M4](../design/M4-rebuild.md).

## Czego się nauczyłeś

- Rozpocznij odczyt modelu za pomocą `projections.start[P](name, version, actorName) { build => ... }`, i
  użyj zwróconego nadzorcy do zapytań i w `RegisterSubscribers`.
- Każda kompilacja ma własne tabele, nazwane w trakcie kompilacji (`build.documentStore(table)`).
- Podniesienie wersji powoduje utworzenie nowej wersji ze sklepu zdarzeń, podczas gdy stara wersja jest obsługiwana, a następnie
  przełącza się w jednej aktualizacji i usuwa stare tabele; `whileBuilding` mówi, co robi stara kompilacja
  Tymczasem.
- `RebuildProjection`, `GetBuilds` i `ToBuildInProgress` uruchamiają, monitorują i odblokowują kompilacje.
- `RebuildAggregates` naprawia jeden agregat, typ lub organizację na miejscu, za pośrednictwem Twojego
  `clearForRebuild`.
- Subskrybenci, których praca jest efektem ubocznym, pozostają bez zarządzania i nigdy nie zostaną odbudowani.

Dalej: [rozdział 11](11-subscribable-projections.md) przesyła bieżące aktualizacje z Twoich projekcji do
ekrany, na których je oglądamy.
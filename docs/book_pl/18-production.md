# Rozdział 18. Uruchamianie w środowisku produkcyjnym

Twoja aplikacja działa na laptopie i w klastrze testowym. Produkcja to zupełnie inna bajka:
Prawdziwy ruch, rzeczywiste przerwy w działaniu, rzeczywiste ulepszenia. Ten rozdział zawiera listę kontrolną, która pokazuje, jak
do monitorowania systemu podczas jego działania (metryki, logi i pytania o status), zmienia obciążenie biblioteki
pomiary przekształcają się w praktyczne porady, a kończy się listą rzeczy do zrobienia po przejściu na wersję 1.0.

---

## Lista kontrolna uruchomienia

Piloci przechodzą przez listę kontrolną przed każdym startem. Jest krótka, nudna i to właśnie ona jest powodem
Samoloty nie zapominają o podwoziu. Oto przykład dla węzła Twojej aplikacji, w kolejności
kroki są wykonywane. Rozdziały [2](02-setting-up.md) do [4](04-first-command.md) pokazują okablowanie
szczegół; to jest widok produkcyjny.

### 1. Najpierw skonfiguruj pulę połączeń

Biblioteka korzysta z domyślnej puli połączeń ScalikeJDBC i musi ona istnieć przed jakimkolwiek kodem biblioteki
biegi:

```scala
import scalikejdbc.{PołączeniowyPula, UstawieniaPołączeniowegoPula}

ConnectionPool.singleton("jdbc:postgresql://db.internal:5432/shop", "shop", sys.env("SHOP_DB_PASSWORD"),
  Ustawienia puli połączeń (rozmiar początkowy = 8, rozmiar maksymalny = 40, limit czasu połączenia w milionach (connectionTimeoutMillis) = 5000L))
```

Uczyń pulę **przynajmniej tak dużą, jak blokujący dyspozytor** (domyślnie 32 wątki, krok 4): co
właściciel i każde wywołanie bazy danych biblioteki jest uruchamiane na tym dyspozytorze i każde może posiadać połączenie.
Testy porównawcze biblioteki wykorzystują 40. Pamiętaj, że `max_connections` PostgreSQL musi obejmować pule
*wszystkich* twoich węzłów.

> **Ostrzeżenie:** Domyślnie ScalikeJDBC ma pulę 8 połączeń z 5-sekundowym oczekiwaniem. Pod obciążeniem,
> praca, która czeka dłużej na połączenie, kończy się niepowodzeniem, a osoba wywołująca polecenie otrzymuje
> `EventHandlingError`.

### 2. Zainicjuj schemat przy każdym uruchomieniu

Każdy komponent PostgreSQL ma `initSchema()`, który tworzy to, czego brakuje, i pozostawia resztę
sam, więc dzwonisz do nich na każdym starcie:

```scala
val mpjsons = nowy MPJsons
val typesNames = new PostgresTypesNamesState().initSchema()
val eventStore = nowy PostgresEventStoreState(mpjsons, typesNames).initSchema()
val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
val registry = new PostgresProjectionRegistry().initSchema()
val sagaState = new PostgresSagaState(mpjsons, typesNames)
sagaState.initSchema() // ta zwraca jednostkę, więc otrzymuje własną linię
```

Sekwencje identyfikatorów są tworzone przez generator identyfikatorów UID (`UidGeneratorActor.postgres(system)`), a każdy
Magazyn dokumentów tworzy własną tabelę.

Gdy kilka węzłów rozpoczyna pracę w tym samym momencie, nie dochodzi do ich wzajemnego zakłócania: każdy schemat
Inicjalizacja przebiega pod nadzorem blokady doradczej PostgreSQL, jeden proces na raz. Węzeł oczekuje do pięciu
minut na blokadę; po tym czasie jej uruchomienie kończy się niepowodzeniem z komunikatem: „Blokada schematu nie była wolna przez 5 minut:
„inny proces inicjuje schemat”.

> **Ostrzeżenie:** nie pomijaj `PostgresCommandResponseState.initSchema()`. Tworzy on unikalny indeks
> na którym opierają się polecenia idempotentne; bez niego każde polecenie idempotentne zakończy się niepowodzeniem.

Ciężkie zadania (duże indeksy, uzupełnianie) *nie* są częścią `initSchema()`. Są to oddzielne wywołania,
działają w tle (krok 7), więc start nigdy na nich nie czeka.

### 3. Ustaw numer instalacji

Łączne identyfikatory są unikalne w różnych instalacjach: numer instalacji *n* tworzy swoje identyfikatory z
*n* × 10^12 i dalej. Ustaw tę samą liczbę na każdym węźle instalacji:

```hocon
reactivecqrs.numer-instalacji = 3
```

Wartość domyślna, 0, to zakres poniżej 10^12, w którym obowiązuje każdy identyfikator utworzony przed wersją 1.0.
generator utworzony przez `UidGeneratorActor.postgres(system)` przesuwa sekwencję identyfikatorów agregatów do przodu
zakres (nigdy nie wraca) i odmawia rozpoczęcia, gdy sekwencja jest już poza zakresem: błąd
numer lub dwie instalacje współdzielące jedną bazę danych. Jeśli sam wybierzesz identyfikatory (`chosenAggregateId`,
[rozdział 5](05-command-flavors.md)), pobierz je z `ReserveAggregateId` lub poniżej 10^12; polecenie
bus odrzuca identyfikator w zakresie innej instalacji.

### 4. Określ rozmiar dyspozytorów

Biblioteka udostępnia w pliku `reference.conf` dwie własne pule wątków:

| Dyspozytor | Co na nim działa | Domyślne |
|---|---|---|
| `reactivecqrs.blocking-dispatcher` | agregacja właścicieli i całej pracy bazy danych biblioteki | 32 wątki |
| `reactivecqrs.handler-dispatcher` | twoje procedury obsługi poleceń | 32 wątki |

Nadpisz je w pliku `application.conf`:

```hocon
reaktywne cqrs {
  blocking-dispatcher.thread-pool-executor.fixed-pool-size = 40 # w pobliżu puli JDBC
  handler-dispatcher.thread-pool-executor.fixed-pool-size = 64 # programy obsługi oczekujące na wejście/wyjście
}
```

Dostosuj rozmiar dyspozytora blokującego do rozmiaru puli JDBC. Przydziel dyspozytorowi obsługi więcej wątków, gdy…
obsługujące czekają (na inną usługę, na plik) i mniej więcej tyle, ile masz rdzeni, gdy
obliczać.

Projekcje również wykonują pracę w bazie danych, dlatego należy je tworzyć w blokującym dyspozytorze:
`Props(...).withDispatcher(BlockingDispatcher.id)` (`io.reactivecqrs.core.util.BlockingDispatcher`).
Projekcja pozostawiona w domyślnym dyspozytorze rejestruje ostrzeżenie w momencie uruchomienia.

### 5. Uruchom jeden moduł sondowania skrzynki nadawczej i jeden moduł harmonogramowania squasha

Oba muszą zostać uruchomione **raz na system**: na pojedynczym węźle utwórz je raz; w klastrze uruchom je za pomocą
`Singletons.start` ([rozdział 17](17-cluster.md)):

```scala
import io.reactivecqrs.core.aggregate.SquashScheduler
import io.reactivecqrs.core.aggregate.SquashScheduler.SquashTarget
import io.reactivecqrs.core.cluster.Singletons
import io.reactivecqrs.core.delivery.OutboxPollerActor
importuj org.apache.pekko.actor.Props

// eventStore i eventBus są podłączone jak poprzednio; cartContext ma automatyczną regułę squash, carts jest jego magistralą poleceń
Singletons.start(system, "outboxPoller", Props(nowy OutboxPollerActor(eventStore, eventBus)))
Singletons.start(system, "squashScheduler",
  Rekwizyty (nowy SquashScheduler (eventStore, Seq (SquashTarget.of (cartContext, carts)))))
```

- **Sam **ankiet skrzynki nadawczej** ponownie dostarcza zdarzenia, których niektórzy subskrybenci nie potwierdzili.
  najpierw po 10 sekundach, potem co 30 sekund; prosi o ponowną publikację po 3 i 6 minutach
  zdarzenie oczekuje, zgłasza, że ​​zostało „niedostarczone” po 10 minutach i próbuje ponownie co 30 minut.
  Wydarzenia pozostałe przed ponownym uruchomieniem są publikowane przy pierwszym głosowaniu.
- **Harmonogram squasha** kompresuje agregaty kontekstów, które mają automatyczną regułę squasha
  (`SquashPolicy.automatic`, [rozdział 14](14-squash.md)); `SquashTarget.of` odrzuca kontekst
  bez niego. Najpierw uruchamia się po minucie, potem co godzinę, maksymalnie 100 agregatów na typ na godzinę.
  biegnij, jeden po drugim.

### 6. Zarejestruj każdego subskrybenta

Wyślij do magistrali zdarzeń każdego węzła listę wszystkiego, co nasłuchuje:
`eventBus ! RegisterSubscribers(Seq(cartItems, ...))`. Bus pyta każdego subskrybenta, czego chce i
nie publikuje niczego, dopóki wszyscy nie odpowiedzą; w tym czasie węzeł nie jest gotowy (Gotowość),
[rozdział 17](17-cluster.md)), a dzienniki magistrali „Magistrala zdarzeń czeka na subskrypcje: …” z
nazwiska zmarłych.

Jak długo trwa „w międzyczasie”? Pomiar z 17.10.2026 r. (`docs/perf/baseline-M5.md`, jedna maszyna): nowy
system z dwiema projekcjami odpowiedział na swoje pierwsze polecenie po 668 ms, a jego projekcje zobaczyły pierwsze
zdarzenie po 767 ms.

### 7. Po aktualizacji: praca online

Niektóre zadania są zbyt ciężkie, aby wykonywać je podczas startu węzła. Uruchamia się je raz, w tle, po
Wersja, która ich potrzebuje, jest już dostępna. Każdą z nich można bezpiecznie zatrzymać i uruchomić ponownie.

```scala
eventStore.buildIndexesConcurrently() // indeksy zbudowane bez blokowania zapisu
val spaces = eventStore.backfillSpaces() // SpaceBackfill(wypełniony, lewy)
val keys = eventStore.backfillEventKeys() // EventKeysBackfill(zapisane, brakujące)
```

- `buildIndexesConcurrently()` tworzy indeksy, których duża tabela nie może uzyskać podczas uruchamiania
  transakcja (`events (aggregate_id, version)` i `aggregates (space_id, id)`), z PostgreSQL
  `UTWÓRZ INDEKS RÓWNOLEGLE`. Nie robi nic, gdy istnieją.
- `backfillSpaces(spaceOf)` nadaje agregatom przechowywanym bez organizacji przestrzeń ich pierwszej
  zdarzenie; w przypadku starych pierwszych zdarzeń, które nie znają swojego, przekazujesz funkcję, która to robi
  ([rozdział 16](16-organizations.md)). Uruchom go przed odbudowaniem projekcji dla jednej organizacji.
- `backfillEventKeys()` nadaje każdemu starszemu zdarzeniu swój klucz (typ i przestrzeń jego agregatu), więc
  Kompilacja projekcji odczytuje tylko potrzebne zdarzenia. Uruchom ją raz *w każdym* węźle 1.0, a następnie
  Ponownie `buildIndexesConcurrently(). Od tego momentu nie wracaj do wcześniejszej wersji:
  wersje zapisują zdarzenia bez kluczy.

### 8. Przed aktualizacją: sprawdzenie zapisanych danych

Przed przeniesieniem bazy danych do nowej wersji sprawdź, czy nowa wersja jest w stanie odczytać wszystkie jej dane.
`StoredDataCheck` odczytuje każdą zapisaną wartość JSON (wydarzenia, zarchiwizowane zdarzenia, sagi, idempotentne odpowiedzi
i tabele projekcyjne, które wymienisz) i tylko odczytuje. Uruchom go z aplikacji, która ma wszystkie Twoje
klas na ścieżce klas, względem **kopii** produkcji:

```scala
import io.reactivecqrs.core.tools.StoredDataCheck

val report = StoredDataCheck.run(mpjsons, typesNames,
  Seq(StoredDataCheck.ProjectionTable[ClusterCartItems]("cluster_carts_v1")))
println(tekst raportu)
```

Tabela projekcji ma nazwę zgodną z nazwą magazynu dokumentów, bez prefiksu `projection_` (dla
zarządzana projekcja z sufiksem kompilacji, takim jak `_v1`). Raport zawiera listę, dla każdej tabeli i klasy,
wiersze odczytane i nieczytelne z przykładami, agregaty nadal bez spacji i
zdarzenia według wersji typu zdarzenia (w wersji 1.0 wszystkie muszą być równe 0). `report.unreadable` musi być równe zero.

Zanim usuniesz starą klasę zdarzeń ze swojego kodu, `StoredDataCheck.usersOf(className)` poinformuje Cię
które agregaty nadal posiadają wydarzenia z tego wydarzenia, w wydarzeniach i archiwum squasha.

## Oglądanie jak to działa

### Metryki

Biblioteka mierzy swoje działania i przekazuje Ci liczby. Implementujesz małą cechę za pomocą
trzy metody i podłącz je przed lub po uruchomieniu aktorów:

```scala
import io.reactivecqrs.core.metrics.{Metryki, ReactiveMetrics}

Klasa ForwardingMetrics rozszerza Metrics {
  nadpisz def count(nazwa: String, tagi: Mapa[String, String], ilość: Long): Jednostka =
    () // dodaj `amount` do licznika `name` swojego systemu metryk, za pomocą `tags`
  nadpisz def time(nazwa: String, tagi: Mapa[String, String], nanos: Long): Jednostka =
    () // zapisz `nanos` w swoim timerze `name`
  nadpisz def gauge(nazwa: String, tagi: Mapa[String, String], wartość: Long): Jednostka =
    () // ustaw swój wskaźnik `name` na `value`
}

ReactiveMetrics(system).use(nowe ForwardingMetrics)
```

Biblioteka często wywołuje je we własnych wątkach: Twoja implementacja musi być szybka i bezpieczna dla wątków,
i nigdy nie wolno rzucać. Dopóki nie wywołasz `use`, nic nie jest mierzone. Tagów jest niewiele i nigdy nie ma identyfikatorów, więc…
Są bezpieczne dla każdego systemu metrycznego. Co zyskujesz:

| Nazwa | Rodzaj | Tagi | Co to oznacza |
|---|---|---|---|
| `reactivecqrs.command` | time | `type`, `command`, `outcome` | ile czasu trwało każde polecenie i jak się zakończyło (klasa odpowiedzi lub `Timeout`) |
| `reactivecqrs.events.stored` | liczba | `typ` | zapisane zdarzenia |
| `reactivecqrs.events.stored.bytes` | liczba | `typ` | ich zapisane rozmiary |
| `reactivecqrs.restore` | time | `type`, `from` | właściciel odczytuje swój agregat ze `snapshot` lub z `events` |
| `reactivecqrs.bus.waiting` | wskaźnik | | dostawy autobusem w trakcie lotu |
| `reactivecqrs.bus.held` | gauge | | dostawy wstrzymane dla abonentów, którzy mają opóźnienia |
| `reactivecqrs.bus.behind` | gauge | `subscriber` | agregaty przechowywane dla każdego abonenta; 0, gdy jest na bieżąco |
| `reactivecqrs.projection.applied` | count | `projection` | wersje zastosowanej projekcji |
| `reactivecqrs.projection.failed` | count | `projection` | agreguje projekcję, która przestała działać ([rozdział 9](09-projection-failures.md)) |
| `reactivecqrs.outbox.waiting` | wskaźnik | | agreguje oczekujące w skrzynce nadawczej przy każdym pobieraniu |
| `reactivecqrs.outbox.given.up` | wskaźnik | | agreguje raporty sondy o niedostarczeniu |
| `reactivecqrs.outbox.republished` | liczba | | ponownych publikacji, o które prosił ankieter |
| `reactivecqrs.message.too.large` | count | `class` | wiadomości są zbyt duże, aby przesyłać je między węzłami ([rozdział 17](17-cluster.md)) |

> **Wskazówka:** trzy alerty rozwiązują większość problemów: `reactivecqrs.outbox.given.up` powyżej zera,
> `reactivecqrs.bus.behind` rośnie dla jednego subskrybenta, a `reactivecqrs.command` z
> `outcome=Timeout` lub `CommandHandlingError`.

> **Pod maską:** pełna lista, wraz z informacją o tym, kiedy wykonano każdy pomiar, znajduje się w
> [ARCHITECTURE.md, sekcja 14](../ARCHITECTURE.md). [Rozdział 19](19-testing.md) pokazuje
> `RecordingMetrics`, która przechowuje pomiary w pamięci na potrzeby testów.

### Pytania o status

Niektórzy aktorzy odpowiadają na pytanie o status, które możesz zadać w dowolnym momencie (ze strony administratora, punktu końcowego stanu zdrowia,
narzędzie wsparcia):

| Zapytaj | Kogo | Odpowiedz |
|---|---|---|
| `GetDeliveryStatus` | szyna zdarzeń każdego węzła | `DeliveryStatus`: dostawy w trakcie realizacji, ostatnia, która zakończyła się niedostarczeniem, zatrzymani abonenci, wstrzymane dostawy i agregaty wstrzymane dla każdego abonenta |
| `GetOutboxStatus` | moduł sondujący skrzynkę nadawczą | `OutboxStatus`: agreguje wiadomości oczekujące w skrzynce nadawczej oraz te, które zostały porzucone |
| `GetProjectionStatus` | każda projekcja | `ProjectionStatus`: nieudane agregaty, ponowne próby i przerwy ([rozdział 9](09-projection-failures.md)) |
| `GetBuilds` | każda zarządzana projekcja | `Builds`: aktywna kompilacja i ta w toku ([rozdział 10](10-rebuilding-projections.md)) |
| `GetSquashStatus` | harmonogram squasha | `SquashStatus`: zgnieciony, odrzucony, wciąż czekam |

W klastrze zapytaj sondę, harmonogram i prognozy za pośrednictwem ich serwerów proxy z dowolnego miejsca
węzeł; zapytaj magistralę każdego węzła osobno.

### Logi warte uwagi

| Linia logu zaczyna się od | Od | Co to znaczy |
|---|---|---|
| "Niedostarczone: ..." | sonda skrzynki nadawczej | zdarzenia agregatu czekały 10 minut na pewnego subskrybenta; są ponawiane co 30 minut |
| „Niedostarczone po…” lub „Przechowywane przez cały czas po…” | magistrala zdarzeń | dostawa osiągnęła 10-minutowy termin; wiersz skrzynki nadawczej pozostaje niezmieniony |
| "Subskrybent ... zatrzymany" | magistrala zdarzeń | abonent został zatrzymany; jego zdarzenia pozostają w skrzynce nadawczej do następnego uruchomienia |
| „Magistrala zdarzeń oczekuje na subskrypcje: …” | magistrala zdarzeń | na początku: zarejestrowany subskrybent nie odpowiedział; węzeł nie jest gotowy |
| „Dostawa czeka na...” i „...dostawy czekają dłużej niż...” | magistrala zdarzeń | abonent odpowiada powoli |
| "Subskrybenci, których wydarzenia odbywają się za ich oknem..." | autobus wydarzeń | projekcja jest w tyle; jej wydarzenia czekają w sklepie |
| "Polecenie obsługi błędów ..., identyfikator błędu: [...]" | właściciel agregatu | wywołujący otrzymał `CommandHandlingError` z tym identyfikatorem błędu |
| "Błąd powtarzania zdarzeń, identyfikator błędu: [...]" | właściciel agregatu | wywołujący otrzymał `EventHandlingError`; wiersz podaje nazwę przyczyny (zbyt wiele zdarzeń, zdarzenie za duże, błąd bazy danych) |
| "... bajtów jest zbyt dużych, aby można je było wysłać między węzłami" | serializator klastra | wiadomość nie mogła przekroczyć węzłów ([rozdział 17](17-cluster.md)) |
| „Bufor jest pełny” | Regiony fragmentów Pekko i pojedyncze serwery proxy | wiadomości zostały usunięte, gdy fragment lub pojedynczy obiekt został przeniesiony |

Identyfikator błędu w dwóch liniach właściciela to ten, który otrzymał dzwoniący: prośba o pomoc, która go cytuje
prowadzi bezpośrednio do śladu stosu.

### PostgreSQL

Uwaga, jak w przypadku każdego zajętego PostgreSQL: połączenia w użyciu (`pg_stat_activity`), automatyczne odkurzanie na gorącym
Tabele (`pg_stat_user_tables`) oraz rozmiary tabel i indeksów. Gorące tabele to `events_to_publish`
(skrzynka nadawcza: wstawianie i usuwanie na każde zdarzenie), `subskrypcje` (aktualizacja na każde zdarzenie na każdą projekcję)
i `aggregates` (aktualizacja na każde polecenie). Nadaj im niski współczynnik skalowania automatycznego odkurzania, aby uruchamiały się często.

## Przy dużym obciążeniu

Biblioteka nie ma globalnego hamulca. Zadania, które nie mogą jeszcze zostać wykonane, oczekują w kolejkach, z których większość jest nieograniczona.
i tylko trzy limity odpowiadają jednocześnie zamiast pozwolić pracy czekać: kolejka szeregowa, polecenie
limity czasu i limity na agregat. W tej sekcji dowiesz się, jak zachowuje się biblioteka, gdy
duże i co z tym zrobić.

**Jak czytać liczby.** *Zmierzone* liczby pochodzą z raportów w `docs/perf/`, każdy z nich jest pojedynczym
uruchomiono 17.10.2026 na jednej maszynie: Windows 10, 12 wątków procesora, OpenJDK 17 z pamięcią 2 GB, PostgreSQL
17.4 na tej samej maszynie i pula JDBC składająca się z 40 połączeń. Nadają się do porównywania, a nie do określania pojemności.
dane dotyczące sprzętu. *Szacunkowe* liczby są arytmetyczne i opierają się na wartościach zmierzonych, i nie zostały zmierzone.

### Mnóstwo poleceń naraz

Zmierzone (`load-M5`): 10 000 poleceń wysłanych na raz, brak subskrybentów.

| Cel | Wynik | Ostatnia odpowiedź po | Oceń |
|---|---|---|---|
| 10 000 nowych wózków (pierwsze polecenia) | wszystkie zakończone sukcesem | 2 342 ms | 4 269 poleceń/s |
| 10 000 istniejących wózków, po jednym poleceniu na każdy | wszystkie zakończone powodzeniem | 891 ms | 11 226 poleceń/s |
| jeden wózek, przetwarzanie współbieżne | wszystkie zakończone powodzeniem | 3 815 ms | 2 621 poleceń/s |
| jeden wózek, przetwarzanie szeregowe | 1 003 zakończonych powodzeniem, 8 997 `CommandRejected` naraz | 594 ms | |

W wielu agregatach akceptowane jest każde polecenie; oczekują one na wątki i połączenia, więc ostatnie
Odpowiedź pojawia się mniej więcej po „polecenia w locie ÷ wskaźnik zatwierdzania”. Szacunkowo na podstawie tego samego uruchomienia: około
15 KB sterty na każde polecenie w trakcie lotu, które tworzy agregat, oraz 8 KB na każde polecenie w istniejącym agregacie
(górne granice), więc 100 000 nowych kart naraz wymagałoby około 1,5 GB pamięci i odpowiadałoby na ostatnie
po około 23 sekundach; milion nie zmieściłby się w typowym stosie.

Co zrobić:

1. **Ogranicz polecenia w locie do krawędzi** swojej aplikacji, zanim dotrą do polecenia
   autobus: mniej więcej tyle, ile wynosi Twój współczynnik zatwierdzania razy akceptowalne opóźnienie. Na mierzonej maszynie,
   10 000/s × 0,5 s = 5 000 poleceń w trakcie realizacji (szacunkowo). Na resztę odpowiedz „zajęty” (HTTP 429 lub 503).
2. **Nadaj ponawianym poleceniom klucz idempotentności** ([rozdział 5](05-command-flavors.md)), aby ponowna próba
   po upływie limitu czasu naliczana jest jednokrotnie, nawet jeśli pierwsza próba została wykonana z opóźnieniem.
3. **Ustaw limit czasu klienta dłuższy niż limit czasu polecenia** (domyślnie 60 sekund) lub skróć go
   `commandTimeout` kontekstu, dzięki czemu odpowiedź biblioteki dociera zanim klient się podda.
4. **Użyj przetwarzania szeregowego dla agregatów, które wielu klientów zapisuje jednocześnie**: pełna kolejka to
   Odpowiadano natychmiast, a klienci mogli się wycofać. A jeszcze lepiej, rozdzielono tak gorący agregat.

### Góra wydarzeń

Jak wyglądałby magazyn 100 milionów zdarzeń, szacowany na podstawie zmierzonych liczb dla każdego zdarzenia (żadna z nich)
to było robione na taką skalę):

| | Oszacowanie | Od |
|---|---|---|
| tabela zdarzeń z indeksami | około 38 GB | 382 bajty na zdarzenie, mierzone notatkami o długości 200 znaków; skalowanie według JSON-a zdarzeń |
| zapisywanie ich przy pełnym obciążeniu, bez subskrybentów | około 2,5 godziny | 11 226 zdarzeń/s (`load-M5`) |
| jedna przebudowa projekcji opartej na zdarzeniach | około 4,1 godziny | 6 769 zdarzeń/s (`baseline-M4`) |
| opróżnienie listy oczekujących na wysłanie 1 miliona agregatów | około 8,3 godziny | 1000 agregatów na 30-sekundowe zapytanie |

Gdy zapisy przekraczają projekcję, magistrala wysyła maksymalnie **okno** 10 000 zdarzeń i 64 MB
jeszcze nie odpowiedział; reszta czeka w magazynie i jest wyciągana do projekcji, gdy nadrabia zaległości. Zmierzono
(`flow-M5` w porównaniu z `baseline-M4`): w 60-sekundowym zanurzeniu z udziałem 16 osób dzwoniących i dwóch projekcji, system
zatwierdzono 671 768 poleceń, a projekcje zostały dotrzymane 89 sekund po załadowaniu (przed oknem)
istniało: 475 403 poleceń, 201 sekund). W klastrze okno jest na magistralę węzła, więc przy trzech
węzły skrzynki pocztowej projekcji mogą pomieścić do trzech okien: rozmiar `DeliveryOptions.window` i
`windowBytes` (`new EventsBusActor(DeliveryOptions(...))`) określająca liczbę węzłów.

Co zrobić:

- **Uważaj na opóźnienie:** `bus.held` i `bus.behind`, `GetDeliveryStatus` i `GetProjectionStatus`.
- **Utrzymuj niskie prognozy na każde zdarzenie.** Odbiorca, który wywołuje powolną usługę zewnętrzną, należy do sagi
  lub własną projekcję. Pozostaw margines między szybkością polecenia a najwolniejszą projekcją.
- Zamiast tego **odbuduj projekcję, która jest znacznie w tyle** ([rozdział 10](10-rebuilding-projections.md))
  pozwalając mu na dogonienie skrzynki odbiorczej przy 1000 agregatach na ankietę; lub zwiększyć tempo ankietera
  podczas gdy odpływa:
  `OutboxPollerOptions(redelivery = RedeliveryPolicy.Options(maxRepublishPerPoll = 5000))`.
- **Zaplanuj okna przebudowy na podstawie szybkości:** około 6 800 zdarzeń/s na kompilację na mierzonej maszynie.
- **Dysk budżetowy z Twoich własnych zdarzeń:** ich rozmiar JSON plus około 190 bajtów na zdarzenie (szacunkowo).

### Agregaty zawierające dziesiątki tysięcy zdarzeń

Domyślnie agregat zatrzymuje się przy **10 000 zdarzeń**: polecenie, które przekroczyłoby limit, kończy się niepowodzeniem
`EventHandlingError` i nazwy logów `TooManyEventsException`. Limit chroni przed niekontrolowanym
agregaty; podnieś je celowo, dla całego sklepu:
`nowy PostgresEventStoreState(mpjsons, typesNames, aggregateVersionLimit = 50000)`.

Zmierzono (obciążenie `M5`, z usuniętym limitem): dla łącznej liczby 50 000 zdarzeń, przywrócenie przez nowego właściciela
zajęło 392 ms, przywrócenie z migawki 12 ms, a polecenie po przywróceniu 3 ms; całość
historia w jednej odpowiedzi (`GetEventsForAggregate`) zajęła 264 ms i 168 MB pamięci w momencie szczytowym.
`baseline-M4`, przywrócenie 500 000 zdarzeń zajęło 2 088 ms, a migawki 1 ms.

Długość nie spowalnia poleceń (właściciel przechowuje stan, a nie zdarzenia), ale spowalnia każde polecenie.
Przywracanie: po pasywacji, przekazaniu i restarcie. Szacunkowo: węzeł może przywrócić maksymalnie około 80
agregaty 50 000 zdarzeń na sekundę bez migawek i około 2 700 z nimi.

Co zrobić:

- **Zgodnie z projektem agregaty powinny być krótkie:** należy je dzielić naturalną granicą, np. kropką lub dokumentem
  wersja.
- **Włącz migawki** ([rozdział 13](13-snapshots.md)) dla każdego typu, którego agregaty przekraczają
  kilka tysięcy wydarzeń.
- **Długie historie squasha** ([rozdział 14](14-squash.md)), na żądanie lub automatycznie.
- **Nie pytaj o całą historię długiego agregatu:** `GetEventsForAggregateForVersion` odczytuje
  do wersji.

### Polecenia i zdarzenia o rozmiarze kilku megabajtów

Domyślnie zdarzenie może przyjąć **100 000 znaków JSON**; przy większej liczbie polecenie nie zostanie wykonane.
`EventHandlingError` i nazwy logów `EventTooLargeException`. Możesz zwiększyć limit dla wybranych
tylko klasy, z `eventSizeLimitPerClassName` (mapą z nazwy klasy na znaki).

Zmierzono (`duże-M5`), każde polecenie zawiera dokument składający się z 5 000 000 losowych znaków (4,8 MB) w jednym
wydarzenie:

| | Zmierzone |
|---|---|
| 20 poleceń jedno po drugim, jedno zbiorcze | mediana 90 ms, najwolniejsze 164 ms; 31,8 MB/s dokumentów |
| 20 poleceń na raz, każde na osobnej zbiorze | ostatnia odpowiedź po 890 ms; stos 117 → 358 MB |
| dysk na zdarzenie (tabela, indeksy, TOAST) | 5,0 MB |
| przywrócenie agregatu 20 takich zdarzeń | 517 ms; stos 117 → 323 MB |
| dostarczenie jednego zdarzenia zserializowane dla innego węzła | 4,8 MB przy limicie 248 KiB |

Szacunkowo: około 12 MB pamięci na jedno takie polecenie w trakcie realizacji, więc 100 poleceń na raz zajmuje około 1,2 GB.

Co zrobić:

- **Trzymaj dokumenty z dala od zdarzeń i poleceń** (wzorzec claim-check): najpierw zapisz plik,
  magazyn obiektów lub osobną tabelę, a następnie wysłać polecenie nadające mu nazwę (identyfikator, skrót, rozmiar).
  Wydarzenia pozostają niewielkie, podobnie jak przywracanie, dostarczanie, odbudowywanie i przesyłanie wiadomości między węzłami.
- **Jeśli dokumenty muszą być zdarzeniami**, utrzymuj limit każdej klasy na jak najniższym poziomie, a także:
    - włącz migawki i trzymaj dokument poza stanem;
    - obniż `fetchSize` magazynu zdarzeń (domyślnie 1000 wierszy, na przykład 10) lub ustaw
      Sterownik PostgreSQL `adaptiveFetch=true` z `maxResultBuffer`, dzięki czemu odczyty pobierają kilka wierszy na raz
      czas;
    - obniż `pageSize` i `chunkEvents` (`ProjectionsSettings(..., builder =) konstruktora
    Ustawienia konstruktora(...))`);
    - ograniczono *bajty* poleceń w locie na krawędzi, a nie tylko ich liczbę (na przykład przesyłanie
      naraz × największy dokument ≤ jedna czwarta sterty);
    - w klastrze zwiększ rozmiar ramki powyżej największego zdarzenia ([rozdział 17](17-cluster.md)).

```scala
val eventStore = nowy PostgresEventStoreState(mpjsons, typesNames,
  Rozmiar pobrania = 10,
  eventSizeLimitPerClassName = Map(classOf[ReceiptAttached].getName -> 6000000) // własne zdarzenie
).initSchema()
```

### Obsługujące polecenia, które działają przez kilka minut

Obsługujące polecenia są uruchamiane w dyspozytorze obsługi, domyślnie 32 wątki. Zmierzone (`heavy-M5`), z
osoby obsługujące 5 sekund zastępują osoby obsługujące minuty:

| Sprawa | Wynik |
|---|---|
| 36 naraz, każde trzymając swój wątek | wszystkie zakończyły się sukcesem, ostatnie po 10,0 s (32 na raz); w międzyczasie magistrala zdarzeń odpowiedziała w ciągu 8 ms, ale lekkie polecenie w innym agregacie czekało 5,4 s |
| 36 naraz, praca na puli własnej aplikacji (`AsyncCommandResult`) | ostatnie po 5,0 s; magistrala odpowiedziała w 0 ms, lekkie polecenie w 3 ms |
| jeden, w tym samym czasie zmieniły się polecenia dotyczące światła (przetwarzanie współbieżne) | zakończyło się powodzeniem po 10,2 s: raz wywołało konflikt i zostało wykonane ponownie; 25 poleceń dotyczących światła czekało do 4,8 s |
| jeden dłuższy niż limit czasu | `CommandHandlingError` w momencie przekroczenia limitu czasu i nic nie jest zapisane |

Tak więc 5-minutowy moduł obsługi z domyślnym 60-sekundowym `commandTimeout` zawsze kończy się
`CommandHandlingError` i jego zdarzenia nigdy nie są zapisywane.

Co zrobić:

- **Przenieś długą pracę poza polecenie.** Polecenie rejestruje żądanie (jedno zdarzenie); sagę lub
  pracownik wykonuje pracę na własnych wątkach, ponawianych i wznawianych; drugie polecenie rejestruje
  wynik. Całość pozostaje dostępna, a żadne przekroczenie limitu czasu, przekazanie ani konflikt nie wpływają na pracę długoterminową.
- **Jeśli polecenie musi to zrobić:**
    - zwróć `AsyncCommandResult` z pracą na własnym ograniczonym puli wątków;
    - podnieś `commandTimeout` tego kontekstu ponad najdłuższy czas trwania;
    - użyj przetwarzania szeregowego dla tego typu, dzięki czemu praca nigdy nie zostanie uruchomiona ponownie po konflikcie;
    - podaj klientom klucz idempotencji i odpowiednio długi limit czasu lub odpowiedz „zaakceptowano” i zgłoś
      wynik później poprzez projekcję;
    - w przypadku typu „Sharded” podnieś limity czasu przekazania i okres karencji menedżera procesu powyżej
      przekroczenie limitu czasu polecenia ([rozdział 17](17-cluster.md)).

> **Pod maską:** każda liczba powyżej, jej obliczenia i uzasadnienie
> zalecenie znajduje się w [SCALING.md](../SCALING.md); surowe raporty znajdują się w `docs/perf/`.

## Po uaktualnieniu do wersji 1.0

Jeżeli przechodzisz z wersji 0.12, przejście do wersji 1.0 jest **jednorazowym, zaplanowanym przystankiem**: węzeł 0.12 nie może dołączyć do węzła
Klaster 1.0. Uaktualnienia ciągłe ([rozdział 17](17-cluster.md)) zaczynają się od wersji 1.0. Instrukcja krok po kroku
Podręcznik migracji, przetestowany na kopii produkcyjnej, pojawi się wraz z wersją 1.0. Do tego czasu, oto
kroki kamieni milowych biblioteki, o które prosi gospodarz, w kolejności, w jakiej je spełnia.

**Przed zatrzymaniem, na kopii produkcji:**

1. Uruchom `StoredDataCheck` (krok 8 listy kontrolnej): każdy wiersz musi zostać odczytany, a każde zdarzenie musi być w
   typ zdarzenia wersja 0.
2. Poszukaj kodu, który porównuje pola `Option` zdarzeń z `null`: z biblioteką JSON 1.0 i
   jeśli pole `Opcja` nie istnieje, wyświetla się `Brak`.
3. Nadaj każdemu poleceniu wysyłanemu przez sagę klucz idempotencji, jego `SagaStep` ([rozdział 12](12-sagas.md)),
   więc saga wznowiona po wypadku nigdy nie stosuje kroku dwa razy.

**W Twoim kodzie:**

4. Zniknęło wersjonowanie zdarzeń: gdy trzeba zmienić kształt zdarzenia, należy dodać nową klasę (np.
   `ItemAddedV2`) i zachowaj starą ([rozdział 6](06-events.md)).
5. Każde pierwsze zdarzenie nadaje nazwę przestrzeni organizacji, „SpaceId(0)” dla platformy; nowy agregat
   którego pierwsze zdarzenie powoduje `SpaceId.unknown` zostaje odrzucone ([rozdział 16](16-organizations.md)).
6. Klasa zdarzeń, która jest generyczna w swoim korzeniu zbiorczym, przyjmuje `ClassTag` (`[R: ClassTag]`); typ
   klucz, który budujesz ręcznie to `AggregateType.of[R]`.
7. Projekcje agregatów wykorzystujące funkcję cofania muszą obsługiwać zdarzenia cofania: przebudowa zapewnia ich dostarczenie.
8. Klasy, które krzyżują węzły, podlegają czterem zasadom [rozdziału 17](17-cluster.md) (dodano pola
   później są `Opcjami`).

**W okablowaniu:**

9. Wywołuj `initSchema()` na każdym komponencie PostgreSQL przy każdym uruchomieniu (krok 2), odpowiedź polecenia
   stan i stan subskrypcji zawarte; ten ostatni tworzy tabelę nieudanych subskrypcji.
10. Wyślij do każdej magistrali zdarzeń „RegisterSubscribers” z każdą projekcją i nasłuchiwaczem i uruchom jeden
    `OutboxPollerActor` na system.
11. Utwórz projekcje dla `BlockingDispatcher.id` i określ rozmiar dyspozytorów (krok 4). Sekwencje identyfikatorów
    krok po 10 000 od pierwszego uruchomienia; biblioteka ustala to sama.
12. Rozpocznij każdy odczytany model za pomocą `Projections.start` ([rozdział 10](10-rebuilding-projections.md)):
    użyj `ActorRef`, który zwraca w przypadku zapytań i `RegisterSubscribers`, nazwij jego tabele za pomocą
    `build.documentStore(...)`, przydziel każdej kompilacji osobne pamięci podręczne, nigdy nie zatwierdzaj ani nie wycofuj
    sesja jest przekazywana słuchaczowi, a następnie usuwane są projekcje za pomocą `Projections.retire(name)`.
    Projekcje z efektami ubocznymi pozostają niezarządzane.
13. Użyj ustawień klastra na każdym węźle, także pojedynczym; uruchom program sondujący i harmonogram squasha
    i biegacze sagi za pomocą `Singletons.start`; rejestrują proxy projekcji na każdym autobusie
    ([rozdział 17](17-cluster.md)).
14. Ustaw `reactivecqrs.installation-number` (krok 3), utwórz generator uid za pomocą
    `UidGeneratorActor.postgres(system)` i sprawdź, czy żaden Twój kod nie odczytuje id jako `Int`.
15. Ustaw `pekko.cluster.app-version` dla każdej wersji i dodaj `ReadinessCheck` do kontroli kondycji.
16. Podłącz swoje metryki (`ReactiveMetrics(system).use(...)`), ustaw pulę JDBC na co najmniej 32
    połączenia, powiązanie poleceń w locie i dostrojenie automatycznego podciśnienia do gorących tabel.

**Po uruchomieniu nowej wersji w tle:**

17. `buildIndexesConcurrently()`.
18. `backfillSpaces(...)`, zanim odbudujesz jakąkolwiek projekcję dla jednej organizacji.
19. Gdy każdy węzeł uruchomi 1.0: `backfillEventKeys()`, a następnie ponownie `buildIndexesConcurrently()`.

**Nowości, na które możesz się zdecydować, jeden kontekst na raz, kiedy tylko chcesz:** migawki
([rozdział 13](13-snapshots.md)), przetwarzanie poleceń szeregowych ([rozdział 5](05-command-flavors.md)),
squash i jego archiwum audytu ([rozdział 14](14-squash.md)), koperta metadanych dotycząca zdarzeń
(`WithMetadata`, [rozdział 6](06-events.md)), wiersze własne zapisane ze zdarzeniami
(`WithOutboxWriter`) i rozmieszczenie `Sharded` ([rozdział 17](17-cluster.md)).

> **Pod maską:** te kroki dotyczą wierszy hosta H1 do H11
> [KNOWN-ISSUES.md, sekcja 4](../KNOWN-ISSUES.md), a plan zatrzymania i migracji to decyzja D81.

## Czego się nauczyłeś

- Węzeł uruchamia się w ustalonej kolejności: pula połączeń, schemat (bezpieczny przy każdym uruchomieniu, jeden węzeł na raz)
  czas), numer instalacji, dyspozytorzy o odpowiednich rozmiarach, jeden moduł sondujący i jeden harmonogram na system oraz
  abonentów autobusu. Ciężkie zadania są wykonywane oddzielnie, w tle.
- `StoredDataCheck` przed uaktualnieniem informuje, czy nowa wersja odczytała wszystkie zapisane dane.
- `ReactiveMetrics(system).use(...)` przekazuje Ci pomiary biblioteki, pytania o status i krótką odpowiedź
  lista wierszy dziennika informuje, kiedy coś czeka lub nie działa.
- Pod obciążeniem: ograniczone polecenia na krawędzi, utrzymywanie tanich projekcji, utrzymywanie krótkich agregatów (migawki)
  i squash help), trzymaj dokumenty z dala od zdarzeń i przenieś długie prace z poleceń.
- Każda liczba w tym rozdziale podaje źródło swojego pochodzenia: zmierzona raz na jednej maszynie lub oszacowana.
- Przejście z wersji 0.12 na 1.0 to planowany przystanek z krótką listą kroków dotyczących kodu, okablowania i danych.

Dalej: [rozdział 19](19-testing.md) pokazuje, jak testować wszystko, od pojedynczego programu obsługi zdarzeń do
klaster w jednej maszynie wirtualnej Java.
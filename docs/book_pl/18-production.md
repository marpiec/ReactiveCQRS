# Rozdział 18. Na produkcji

Twoja aplikacja działa na laptopie i w klastrze testowym. Produkcja to jednak zupełnie inna bajka:
prawdziwy ruch, prawdziwe awarie, prawdziwe aktualizacje. Ten rozdział daje ci listę kontrolną startu,
pokazuje, jak obserwować działający system (metryki, logi i pytania o stan), przekłada pomiary
biblioteki pod obciążeniem na praktyczne rady, a kończy się listą rzeczy do zrobienia przy przejściu
na 1.0.

---

## Lista kontrolna startu

Piloci przed każdym startem przechodzą przez listę kontrolną. Jest krótka, jest nudna i to dzięki niej
samoloty nie zapominają o podwoziu. Oto taka lista dla węzła twojej aplikacji, w kolejności, w jakiej
wykonują się kroki. Rozdziały od [2](02-setting-up.md) do [4](04-first-command.md) pokazały
szczegółowo, jak złożyć system; tutaj patrzymy na to samo z perspektywy produkcji.

### 1. Najpierw ustaw pulę połączeń

Biblioteka korzysta z domyślnej puli połączeń ScalikeJDBC, a ta musi istnieć, zanim ruszy jakikolwiek
kod biblioteki:

```scala
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

ConnectionPool.singleton("jdbc:postgresql://db.internal:5432/shop?tcpKeepAlive=true&socketTimeout=600&logServerErrorDetail=false",
  "shop", sys.env("SHOP_DB_PASSWORD"),
  ConnectionPoolSettings(initialSize = 8, maxSize = 40, connectionTimeoutMillis = 5000L, validationQuery = "SELECT 1"))
```

Ustaw pulę **co najmniej tak dużą jak dispatcher blokujący** (domyślnie 32 wątki, krok 4): każdy
właściciel i każde wywołanie bazy danych w bibliotece działa na tym dispatcherze i każde może trzymać
połączenie. Benchmarki biblioteki używają 40. Pamiętaj też, że `max_connections` w PostgreSQL musi
wystarczyć na pule *wszystkich* twoich węzłów.

> **Ostrzeżenie:** ScalikeJDBC domyślnie tworzy pulę 8 połączeń i czeka na wolne połączenie 5 sekund.
> Pod obciążeniem praca, która czeka dłużej, kończy się błędem, a nadawca polecenia dostaje
> `EventHandlingError`. Węzeł, którego pula jest mniejsza niż jego dispatcher blokujący, zapisuje przy
> starcie ostrzeżenie w logu.

Reszta tego kodu chroni przed bazą danych, która przestaje odpowiadać: po przełączeniu awaryjnym
połączenie może czekać na odpowiedź, która nigdy nie przyjdzie, i na zawsze trzymać wątek biblioteki.

- `socketTimeout=600`: odczyt, który przez 10 minut nie dostaje odpowiedzi, kończy się błędem. Ustaw go
  powyżej najdłuższego polecenia, jakie wykonują twoje węzły, a długie zadania z kroku 7 (budowę
  indeksów, uzupełnianie danych) uruchamiaj z procesu bez tego ustawienia.
- `tcpKeepAlive=true`: system operacyjny zauważa połączenie, którego drugi koniec zniknął.
- `validationQuery`: połączenie jest sprawdzane, zanim zostanie wydane, więc po restarcie bazy martwe
  połączenia są wymieniane, zamiast każde psuć jeden kawałek pracy. Kosztuje to jedną wymianę z bazą
  przy każdym pobraniu połączenia.
- `logServerErrorDetail=false`: błędy sterownika nie cytują wierszy (zobacz niżej „Dane osobowe a
  logi”).

W bazie danych `ALTER ROLE shop SET idle_in_transaction_session_timeout = '10min';` kończy sesje, które
zawieszony albo martwy proces zostawił otwarte w transakcji. Także singletony samej biblioteki nie czekają
w nieskończoność: poller outboxu i scheduler squasha porzucają odczyt, który trwa dłużej niż pięć minut
(`readTimeout` w ich opcjach), zapisują błąd w logu i czytają od nowa.

### 2. Inicjalizuj schemat przy każdym starcie

Każdy komponent PostgreSQL ma metodę `initSchema()`, która tworzy to, czego brakuje, a resztę zostawia
w spokoju, więc wywołujesz je przy każdym starcie:

<!-- snippet: cart/ProductionSpec.scala#tables -->
```scala
val mpjsons = new MPJsons
val typesNames = new PostgresTypesNamesState().initSchema()
val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()
val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
val registry = new PostgresProjectionRegistry().initSchema()
val sagaState = new PostgresSagaState(mpjsons, typesNames)
sagaState.initSchema() // this one returns Unit, so it gets a line of its own
```

Sekwencje identyfikatorów tworzy generator identyfikatorów (`UidGeneratorActor.postgres(system)`), a
każdy magazyn dokumentów tworzy własną tabelę.

Kiedy kilka węzłów startuje w tej samej chwili, nie wchodzą sobie w drogę: każda inicjalizacja
schematu działa pod blokadą doradczą PostgreSQL (advisory lock), jeden proces naraz. Węzeł czeka na
blokadę do pięciu minut; potem jego start kończy się błędem
„The schema lock was not free for 5 minutes: another process initializes the schema”.

Start nie wchodzi też w drogę długim transakcjom, takim jak kopia zapasowa albo eksport organizacji.
`initSchema()` najpierw czyta katalog i dotyka tabeli tylko wtedy, gdy czegoś w niej brakuje, więc
zwykły start nie zakłada żadnej blokady tabeli. Gdy musi zmienić tabelę (na przykład przy pierwszym
starcie nowej wersji), a długa transakcja ją trzyma, czeka na blokadę najwyżej dwie sekundy naraz, w
sumie około minuty. Potem start kończy się błędem „The start could not finish ...: it waited ... for
the table's lock, which a long transaction holds”. Znajdź tę transakcję w `pg_stat_activity` i uruchom
węzeł ponownie, gdy się zakończy. W tym czasie wszystkie inne węzły pracują dalej.

> **Ostrzeżenie:** nie pomijaj `PostgresCommandResponseState.initSchema()`. Tworzy unikalny indeks, na
> którym opierają się polecenia idempotentne; bez niego każde polecenie idempotentne kończy się błędem.

Ciężkie zadania (duże indeksy, uzupełnianie starych danych) *nie* są częścią `initSchema()`. To osobne
wywołania, które uruchamiasz w tle (krok 7), więc start nigdy na nie nie czeka.

### 3. Ustaw numer instalacji

Identyfikatory agregatów są unikalne między instalacjami: instalacja numer *n* tworzy identyfikatory
od *n* × 10^12 w górę. Ustaw ten sam numer na każdym węźle instalacji:

```hocon
reactivecqrs.installation-number = 3
```

Wartość domyślna, 0, to dawny zakres poniżej 10^12, w którym są wszystkie identyfikatory nadane przed
1.0. Generator identyfikatorów utworzony przez `UidGeneratorActor.postgres(system)` przesuwa sekwencję
identyfikatorów agregatów do przodu, w ten zakres (nigdy wstecz), i odmawia startu, gdy sekwencja
wyszła już poza zakres: to znak złego numeru albo dwóch instalacji na jednej bazie danych. Jeśli to ty
wybierasz identyfikatory (`chosenAggregateId`, [rozdział 5](05-command-flavors.md)), bierz je z
`ReserveAggregateId` albo spod 10^12; szyna poleceń odrzuca identyfikator z zakresu innej instalacji.

### 4. Dobierz rozmiar dispatcherów

Biblioteka ma dwie własne pule wątków, zdefiniowane w jej `reference.conf`:

| Dispatcher | Co na nim działa | Domyślnie |
|---|---|---|
| `reactivecqrs.blocking-dispatcher` | właściciele agregatów i cała praca biblioteki z bazą danych | 32 wątki |
| `reactivecqrs.handler-dispatcher` | twoje handlery poleceń | 32 wątki |

Nadpisz je w swoim `application.conf`:

```hocon
reactivecqrs {
  blocking-dispatcher.thread-pool-executor.fixed-pool-size = 40   # near the JDBC pool
  handler-dispatcher.thread-pool-executor.fixed-pool-size = 64    # handlers that wait for I/O
}
```

Dispatcher blokujący ustaw na rozmiar zbliżony do puli JDBC. Dispatcherowi handlerów daj więcej
wątków, gdy twoje handlery czekają (na inną usługę, na plik), a mniej więcej tyle, ile masz rdzeni, gdy
liczą.

Projekcje też pracują z bazą danych, więc twórz je na dispatcherze blokującym:
`Props(...).withDispatcher(BlockingDispatcher.id)` (`io.reactivecqrs.core.util.BlockingDispatcher`).
Projekcja pozostawiona na domyślnym dispatcherze zapisuje przy starcie ostrzeżenie w logu.

### 5. Uruchom jeden poller outboxu i jeden scheduler squasha

Oba muszą działać **raz na cały system**: na pojedynczym węźle utwórz je raz, a w klastrze uruchom je
przez `Singletons.start` ([rozdział 17](17-cluster.md)):

<!-- snippet: cart/ProductionSpec.scala#singletons -->
```scala
import io.reactivecqrs.core.aggregate.SquashScheduler
import io.reactivecqrs.core.aggregate.SquashScheduler.SquashTarget
import io.reactivecqrs.core.cluster.Singletons
import io.reactivecqrs.core.delivery.OutboxPollerActor
import org.apache.pekko.actor.Props

// eventStore and eventBus as wired before; cartContext has an automatic squash rule, carts is its command bus
val outboxPoller = Singletons.start(system, "outboxPoller", Props(new OutboxPollerActor(eventStore, eventBus)))
val squashScheduler = Singletons.start(system, "squashScheduler",
  Props(new SquashScheduler(eventStore, Seq(SquashTarget.of(cartContext, carts)))))
```

- **Poller outboxu** jako jedyny dostarcza ponownie zdarzenia, których któryś subskrybent nie
  potwierdził. Pierwszy raz przegląda outbox po 10 sekundach, potem co 30 sekund; o ponowną publikację
  prosi po 3 i po 6 minutach czekania zdarzenia, po 10 minutach zgłasza je jako „niedostarczone”, a
  potem próbuje ponownie co 30 minut. Zdarzenia, które zostały sprzed restartu, wychodzą przy pierwszym
  przeglądzie.
- **Scheduler squasha** zwija historię agregatów tych kontekstów, które mają regułę automatycznego
  squasha (`SquashPolicy.automatic`, [rozdział 14](14-squash.md)); `SquashTarget.of` odrzuca kontekst
  bez takiej reguły. Pierwszy raz rusza po minucie, potem co godzinę, najwyżej 100 agregatów każdego
  typu na przebieg, po jednym naraz.

### 6. Zarejestruj wszystkich subskrybentów

Wyślij szynie zdarzeń każdego węzła listę wszystkiego, co słucha:
`eventBus ! RegisterSubscribers(Seq(cartItems, ...))`. Szyna pyta każdego subskrybenta, czego chce, i
nic nie publikuje, dopóki wszyscy nie odpowiedzą; w tym czasie węzeł nie jest gotowy (`Readiness`,
[rozdział 17](17-cluster.md)), a szyna zapisuje w logu
„Event bus waits for the subscriptions of: ...” z nazwami spóźnialskich.

Wysyłaj jedną listę. Drugie `RegisterSubscribers` wysłane, zanim szyna wystartuje, dopisuje się do
pierwszego. Takie, które przyjdzie po starcie, też rejestruje swoich subskrybentów, ale omija ich to,
co opublikowano, zanim odpowiedzieli, a szyna ostrzega:
„Event bus registered ... after its start”.
Nową projekcję, która musi zobaczyć całą historię, uruchamiasz przez `Projections.start`
([rozdział 10](10-rebuilding-projections.md)), które buduje ją z zapisanych zdarzeń. Subskrybent,
który na pytanie szyny odpowiada błędem (projekcja zarządzana, której rejestr jest niedostępny),
zostaje wymieniony z nazwy w ostrzeżeniu i jest pytany ponownie przy każdym takcie zegara szyny.

Subskrybent, który nigdy nie odpowiada, nie blokuje węzła na zawsze. Po `registrationTimeout` (2 minuty)
szyna startuje bez niego i loguje „Event bus starts without the subscriptions of: ...”; ten, który
zatrzyma się, zanim odpowie, zostaje pominięty od razu („... stopped before it answered: it never
subscribed”). Dopóki każdy zarejestrowany subskrybent nie odpowie, żadne zdarzenie nie opuszcza
outboxu; gdy odpowiedzą wszyscy, właściciele publikują ponownie to, co zachowali. Taki, który nigdy
nie wróci, sprawia, że outbox rośnie: usuń go przez `RemoveSubscriber` albo zrestartuj węzeł.

Jak długo trwa to „w tym czasie”? Zmierzone 17/10/2026 (`docs/perf/baseline-M5.md`, jedna maszyna):
nowy system z dwiema projekcjami odpowiedział na pierwsze polecenie po 668 ms, a jego projekcje
zobaczyły pierwsze zdarzenie po 767 ms.

### 7. Po aktualizacji: zadania online

Niektóre zadania są zbyt ciężkie, żeby wykonywać je przy starcie węzła: duże indeksy i uzupełnienia
przechodzące przez wszystkie zdarzenia. Wykonują się raz, w tle, gdy wersja, która ich potrzebuje, już
działa. Najprościej pozwolić aplikacji uruchomić je samej, a pełnym budowom projekcji kazać na nie
poczekać:

```scala
import io.reactivecqrs.core.jobs.{OnlineJobs, OnlineJobsSettings}
import io.reactivecqrs.core.rebuild.BuildHold

val onlineJobs = OnlineJobs.start(system, OnlineJobsSettings(eventStore, spaceOf = spaceOfOldAggregates))
val projections = new Projections(system, ProjectionsSettings(eventStore, subscriptions, registry,
  contexts, holdBuilds = Some(BuildHold.untilStoreReady(eventStore))))
```

`OnlineJobs.start` (na każdym węźle; w klastrze to singleton) wykonuje trzy zadania, jedno po drugim:

1. **`indexes`**: duże indeksy magazynu zdarzeń i indeks każdej tabeli dokumentów według organizacji;
2. **`event-keys`**: klucz każdego starszego zdarzenia, gdy żaden węzeł klastra nie działa już na
   starszej wersji (czeka, dopóki taki działa: zapisywałby zdarzenia bez kluczy);
3. **`spaces`**: organizację agregatów zapisanych, zanim ją miały, z twoim `spaceOf`
   ([rozdział 16](16-organizations.md)).

Każde idzie w tempie opisanym niżej i każde zapisuje postęp w tabeli `online_jobs`: węzeł, który
zatrzyma się w połowie, zostawia pracę następnemu, a ten pomija to, co skończone, i kontynuuje klucze
zdarzeń od ostatniej partii. Naraz pracuje tylko jeden (trzyma blokadę doradczą PostgreSQL), a jego
budowy indeksów idą osobnym połączeniem, którego limit czasu to `longStatementTimeout` (6 godzin), a
nie `socketTimeout` twojej puli, który przerwałby długie `CREATE INDEX CONCURRENTLY`. Zapytaj, jak mu
idzie:

```scala
import io.reactivecqrs.core.jobs.OnlineJobs.{GetOnlineJobsStatus, RetryOnlineJobs}

onlineJobs ? GetOnlineJobsStatus  // OnlineJobsStatus(jobs, buildHoldReleased, readiness)
onlineJobs ? RetryOnlineJobs      // runs the failed jobs again
eventStore.releaseBuildHold()     // the override: held builds start, ready or not
```

Stan (`state`) każdego zadania to `OnlineJobState.Waiting`, `Running`, `Done` albo `Failed`, z licznikami (zdarzenia z kluczem,
pozostałe zdarzenia, uzupełnione i pozostałe przestrzenie) i, przy kluczach zdarzeń, ze spotkanymi
zdarzeniami-sierotami. Ze wstrzymaniem pełne budowy ruszają, gdy indeksy i klucze są gotowe
([rozdział 10](10-rebuilding-projections.md)). Zadanie zakończone porażką trzyma je dalej, a
`Builds.heldBy` podaje przyczynę; usuń ją i wyślij `RetryOnlineJobs` (zadanie zakończone porażką rusza
też ponownie przy następnym starcie) albo, jeśli budowy mają ruszyć mimo to, wywołaj
`eventStore.releaseBuildHold()`, które zwalnia je na każdym węźle, na stałe. Śledzą to metryki
`reactivecqrs.online.job` (0 czeka, 1 działa, 2 gotowe, 3 porażka) i `reactivecqrs.online.job.progress`,
według `job`, a każda zmiana stanu zadania to jedna linia w logu.

Możesz też uruchomić zadania sam, z zadania serwisowego:

<!-- snippet: cart/ProductionSpec.scala#maintenance -->
```scala
eventStore.buildIndexesConcurrently()          // indexes built without blocking writes
val spaces = eventStore.backfillSpaces()       // SpaceBackfill(filled, left)
val keys = eventStore.backfillEventKeys()      // EventKeysBackfill(written, missing, orphans)
```

- `buildIndexesConcurrently()` buduje indeksy, których duża tabela nie może dostać w transakcji przy
  starcie (`events (aggregate_id, version)`, `aggregates (space_id, id)`, a od ostatnich kroków 1.0
  także indeks własnych wierszy agregatów według typu), za pomocą `CREATE INDEX CONCURRENTLY` z
  PostgreSQL. Gdy już istnieją, nic nie robi. Dopóki nie powstanie ostatni z nich, budowa projekcji w
  dużej instalacji czyta stronę agregatów w około 100 ms zamiast 1 ms. Od 1.0.1 indeks na
  `events (aggregate_id, version)` jest unikalny, więc żaden agregat nie dostanie dwóch zdarzeń o jednej
  wersji: uruchom go ponownie po aktualizacji do 1.0.1. Jeśli któryś agregat ma już taką parę, zostaje
  zwykły indeks, w logu pojawia się błąd, a `StoredDataCheck` wymienia te pary (`repeatedVersions`).
- `backfillSpaces(spaceOf)` nadaje agregatom zapisanym bez organizacji przestrzeń z ich pierwszego
  zdarzenia; dla starych pierwszych zdarzeń, które swojej przestrzeni nie znają, przekazujesz funkcję,
  która ją zna ([rozdział 16](16-organizations.md)). Zrób to po pierwszym starcie nowej wersji (na
  schemacie 0.12 niczego nie zrobi) i zanim przebudujesz projekcję dla jednej organizacji.
- `backfillEventKeys()` nadaje każdemu starszemu zdarzeniu klucz (typ i przestrzeń jego agregatu),
  dzięki czemu budowa projekcji czyta tylko te zdarzenia, których potrzebuje. Uruchom to, gdy *każdy*
  węzeł działa już na 1.0, a potem jeszcze raz `buildIndexesConcurrently()`. Od tej chwili nie wracaj
  do wcześniejszej wersji: wcześniejsze wersje zapisują zdarzenia bez kluczy. Zdarzenia, których
  agregat nie ma własnego wiersza (*sieroty* po jakimś dawnym usunięciu), nie mogą dostać klucza;
  zadanie zgłasza je w `orphans` i zapisuje ostrzeżenie w logu, ale nie wstrzymują jego zakończenia:
  nie czyta ich żadne odtwarzanie, budowa ani uzgadnianie (wszystkie zaczynają od własnego wiersza
  agregatu), i tak samo jest z sierotą powstałą później, której zdarzenia zachowują klucze prowadzące
  donikąd. Nic ich nie usuwa: decyzja należy do ciebie (usuń je jak w „Usuwaniu agregatu ręcznie”
  niżej albo zostaw).

### 8. Przed aktualizacją: sprawdzenie zapisanych danych

Zanim przeniesiesz bazę danych na nową wersję, sprawdź, czy nowa wersja potrafi przeczytać wszystko,
co w niej jest. `StoredDataCheck` czyta każdą zapisaną wartość JSON (zdarzenia, zarchiwizowane
zdarzenia, sagi, odpowiedzi na polecenia idempotentne i tabele projekcji, które wskażesz) i niczego nie
zmienia, tylko czyta. Uruchom go ze swojej aplikacji, która ma na classpath wszystkie twoje klasy, na
**kopii** produkcji:

<!-- snippet: cart/ProductionSpec.scala#check -->
```scala
import io.reactivecqrs.core.tools.StoredDataCheck

val report = StoredDataCheck.run(mpjsons, typesNames,
  Seq(StoredDataCheck.ProjectionTable[CartSummary]("carts_list")))
println(report.text)
```

Tabelę projekcji nazywasz tak jak jej magazyn dokumentów, bez przedrostka `projection_` (w projekcji
zarządzanej z przyrostkiem budowy, na przykład `_v1`). Raport podaje, dla każdej tabeli i klasy,
przeczytane wiersze i te nieczytelne, z przykładami, agregaty wciąż bez przestrzeni, zdarzenia według
wersji typu zdarzenia (w 1.0 wszystkie muszą mieć 0) oraz tabele, których nie znalazł
(`report.missingTables`): nazwę z literówką albo, w bazie sprzed 1.0, `events_archive`.
`report.unreadable` musi wynosić zero. Od 1.0.1 raport wymienia też wersje zapisane dwa razy
(`report.repeatedVersions`, przez które nie powstaje unikalny indeks z kroku 7) oraz, gdy podasz
`contexts = Seq(...)` z każdym kontekstem agregatu aplikacji, typy agregatów, których żaden z nich nie
ma (`report.typesWithoutContext`): ich szyny poleceń odmówiłyby odtworzenia tych agregatów
(`reactivecqrs.aggregate-type-check`, dodatek E). Oba muszą być puste. Od 1.0.1-RC2 raport wymienia
też zdarzenia-sieroty, których agregat nie ma własnego wiersza (`report.orphans`: ile ich jest, z ilu
agregatów, i pierwszą setkę agregatów z liczbą i klasami zdarzeń oraz identyfikatorami i czasami
pierwszego i ostatniego zdarzenia). Nic ich nie czyta; czy mają zniknąć, decydujesz ty.

Zanim usuniesz z kodu starą klasę zdarzenia, `StoredDataCheck.usersOf(className)` powie ci, które
agregaty wciąż mają zdarzenia tej klasy, w zdarzeniach i w archiwum squasha.

## Obserwowanie działającego systemu

### Metryki

Biblioteka mierzy to, co robi, i przekazuje ci liczby. Implementujesz mały trait z trzema metodami,

<!-- snippet: cart/ProductionSpec.scala#forwardingMetrics -->
```scala
import io.reactivecqrs.core.metrics.Metrics

class ForwardingMetrics extends Metrics {
  override def count(name: String, tags: Map[String, String], amount: Long): Unit =
    () // add `amount` to your metrics system's counter `name`, with `tags`
  override def time(name: String, tags: Map[String, String], nanos: Long): Unit =
    () // record `nanos` in your timer `name`
  override def gauge(name: String, tags: Map[String, String], value: Long): Unit =
    () // set your gauge `name` to `value`
}
```

i podłączasz go przed startem aktorów albo po nim:

<!-- snippet: cart/ProductionSpec.scala#useMetrics -->
```scala
import io.reactivecqrs.core.metrics.ReactiveMetrics

ReactiveMetrics(system).use(new ForwardingMetrics)
```

Biblioteka wywołuje te metody często i na własnych wątkach: twoja implementacja musi być szybka,
bezpieczna wątkowo i nigdy nie może rzucić wyjątku. Dopóki nie wywołasz `use`, nic nie jest mierzone.
Tagów jest niewiele i nigdy nie są to identyfikatory, więc nadają się do każdego systemu metryk. Oto
co dostajesz:

| Nazwa | Rodzaj | Tagi | Co ci mówi |
|---|---|---|---|
| `reactivecqrs.command` | czas | `type`, `command`, `outcome` | ile trwało każde polecenie i jak się skończyło (klasa odpowiedzi albo `Timeout`) |
| `reactivecqrs.events.stored` | licznik | `type` | zapisane zdarzenia |
| `reactivecqrs.events.stored.bytes` | licznik | `type` | ich rozmiary w magazynie |
| `reactivecqrs.restore` | czas | `type`, `from` | właściciel odczytał swój agregat, z migawki (`snapshot`) albo ze zdarzeń (`events`) |
| `reactivecqrs.bus.waiting` | wskaźnik | | dostarczenia szyny w toku |
| `reactivecqrs.bus.held` | wskaźnik | | dostarczenia wstrzymane dla subskrybentów, którzy zostają w tyle |
| `reactivecqrs.bus.behind` | wskaźnik | `subscriber` | agregaty wstrzymane dla każdego subskrybenta; 0, gdy nadąża |
| `reactivecqrs.projection.applied` | licznik | `projection` | wersje zastosowane przez projekcję |
| `reactivecqrs.projection.failed` | licznik | `projection` | agregaty, przy których projekcja się poddała ([rozdział 9](09-projection-failures.md)) |
| `reactivecqrs.outbox.waiting` | wskaźnik | | agregaty czekające w outboxie przy każdym przeglądzie |
| `reactivecqrs.outbox.given.up` | wskaźnik | | agregaty, które poller zgłosił jako niedostarczone |
| `reactivecqrs.outbox.republished` | licznik | | ponowne publikacje, o które poprosił poller |
| `reactivecqrs.outbox.read.abandoned` | licznik | | odczyty outboxu porzucone po `readTimeout`: połączenie przestało odpowiadać |
| `reactivecqrs.message.too.large` | licznik | `class` | komunikaty zbyt duże, by przejść między węzłami ([rozdział 17](17-cluster.md)) |
| `reactivecqrs.restore.failures` | wskaźnik | | zapisane agregaty, których nie da się odtworzyć (przy każdym przeglądzie outboxu) |
| `reactivecqrs.online.job` | wskaźnik | `job` | stan każdego zadania online: 0 czeka, 1 działa, 2 gotowe, 3 porażka |
| `reactivecqrs.online.job.progress` | wskaźnik | `job` | jak daleko doszło: zindeksowane tabele dokumentów, zdarzenia z kluczem, uzupełnione przestrzenie |
| `reactivecqrs.builds.held` | wskaźnik | | pełne budowy czekające na `BuildHold` ([rozdział 10](10-rebuilding-projections.md)); 0, gdy wstrzymanie się skończy |

> **Wskazówka:** trzy alerty wyłapują większość kłopotów: `reactivecqrs.outbox.given.up` powyżej zera,
> `reactivecqrs.bus.behind` rosnące dla jednego subskrybenta i `reactivecqrs.command` z
> `outcome=Timeout` albo `CommandHandlingError`.

> **Pod maską:** pełną listę, z informacją, kiedy powstaje każdy pomiar, znajdziesz w
> [sekcji 14 ARCHITECTURE.md](../ARCHITECTURE.md). [Rozdział 19](19-testing.md) pokazuje
> `RecordingMetrics`, które na potrzeby testów trzyma pomiary w pamięci.

### Pytania o stan

Niektórzy aktorzy odpowiadają na pytanie o stan, które możesz zadać w dowolnej chwili (ze strony
administracyjnej, z endpointu health check, z narzędzia zespołu wsparcia):

| Pytanie | Do kogo | Odpowiedź |
|---|---|---|
| `GetDeliveryStatus` | szyna zdarzeń każdego węzła | `DeliveryStatus`: dostarczenia w toku, ostatnie, które skończyły się jako niedostarczone, zatrzymani subskrybenci, wstrzymane dostarczenia i agregaty wstrzymane dla każdego subskrybenta |
| `GetOutboxStatus` | poller outboxu | `OutboxStatus`: agregaty czekające w outboxie i te, które poller zgłosił jako niedostarczone |
| `GetProjectionStatus` | każda projekcja | `ProjectionStatus`: agregaty z błędami, ponawiane próby i luki ([rozdział 9](09-projection-failures.md)) |
| `GetBuilds` | każda projekcja zarządzana | `Builds`: aktywna budowa i ta w toku ([rozdział 10](10-rebuilding-projections.md)) |
| `GetBuildStatus` | każda projekcja zarządzana | `BuildStatus`: faza trwającej budowy i to, jak daleko doszła, tak jak ostatnio zgłosił jej budowniczy (`reportedAt`), od razu; `heldBy`, gdy czeka wstrzymana |
| `GetSquashStatus` | scheduler squasha | `SquashStatus`: zwinięte, odrzucone, wciąż czekające |
| `GetOnlineJobsStatus` | wykonawca zadań online | `OnlineJobsStatus`: stan i liczniki każdego zadania, czy zwolniono wstrzymanie budów i czego magazynowi brakuje do budów |

W klastrze pytania do pollera, schedulera i projekcji wysyłaj przez ich proxy, z dowolnego węzła;
szynę każdego węzła pytaj osobno.

### Logi warte alertu

| Linia logu zaczyna się od | Skąd | Co oznacza |
|---|---|---|
| „Undelivered: ...” | poller outboxu | zdarzenia agregatu czekały 10 minut na któregoś subskrybenta; są ponawiane co 30 minut |
| „Undelivered after ...” albo „Held for a full window after ...” | szyna zdarzeń | dostarczenie doszło do swojego 10-minutowego terminu; jego wiersz w outboxie zostaje |
| „Subscriber ... stopped” (jako błąd) | szyna zdarzeń | subskrybent się zatrzymał i nic go nie zastępuje; jego zdarzenia czekają w outboxie do następnego startu. Ta sama linia na poziomie INFO jest normalna: węzeł się wyłącza albo zastępca projekcji jest już w drodze |
| „Event bus waits for the subscriptions of: ...” | szyna zdarzeń | przy starcie: zarejestrowany subskrybent nie odpowiedział; węzeł nie jest gotowy |
| „Event bus starts without the subscriptions of: ...” | szyna zdarzeń | zarejestrowany subskrybent nie odpowiedział w `registrationTimeout`; szyna działa, ale żadne zdarzenie nie opuszcza outboxu, dopóki nie odpowie |
| „Subscriber ... stopped before it answered” | szyna zdarzeń | zarejestrowany subskrybent zatrzymał się przy starcie; żadne zdarzenie nie opuszcza outboxu, dopóki nie zostanie usunięty albo węzeł nie wystartuje ponownie |
| „Event bus asked ... for its subscriptions and got a failure” | szyna zdarzeń | subskrybent nie mógł odpowiedzieć; jest pytany ponownie przy każdym takcie, a linia podaje przyczynę |
| „Aggregate ... cannot be restored; it is recorded” albo „Building ...: ... cannot be restored; it is recorded” (ostrzeżenia) | właściciel, budowniczy | agregat, którego twój kod nie umie odtworzyć; raz na agregat. Zob. „Gdy zapisane dane są błędne” |
| „Online job ... failed: ...” | wykonawca zadań online | zadanie się nie powiodło; budowy wstrzymane na nie czekają do `RetryOnlineJobs`, następnego startu albo `releaseBuildHold()` |
| „Full builds of projections are held: ...” (ostrzeżenie) | `Projections` | `BuildHold` wstrzymuje budowy z podanego powodu; gdy wstrzymanie się skończy, pojawia się „... are released” |
| „Event bus registered ... after its start” | szyna zdarzeń | `RegisterSubscribers` przyszło po starcie: ci subskrybenci ominęli to, co opublikowano wcześniej |
| „Delivery waits for ...” i „... deliveries wait longer than ...” | szyna zdarzeń | subskrybent wolno odpowiada |
| „Subscribers with events held beyond their window ...” | szyna zdarzeń | projekcja zostaje w tyle; jej zdarzenia czekają w magazynie zdarzeń |
| „Error handling command ..., errorId: [...]” | właściciel agregatu | nadawca dostał `CommandHandlingError` z tym identyfikatorem błędu |
| „Error persisting events, errorId: [...]” | właściciel agregatu | nadawca dostał `EventHandlingError`; linia podaje przyczynę (za dużo zdarzeń, za duże zdarzenie, błąd bazy danych) |
| „A ... of ... bytes is too large to be sent between nodes” | serializator klastra | komunikat nie mógł przejść między węzłami ([rozdział 17](17-cluster.md)) |
| „Messages between nodes would go through ...” | kontrola gotowości | serializator biblioteki nie jest podpięty: węzeł pozostaje niegotowy, dopóki w jego konfiguracji nie ma `pekko.actor.serialization-bindings { "java.io.Serializable" = reactivecqrs }` ([rozdział 17](17-cluster.md)) |
| „Buffer is full” | regiony shardów i proxy singletonów w Pekko | komunikaty przepadły, gdy shard albo singleton się przenosił |

Identyfikator błędu w obu liniach właściciela to ten sam, który dostał nadawca: zgłoszenie do
wsparcia, które go przytacza, prowadzi prosto do śladu stosu.

### Dane osobowe a logi

Logi przechowuje się długo i czyta je wiele osób, a żadne usuwanie danych do nich nie sięga
([rozdział 15](15-rewriting-history.md)). Dlatego biblioteka nie wpuszcza wartości, z którymi pracuje, do
swoich logów ani do tego, co zapisuje i odpowiada o awariach:

- Jej polecenia SQL działają z wyłączonym logowaniem SQL ScalikeJDBC. Polecenie, które zawiedzie, nie
  trafia do logu ze swoimi parametrami, a byłyby nimi całe zdarzenia, dokumenty i zlecenia sag.
- Awaria jest nazwana bez wartości, na których się wydarzyła: `MatchError` staje się „wartością klasy
  ...”, błąd PostgreSQL jego SQLState i ograniczeniem, błąd mpjsons typem i pozycją. Pozostałe wyjątki
  zachowują komunikat, przycięty do 1 000 znaków.

Twoja część:

- Nie umieszczaj danych osobowych w komunikatach własnych wyjątków: biblioteka je loguje, a gdy listener
  zawiedzie, zapisuje w `subscription_failures`.
- Dodaj `logServerErrorDetail=false` do adresu JDBC, żeby wyjątki sterownika też nie cytowały wierszy.
- Dla sesji, które otwiera twój kod, ustaw raz przy starcie `GlobalSettings.loggingSQLErrors = false`
  albo otwieraj je z `import io.reactivecqrs.core.util.SqlLogging.withoutValues` w zasięgu.

Żeby zdiagnozować awarię razem z wartościami, uruchom węzeł z `-Dreactivecqrs.log-values=true`.
ScalikeJDBC loguje wtedy polecenia tak, jak jest skonfigurowane, a każda awaria trafia w całości do
loggera `io.reactivecqrs.detail`. Potem wyłącz to z powrotem: te linie zawierają dane osobowe.

### Zegary i strefy czasowe

Uruchamiaj każdą JVM w UTC (`-Duser.timezone=UTC`) i trzymaj zegary węzłów w zgodzie (NTP). Zdarzenia
biorą czas z zegara węzła, który je zapisuje, a poller outboxu porównuje go z własnym: węzeł, którego
zegar spieszy się lub spóźnia o minuty, powoduje za wczesne albo za późne ponowne dostarczenia i
rezygnacje, a odczyty „na chwilę” widzą jego zdarzenia w złym czasie. Niektóre czasy (archiwa,
migawki, kroki schematu) są zapisywane w strefie sesji, którą pgjdbc bierze z JVM.

### PostgreSQL

Obserwuj to samo, co w każdym mocno obciążonym PostgreSQL: używane połączenia (`pg_stat_activity`),
autovacuum na gorących tabelach (`pg_stat_user_tables`) oraz rozmiary tabel i indeksów. Gorące tabele
to `events_to_publish` (outbox: jedno wstawienie i jedno usunięcie na zdarzenie), `subscriptions`
(jedna aktualizacja na zdarzenie na projekcję) i `aggregates` (jedna aktualizacja na polecenie). Ustaw
im niski współczynnik skali autovacuum, żeby uruchamiał się często.

## Gdy zapisane dane są błędne

Lata historii zbierają trochę niespodzianek: zdarzenie, którego twój handler już nie przyjmuje,
wiersz, który stary skrypt usunął do połowy. Biblioteka nigdy nie naprawia takich danych za twoimi
plecami; znajduje je, mówi ci o nich i dba, żeby reszta działała. Ta sekcja to strona operatora;
[rozdział 10](10-rebuilding-projections.md) pokazuje, co w tym czasie robi budowa, a
[rozdział 21](21-rebuilds-and-repairs.md) przypisuje każdemu rodzajowi takich danych jego sprawdzenie i naprawy.

### Agregaty, których nie da się odtworzyć

Agregatu nie da się odtworzyć, gdy jednego z jego zdarzeń nie da się odczytać albo gdy twój handler
zdarzeń rzuca na nim wyjątek (`MatchError` dla zdarzenia, o którym kod zapomniał, sprawdzenie, którego
stare dane nie przechodzą). Kto na niego trafi, ten go zapisuje w tabeli `restore_failures`: właściciel
agregatu (przy poleceniu, zapytaniu albo ponownej publikacji), budowa projekcji (w każdej ze swoich
faz) albo `RestoreCheck` opisany niżej. Każdy zapis zawiera agregat, jego typ i organizację, wersję i
identyfikator zdarzenia, na którym się nie udało, klasę i komunikat wyjątku (bez wartości, jak każda
linia logu), miejsce, czas pierwszego i ostatniego razu, liczbę razy i informację, czy operator pominął
go w budowie. Do logu trafia jedno ostrzeżenie na agregat, nie na każdą ponowną próbę, a wskaźnik
`reactivecqrs.restore.failures` liczy je przy każdym przeglądzie outboxu. Odczytaj je:

```scala
import io.reactivecqrs.core.eventstore.RestoreFailureFilter

eventStore.restoreFailures()                                              // all of them, the oldest first
eventStore.restoreFailures(RestoreFailureFilter(projection = Some("carts"))) // those of one projection's builds
```

Zapis znika, gdy agregat znów da się w tym miejscu odtworzyć, gdy zostanie usunięty na dobre albo
wyczyszczony z organizacją, albo gdy jego budowa zostanie porzucona. Właściciel, który nie może
odtworzyć swojego agregatu, odpowiada projekcji proszącej o jego zdarzenia komunikatem
`CannotRestore(aggregateId, reason)`, więc niepowodzenie projekcji od razu mówi dlaczego, zamiast
„versions from N are missing” dziesięć minut później.

Przed aktualizacją znajdź je wszystkie naraz, nowym kodem, na kopii produkcji:

```scala
import io.reactivecqrs.core.tools.RestoreCheck

val report = RestoreCheck.run(eventStore, Seq(cartContext))  // restores every cart with this code
println(report.text)
```

Odtwarza każdy agregat przekazanych kontekstów, zapisuje te, których się nie udało (i usuwa zapisy
tych, które przeszły), i wymienia pierwszą setkę identyfikatorów (`report.failed`; przyczyny są w zapisach). Czyta po stronie agregatów naraz; daj mu `pause`,
żeby uruchomić go na działającej bazie.

I co dalej? Trzy wyjścia, najlepsze pierwsze:

1. **Popraw kod.** Handler, który rzuca wyjątek na starych danych, to błąd handlera: niech przyjmie to
   zdarzenie, wdróż poprawkę i wyślij `RetryAllFailedSubscriptions` do projekcji, którym się nie udało
   (przez `ToBuildInProgress` do trwającej budowy). Agregat znów się odtwarza, zapisy znikają, a
   wstrzymana budowa przełącza się przy najbliższym sprawdzeniu.
2. **Pomiń agregat w jednej budowie**, gdy jego dokumenty mogą poczekać: `SkipAggregate(aggregateId)`
   ([rozdział 10](10-rebuilding-projections.md)). Dane zostają; budowa po prostu go pomija, a zapis
   mówi „skipped”.
3. **Usuń agregat**, gdy w ogóle nie powinien istnieć. O tym następna sekcja.

### Usuwanie agregatu ręcznie

Biblioteka nie ma wywołania, które usuwa agregat niedający się odtworzyć: trwałe usunięcie to
zdarzenie, a agregat, którego nie da się odtworzyć, nie przyjmie polecenia. Usuwasz go w jednej
transakcji, tymi poleceniami, które wymieniają każdą tabelę biblioteki trzymającą wiersze jednego
agregatu (tu `4711`; wpisz swój identyfikator w każdej linii):

```sql
BEGIN;
-- Stop (ROLLBACK) if this lists anything: copies of the aggregate read its events.
SELECT id FROM aggregates WHERE base_id = 4711 AND id <> 4711;
DELETE FROM noop_events WHERE id IN (SELECT id FROM events WHERE aggregate_id = 4711);
DELETE FROM events WHERE aggregate_id = 4711;              -- its event_keys go with them
DELETE FROM events_archive WHERE aggregate_id = 4711;
DELETE FROM events_to_publish WHERE aggregate_id = 4711;
DELETE FROM snapshots WHERE aggregate_id = 4711;
DELETE FROM aggregate_lineage WHERE aggregate_id = 4711 OR copied_from = 4711;
DELETE FROM history_rewrites WHERE aggregate_id = 4711;
DELETE FROM subscriptions WHERE aggregate_id = 4711;
DELETE FROM subscription_failures WHERE aggregate_id = 4711;
DELETE FROM commands_responses WHERE aggregate_id = 4711;
DELETE FROM restore_failures WHERE aggregate_id = 4711;
DELETE FROM aggregates WHERE id = 4711;                    -- its own row, and its rows of the bases it copied
COMMIT;
```

Uruchom je, gdy nic nie pisze do agregatu (nic nie może: jego właściciel nie potrafi go odtworzyć).
Pierwsze zapytanie nie może niczego zwrócić: agregat, z którego zduplikowano inne, niesie część ich
historii, a jego usunięcie by je zepsuło. Potem wyślij `RetryAllFailedSubscriptions` do każdej
projekcji, której się na nim nie udało (i przez `ToBuildInProgress` do trwającej budowy): ponowna próba
dla agregatu, którego już nie ma, czyści jego niepowodzenie, jak dla usuniętego na dobre, a budowa,
która czekała tylko na niego, przełącza się przy najbliższym sprawdzeniu, bez restartu.

Co zostaje: dokumenty, które twoje projekcje wyprowadziły z agregatu, zanim zaczął zawodzić. Wyczyść
je tak, jak zrobiłoby to trwałe usunięcie, albo przebudową celowaną agregatu
(`RebuildAggregates(OneAggregate(id))`, którego `clearForRebuild` je usuwa, a potem nie znajduje nic
do dostarczenia). Te same polecenia usuwają zdarzenia-sieroty (agregat, którego wiersza już nie ma):
linie z `aggregates` niczego wtedy nie usuwają.

> **Ostrzeżenie:** to jedyne miejsce, w którym sam usuwasz zdarzenia. Najpierw zrób kopię zapasową,
> sprawdź identyfikator agregatu dwa razy i zachowaj wyniki `StoredDataCheck` i `restoreFailures`
> sprzed zmiany.

### Wersje, których brakuje w magazynie

Zdarzenia każdego agregatu mają numery 1, 2, 3 i tak dalej, bez dziur: zapisuje je jeden właściciel,
po kolei. Dziura może przyjść tylko z zewnątrz, na przykład z wiersza usuniętego ręcznie lata temu.
Odtwarzanie jej nie zauważa (stosuje zdarzenia, które znajdzie), ale budowa tak: nie może dostarczyć
wersji 27 modelowi odczytu, który nigdy nie dostał 26. Od wydania po 1.0.1-RC2 budowa odnotowuje tam porażkę
agregatu, raz, z „versions from 26 to 26 are missing from the store”, zapisuje ją (`restoreFailures`,
klasa błędu `missing versions`) i buduje dalej pozostałe; wcześniej prosiła o brakujące wersje w
kółko, bez słowa. Żeby przejść przez dziurę i zastosować to, co po niej, pomiń brakujące wersje:

```scala
cartNames ? ToBuildInProgress(SkipFailedVersion(cartId, pastMissing = true))
```

`SkipAggregate` pominąłby zamiast tego cały agregat. Znajdź takie agregaty przed aktualizacją:
wymienia je `RestoreCheck` (`report.gaps`: pierwsza brakująca wersja każdego agregatu i ile ich
brakuje), a także to zapytanie tylko do odczytu, które czyta całą tabelę `events` (uruchom je na
kopii albo gdy baza ma spokój):

```sql
SELECT a.id, a.type_id, a.base_version - coalesce(a.squashed_version, 1) + 1 - count(DISTINCT e.version) AS missing
  FROM aggregates a
  JOIN events e ON e.aggregate_id = a.id AND e.version BETWEEN coalesce(a.squashed_version, 1) AND a.base_version
 WHERE a.id = a.base_id
 GROUP BY a.id, a.type_id, a.base_version, a.squashed_version
HAVING count(DISTINCT e.version) < a.base_version - coalesce(a.squashed_version, 1) + 1;
```

Agregat po squashu liczy się od punktu squasha; duplikat liczy tylko własne zdarzenia.
`RestoreFailureFilter(source = Some(RestoreFailureSource.Check))` wymienia tylko to, co zapisał
sprawdzian.

## Pod dużym obciążeniem

Biblioteka nie ma globalnego hamulca. Praca, która nie może jeszcze ruszyć, czeka w kolejkach, w
większości nieograniczonych, i tylko trzy limity odpowiadają od razu, zamiast kazać pracy czekać:
kolejka szeregowa, limit czasu polecenia i limity na agregat. Ta sekcja mówi, jak biblioteka się
zachowuje, gdy wszystkiego robi się dużo, i co wtedy zrobić.

**Jak czytać liczby.** Liczby *zmierzone* pochodzą z raportów w `docs/perf/`, a każdy raport to jeden
przebieg z 17/10/2026 na jednej maszynie: Windows 10, 12 wątków procesora, OpenJDK 17 z 2 GB sterty,
PostgreSQL 17.4 na tej samej maszynie i pula JDBC z 40 połączeniami. Nadają się do porównań, ale nie
mówią, ile udźwignie twój sprzęt. Liczby *oszacowane* to rachunki na zmierzonych i same nie były
mierzone.

### Tłum poleceń naraz

Zmierzone (`load-M5`): 10 000 poleceń wysłanych naraz, bez subskrybentów.

| Cel | Wynik | Ostatnia odpowiedź po | Tempo |
|---|---|---|---|
| 10 000 nowych koszyków (pierwsze polecenia) | wszystkie się udały | 2 342 ms | 4 269 poleceń/s |
| 10 000 istniejących koszyków, po jednym poleceniu | wszystkie się udały | 891 ms | 11 226 poleceń/s |
| jeden koszyk, przetwarzanie współbieżne | wszystkie się udały | 3 815 ms | 2 621 poleceń/s |
| jeden koszyk, przetwarzanie szeregowe | 1 003 udane, 8 997 od razu `CommandRejected` | 594 ms | |

Na wielu agregatach każde polecenie zostaje przyjęte; polecenia stoją w kolejce do wątków i połączeń,
więc ostatnia odpowiedź przychodzi po mniej więcej „polecenia w toku ÷ tempo zatwierdzeń”. Oszacowane
na podstawie tego samego przebiegu: około 15 KB sterty na każde polecenie w toku, które tworzy agregat,
i 8 KB na polecenie do istniejącego agregatu (górne granice), więc 100 000 nowych koszyków naraz
potrzebowałoby około 1,5 GB sterty, a ostatnia odpowiedź przyszłaby po około 23 sekundach; milion nie
zmieściłby się w typowej stercie.

Co robić:

1. **Ogranicz liczbę poleceń w toku na wejściu** do aplikacji, zanim dotrą do szyny poleceń: do mniej
   więcej tempa zatwierdzeń razy opóźnienie, które akceptujesz. Na mierzonej maszynie to
   10 000/s × 0,5 s = 5 000 poleceń w toku (oszacowanie). Pozostałym odpowiadaj „zajęte” (HTTP 429
   albo 503).
2. **Daj ponawianym poleceniom klucz idempotencji** ([rozdział 5](05-command-flavors.md)), żeby
   ponowienie po przekroczeniu czasu zostało zastosowane raz, nawet gdy pierwsza próba przeszła z
   opóźnieniem.
3. **Ustaw klientowi dłuższy limit czasu niż limit czasu polecenia** (domyślnie 60 sekund) albo skróć
   `commandTimeout` kontekstu, żeby odpowiedź biblioteki dotarła, zanim klient się podda.
4. **Używaj przetwarzania szeregowego dla agregatów, do których wielu klientów pisze naraz**: przy
   pełnej kolejce odpowiedź przychodzi od razu, a klienci mogą się wycofać i spróbować później. Jeszcze
   lepiej: podziel taki gorący agregat.

### Góra zdarzeń

Tak wyglądałby magazyn ze 100 milionami zdarzeń według oszacowań opartych na liczbach zmierzonych dla
jednego zdarzenia (nic z tego nie było uruchamiane w takiej skali):

| | Oszacowanie | Na podstawie |
|---|---|---|
| tabela zdarzeń z indeksami | około 38 GB | 382 bajty na zdarzenie, zmierzone z notatkami o długości 200 znaków; przeskaluj według JSON-a swoich zdarzeń |
| zapisanie ich przy pełnym obciążeniu, bez subskrybentów | około 2,5 godziny | 11 226 zdarzeń/s (`load-M5`) |
| jedna przebudowa projekcji opartej na zdarzeniach | około 4,1 godziny | 6 769 zdarzeń/s (`baseline-M4`) |
| opróżnienie zaległości outboxu z 1 miliona agregatów | około 8,3 godziny | 1 000 agregatów na 30-sekundowy przegląd |

Kiedy zapisy wyprzedzają projekcję, szyna wysyła jej najwyżej **okno** 10 000 zdarzeń i 64 MB, na
które projekcja jeszcze nie odpowiedziała; reszta czeka w magazynie i jest dociągana dla projekcji, w miarę jak
ta nadrabia zaległości. Zmierzone (`flow-M5` w porównaniu z `baseline-M4`): w 60-sekundowym teście pod
stałym obciążeniem, z 16 nadawcami i dwiema projekcjami, system zatwierdził 671 768 poleceń, a
projekcje nadrobiły zaległości 89 sekund po zakończeniu obciążenia (zanim pojawiło się okno: 475 403
polecenia, 201 sekund). W klastrze okno dotyczy szyny każdego węzła z osobna, więc przy trzech węzłach
skrzynka pocztowa projekcji może pomieścić do trzech okien: dobierz `DeliveryOptions.window` i `windowBytes`
(`new EventsBusActor(DeliveryOptions(...))`) do liczby węzłów.

Co robić:

- **Obserwuj opóźnienie:** `bus.held` i `bus.behind`, `GetDeliveryStatus` i `GetProjectionStatus`.
- **Dbaj o niski koszt jednego zdarzenia w projekcjach.** Listener, który woła powolną usługę zewnętrzną,
  powinien trafić do sagi albo do osobnej projekcji. Zostaw zapas między tempem poleceń a najwolniejszą
  projekcją.
- **Przebuduj projekcję, która mocno zostaje w tyle** ([rozdział 10](10-rebuilding-projections.md)),
  zamiast pozwalać jej nadrabiać przez outbox po 1 000 agregatów na przegląd; albo przyspiesz poller,
  dopóki outbox się nie opróżni:
  `OutboxPollerOptions(redelivery = RedeliveryPolicy.Options(maxRepublishPerPoll = 5000))`.
- **Planuj czas przebudowy na podstawie tempa:** około 6 800 zdarzeń/s na budowę na mierzonej
  maszynie.
- **Licz miejsce na dysku według własnych zdarzeń:** rozmiar ich JSON-a plus około 190 bajtów na
  zdarzenie (oszacowanie).

### Agregaty z dziesiątkami tysięcy zdarzeń

Domyślnie agregat zatrzymuje się na **10 000 zdarzeń**: polecenie, które przekroczyłoby ten limit,
kończy się błędem `EventHandlingError`, a log wymienia `TooManyEventsException`. Limit chroni przed
agregatami, które rozrastają się bez kontroli; podnoś go świadomie, dla całego magazynu:
`new PostgresEventStoreState(mpjsons, typesNames, aggregateVersionLimit = 50000)`.

Zmierzone (`load-M5`, ze zniesionym limitem): dla agregatu z 50 000 zdarzeń odtworzenie przez nowego
właściciela trwało 392 ms, odtworzenie z migawki 12 ms, a polecenie po odtworzeniu 3 ms; cała historia
w jednej odpowiedzi (`GetEventsForAggregate`) zajęła 264 ms i w szczycie 168 MB sterty. W
`baseline-M4` odtworzenie 500 000 zdarzeń trwało 2 088 ms, a z migawki 1 ms.

Długość nie spowalnia poleceń (właściciel trzyma stan, nie zdarzenia), ale spowalnia każde
odtworzenie: po pasywacji, po przekazaniu na inny węzeł, po restarcie. Oszacowanie: bez migawek węzeł
odtworzy najwyżej około 80 agregatów po 50 000 zdarzeń na sekundę, a z migawkami około 2 700.

Co robić:

- **Projektuj krótkie agregaty:** dziel je według naturalnej granicy, na przykład okresu albo wersji
  dokumentu.
- **Włącz migawki** ([rozdział 13](13-snapshots.md)) dla każdego typu, którego agregaty rosną ponad
  kilka tysięcy zdarzeń.
- **Zwijaj długie historie squashem** ([rozdział 14](14-squash.md)), na żądanie albo automatycznie.
- **Nie proś o całą historię długiego agregatu:** `GetEventsForAggregateForVersion` czyta do wskazanej
  wersji.

### Polecenia i zdarzenia po kilka megabajtów

Domyślnie zdarzenie może mieć **100 000 znaków JSON-a**; większe kończy polecenie błędem
`EventHandlingError`, a log wymienia `EventTooLargeException`. Limit możesz podnieść tylko dla
wybranych klas, przez `eventSizeLimitPerClassName` (mapę z nazwy klasy na liczbę znaków).

Zmierzone (`large-M5`), gdy każde polecenie niesie w jednym zdarzeniu dokument z 5 000 000 losowych
znaków (4,8 MB):

| | Zmierzone |
|---|---|
| 20 poleceń jedno po drugim, jeden agregat | mediana 90 ms, najwolniejsze 164 ms; 31,8 MB/s dokumentów |
| 20 poleceń naraz, każde na własnym agregacie | ostatnia odpowiedź po 890 ms; sterta 117 → 358 MB |
| miejsce na dysku na jedno zdarzenie (tabela, indeksy, TOAST) | 5,0 MB |
| odtworzenie agregatu z 20 takich zdarzeń | 517 ms; sterta 117 → 323 MB |
| dostarczenie jednego zdarzenia zserializowane dla innego węzła | 4,8 MB przy limicie 248 KiB |

Oszacowanie: około 12 MB sterty na każde takie polecenie w toku, więc 100 naraz zajmuje około 1,2 GB.

Co robić:

- **Trzymaj dokumenty poza zdarzeniami i poleceniami** (wzorzec claim-check): najpierw zapisz plik, w
  magazynie obiektów albo we własnej tabeli, a potem wyślij polecenie, które go wskazuje
  (identyfikator, skrót, rozmiar). Zdarzenia zostają małe, a razem z nimi odtworzenia, dostarczenia,
  przebudowy i komunikaty między węzłami.
- **Jeśli dokumenty muszą być zdarzeniami,** trzymaj limit każdej klasy tak nisko, jak się da, i:
  - włącz migawki i trzymaj dokument poza stanem;
  - obniż `fetchSize` magazynu zdarzeń (domyślnie 1 000 wierszy; na przykład do 10) albo ustaw w
    sterowniku PostgreSQL `adaptiveFetch=true` z `maxResultBuffer`, żeby odczyty brały po kilka
    wierszy naraz;
  - obniż `pageSize` i `chunkEvents` w ustawieniach budowy projekcji
    (`ProjectionsSettings(..., builder = BuilderSettings(...))`);
  - na wejściu ograniczaj *bajty* poleceń w toku, nie tylko ich liczbę (na przykład: liczba
    jednoczesnych przesyłań × największy dokument ≤ jedna czwarta sterty);
  - w klastrze podnieś rozmiar ramki powyżej największego zdarzenia ([rozdział 17](17-cluster.md)).

<!-- snippet: cart/ProductionSpec.scala#largeEvents -->
```scala
val eventStore = new PostgresEventStoreState(mpjsons, typesNames,
  fetchSize = 10,
  eventSizeLimitPerClassName = Map(classOf[ReceiptAttached].getName -> 6000000) // an event of your own
).initSchema()
```

### Handlery poleceń, które działają minutami

Handlery poleceń działają na dispatcherze handlerów, domyślnie na 32 wątkach. Zmierzone (`heavy-M5`),
z handlerami trwającymi 5 sekund w zastępstwie handlerów trwających minuty:

| Przypadek | Wynik |
|---|---|
| 36 handlerów naraz, każdy zajmuje swój wątek | wszystkie się udały, ostatni po 10,0 s (po 32 naraz); w tym czasie szyna zdarzeń odpowiadała w 8 ms, ale lekkie polecenie na innym agregacie czekało 5,4 s |
| 36 handlerów naraz, praca na własnej puli aplikacji (`AsyncCommandResult`) | ostatni po 5,0 s; szyna odpowiadała w 0 ms, lekkie polecenie w 3 ms |
| jeden handler, na agregacie, który w tym czasie zmieniały lekkie polecenia (przetwarzanie współbieżne) | udał się po 10,2 s: raz wszedł w konflikt i wykonał się ponownie; 25 lekkich poleceń czekało do 4,8 s |
| jeden handler dłuższy niż jego limit czasu | `CommandHandlingError` po upływie limitu i nic nie zostało zapisane |

Handler trwający 5 minut przy domyślnym 60-sekundowym `commandTimeout` zawsze kończy się więc
`CommandHandlingError`, a jego zdarzenia nigdy nie zostają zapisane.

Co robić:

- **Wynieś długą pracę poza polecenie.** Polecenie zapisuje prośbę (jedno zdarzenie); saga albo worker
  wykonuje pracę na własnych wątkach, z ponawianiem i możliwością wznowienia; drugie polecenie zapisuje
  wynik. Agregat pozostaje dostępny, a długiej pracy nie dotyka żaden limit czasu, przekazanie ani
  konflikt.
- **Jeśli polecenie musi to zrobić samo:**
  - zwracaj `AsyncCommandResult`, a pracę wykonuj na własnej, ograniczonej puli wątków;
  - podnieś `commandTimeout` tego kontekstu powyżej najdłuższego wykonania;
  - użyj przetwarzania szeregowego dla tego typu, żeby praca nigdy nie wykonała się drugi raz po
    konflikcie;
  - daj klientom klucz idempotencji i wystarczająco długi limit czasu albo odpowiadaj „przyjęte”, a
    wynik podawaj później przez projekcję;
  - dla typu `Sharded` podnieś limity czasu przekazania i okres karencji (grace period) menedżera
    procesów powyżej limitu czasu polecenia ([rozdział 17](17-cluster.md)).

> **Pod maską:** każda liczba z tej sekcji, z rachunkami i uzasadnieniem każdej rekomendacji, jest w
> [SCALING.md](../SCALING.md); surowe raporty są w `docs/perf/`.

## Przejście na 1.0

Jeśli przychodzisz z 0.12, przejście na 1.0 to **jednorazowe, zaplanowane zatrzymanie**: węzeł 0.12
nie dołączy do klastra 1.0. Aktualizacje kroczące ([rozdział 17](17-cluster.md)) są możliwe dopiero od
1.0. Instrukcja krok po kroku, którą najpierw warto przećwiczyć na kopii produkcji, to
[UPGRADE-0.12-TO-1.0.md](../UPGRADE-0.12-TO-1.0.md). Oto zmiany, jakich wymaga od aplikacji (hosta),
w kolejności, w jakiej je spotkasz.

**Przed zatrzymaniem, na kopii produkcji:**

1. Uruchom `StoredDataCheck` (krok 8 listy kontrolnej): każdy wiersz musi dać się przeczytać, a każde
   zdarzenie musi mieć wersję typu zdarzenia 0.
2. Poszukaj kodu, który porównuje pola `Option` w zdarzeniach z `null`: z biblioteką JSON w 1.0
   brakujące pole `Option` jest odczytywane jako `None`.
3. Daj każdemu poleceniu wysyłanemu przez sagę klucz idempotencji, czyli jego `SagaStep`
   ([rozdział 12](12-sagas.md)), żeby saga wznowiona po awarii nigdy nie zastosowała kroku dwa razy.

**W twoim kodzie:**

4. Wersjonowania zdarzeń już nie ma: gdy kształt zdarzenia musi się zmienić, dodaj nową klasę (na
   przykład `ItemAddedV2`) i zachowaj starą ([rozdział 6](06-events.md)).
5. Każde pierwsze zdarzenie podaje przestrzeń swojej organizacji, `SpaceId(0)` dla platformy; nowy
   agregat, którego pierwsze zdarzenie podaje `SpaceId.unknown`, jest odrzucany
   ([rozdział 16](16-organizations.md)).
6. Klasa zdarzenia generyczna względem korzenia agregatu przyjmuje `ClassTag` (`[R: ClassTag]`);
   klucz typu, który budujesz ręcznie, to `AggregateType.of[R]`.
7. Projekcje agregatów, które korzystają z cofnięcia, muszą obsługiwać zdarzenia cofnięcia: przebudowa
   je dostarcza.
8. Klasy, które przechodzą między węzłami, trzymają się czterech zasad z
   [rozdziału 17](17-cluster.md) (pola dodane później są typu `Option`).

**Przy składaniu systemu:**

9. Wywołuj `initSchema()` na każdym komponencie PostgreSQL przy każdym starcie (krok 2), także na
   stanie odpowiedzi poleceń i na stanie subskrypcji; ten drugi tworzy tabelę subskrypcji z błędami.
10. Wyślij każdej szynie zdarzeń `RegisterSubscribers` ze wszystkimi projekcjami i listenerami i
    uruchom jeden `OutboxPollerActor` na system.
11. Twórz projekcje na `BlockingDispatcher.id` i dobierz rozmiar dispatcherów (krok 4). Sekwencje
    identyfikatorów od pierwszego startu rosną co 10 000; biblioteka ustawia to sama.
12. Uruchamiaj każdy model odczytu przez `Projections.start`
    ([rozdział 10](10-rebuilding-projections.md)): `ActorRef`, który zwraca, używaj do zapytań i w
    `RegisterSubscribers`, tabele nazywaj przez `build.documentStore(...)`, każdej budowie dawaj osobne
    cache, nigdy nie zatwierdzaj ani nie wycofuj sesji, którą dostaje listener, a usunięte projekcje
    wycofuj z użytku przez `Projections.retire(name)`. Projekcje z efektami ubocznymi zostają
    niezarządzane.
13. Używaj ustawień klastra na każdym węźle, także na pojedynczym, z podpiętym w nich serializatorem
    biblioteki; poller, scheduler squasha i runnery sag uruchamiaj przez `Singletons.start`; proxy
    projekcji rejestruj na każdej szynie ([rozdział 17](17-cluster.md)).
14. Ustaw `reactivecqrs.installation-number` (krok 3), twórz generator identyfikatorów przez
    `UidGeneratorActor.postgres(system)` i sprawdź, czy żaden twój kod nie czyta identyfikatora jako
    `Int`.
15. Ustawiaj `pekko.cluster.app-version` dla każdego wydania i dodaj `ReadinessCheck` do swoich health
    checków.
16. Podłącz swoje metryki (`ReactiveMetrics(system).use(...)`), ustaw pulę JDBC na co najmniej 32
    połączenia, ogranicz liczbę poleceń w toku i dostrój autovacuum dla gorących tabel.

**Gdy nowa wersja już działa, w tle:**

17. `buildIndexesConcurrently()`.
18. `backfillSpaces(...)`, zanim przebudujesz jakąkolwiek projekcję dla jednej organizacji.
19. Gdy każdy węzeł działa już na 1.0: `backfillEventKeys()`, a potem jeszcze raz
    `buildIndexesConcurrently()`.

**Nowości, które możesz włączyć, po jednym kontekście naraz, kiedy tylko zechcesz:** migawki
([rozdział 13](13-snapshots.md)), szeregowe przetwarzanie poleceń ([rozdział 5](05-command-flavors.md)),
squash i jego archiwum audytowe ([rozdział 14](14-squash.md)), koperta metadanych na zdarzeniach
(`WithMetadata`, [rozdział 6](06-events.md)), własne wiersze zapisywane razem ze zdarzeniami
(`WithOutboxWriter`) i rozmieszczenie `Sharded` ([rozdział 17](17-cluster.md)).

> **Pod maską:** te kroki to wiersze dla aplikacji (hosta) od H1 do H11 w
> [sekcji 4 KNOWN-ISSUES.md](../KNOWN-ISSUES.md), a plan „zatrzymaj i zmigruj” to decyzja D81.

## Co już umiesz

- Węzeł startuje w ustalonej kolejności: pula połączeń, schemat (bezpieczny przy każdym starcie, jeden
  węzeł naraz), numer instalacji, dispatchery o dobranym rozmiarze, jeden poller i jeden scheduler na
  system oraz subskrybenci szyny. Ciężkie zadania działają osobno, w tle.
- `StoredDataCheck` mówi ci przed aktualizacją, czy nowa wersja przeczyta wszystko, co zapisane.
- `ReactiveMetrics(system).use(...)` przekazuje ci pomiary biblioteki; pytania o stan i krótka lista
  linii logu mówią ci, kiedy coś czeka albo się psuje.
- Pod obciążeniem: ogranicz polecenia na wejściu, trzymaj projekcje tanie, agregaty krótkie (pomagają
  migawki i squash), dokumenty poza zdarzeniami, a długą pracę poza poleceniami.
- Każda liczba w tym rozdziale mówi, skąd pochodzi: zmierzona raz na jednej maszynie albo oszacowana.
- Przejście z 0.12 na 1.0 to zaplanowane zatrzymanie z krótką listą kroków w kodzie, w składaniu
  systemu i w danych.

Dalej: [rozdział 19](19-testing.md) pokazuje, jak to wszystko przetestować, od pojedynczego handlera
zdarzenia po klaster w jednej JVM.

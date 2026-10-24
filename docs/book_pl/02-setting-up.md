# Rozdział 2. Przygotowanie warsztatu

Pod koniec tego rozdziału będziesz mieć projekt w Scali, który kompiluje się z ReactiveCQRS, przygotowaną
dla biblioteki bazę PostgreSQL i garść linijek startowych, których potrzebuje każda aplikacja: pulę połączeń,
tabele biblioteki i plik konfiguracyjny. To najmniej efektowny rozdział książki, a zarazem ten, który
później oszczędzi ci najwięcej drapania się po głowie. Zróbmy miejsce na stole warsztatowym.

---

## Co musisz mieć na biurku

- **JDK w wersji 25 lub nowszej.** Biblioteka JSON, z której korzysta biblioteka, mpjsons 0.7.8,
  wymaga do działania Javy 25. Własne klasy biblioteki są nadal kompilowane dla Javy 21 (`-release 21`
  w jej ustawieniach kompilacji).
- **Scala 2.13.** Biblioteka jest kompilowana Scalą 2.13.18 i publikowana tylko dla 2.13.
- **sbt**, narzędzie do budowania projektów w Scali. Wystarczy dowolne wydanie 1.x; sama biblioteka
  buduje się wersją 1.11.7.
- **PostgreSQL**, działający gdzieś w zasięgu twojego komputera. Pomiary biblioteki (zobacz
  [docs/SCALING.md](../SCALING.md)) przeprowadzono na PostgreSQL 17.4, więc nowe wydanie to bezpieczny
  wybór.
- **Klient SQL**, na przykład `psql`, żeby podglądać, co zapisuje biblioteka. Nieobowiązkowy, ale to
  połowa zabawy.

Pekko, ScalikeJDBC i sterownika PostgreSQL nie instalujesz samodzielnie: przychodzą razem z biblioteką.

## Krok 1: baza danych dla biblioteki

ReactiveCQRS trzyma wszystko w PostgreSQL: zdarzenia, agregaty, modele odczytu, postęp sag. Daj bibliotece
bazę danych i użytkownika. W `psql`, po połączeniu jako superużytkownik:

```sql
CREATE USER reactivecqrs WITH PASSWORD 'reactivecqrs';
CREATE DATABASE reactivecqrs OWNER reactivecqrs;
```

To dokładnie te nazwy, których oczekują testy samej biblioteki: baza `reactivecqrs` na `localhost:5432`,
z użytkownikiem i hasłem `reactivecqrs` (zobacz `TestDatabase` w module `testkit`). Ta jedna baza posłuży
więc twoim eksperymentom, a kiedy tylko zechcesz, także testom biblioteki. W prawdziwej aplikacji wybierz
własne nazwy i hasło, którego nikt nie zgadnie.

> **Wskazówka:** biblioteka tworzy swoje tabele w tym schemacie, którego używa twoje połączenie, więc
> jedna baza może pomieścić kilka niezależnych kopii. Wykonaj `CREATE SCHEMA shop AUTHORIZATION reactivecqrs;`
> i dopisz `?currentSchema=shop` do adresu URL JDBC z kroku 3. Testy biblioteki stosują ten sam trik:
> każdy przebieg dostaje świeży schemat o nazwie `test_run_<seconds>_<number>`, a schematy starsze niż
> sześć godzin są usuwane.

## Krok 2: dodaj bibliotekę do projektu

Biblioteka jest publikowana jako dwa artefakty. Oto jej współrzędne, wzięte wprost z jej własnej
konfiguracji budowania (`project/Common.scala` i `build.sbt`):

| | |
|---|---|
| Organizacja | `io.reactivecqrs` |
| Artefakty | `reactivecqrs-api` i `reactivecqrs-core` |
| Wersja | `1.0.0` |
| Sufiks wersji Scali | tak (`_2.13`), więc użyj `%%` |

Minimalny `build.sbt` aplikacji:

```scala
ThisBuild / scalaVersion := "2.13.18"

resolvers += "Neula releases" at "https://nexus.neula.in/repository/neula-releases"

libraryDependencies ++= Seq(
  "io.reactivecqrs" %% "reactivecqrs-core" % "1.0.0",
  "ch.qos.logback"  %  "logback-classic"   % "1.5.32"   // or any other slf4j backend
)

run / fork := true   // run the application in a JVM of its own
```

Co dają ci te dwa artefakty?

- **`reactivecqrs-api`** to wszystko, czego dotyka kod twojej domeny: `AggregateContext`, klasy bazowe
  poleceń i zdarzeń, wyniki poleceń i odpowiedzi na nie, identyfikatory. Jest mały: potrzebuje modułu
  aktorów Pekko (z jego mostem do slf4j), mpjsons, czyli biblioteki JSON, którą poznasz w kroku 4,
  i kilku pomocników.
- **`reactivecqrs-core`** to cała maszyneria: aktorzy, magazyn zdarzeń, dostarczanie, projekcje, sagi.
  Przynosi ze sobą `reactivecqrs-api`, a do tego moduły klastrowe Pekko, ScalikeJDBC i sterownik JDBC
  dla PostgreSQL.

**Skąd biorą się artefakty.** Proces budowania biblioteki publikuje je w repozytorium Mavena
`https://nexus.neula.in/repository/neula-releases` (ustawienie `publishTo` w `project/Common.scala`);
stąd linijka `resolvers` powyżej. Zapytaj swój zespół, czy potrzebujesz do niego danych logowania. Jeśli
zamiast tego pracujesz na klonie biblioteki, uruchom w nim `sbt publishLocal` i usuń linijkę `resolvers`:
sbt domyślnie zagląda do swojego lokalnego repozytorium.

Część biblioteki loguje bezpośrednio przez slf4j, a przy konfiguracji z kroku 5 trafiają tam też linie
logów Pekko (w tym te od aktorów biblioteki). Żeby slf4j cokolwiek wypisał, potrzebuje backendu; testy
i benchmarki biblioteki używają `logback-classic` 1.5.32, tak jak powyżej. `run / fork := true` ustawia
też moduł benchmarków biblioteki: twój program dostaje własną JVM, więc może się czysto zatrzymać i nie
miesza się z tą, w której działa sbt.

> **Ostrzeżenie:** Pekko nie wystartuje z modułami w różnych wersjach (ustawienie
> `pekko.fail-mixed-versions` jest włączone). ReactiveCQRS używa Pekko **1.7.1**. Jeśli dodajesz do
> projektu kolejne moduły Pekko (streams, testkit, narzędzia klastrowe), nadaj im tę samą wersję.

## Krok 3: pula połączeń

Biblioteka nigdy sama nie otwiera połączenia z bazą. Cały jej SQL przechodzi przez **domyślną pulę
połączeń ScalikeJDBC**, tę, którą ScalikeJDBC nazywa pulą *singleton*. Ta pula należy do twojej
aplikacji: to aplikacja ją tworzy, ustala jej rozmiar i ją zamyka. I musi ją utworzyć, **zanim ruszy
jakikolwiek kod biblioteki**, bo niektóre klasy biblioteki rozmawiają z bazą już w chwili, gdy powstają.

Tak robi to `SetupCheck` z przykładu E1, czyli program z końca tego rozdziału, po zaimportowaniu
`scalikejdbc.{ConnectionPool, ConnectionPoolSettings}`. Testy samej biblioteki (`TestDatabase`) robią
to samo z 20 połączeniami; tutaj pula ma ich 32 (o tej liczbie za chwilę):

<!-- snippet: e1/SetupCheck.scala#pool -->
```scala
Class.forName("org.postgresql.Driver")
ConnectionPool.singleton(
  "jdbc:postgresql://localhost:5432/reactivecqrs",
  "reactivecqrs",
  "reactivecqrs",
  ConnectionPoolSettings(initialSize = 5, maxSize = 32, connectionTimeoutMillis = 3000L))
```

- `initialSize`: połączenia otwierane na starcie.
- `maxSize`: największa liczba połączeń, jaką pula kiedykolwiek otworzy.
- `connectionTimeoutMillis`: jak długo wątek czeka na wolne połączenie, zanim się podda.

Dlaczego 32? Domyślna pula ScalikeJDBC ma 8 połączeń, ale biblioteka wykonuje operacje na bazie danych
w 32 własnych wątkach (*dispatcher blokujący* z kroku 5). Sekcja 2 w [docs/SCALING.md](../SCALING.md)
zaleca pulę co najmniej tej wielkości; benchmarki z tego dokumentu używają 40. Wtedy `max_connections`
w PostgreSQL musi pomieścić pule wszystkich twoich maszyn razem.

Gdy aplikacja się zatrzymuje, zamknij pulę wywołaniem `ConnectionPool.closeAll()`.

> **Ostrzeżenie:** jeśli zapomnisz o puli albo utworzysz ją za późno, pierwsze wywołanie biblioteki
> skończy się wyjątkiem
> `java.lang.IllegalStateException: Connection pool is not yet initialized.(name:default)`.
> Za mała pula zawodzi ciszej: pod obciążeniem wątek, który czeka na połączenie dłużej niż
> `connectionTimeoutMillis`, kończy swoją pracę błędem, a polecenie, które na to trafi, dostaje
> w odpowiedzi błąd. Kolejność jest zawsze ta sama: **najpierw pula, potem tabele, potem cała reszta.**

> **Wskazówka:** jeśli twoja aplikacja ma już `DataSource` z pulą połączeń (choćby HikariCP), przekaż
> ScalikeJDBC właśnie jego: `ConnectionPool.singleton(new DataSourceConnectionPool(dataSource))`. A adres
> URL, użytkownika i hasło czytaj z konfiguracji, zamiast wpisywać je w kod.

## Krok 4: tabele

Każda część biblioteki, która coś przechowuje, ma **klasę stanu** z metodą `initSchema()`.
`initSchema()` tworzy tabele, sekwencje, indeksy i procedury składowane, których ta część potrzebuje.
Większość tych klas przyjmuje dwóch pomocników, więc najpierw ich poznajmy.

### Pomocnik do JSON-a: MPJsons

Zdarzenia, odpowiedzi na polecenia idempotentne, postęp sag i dokumenty modeli odczytu są przechowywane
jako JSON. Biblioteka zapisuje i czyta ten JSON za pomocą **mpjsons** (`io.mpjsons.MPJsons`), małej
biblioteki, która ogląda twoje case classy przez refleksję: nie piszesz żadnych kodeków. Utwórz jedną
instancję na starcie i przekaż ją każdej klasie, która o nią prosi. Wszystkie testy i benchmarki
biblioteki używają jej z domyślnymi ustawieniami:

```scala
import io.mpjsons.MPJsons

val mpjsons = new MPJsons
```

### Słownik nazw klas: PostgresTypesNamesState

Nazwy klas są długie, a biblioteka zapisuje jedną przy każdym zdarzeniu. Dlatego prowadzi słownik
w tabeli `types_names`: każda nazwa klasy raz dostaje mały numer, a pozostałe tabele przechowują już
tylko ten numer. Tym słownikiem jest `PostgresTypesNamesState`, a prawie każda inna klasa stanu
przyjmuje go jako parametr, więc idzie on na pierwszy ogień.

### Wszystkie pięć, po kolei

W tej kolejności robi to `SetupCheck` z przykładu E1, czyli program z końca tego rozdziału:

<!-- snippet: e1/SetupCheck.scala#tables -->
```scala
val mpjsons = new MPJsons

val typesNames = new PostgresTypesNamesState().initSchema()
val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()

val sagaState = new PostgresSagaState(mpjsons, typesNames)
sagaState.initSchema()   // this one returns nothing, so it takes a line of its own

val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
```

Cztery z pięciu metod `initSchema()` zwracają sam obiekt, dlatego mieszczą się w jednej linijce. Tylko
słownik musi być pierwszy; po nim kolejność nie ma znaczenia.

| Wywołanie | Tworzy | Potrzebne do |
|---|---|---|
| `PostgresTypesNamesState.initSchema()` | `types_names` | wszystkiego: to słownik nazw klas |
| `PostgresEventStoreState.initSchema()` | `events`, `aggregates`, `events_to_publish` (outbox), `noop_events`, `snapshots`, `event_keys`, `events_archive`, `schema_steps`, `aggregate_lineage`, `history_rewrites`, `restore_failures`, `online_jobs` oraz procedury składowane, które zapisują zdarzenia | wszystkiego: to jest magazyn zdarzeń |
| `PostgresCommandResponseState.initSchema()` | `commands_responses` | każdej szyny poleceń; poleceń idempotentnych ([rozdział 5](05-command-flavors.md)) |
| `PostgresSagaState.initSchema()` | `sagas` | sag ([rozdział 12](12-sagas.md)) |
| `PostgresSubscriptionsState.initSchema()` | `subscriptions`, `subscription_failures` | projekcji ([rozdział 8](08-projections.md)) |

Kilka rzeczy samo przygotowuje sobie miejsce w bazie i nie wymaga od ciebie żadnego wywołania: generator
identyfikatorów tworzy swoje trzy sekwencje w chwili, gdy go tworzysz ([rozdział 4](04-first-command.md)),
a każdy magazyn modelu odczytu tworzy własną tabelę ([rozdział 8](08-projections.md)). Jeśli używasz
projekcji zarządzanych, ich rejestr też ma `initSchema()`: `new PostgresProjectionRegistry().initSchema()`
([rozdział 10](10-rebuilding-projections.md)).

> **Wskazówka:** `PostgresEventStoreState` po swoich dwóch parametrach przyjmuje kilka ustawień
> opcjonalnych. Dwa warto znać od pierwszego dnia: `eventSizeLimit` (najwyżej 100 000 znaków JSON-a na
> zdarzenie) i `aggregateVersionLimit` (najwyżej 10 000 zdarzeń na agregat). Polecenie, które
> przekroczyłoby którykolwiek z nich, dostaje w odpowiedzi `EventHandlingError` i nic nie zostaje
> zapisane. Wartości domyślne pasują do większości domen; [rozdział 6](06-events.md) mówi o obu limitach
> więcej, a rozdziały 13 i 14 pokazują, jak żyć z długimi historiami.

> **Ostrzeżenie:** nie pomijaj `PostgresCommandResponseState.initSchema()`, nawet jeśli nie masz jeszcze
> żadnych poleceń idempotentnych. To wywołanie tworzy unikalny indeks, na którym polegają polecenia
> idempotentne; bez niego każde polecenie idempotentne kończy się błędem.

### Bezpieczne przy każdym starcie

Wywołuj je wszystkie **przy każdym starcie aplikacji**. Są napisane tak, żeby były idempotentne: każda
instrukcja to `CREATE ... IF NOT EXISTS` albo coś z tej samej rodziny, więc w bazie, która ma już
wszystko, niczego nie zmieniają. W bazie utworzonej przez starszą wersję biblioteki dodają to, czego
brakuje (tu kolumnę, tam indeks), i właśnie tak przebiega większość aktualizacji.

Kilka maszyn startujących w tej samej chwili to też nie problem. Każde `initSchema()` działa pod blokadą
doradczą PostgreSQL (advisory lock), czyli **blokadą schematu**, więc maszyny wykonują je po kolei,
zamiast wchodzić sobie w paradę. Maszyna czeka na swoją kolej do pięciu minut, a potem się poddaje.

> **Pod maską:** to w procedurach składowanych (`add_event`, `add_events`, `add_undo_event`,
> `add_duplication_event`) mieszka blokada optymistyczna: odmawiają zapisu zdarzenia, gdy zapisana wersja
> agregatu nie jest tą, której oczekiwało polecenie. Tabele opisuje
> [sekcja 4 w ARCHITECTURE.md](../ARCHITECTURE.md), a blokadę schematu sekcja 13.2. Tych kilka ciężkich
> zadań, których może potrzebować bardzo duża, starsza baza (budowa indeksu bez blokowania zapisów,
> uzupełnienie danych w starych wierszach), to osobne wywołania, nigdy część `initSchema()`.

Znajdziesz też wersje `Memory...` kilku z tych klas (`MemoryEventStoreState`, `MemoryUidGenerator`
i spółka). To dublery testowe na potrzeby testów samej biblioteki, a nie sposób na działanie bez
PostgreSQL.

## Krok 5: konfiguracja

ReactiveCQRS czyta swoje ustawienia tak samo jak Pekko, przez Typesafe Config. Wartości domyślne mieszkają
w `reference.conf` wewnątrz `reactivecqrs-core`, a twój `application.conf` nadpisuje to, czego
potrzebujesz. Na początek `application.conf` w ogóle nie jest potrzebny. Oto, co mówią wartości domyślne:

| Klucz | Domyślnie | Do czego służy |
|---|---|---|
| `reactivecqrs.installation-number` | `0` | Który zakres identyfikatorów agregatów rozdaje ta instalacja (od numeru × 10^12 w górę). Wszystkie maszyny jednej instalacji używają tego samego numeru. |
| `reactivecqrs.blocking-dispatcher` | 32 wątki | Tu działają właściciele agregatów i praca biblioteki na bazie danych. Trzymaj jego rozmiar blisko rozmiaru puli połączeń. Tu będą działać też twoje projekcje ([rozdział 8](08-projections.md)). |
| `reactivecqrs.handler-dispatcher` | 32 wątki | Tu działają twoje handlery poleceń, więc powolny handler wstrzymuje tylko polecenia; szyny zdarzeń, projekcji ani sag nie wstrzymuje nigdy. |

Numer instalacji ma znaczenie, gdy kilka instalacji twojej aplikacji może kiedyś dzielić dane: każda
tworzy wtedy identyfikatory, jakich nie tworzy żadna inna, więc organizacja może się między nimi
przenosić ([rozdział 16](16-organizations.md)). Jeśli masz jedną instalację, domyślne `0` wystarczy;
resztę historii opowiada rozdział 17.

Reszta `reference.conf` przygotowuje grunt pod klaster (serializator komunikatów między maszynami
i limity czasu przekazania). Nie ruszaj jej aż do [rozdziału 17](17-cluster.md).

Dobry pierwszy `src/main/resources/application.conf`:

```hocon
pekko {
  loglevel = "INFO"
  loggers = ["org.apache.pekko.event.slf4j.Slf4jLogger"]
  logging-filter = "org.apache.pekko.event.slf4j.Slf4jLoggingFilter"
}

reactivecqrs {
  installation-number = 0

  # Keep these near the size of your connection pool.
  blocking-dispatcher.thread-pool-executor.fixed-pool-size = 32
  handler-dispatcher.thread-pool-executor.fixed-pool-size = 32
}
```

Blok `pekko` kieruje linie logów zapisywane przez Pekko (w tym te od aktorów biblioteki) do slf4j, przez
który loguje reszta biblioteki, więc wszystko trafia w jedno miejsce. Do tego pasujący
`src/main/resources/logback.xml`:

```xml
<configuration>
  <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    <encoder>
      <pattern>%d{HH:mm:ss.SSS} %-5level %logger{36} - %msg%n</pattern>
    </encoder>
  </appender>
  <root level="INFO">
    <appender-ref ref="STDOUT"/>
  </root>
</configuration>
```

ScalikeJDBC loguje każdą instrukcję SQL na poziomie `DEBUG`; poziom `INFO` głównego loggera trzyma je
z dala od ciebie. Obniż go do `DEBUG` w dniu, w którym zechcesz popatrzeć, jak biblioteka pracuje.

## Dom dla twojego kodu

Domena testowa w tym repozytorium ma po jednym pakiecie na agregat i po jednym pliku na każdy rodzaj
elementu. To dobry układ do naśladowania:

```
src/main/scala/com/example/shop/
  Main.scala                            the wiring: pool, tables, actors (chapter 4)
  cart/
    Model.scala                         the state: ShoppingCart, Item
    Commands.scala                      CreateShoppingCart, AddItem, RemoveItem
    Events.scala                        ShoppingCartCreated, ItemAdded, ItemRemoved
    CommandsHandlers.scala              the rules, as plain functions (optional)
    EventsHandlers.scala                how each event changes the state (optional)
    ShoppingCartContext.scala           ties it all together (chapter 3)
    ShoppingCartsListProjection.scala   a read model (chapter 8)
  invoice/
    ...                                 the next aggregate, same shape
```

Dwa pliki z handlerami są opcjonalne: małe agregaty trzymają handlery wprost w kontekście, tak jak
w rozdziale 3. Porównaj z `testdomain/src/main/scala/io/reactivecqrs/testdomain/shoppingcart/`, który ma
właśnie taki układ (jego kontekst nazywa się `ShoppingCartAggregateContext`).

> **Wskazówka:** w większej aplikacji daj domenie osobny moduł. Agregaty, polecenia, zdarzenia i konteksty
> potrzebują tylko `reactivecqrs-api`, więc moduł domeny zależy wyłącznie od niego, a moduł aplikacji
> (łączenie wszystkiego w całość, projekcje, sagi) dokłada `reactivecqrs-core`. Wtedy domena nie może
> przez przypadek sięgnąć do bazy danych.

```scala
lazy val domain = project.settings(
  libraryDependencies += "io.reactivecqrs" %% "reactivecqrs-api" % "1.0.0")

lazy val app = project.dependsOn(domain).settings(
  libraryDependencies += "io.reactivecqrs" %% "reactivecqrs-core" % "1.0.0")
```

## Sprawdźmy, czy wszystko działa

Zanim cokolwiek zbudujemy, upewnijmy się, że warsztat jest gotowy. Ten mały program, `SetupCheck`
z przykładu E1, wykonuje kroki 3 i 4 i nic poza tym:

<!-- snippet: e1/SetupCheck.scala#check -->
```scala
import io.mpjsons.MPJsons
import io.reactivecqrs.core.commandhandler.PostgresCommandResponseState
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.saga.PostgresSagaState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

object SetupCheck {

  final case class Tables(eventStore: PostgresEventStoreState, responses: PostgresCommandResponseState,
                          sagaState: PostgresSagaState, subscriptions: PostgresSubscriptionsState)

  def main(args: Array[String]): Unit = {
    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton(
      "jdbc:postgresql://localhost:5432/reactivecqrs",
      "reactivecqrs",
      "reactivecqrs",
      ConnectionPoolSettings(initialSize = 5, maxSize = 32, connectionTimeoutMillis = 3000L))
    try {
      initTables()
      println("The tables are ready.")
    } finally {
      ConnectionPool.closeAll()
    }
  }

  def initTables(): Tables = {
    val mpjsons = new MPJsons

    val typesNames = new PostgresTypesNamesState().initSchema()
    val eventStore = new PostgresEventStoreState(mpjsons, typesNames).initSchema()
    val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()

    val sagaState = new PostgresSagaState(mpjsons, typesNames)
    sagaState.initSchema()   // this one returns nothing, so it takes a line of its own

    val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
    Tables(eventStore, responses, sagaState, subscriptions)
  }
}
```

Umieść go w pakiecie swojego projektu i uruchom poleceniem `sbt "runMain com.example.shop.SetupCheck"`
(w tym repozytorium: `sbt "examples/runMain io.reactivecqrs.examples.e1.SetupCheck"`). Program wypisuje
`The tables are ready.`, a `\dt` w `psql` pokazuje siedemnaście tabel: `aggregate_lineage`, `aggregates`,
`commands_responses`, `event_keys`, `events`, `events_archive`, `events_to_publish`,
`history_rewrites`, `noop_events`, `online_jobs`, `restore_failures`, `sagas`, `schema_steps`,
`snapshots`, `subscription_failures`, `subscriptions` i `types_names`. Wszystkie puste, wszystkie czekają. (Tę listę sprawdza
`SetupCheckSpec`.)

Teraz uruchom go drugi raz. Ten sam komunikat, żadnych błędów, nic się nie zmieniło: to idempotentność
z kroku 4 w działaniu.

## Co już umiesz

- Potrzebujesz JDK 25 lub nowszego, Scali 2.13, sbt i PostgreSQL; Pekko, ScalikeJDBC i sterownik JDBC
  biblioteka przynosi ze sobą.
- Biblioteka to `io.reactivecqrs %% reactivecqrs-api` (dla twojej domeny)
  i `io.reactivecqrs %% reactivecqrs-core` (maszyneria), w wersji `1.0.0`.
- Twoja aplikacja musi utworzyć domyślną pulę połączeń ScalikeJDBC, z co najmniej 32 połączeniami,
  **zanim** ruszy jakikolwiek kod biblioteki.
- Każda klasa stanu tworzy swoje tabele przez `initSchema()`; słownik nazw klas idzie pierwszy, a każde
  z tych wywołań można bezpiecznie powtarzać przy każdym starcie, nawet z kilku maszyn naraz.
- Jedna instancja `MPJsons`, utworzona przez `new MPJsons`, obsługuje całą aplikację.
- Wartości domyślne z `reference.conf` (numer instalacji, dispatcher blokujący i dispatcher handlerów)
  działają od razu; `application.conf` je nadpisuje.

Dalej: [rozdział 3](03-first-aggregate.md) kładzie na stole warsztatowym pierwszy agregat.

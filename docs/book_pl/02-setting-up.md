# Rozdział 2. Konfiguracja warsztatu

Pod koniec tego rozdziału będziesz mieć projekt Scala, który kompiluje się z ReactiveCQRS,
Baza danych PostgreSQL gotowa na to i kilka linii startowych, których potrzebuje każda aplikacja:
pula połączeń, tabele biblioteki i plik konfiguracyjny. To najmniej efektowny rozdział
książki i ten, który oszczędzi ci najwięcej główkowania później. Posprzątajmy stół warsztatowy.

---

## Czego potrzebujesz na swoim biurku

- **JDK w wersji 17 lub nowszej.** Biblioteka jest skompilowana dla Java 17 (w wersji `-release 17`
  kompilacja). Plan wydania 1.0 zakłada przejście na Javę 21, więc jeśli wybierasz dziś JDK,
  21 pozwala zaoszczędzić na późniejszej aktualizacji.
- **Scala 2.13.** Biblioteka została skompilowana przy użyciu języka Scala 2.13.18 i opublikowana wyłącznie dla wersji 2.13.
- **sbt**, narzędzie do kompilacji w Scali. Dowolna wersja 1.x wystarczy; kompilacja biblioteki korzysta z wersji 1.11.7.
- **PostgreSQL**, działający w miejscu, do którego Twój komputer ma dostęp. Pomiary biblioteki (patrz
  [docs/SCALING.md](../SCALING.md)) działało na bazie PostgreSQL 17.4, więc nowsza wersja będzie bezpiecznym wyborem.
- **Klient SQL**, taki jak `psql`, aby podejrzeć, co zapisuje biblioteka. Opcjonalny, ale połowa zabawy.

Nie instalujesz samodzielnie Pekko, ScalikeJDBC ani sterownika PostgreSQL: są one dołączone do biblioteki.

## Krok 1: baza danych dla biblioteki

ReactiveCQRS przechowuje wszystko w PostgreSQL: zdarzenia, agregaty, modele odczytu, postęp sagi.
To baza danych i użytkownik. W `psql`, połączony jako superużytkownik:

```sql
UTWÓRZ UŻYTKOWNIKA reactivecqrs Z HASŁEM 'reactivecqrs';
UTWÓRZ BAZĘ DANYCH reactivecqrs WŁAŚCICIEL reactivecqrs;
```

To są właśnie te nazwy, których oczekuje własny zestaw testów biblioteki: baza danych `reactivecqrs` na
`localhost:5432`, z użytkownikiem i hasłem `reactivecqrs` (patrz `TestDatabase` w `testutils`
moduł). Dzięki temu ta jedna baza danych służy do przeprowadzania eksperymentów i, kiedy tylko chcesz, do uruchamiania
Testy biblioteki. Do swojej prawdziwej aplikacji wybierz własne nazwy i hasło, którego nikt nie odgadnie.

> **Wskazówka:** biblioteka tworzy swoje tabele w dowolnym schemacie używanym przez połączenie, więc jedna baza danych
> może pomieścić kilka niezależnych kopii. Uruchom `CREATE SCHEMA shop AUTHORIZATION reactivecqrs;` i dodaj
> `?currentSchema=shop` do adresu URL JDBC z kroku 3. Zestaw testów wykonuje tę samą sztuczkę: każde uruchomienie
> otrzymuje nowy schemat o nazwie `test_run_<sekundy>_<liczba>`, a schematy starsze niż sześć godzin są
> upuszczono.

## Krok 2: dodaj bibliotekę do kompilacji

ReactiveCQRS jest publikowany jako dwa artefakty. Oto jego współrzędne, prosto z biblioteki
własna kompilacja (`project/Common.scala` i `build.sbt`):

| | |
|---|---|
| Organizacja | `io.reactivecqrs` |
| Artefakty | `reactivecqrs-api` i `reactivecqrs-core` |
| Wersja | `1.0.0-SNAPSHOT` |
| Sufiks Scala | tak (`_2.13`), więc użyj `%%` |

Minimalny plik `build.sbt` dla aplikacji:

```scala
ThisBuild / scalaVersion := "2.13.18"

resolvers += „Neula releases” na stronie „https://nexus.neula.in/repository/neula-releases”

Zależności biblioteki ++= Seq(
  „io.reactivecqrs” %% „reactivecqrs-core” % „1.0.0-SNAPSHOT”,
  „ch.qos.logback” % „logback-classic” % „1.5.32” // lub dowolny inny back-end slf4j
)

run / fork := true // uruchom aplikację na własnej maszynie wirtualnej Java
```

Co dają ci te dwa artefakty?

- **`reactivecqrs-api`** to wszystko, czego dotyka kod Twojej domeny: `AggregateContext`, polecenie i
  Klasy bazowe zdarzeń, wyniki i odpowiedzi poleceń, identyfikatory. Jest mały: wymaga modułu aktora Pekko.
  (z mostem slf4j), mpjsons, bibliotekę JSON, którą poznasz w kroku 4, i kilka pomocniczych programów.
- **`reactivecqrs-core`** to mechanizm: aktorzy, magazyn zdarzeń, dostawa, projekcje,
  sagi. Zawiera `reactivecqrs-api`, a także moduły klastra Pekko, ScalikeJDBC i
  Sterownik JDBC PostgreSQL.

**Skąd pochodzą artefakty.** Kompilacja biblioteki publikuje je w repozytorium Maven
`https://nexus.neula.in/repository/neula-releases` (ustawienie `publishTo` w
`project/Common.scala`); to jest wiersz `resolvers` powyżej. Zapytaj swój zespół, czy potrzebujesz
poświadczenia dla niego. Jeśli zamiast tego pracujesz na klonie biblioteki, uruchom `sbt publishLocal` w
sklonuj i usuń linię `resolvers`: sbt domyślnie przeszukuje lokalne repozytorium.

Części biblioteki logują się bezpośrednio przez slf4j oraz za pomocą konfiguracji z kroku 5 dziennika Pekko
linie (w tym te należące do aktorów biblioteki) również tam trafiają. slf4j potrzebuje zaplecza do drukowania
cokolwiek; własne testy i testy porównawcze biblioteki wykorzystują `logback-classic` 1.5.32, jak powyżej.
`run / fork := true` to również to, co robi moduł benchmark biblioteki: Twój program otrzymuje maszynę wirtualną Java (JVM) swojej maszyny wirtualnej.
własny, więc może zatrzymać się czysto i nie zostanie pomylony z sbt.

> **Ostrzeżenie:** Pekko odmawia uruchomienia, gdy jego moduły występują w różnych wersjach (jego
> Ustawienie `pekko.fail-mixed-versions` jest włączone). ReactiveCQRS używa Pekko **1.7.1**. Jeśli dodasz więcej
> Moduły Pekko do kompilacji (strumienie, zestaw testowy, narzędzia klastra) – nadaj im tę samą wersję.

## Krok 3: pula połączeń

ReactiveCQRS nigdy nie otwiera połączenia z bazą danych samodzielnie. Cały jego kod SQL przechodzi przez
**Domyślna pula połączeń ScalikeJDBC**, ta, którą ScalikeJDBC nazywa pulą *singleton*.
Aplikacja jest właścicielem tej puli: tworzy ją, ustala jej rozmiar i zamyka. Musi ją utworzyć **przed
każdy kod biblioteczny jest uruchamiany**, ponieważ niektóre klasy biblioteczne komunikują się z bazą danych w momencie ich uruchomienia
zrobiony.

W ten sposób działają własne testy biblioteki (TestDatabase), przy czym pula testów została rozszerzona z 20
połączenia do numeru 32 (więcej o tym numerze poniżej):

```scala
import scalikejdbc.{PołączeniowyPula, UstawieniaPołączeniowegoPula}

Klasa.forName("org.postgresql.Driver")
ConnectionPool.singleton(
  "jdbc:postgresql://localhost:5432/reactivecqrs",
  „reaktywne cqrs”,
  „reaktywne cqrs”,
  Ustawienia puli połączeń (rozmiar początkowy = 5, rozmiar maksymalny = 32, limit czasu połączenia w milionach (connectionTimeoutMillis) = 3000L))
```

- `initialSize`: połączenia otwierane na początku.
- `maxSize`: największa liczba połączeń, jakie pula kiedykolwiek otworzy.
- `connectionTimeoutMillis`: jak długo wątek czeka na wolne połączenie, zanim zrezygnuje.

Dlaczego 32? Domyślnie ScalikeJDBC ma pulę 8 połączeń, ale biblioteka wykonuje swoją pracę w bazie danych
na 32 własnych wątkach (*blokujący dyspozytor* kroku 5). [docs/SCALING.md](../SCALING.md),
Sekcja 2 zaleca pulę co najmniej tak dużą; jej testy porównawcze wykorzystują 40.
`max_connections` musi zatem obejmować pule wszystkich twoich maszyn jednocześnie.

Po zatrzymaniu aplikacji zamknij pulę poleceniem `ConnectionPool.closeAll()`.

> **Ostrzeżenie:** zapomnij o puli lub utwórz ją zbyt późno, a pierwsze wywołanie biblioteki zakończy się niepowodzeniem
> `java.lang.IllegalStateException: Pula połączeń nie została jeszcze zainicjowana.(name:default)`.
> Zbyt mała pula ulega awarii ciszej: pod obciążeniem wątek, który czeka dłużej niż
> `connectionTimeoutMillis` dla połączenia nie działa, a polecenie, które to powoduje, jest
> odpowiedziałem z błędem. Kolejność jest zawsze taka sama: **najpierw basen, potem stoły, a potem
> wszystko inne.**

> **Wskazówka:** jeśli Twoja aplikacja ma już zbiorcze `Źródło danych` (np. HikariCP), przekaż je do
> Zamiast tego ScalikeJDBC: `ConnectionPool.singleton(new DataSourceConnectionPool(dataSource))`. I
> odczytaj adres URL, nazwę użytkownika i hasło z konfiguracji zamiast wpisywać je do kodu.

## Krok 4: tabele

Każda część biblioteki przechowująca coś ma **klasę stanu** z `initSchema()`
metoda `initSchema()` tworzy tabele, sekwencje, indeksy i procedury składowane, które są częścią
potrzeby. Większość z nich potrzebuje dwóch pomocników, więc poznajmy ich najpierw.

### Pomocnik JSON: MPJsons

Wydarzenia, odpowiedzi na polecenia idempotentne, postęp sagi i dokumenty modelu odczytu są przechowywane jako
JSON. Biblioteka zapisuje i odczytuje ten JSON za pomocą **mpjsons** (`io.mpjsons.MPJsons`), małego
Biblioteka, która analizuje klasy przypadków poprzez refleksję: nie piszesz żadnych kodeków. Utwórz jedną instancję
na początku i przekaż go każdej klasie, która o niego poprosi. Wszystkie specyfikacje i testy porównawcze biblioteki go wykorzystują.
z ustawieniami domyślnymi:

```scala
importuj io.mpjsons.MPJsons

val mpjsons = nowy MPJsons
```

### Słownik nazw klas: PostgresTypesNamesState

Nazwy klas są długie, a biblioteka zapisuje je przy każdym zdarzeniu. Dlatego przechowuje słownik w
tabela o nazwie `types_names`: każda nazwa klasy otrzymuje małą liczbę raz, a pozostałe tabele przechowują
liczba. Ten słownik to `PostgresTypesNamesState` i prawie każda inna klasa stanu go bierze
jako parametr, więc jest na pierwszym miejscu.

### Wszystkie pięć, w kolejności

Jest to kolejność, w jakiej biblioteka korzysta z własnego okablowania (`ReactiveTestDomainSpec` w `testdomain`
moduł):

```scala
importuj io.mpjsons.MPJsons
import io.reactivecqrs.core.commandhandler.PostgresCommandResponseState
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.saga.PostgresSagaState
import io.reactivecqrs.core.types.PostgresTypesNamesState

val mpjsons = nowy MPJsons

val typesNames = new PostgresTypesNamesState().initSchema()
val eventStore = nowy PostgresEventStoreState(mpjsons, typesNames).initSchema()
val responses = new PostgresCommandResponseState(mpjsons, typesNames).initSchema()

val sagaState = new PostgresSagaState(mpjsons, typesNames)
sagaState.initSchema() // ta funkcja nie zwraca niczego, więc przyjmuje własną linię

val subscriptions = new PostgresSubscriptionsState(typesNames).initSchema()
```

Cztery z pięciu metod `initSchema()` zwracają sam obiekt, dlatego mieszczą się na jednym
wiersz. Tylko słownik musi być pierwszy; po nim kolejność nie ma znaczenia.

| Zadzwoń | Tworzy | Potrzebne do |
|---|---|---|
| `PostgresTypesNamesState.initSchema()` | `types_names` | wszystko: słownik nazw klas |
| `PostgresEventStoreState.initSchema()` | `events`, `aggregates`, `events_to_publish` (skrzynka nadawcza), `noop_events`, `snapshots`, `event_keys`, `events_archive`, `schema_steps`, `aggregate_lineage`, `history_rewrites` oraz procedury składowane, które zapisują zdarzenia | wszystko: to jest magazyn zdarzeń |
| `PostgresCommandResponseState.initSchema()` | `commands_responses` | każda magistrala poleceń; polecenia idempotentne ([rozdział 5](05-command-flavors.md)) |
| `PostgresSagaState.initSchema()` | `sagas` | sagas ([rozdział 12](12-sagas.md)) |
| `PostgresSubscriptionsState.initSchema()` | `subskrypcje`, `subskrypcje_błędy` | projekcje ([rozdział 8](08-projections.md)) |

Kilka rzeczy tworzy własną pamięć masową i nie wymaga od Ciebie żadnego połączenia: generator identyfikatorów tworzy swój własny
trzy sekwencje, gdy je tworzysz ([rozdział 4](04-pierwsze-polecenie.md)) i każdy model odczytu przechowuje
tworzy własną tabelę ([rozdział 8](08-projections.md)). Jeśli używasz zarządzanych projekcji, ich
rejestr ma również `initSchema()`: `new PostgresProjectionRegistry().initSchema()`
([rozdział 10](10-rebuilding-projections.md)).

> **Wskazówka:** `PostgresEventStoreState` przyjmuje kilka opcjonalnych ustawień po dwóch parametrach.
> Warto znać dwa od pierwszego dnia: `eventSizeLimit` (maksymalnie 100 000 znaków JSON na
> event) i `aggregateVersionLimit` (maksymalnie 10 000 zdarzeń na agregat). Polecenie, które
> przerwanie albo kończy się błędem `EventHandlingError` i nic nie jest zapisywane. Domyślne ustawienia są odpowiednie
> większość domen; [rozdział 6](06-events.md) mówi więcej o obu ograniczeniach, a rozdziały 13 i 14 pokazują
> jak żyć z długą historią.

> **Ostrzeżenie:** nie pomijaj `PostgresCommandResponseState.initSchema()`, nawet jeśli nie masz
> polecenia idempotentne. Tworzy unikalny indeks, na którym opierają się polecenia idempotentne; bez
> to każde polecenie idempotentne kończy się niepowodzeniem.

### Bezpieczny do biegania przy każdym starcie

Wywołuj wszystkie te funkcje **przy każdym uruchomieniu aplikacji**. Są napisane tak, aby były idempotentne: za każdym razem
polecenie to `CREATE ... IF NOT EXISTS` lub jego odpowiednik, więc w bazie danych, która ma wszystko, co
Nic nie zmieniają. W bazie danych utworzonej przez starszą wersję biblioteki dodają to, czego brakuje.
(kolumna tu, indeks tam), w ten sposób odbywa się większość uaktualnień.

Kilka maszyn uruchamianych w tym samym momencie również działa dobrze. Każdy `initSchema()` działa pod
Blokada doradcza PostgreSQL, **blokada schematu**, dzięki której maszyny pracują na zmianę, zamiast się potykać
Maszyna czeka na swoją kolej do pięciu minut, zanim się podda.

> **Pod maską:** procedury składowane (`add_event`, `add_events`, `add_undo_event`,
> `add_duplication_event`) to miejsce, w którym znajduje się blokada optymistyczna: odmawiają zapisania zdarzenia, gdy
> Zapisana wersja agregatu nie jest tą, której oczekiwało polecenie. Tabele są opisane w
> [ARCHITECTURE.md, sekcja 4](../ARCHITECTURE.md) i blokada schematu w sekcji 13.2. Kilka
> ciężkie zadania, których może wymagać bardzo duża, starsza baza danych (budowanie indeksu bez blokowania zapisu,
> wypełnianie danych dla starych wierszy) to osobne wywołania, nigdy nie będące częścią `initSchema()`.

Znajdziesz również wersje `Memory...` kilku z tych klas (`MemoryEventStoreState`,
`MemoryUidGenerator` i podobne). Są to dublety testowe dla własnych testów biblioteki, a nie sposób na
uruchomić bez PostgreSQL.

## Krok 5: konfiguracja

ReactiveCQRS odczytuje swoje ustawienia tak jak Pekko, za pomocą Typesafe Config. Jego ustawienia domyślne są dostępne w
`reference.conf` wewnątrz `reactivecqrs-core`, a twój `application.conf` zastępuje wszystko, co
Potrzeba. Aby zacząć, nie potrzebujesz żadnego pliku `application.conf`. Oto, co mówią ustawienia domyślne:

| Klucz | Domyślny | Co robi |
|---|---|---|
| `reactivecqrs.installation-number` | `0` | Jaki zakres identyfikatorów zbiorczych udostępnia ta instalacja (liczba × 10^12 w górę). Każdy komputer w danej instalacji używa tego samego numeru. |
| `reactivecqrs.blocking-dispatcher` | 32 wątki | Miejsce, w którym działają właściciele agregatów i baza danych biblioteki. Utrzymuj rozmiar zbliżony do rozmiaru puli połączeń. Twoje projekcje również będą tu uruchamiane ([rozdział 8](08-projections.md)). |
| `reactivecqrs.handler-dispatcher` | 32 wątki | Miejsce, w którym działają procedury obsługi poleceń, więc powolna procedura wstrzymuje tylko polecenia, nigdy magistralę zdarzeń, projekcje ani sagi. |

Numer instalacji ma znaczenie, gdy pewnego dnia może nastąpić kilka instalacji Twojej aplikacji.
udostępniają swoje dane: każdy z nich tworzy identyfikatory, a żadna inna instalacja nie jest wykonywana, więc organizacja może się przemieszczać
między nimi ([rozdział 16](16-organizations.md)). Jeśli uruchamiasz pojedynczą instalację, domyślne `0` jest w porządku; rozdział
Resztę historii opowiada numer 17.

Pozostała część pliku `reference.conf` przygotowuje grunt pod klaster (serializator wiadomości pomiędzy
maszyny i przekroczenia limitu czasu przekazania). Pozostaw to w spokoju do [rozdziału 17](17-cluster.md).

Dobry pierwszy `src/main/resources/application.conf`:

```hocon
punk {
  poziom logu = "INFO"
  rejestratory = ["org.apache.pekko.event.slf4j.Slf4jLogger"]
  filtr-logowania = "org.apache.pekko.event.slf4j.Slf4jLoggingFilter"
}

reaktywne cqrs {
  numer instalacji = 0

  # Utrzymuj je w granicach rozmiaru puli połączeń.
  blokujący-dyspozytor.pula-wątków-executor.stały-rozmiar-puli = 32
  handler-dispatcher.thread-pool-executor.fixed-pool-size = 32
}
```

Blok `pekko` wysyła linie dziennika zapisane przez Pekko (linie aktorów biblioteki wśród
je) do slf4j, gdzie loguje się reszta biblioteki, więc wszystko trafia w jedno miejsce. I
pasujące `src/main/resources/logback.xml`:

```xml
<konfiguracja>
  <nazwa appendera="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    <koder>
      <pattern>%d{HH:mm:ss.SSS} %-5level %logger{36} - %msg%n</pattern>
    </koder>
  </appender>
  <root level="INFO">
    <appender-ref ref="WYJŚCIE STANDARDOWE"/>
  </root>
</konfiguracja>
```

ScalikeJDBC rejestruje każde polecenie SQL na poziomie `DEBUG`; główny poziom `INFO` uniemożliwia ich dostęp
po swojemu. Przełącz na `DEBUG` w dzień, w którym chcesz oglądać bibliotekę w pracy.

## Dom dla Twojego kodu

Domena testowa w tym repozytorium przechowuje jeden pakiet na agregat, z jednym plikiem na rodzaj
rzecz. To dobry kształt do skopiowania:

```
src/main/scala/com/example/shop/
  Main.scala okablowanie: basen, stoły, aktorzy (rozdział 4)
  wózek/
    Model.scala stan: Koszyk, Element
    Polecenia.scala Utwórz koszyk, Dodaj element, Usuń element
    Events.scala Utworzono koszyk, dodano element, usunięto element
    CommandsHandlers.scala reguły jako zwykłe funkcje (opcjonalnie)
    EventsHandlers.scala – jak każde zdarzenie zmienia stan (opcjonalnie)
    ShoppingCartAggregateContext.scala łączy wszystko w całość (rozdział 3)
    ShoppingCartsListProjection.scala – model odczytu (rozdział 8)
  Faktura/
    ...następny agregat, ten sam kształt
```

Dwa pliki obsługi są opcjonalne: małe agregaty przechowują swoje procedury obsługi bezpośrednio w kontekście,
jak w rozdziale 3. Porównaj z `testdomain/src/main/scala/io/reactivecqrs/testdomain/shoppingcart/`,
który ma dokładnie taki kształt.

> **Wskazówka:** w większej aplikacji, dodaj domenę do własnego modułu. Agregaty, polecenia,
> zdarzenia i konteksty wymagają tylko `reactivecqrs-api`, więc moduł domeny zależy tylko od tego,
> a moduł aplikacji (okablowanie, projekcje, sagi) dodaje `reactivecqrs-core`. Domena
> wtedy nie można dotrzeć do bazy danych przez przypadek.

```scala
leniwa domena val = project.settings(
  Zależności biblioteki += "io.reactivecqrs" %% "reactivecqrs-api" % "1.0.0-SNAPSHOT")

leniwa aplikacja val = project.dependsOn(domena).settings(
  Zależności biblioteki += "io.reactivecqrs" %% "reactivecqrs-core" % "1.0.0-SNAPSHOT")
```

## Sprawdzanie, czy wszystko działa

Udowodnijmy, że warsztat jest gotowy, zanim cokolwiek w nim zbudujemy. Ten mały program wykonuje kroki 3.
i 4 i nic więcej:

```scala
pakiet com.example.shop

importuj io.mpjsons.MPJsons
import io.reactivecqrs.core.commandhandler.PostgresCommandResponseState
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.saga.PostgresSagaState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import scalikejdbc.{PołączeniowyPula, UstawieniaPołączeniowegoPula}

obiekt SetupCheck {
  def main(args: Array[String]): Unit = {
    Klasa.forName("org.postgresql.Driver")
    ConnectionPool.singleton("jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs",
      Ustawienia puli połączeń (rozmiar początkowy = 5, rozmiar maksymalny = 32, limit czasu połączenia w milionach (connectionTimeoutMillis) = 3000L))
    próbować {
      val mpjsons = nowy MPJsons
      val typesNames = new PostgresTypesNamesState().initSchema()
      nowy PostgresEventStoreState(mpjsons, typesNames).initSchema()
      nowy PostgresCommandResponseState(mpjsons, typesNames).initSchema()
      nowy PostgresSagaState(mpjsons, typesNames).initSchema()
      nowy PostgresSubscriptionsState(typesNames).initSchema()
      println("Tabele są gotowe.")
    } Wreszcie {
      ConnectionPool.closeAll()
    }
  }
}
```

Uruchom poleceniem `sbt "runMain com.example.shop.SetupCheck"`. Wyświetli się komunikat `Tabele są gotowe.` i
`\dt` w `psql` wyświetla trzynaście tabel: `aggregates`, `commands_responses`, `event_keys`, `events`,
`archiwum_wydarzeń`, `wydarzenia_do_opublikowania`, `wydarzenia_noop`, `sagi`, `kroki_schematu`, `migawki`,
`subscription_failures`, `subscriptions` i `types_names`. Wszystkie puste, wszystkie oczekujące.

Teraz uruchom go ponownie. Ten sam komunikat, żadnych błędów, nic się nie zmieniło: to jest idempotentność
krok 4 w działaniu.

## Czego się nauczyłeś

- Potrzebny jest JDK 17 lub nowszy, Scala 2.13, sbt i PostgreSQL; biblioteka zawiera Pekko, ScalikeJDBC
  i sterownik JDBC.
- Biblioteka to `io.reactivecqrs %% reactivecqrs-api` (dla Twojej domeny) i
  `io.reactivecqrs %% reactivecqrs-core` (maszyna), wersja `1.0.0-SNAPSHOT`.
- Twoja aplikacja musi utworzyć domyślną pulę połączeń ScalikeJDBC **przed** jakimkolwiek kodem biblioteki
  przebiegów, z co najmniej 32 połączeniami.
- Każda klasa stanu tworzy swoje tabele za pomocą `initSchema()`; najpierw jest umieszczany słownik nazw klas,
  i wszystkie z nich można bezpiecznie wywołać przy każdym uruchomieniu, nawet z kilku maszyn jednocześnie.
- Jedna instancja `MPJsons` utworzona przy użyciu `new MPJsons` obsługuje całą aplikację.
- Domyślne ustawienia w pliku `reference.conf` (numer instalacji, blokowanie i obsługa dyspozytorów) działają
  gotowe; `application.conf` je zastępuje.

Następnie: [rozdział 3](03-first-aggregate.md) umieszcza pierwszy agregat na stole roboczym.
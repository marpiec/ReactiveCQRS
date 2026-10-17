# Załącznik C. Rozwiązywanie problemów

Na coś udzielono odpowiedzi z błędem, na ekranie wyświetlają się wczorajsze dane lub dziennik krzyczy
„Niedostarczone”. Bez paniki: ReactiveCQRS został stworzony tak, aby nic nie ginęło bezgłośnie i prawie
Każdy problem pozostawia wyraźny ślad. Ten dodatek omawia objawy, prawdopodobne przyczyny i sposoby postępowania.
Znajdź swój objaw w nagłówkach, przeczytaj przyczyny w kolejności (najczęstsze są pierwsze) i działaj
zejść po schodach.

---

## Po pierwsze, trzy miejsca, w których warto szukać

1. **Dziennik.** `CommandHandlingError` i `EventHandlingError` zawierają `errorId`, losowy ciąg znaków
   z 16 liter i cyfr. Wyszukaj go w dzienniku: wiersz z nim wskazuje przyczynę,
   wyjątek.
2. **Komunikaty o stanie.** Każda część biblioteki odpowiada na pytanie o stan: magistrala zdarzeń
   (`GetDeliveryStatus`), sonda skrzynki nadawczej (`GetOutboxStatus`), każda projekcja
   (`GetProjectionStatus`), każdy nadzorca zarządzanej projekcji (`GetBuilds`) i squash
   harmonogram (`GetSquashStatus`). [Dodatek B](appendix-b-cheat-sheet.md) pokazuje, na co odpowiadają poszczególne pytania.
3. **Metryki.** Po wywołaniu `ReactiveMetrics(system).use(...)` zobaczysz wyniki polecenia i
   czasy, poziom skrzynki nadawczej, dostawy oczekujące i wstrzymane oraz awarie projekcji
   ([rozdział 18](18-production.md)).

---

## Polecenia

### Polecenie odpowiada `CommandHandlingError`

**Co widzisz:** `CommandHandlingError(commandName, errorId, commandId)`. Nic nie zostało zapisane.

**Prawdopodobne przyczyny**, na podstawie tego, co jest napisane w dzienniku obok `errorId`:

| W dzienniku jest napisane | Co się stało |
|---|---|
| `Polecenie AddItem przekroczyło limit czasu po 60 sekundach i nie zostało zastosowane, identyfikator błędu: [...]` | Obsługujący nie zakończył działania w ramach limitu czasu `commandTimeout` kontekstu. W trybie szeregowym czas jest liczony od momentu nadejścia polecenia, więc czas oczekiwania w kolejce również się liczy. |
| `Obsługa błędu polecenia AddItem, errorId: [...]`, następnie wyjątek | Obsługa polecenia zgłosiła błąd lub nie ma przypadku dla tego polecenia (`MatchError`). Albo agregat nie istnieje (`Aggregate 1234 nie istnieje`) lub został usunięty (`jest już usunięty`). Albo nie można go odczytać z magazynu (`AggregateRestoreFailedException`). |
| `Przywracanie agregatu 1234 typu ShoppingCart nie powiodło się, spróbuj ponownie za 5 sekund` | Właściciel nie mógł odczytać agregatu: baza danych jest niedostępna lub zapisane zdarzenie nie jest już odczytywane (patrz [poniżej](#a-stored-event-no-longer-reads-after-a-class-change)). Przez 5 sekund każde polecenie do niego powoduje ten błąd; następnie nowy właściciel próbuje ponownie. |
| `Brak puli identyfikatorów agregatu dla ShoppingCart` i `Nie można skierować polecenia ... bez identyfikatorów` | Generator identyfikatorów UID nie przekazał identyfikatorów; `commandId` to zatem `CommandId(0)`. Następne polecenie pyta ponownie. |

Przekroczenia limitu czasu mają klasyczną przyczynę: procedury obsługi wątków, które je zatrzymują. Procedury obsługi poleceń działają na
`reactivecqrs.handler-dispatcher`, 32 wątki. Zmierzone na jednej maszynie
([docs/perf/heavy-M5.md](../perf/heavy-M5.md), 17/10/2026): 36 modułów obsługi trwających po 5 sekund każdy, wysłanych w
raz, wszystkie się powiodły, ostatnie po 10,0 sekundach, bo 32 uruchomiono na raz; lekkie polecenie na
inny agregat czekał tymczasem 5,4 sekundy.

**Co zrobić:**

1. Znajdź `errorId` w dzienniku i odczytaj wyjątek.
2. W przypadku powolnego pracownika przenieś powolną pracę poza dowództwo: wykonuje ją saga lub pracownik, a
   drugie polecenie rejestruje wynik ([rozdział 12](12-sagas.md)). Jeśli polecenie musi to zrobić,
   zwróć `AsyncCommandResult`, którego `Future` działa w Twojej własnej puli wątków i podnieś
   `commandTimeout` dla tego kontekstu.
3. Ponów próbę w bezpieczny sposób: nadaj poleceniom klucz idempotentności ([rozdział 5](05-command-flavors.md)), aby wysyłanie
   Nigdy nie można zastosować tego dwa razy.
4. W przypadku niepowodzenia przywracania sprawdź najpierw bazę danych, a następnie zapoznaj się z poniższą sekcją dotyczącą zapisanych zdarzeń.

> **Pamiętaj:** polecenie, które przekroczyło limit czasu, nigdy nie jest później zapisywane, nawet jeśli jego obsługa zostanie zakończona
> później. Niezależnie od przyczyny, `CommandHandlingError` oznacza, że ​​to polecenie niczego nie zapisało.

### Polecenie odpowiada `AggregateConcurrentModificationError`

**Co widzisz:** `AggregateConcurrentModificationError(aggregateId, aggregateType, expected, was)`.
Nic nie zostało zapisane. `expected` to wersja, którą nazwałeś poleceniem, `wa` to wersja zbiorcza
ma.

**Prawdopodobne przyczyny:**

- **Ktoś inny dotarł tam pierwszy.** Inna komenda zmieniła agregat po tym, jak go przeczytałeś. To
  czy optymistyczne blokowanie spełnia swoje zadanie ([rozdział 5](05-command-flavors.md)).
- **`was` ma wersję 0:** agregat nie istnieje. Sprawdź identyfikator i magistralę poleceń, którą wysłałeś.
  do tego.
- **Funkcja `ConcurrentCommand` kolidowała 10 razy z rzędu** z zapisami, których jej właściciel sam nie wykonał:
  właścicieli tego samego agregatu na innych węzłach (typ „Lokalny” w klastrze) lub zapisy dokonywane poza nim
  biblioteka. Konflikty między poleceniami jednego właściciela nigdy nie wyczerpują tych prób.
- **FirstCommand`, którego `chosenAggregateId` już istnieje:** utworzenie zostało wysłane dwukrotnie
  bez klucza idempotentności lub identyfikator został użyty wcześniej.

**Co zrobić:**

1. Gdy użytkownik edytuje: ponownie odczytaj agregat (`GetAggregate`), wyświetl nowy stan i
   wyślij polecenie ponownie z nową `expectedVersion`.
2. Jeśli kolejność nie ma znaczenia (dodanie przedmiotu do koszyka), użyj polecenia `ConcurrentCommand`:
   Biblioteka uruchamia ją ponownie w nowym stanie.
3. Gdy wielu autorów jednocześnie tworzy jeden zbiór, przełącz kontekst na tryb seryjny
   (`override val commandProcessing: CommandProcessing = CommandProcessing.Serial()`), co nigdy nie
   konflikty. W klastrze typ „Sharded” przekazuje każde polecenie agregatu jednemu właścicielowi.
   Jeszcze lepiej, rozłóż gorący kruszywo.
4. W przypadku kreacji, które mogą być wysyłane dwukrotnie, połącz `chosenAggregateId` z kluczem idempotencji:
   druga kopia otrzymuje odpowiedź `SuccessResponse` z pierwszej kopii zamiast błędu.

> **Wskazówka:** polecenia współbieżne radzą sobie z wieloma zadaniami. Pomiary na jednej maszynie
> ([docs/perf/load-M5.md](../perf/load-M5.md), 17/10/2026): 10 000 współbieżnych poleceń wysłanych w
> raz w jednym agregacie wszystkie wyniki się powiodły, 2 621 na sekundę.

### Polecenie odpowiada na `FailureResponse`, ale nie Twoją wiadomością

**Co widzisz:** `FailureResponse(exceptions)`. Zwykle jest to Twoja własna wiadomość od
`CommandFailure`. Jeśli nie, biblioteka odrzuciła polecenie przed jego uruchomieniem:

| Wiadomość zaczyna się od | Dlaczego | Co zrobić |
|---|---|---|
| „Nowy agregat wymaga spacji” | Pierwsze zdarzenie nowego agregatu nie zawiera spacji: jego `spaceId` jest ujemne, np. `SpaceId.unknown`. | Każde `FirstEvent` nazywa spację: `SpaceId(0)` dla platformy lub organizacji ([rozdział 16](16-organizations.md)). |
| „Aggregate id ... nie jest dodatni”, „... nie jest poniżej 2^53” lub „... mieści się w zakresie instalacji ...” | Biblioteka nie zezwala na `chosenAggregateId`. | Pobieranie wybranych identyfikatorów z `ReserveAggregateId` zamiast ich tworzenia. Identyfikatory poniżej 10^12 (identyfikatory systemowe i starsze) również są dozwolone. |
| „Polecenie ... jest zbyt duże, aby można je było wysłać między węzłami” | W klastrze polecenie typu „Sharded”, które nie mieści się w jednej wiadomości między węzłami (domyślnie 248 KiB). | Zachowaj małe rozmiary poleceń: najpierw przechowuj duże dokumenty w innym miejscu i wyślij referencję. Zobacz [MessageTooLargeException](#messagetoolargeexception-in-a-cluster). |
| „Polecenia ... nie można wysłać między węzłami” | Serializator klastra nie może zapisać polecenia. | Uczyń z niego prostą klasę przypadku, którą obiekty mpjson mogą zapisywać i odczytywać ([rozdział 17](17-cluster.md)). |

### Polecenie odpowiada `EventHandlingError`

**Co widzisz:** `EventHandlingError(eventName, errorId, commandId)`. Nic nie zostało zapisane.

**Prawdopodobne przyczyny**, według wiersza dziennika z `errorId`:

| W dzienniku jest napisane | Co się stało |
|---|---|
| `Obsługa błędu zdarzenia ItemAdded, errorId: [...]` | Obsługa zdarzenia spowodowała nowe zdarzenie lub nie ma dla niego żadnego przypadku. |
| `Błąd utrwalania zdarzeń, identyfikator błędu: [...]` z wyjątkiem `TooManyEventsException` | Agregat osiągnął limit zdarzeń; patrz [poniżej](#an-aggregate-stops-taking-commands-at-10-000-events). |
| ... z wyjątkiem `EventTooLargeException` | Rozmiar pliku JSON zdarzenia przekracza limit; patrz [poniżej](#an-event-is-too-large). |
| ... z błędem połączenia | Baza danych jest wyłączona lub w danym momencie (5 sekund w przypadku domyślnej puli ScalikeJDBC) nie było wolnego połączenia. |
| ... „Cofnij nie może dotrzeć do spłaszczonej historii” | Cofnięcie anulowałoby zdarzenia znajdujące się poniżej punktu spłaszczenia ([rozdział 14](14-squash.md)). |
| ... „Duplikacja nie może odczytać spłaszczonej historii” | Duplikacja wersji znajdującej się poniżej punktu spłaszczenia bazy. |
| ... „Być może FirstEvent nie zostało użyte” | Pierwszym zdarzeniem nowego agregatu jest zwykłe `Event`, a nie `FirstEvent`. |
| ... `RewriteChangesClassException` | Podczas przepisywania historii próbowano zmienić klasę zdarzenia ([rozdział 15](15-rewriting-history.md)). |
| ... błąd dotyczący tabeli `commands_responses` | Polecenie idempotentne, ale `PostgresCommandResponseState.initSchema()` nie zostało wywołane na początku. |

**Co zrobić:** popraw nazwy logów. W przypadku błędów połączenia, przydziel puli JDBC co najmniej tyle samo
połączenia, ponieważ `reactivecqrs.blocking-dispatcher` ma wątki (domyślnie 32) i sprawiają, że
`max_connections` programu PostgreSQL obejmuje pule każdego węzła.

### Polecenie odpowiada `CommandRejected`

**Co widzisz:** `CommandRejected(aggregateId, reason)`. Polecenie w ogóle się nie uruchomiło, więc
ponowne wysłanie go później jest bezpieczne.

| Powód mówi | Dlaczego | Co robić |
|---|---|---|
| „1000 poleceń agregatu już czeka” | Tryb szeregowy, a kolejka agregatu jest pełna. | Wycofaj się i wyślij go ponownie, z kluczem idempotentności. Jeśli zdarza się to często, agregat jest punktem zapalnym: podziel go. |
| „...zostaje napisane, gdy każdy węzeł uruchomi wersję ... lub nowszą” | Trwa proces stopniowej aktualizacji. | Zobacz [poniżej](#a-rolling-upgrade-with-a-new-event-class). |

### Zapytanie wygasło, ale polecenie zostało zastosowane później

**Co widzisz:** Twój kod otrzymuje wyjątek `AskTimeoutException`, ale później zmiana jest widoczna.

**Dlaczego:** limit czasu Twojego zapytania jest krótszy niż czas wykonania polecenia. Polecenia mogą czekać: w
kolejki szeregowej, za powolnymi modułami obsługi i w klastrze, podczas gdy ich właściciel jest przekazywany innemu
węzeł (do 90 sekund). Domyślny limit czasu biblioteki wynosi 60 sekund.

**Co zrobić:**

1. Ustaw czas oczekiwania na odpowiedź na żądanie na dłuższy niż `commandTimeout` kontekstu lub skróć go
   `commandTimeout`, więc odpowiedź biblioteki zawsze jest wyświetlana jako pierwsza.
2. Nadaj poleceniom klucz idempotentności i spróbuj ponownie z tym samym kluczem: polecenie, które zostało wykonane późno
   Nigdy nie jest stosowane dwukrotnie.
3. Ogranicz liczbę poleceń w locie na krawędzi aplikacji i odpowiadaj „zajęty” (HTTP 429 lub
    503) poza tym. [docs/SCALING.md](../SCALING.md), sekcja 2, pokazuje jak ustalić rozmiar limitu.

### `GetAggregateMinVersion` kończy się niepowodzeniem z wyjątkiem `AggregateInIncorrectVersionException`

**Co widzisz:** `Failure(AggregateInIncorrectVersionException)`: agregat nie osiągnął
wersja w `maxMillis`.

**Prawdopodobne przyczyny:** polecenie nie powiodło się lub nie zostało wysłane; poprosiłeś o wersję, którą polecenie wysyła
nie produkować (polecenie z trzema zdarzeniami przesuwa wersję o trzy); lub właściciel zatrzymał się, gdy
czekałeś.

**Co zrobić:** weź wersję z `SuccessResponse` polecenia i poproś o dokładnie tę;
sprawdź odpowiedź polecenia; zezwól na większą wartość `maxMillis` jeśli agregat jest zajęty.

### Agregat przestaje przyjmować polecenia po 10 000 zdarzeń

**Co widzisz:** każde polecenie dla jednego agregatu odpowiada na `EventHandlingError`, a nazwy dzienników
`TooManyEventsException`: "Zbyt wiele wersji dla łącznej liczby 1234@..., maksymalna liczba wersji to 10000".

**Dlaczego:** wartość `aggregateVersionLimit` magazynu zdarzeń, domyślnie 10 000, chroni przed niekontrolowanym wzrostem
agregaty. Wersje nigdy nie przestają działać: migawki i funkcje squash sprawiają, że ładowanie długiego agregatu jest tańsze,
ale żadne z nich nie znosi tego ograniczenia.

**Co zrobić:**

1. Z założenia: rozdzielaj rzeczy długowieczne wzdłuż granicy naturalnej (miesiąc, pora roku, dokument)
   wersja) na własne agregaty.
2. Jeśli agregat rzeczywiście potrzebuje więcej, zbuduj sklep z wyższym limitem, np.
   `nowy PostgresEventStoreState(mpjsons, typesNames, aggregateVersionLimit = 50000)`.
   dotyczy każdego typu agregatu danego sklepu.
3. Włącz migawki dla takich typów ([rozdział 13](13-snapshots.md)). Pomiary przeprowadzono na jednej maszynie
   ([docs/perf/load-M5.md](../perf/load-M5.md), 17.10.2026): przywrócenie agregatu 50 000
   Odtworzenie każdego zdarzenia zajęło 392 ms, a migawki 12 ms.

### Wydarzenie jest zbyt duże

**Co widzisz:** `EventHandlingError` i nazwy dziennika `EventTooLargeException`: „Zdarzenie typu
... jest za duży: ... znaków JSON, maksymalny rozmiar to 100000 znaków".

**Co zrobić:**

1. Trzymaj dokumenty poza zdarzeniami (wzorzec „sprawdzania roszczeń”): najpierw zapisz plik w obiekcie
   przechowywać lub mieć własną tabelę, a następnie wysłać polecenie, które ją nazywa (identyfikator, hasz, rozmiar) i zachować
   tylko to odniesienie w zdarzeniu i stanie.
2. Jeśli klasa zdarzeń naprawdę musi przenosić więcej, podnieś limit tylko dla tej klasy,
   `eventSizeLimitPerClassName = Map(classOf[ReportAttached].getName -> 1000000)` podczas kompilacji
   `PostgresEventStoreState` (`ReportAttached` oznacza Twoją własną klasę).

> **Ostrzeżenie:** duże wydarzenia kosztują wszędzie: przy każdym przywracaniu, ponownej publikacji, odbudowie i wiadomości
> między węzłami. Zmierzono na jednej maszynie ([docs/perf/large-M5.md](../perf/large-M5.md), 17.10.2026): przywracanie
> suma 20 zdarzeń o rozmiarze 5 MB zajęła 517 ms, a stos wzrósł ze 117 do 323 MB, a jedno takie
> zdarzenie serializowane dla innego węzła ma rozmiar 4,8 MB, co znacznie przekracza 248 KiB, jakie może przenosić komunikat.
> [docs/SCALING.md](../SCALING.md), sekcja 7, zawiera pełną listę elementów, które należy dostroić.

---

## Dostawa i prognozy

### Szyna zdarzeń niczego nie publikuje

**Co widzisz:** polecenia są wykonywane pomyślnie, ale projekcja się nie zmienia. Co 5 sekund w dzienniku pojawia się komunikat „Zdarzenie
magistrala oczekuje na RegisterSubscribers od hosta; do tego czasu nic nie jest publikowane” lub „magistrala zdarzeń”
oczekuje na subskrypcje: ... z listą nazw. Węzeł nie zgłasza gotowości.

**Prawdopodobne przyczyny:**

- Gospodarz nigdy nie wysłał `RegisterSubscribers` na magistralę ([rozdział 4](04-first-command.md)).
- Abonent na liście nigdy nie odpowiada na pytanie autobusu dotyczące jego subskrypcji: aktor, który
  nie jest projekcją ani zarządzaną projekcją, której nadzorca nie może odczytać jej rejestru (dziennika)
  mówi „Projekcja ... nie mogła odczytać swoich kompilacji; spróbuj ponownie za 10 sekund”, na przykład dlatego, że
  `PostgresProjectionRegistry.initSchema()` nie zostało wywołane).

Tymczasem nic nie ginie: zdarzenia są zapisywane i czekają w skrzynce nadawczej, a magistrala je przechowuje.
publikacji do momentu jej rozpoczęcia.

**Co zrobić:** wysłać `RegisterSubscribers` raz na początku, z każdą projekcją, aktorem słuchacza i
nadzorca („RegisterSubscribers(Nil)”, gdy nie ma żadnych). Funkcja „czeka na subskrypcje”
w wierszu znajdują się nazwy brakujących linii.

> **Pamiętaj:** subskrybenci są na stałe na początku. Brak projekcji na liście po prostu powoduje
> nic i nikt nie narzeka: wydarzenia, na które nikt się nie zapisał, od razu opuszczają skrzynkę nadawczą.

### Projekcja przestaje się aktualizować

**Co widzisz:** model odczytu nie zmienia się już dla każdego agregatu ani tylko dla niektórych.

Zapytaj projekcję `GetProjectionStatus` i magistralę `GetDeliveryStatus`, a następnie:

| Co znajdziesz | Prawdopodobna przyczyna | Co zrobić |
|---|---|---|
| Agregat znajduje się w `ProjectionStatus.failures` | Nasłuchiwacz nie wywiązał się z tego zadania i zrezygnował | [Nieudana subskrypcja](#a-subscription-failed) |
| Agregat jest w trybie „ponowna próba” | Nasłuchiwanie kończy się niepowodzeniem i jest ponawiane przez około dwie minuty | Przeczytaj ostrzeżenie „Projekcja ... nie powiodła się w ...” w dzienniku |
| Agregat znajduje się w `waitingForGap` | Projekcja pomija wersje i poprosiła o nie właściciela | Zwykle problem rozwiązuje się sam; po 10 minutach problem staje się nieaktualny. Sprawdź, czy właściciel może przywrócić agregat |
| `DeliveryStatus.stopped` nadaje nazwę projekcji | Aktor projekcji został zatrzymany; jego zdarzenia oczekują w skrzynce nadawczej | Znajdź przyczynę w dzienniku. Niezarządzany: uruchom ponownie węzeł. Zarządzany: jego nadzorca uruchamia go ponownie po 10 sekundach |
| `DeliveryStatus.behind` określa to | Jest wolniejszy niż zapisy; magistrala zatrzymuje to, co nie mieści się w jej oknie | Pozwól jej nadrobić zaległości; jeśli jest daleko w tyle, przebuduj ją ([rozdział 10](10-rebuilding-projections.md)); utrzymuj słuchaczy tanimi |
| Nic zupełnie | Nie jest zarejestrowany na magistrali lub magistrala nie została uruchomiona | [Magistrala zdarzeń niczego nie publikuje](#the-event-bus-publishes-nothing) |
| Zarządzana projekcja wyświetla stare dane, podczas gdy `GetBuilds` pokazuje trwającą kompilację | Została uruchomiona wraz z `ServeStale` | Zaprojektowano ją tak, aby przełączała się, gdy nowa kompilacja zostanie dotrzymana |

> **Wskazówka:** projekcja utworzona bez `.withDispatcher(BlockingDispatcher.id)` mówi tak na początku:
> „Projekcja… uruchamia transakcje nasłuchujące w domyślnym dyspozytorze”. Najpierw to napraw:
> spowalnia również wszystko inne w domyślnym dyspozytorze.

### Subskrypcja nie powiodła się

**Co widzisz:** błąd w dzienniku: „Projekcja ... zrezygnowała z ShoppingCart 1234 w wersji 7
(...); jego dostarczenie kończy się niepowodzeniem, dopóki nie zostanie powtórzone lub pominięte”, a łączna wartość w
`ProjectionStatus.failures`. Pozostałe agregaty nadal przepływają.

**Prawdopodobne przyczyny:**

- Słuchacz wrzucił tę wersję na około dwie minuty prób: błąd, ograniczenie twojego
  własna tabela lub dokument, który nie istnieje ("Dokument do aktualizacji nie istnieje").
- W ciągu 10 minut pytano o brakujące wersje, ale nie było odpowiedzi („wersje z ... są
  „brakujący”): właściciel nie może przywrócić agregatu lub, w klastrze, zdarzenie jest zbyt duże, aby do niego dotrzeć
  węzeł projekcji.
- W trakcie kompilacji: zdarzenie, którego nie można odczytać ze sklepu.

Jeśli subskrypcja agregatu nie powiedzie się, agregatu tego nie można ani stłumić, ani usunąć
na stałe.

**Co zrobić:**

1. Odczytaj `error` w komunikacie o błędzie i w dzienniku.
2. Usuń przyczynę i wdróż poprawkę.
3. Wyślij `RetryFailedSubscription(aggregateId)` lub `RetryAllFailedSubscriptions`. Zarejestrowano błędy
   przed startem są ponawiane przy starcie, 20 na sekundę.
4. Wyślij `SkipFailedVersion(aggregateId)` tylko wtedy, gdy tej wersji nie można zastosować.
5. W przypadku kompilacji w toku, zawiń wiadomość: `ToBuildInProgress(RetryAllFailedSubscriptions)`.
   kompilacja z nieudanymi subskrypcjami nie zastępuje aktywnej.

```scala
zaimportuj org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import io.reactivecqrs.core.projection._
import scala.concurrent.Await
import scala.concurrent.duration._

niejawny limit czasu wartości: Limit czasu = Limit czasu (5 sekund)

val status = Await.result((projekcja ? GetProjectionStatus).mapTo[ProjectionStatus], 5.sekund)
status.failures.foreach(f => println(s"${f.aggregateId} nie powiodło się w ${f.version}: ${f.error}"))

// Po wdrożeniu poprawki:
Await.result((projekcja ? RetryFailedSubscription(cartId)).mapTo[FailedSubscriptionsHandled], 5.sekund)
```

> **Ostrzeżenie:** `SkipFailedVersion` oznacza wersję jako zastosowaną bez jej zastosowania, więc odczyt
> model pomija tę zmianę. Następnie przebuduj to, co projekcja utworzyła na podstawie tego agregatu:
> `RebuildAggregates(RebuildTarget.OneAggregate(cartId))` dla zarządzanej projekcji, która implementuje
> `clearForRebuild` ([rozdział 10](10-rebuilding-projections.md)).

### Wydarzenia pozostają w skrzynce nadawczej (oznaczone w dzienniku jako „Niedostarczone”)

**Co widzisz:** błędy z sondy skrzynki nadawczej: „Niedostarczone: Koszyk 1234 z wersji 7
nadal czeka w skrzynce nadawczej po 10 minutach; jest ponownie publikowana co 30 minut”, i od
autobus, „Niedostarczone po 10 minutach: ... pozostaje w skrzynce nadawczej do momentu ponownej publikacji”.
Wcześniej pojawiały się ostrzeżenia takie jak „Dostawa oczekuje przez 1 minutę: ...”. Funkcja `GetOutboxStatus` wymienia je w
`poddał się`.

**Prawdopodobne przyczyny:**

- **Abonent, który nigdy nie odpowiada:** słuchacz utknięty w długiej transakcji (czekający na
  blokada bazy danych, wywoływanie powolnej usługi zewnętrznej) lub abonent napisany ręcznie, który nie wysyła
  `MessageAck`.
- **Abonent, który się zatrzymał:** jego zdarzenia pozostają w skrzynce nadawczej do momentu ponownego uruchomienia węzła
  (`DeliveryStatus.stopped` określa jego nazwę).
- **Autobus nie wystartował:** patrz [powyżej](#the-event-bus-publishes-nothing).
- **Właściciel nie może ich ponownie opublikować:** nie można przywrócić agregatu; lub w dzienniku pojawia się komunikat „Nie
  magistrala poleceń zarejestrowana dla ..., nie może zostać ponownie opublikowana", ponieważ magistrala poleceń tego typu została zbudowana z
  innej magistrali zdarzeń niż ta, z której korzysta moduł sondujący.
- **Abonent daleko w tyle:** to, czego nie wziął, czeka na swoją kolej w sklepie.
- **W klastrze:** zdarzenie lub stan jest zbyt duży, aby dotrzeć do węzła subskrybenta.

I jedna przyczyna, dla której nie ma żadnego dziennika: **nie utworzono `OutboxPollerActor`.** Następnie zdarzenia, których pierwsze
dostawy nie zostały ukończone (awaria, ponowne uruchomienie, brak abonenta) nigdy nie są publikowane
ponownie, a skrzynka nadawcza tylko rośnie.

**Co zrobić:**

1. Zapytaj `GetDeliveryStatus` (na magistrali każdego węzła), `GetOutboxStatus` i `GetProjectionStatus`.
2. Napraw lub uruchom ponownie subskrybenta; zdarzenia oczekujące następują automatycznie.
3. Sprawdź, czy na system (w klastrze, uruchomionym z
   `Singletons.start`).
4. Po dłuższej przerwie pamiętaj, że sonda ponownie publikuje maksymalnie 1000 agregatów na
   Ankieta 30-sekundowa: 1 milion oczekujących agregatów zajmuje około 8,3 godziny (szacunkowo)
   [docs/SCALING.md](../SCALING.md), sekcja 4). Odbudowanie projekcji może być szybsze; lub podniesienie
   tempo, w jakim zaległości się wyczerpują:

   ```scala
   importuj org.apache.pekko.actor.Props
   import io.reactivecqrs.core.delivery.{OutboxPollerActor, OutboxPollerOptions, RedeliveryPolicy}

   val outboxPoller = system.actorOf(Props(nowy OutboxPollerActor(eventStore, eventBus,
     OutboxPollerOptions(redelivery = RedeliveryPolicy.Options(maxRepublishPerPoll = 5000)))), "outboxPoller")
   ```

> **Pod maską:** moduł sondujący skrzynkę nadawczą jest jedynym właścicielem ponownych dostaw (D8): co 30 sekund
> odczytuje, które agregaty czekają, a czysta „Polityka ponownego dostarczenia” decyduje, kto ma otrzymać. Pełna historia w
> [ARCHITECTURE.md, sekcja 5.1](../ARCHITECTURE.md).

### Nowa projekcja pomija stare agregaty

**Co widzisz:** dodałeś projekcję; pokazuje ona agregaty, które uległy zmianie od tego czasu, ale nie
inni.

**Dlaczego:** niezarządzana projekcja zaczyna nic nie wiedzieć. Gdy agregat otrzymuje nowe zdarzenie,
projekcja zauważa lukę i pyta o wcześniejsze zdarzenia tego agregatu; agregat, który nigdy
zmiany ponownie nigdy do niego nie docierają.

**Co zrobić:** uruchom projekcję jako zarządzaną za pomocą `Projections.start`: jej pierwszej kompilacji
odczytuje cały magazyn zdarzeń przed jego obsługą ([rozdział 10](10-rebuilding-projections.md)). Dla
zarządzana projekcja, która poszła nie tak, wyślij jej nadzorcę `RebuildProjection`.

### Zapytanie zwraca „Brak” (lub stare dane) zaraz po poleceniu

**Dlaczego:** strona odczytu jest ostatecznie spójna. Polecenie powiodło się; projekcja stosuje swoją
wydarzenia chwilę później.

**Co zrobić:** odpowiedz na zapytanie za pomocą `delayIfNotAvailable` i terminu, aby poczekać na dane
([rozdział 8](08-projections.md)); lub zapytaj magistralę poleceń `GetAggregateMinVersion` o wersję
z `SuccessResponse` ([rozdział 7](07-reading-the-past.md)). Jeśli odpowiedź nadal brzmi `Brak`, sprawdź
projekcja z sekcjami powyżej.

---

## Długie historie

### Odmówiono przyjęcia dyni

**Co widzisz:** `SquashRefused(id, reason)` i nic się nie zmieniło. Harmonogram squasha zlicza jego
odmowy (`SquashStatus.refused`) i próbuje tych agregatów ponownie w późniejszym przebiegu.

| Powód mówi | Co to znaczy | Co robić |
|---|---|---|
| "squash nie jest włączony dla ..." | Kontekst nie zawiera żadnej polityki `squash`. | Podaj kontekstowi jedną: `override val squash: Option[SquashPolicy[ShoppingCart]] = Some(SquashPolicy(...))` ([chapter 14](14-squash.md)). |
| "wersja ... nie mieści się pomiędzy 1 a ..." | `upTo` ma wartość 0, wartość ujemną lub jest wyższa od bieżącej wersji. | Wybierz wersję, którą zawiera agregat. |
| "już upchnięte do wersji ..." | Już jest tak upchnięte. | Wybierz późniejszą wersję albo zostaw. |
| „wydarzenia w wersji lub starszej… nadal czekają w skrzynce nadawczej” | Niektórzy subskrybenci nie potwierdzili jeszcze tych zdarzeń. | Poczekaj lub zobacz [Wydarzenia pozostają w skrzynce nadawczej](#events-stay-in-the-outbox-undelivered-in-the-log). |
| „subskrypcja agregatu nie powiodła się” | Projekcja tego agregatu została przerwana. | Najpierw [napraw subskrypcję](#a-subscription-failed). |
| "cofanie po wersji ... cofnięte zdarzenia w tej samej wersji lub niższej" | Późniejsze cofnięcie anuluje zdarzenia, które miałyby zostać zwinięte. | Zgnieć do wersji cofnięcia lub nowszej albo poniżej pierwszego cofniętego zdarzenia. |
| "pierwsze zdarzenie duplikatu ..., które odłączono przez squash, zostało cofnięte" | Duplikatu skopiowanego poniżej punktu nie można odłączyć. | Zgnieć w lub poniżej wersji, w której skopiowano duplikat. |
| "Pierwsze zdarzenie duplikatu..., które squash odłącza, wciąż czeka w skrzynce nadawczej" | Pierwsze zdarzenie duplikatu nie zostało jeszcze dostarczone. | ​​Poczekaj i spróbuj ponownie. |
| "subskrypcja duplikatu ..., którą squash odłącza, jest nieudana" | Projekcja zrezygnowała z tego duplikatu. | Najpierw napraw tę subskrypcję. |
| "duplikaty agregatu uległy zmianie w międzyczasie" | Ktoś zduplikował go podczas squasha. | Spróbuj ponownie. |
| "agregat jest usuwany w wersji..." | Jego stan jest usuwany w tej wersji. | Nie ma tam nic do usuwania. |
| "nie udało się wykonać zdarzenia squash: ..." | Wystąpił błąd `toEvent`. | Napraw `toEvent`. |
| "obsługa zdarzeń kończy się niepowodzeniem przy zdarzeniu squash..." | Twoje procedury obsługi zdarzeń nie obsługują zdarzenia squash ani nie wyrzucają go. | Obsłuż zdarzenie squash w procedurach obsługi zdarzeń. |
| „Zdarzenie squash… nie zwraca stanu, który zastępuje” | Zastosowane do stanu początkowego, zdarzenie squash zwraca inny stan. | Napraw `toEvent` lub procedurę obsługi, aby stan był zwracany w ten sam sposób. Jeśli pominięcie danych jest zamierzone, włącz `SquashMayChangeState` do klasy zdarzenia. |
| "duplikat ...: " po którym następuje jedno z trzech ostatnich | To samo sprawdzenie nie powiodło się w przypadku duplikatu, dynia zostałaby odłączona. | Jak wyżej. |
| „...zostaje napisane, gdy każdy węzeł uruchamia wersję ... lub nowszą” | Trwa stopniowa aktualizacja, a klasa zdarzeń squash jest nowa. | Najpierw należy zakończyć wdrażanie. |
| „Agregatu nie można przywrócić: ...” | Właściciel nie może odczytać agregatu. | Zobacz przyczyny przywracania [powyżej](#a-command-answers-commandhandlingerror). |
| "squash nie powiódł się: ..." | Baza danych uległa awarii. | Sprawdź bazę danych i spróbuj ponownie. |

> **Wskazówka:** sprawdzenie „zwraca stan” porównuje się z `==`. Klasy przypadków zawierające `Vector`,
> „Lista”, „Mapa” lub „Zestaw” porównują według zawartości, jak można się spodziewać. „Tablica” porównuje według referencji,
> więc stan posiadający jedynkę nigdy nie jest równy swojej kopii, a każde złamanie jest odrzucane. Użyj `Wektora`.

### Odczyt historyczny kończy się niepowodzeniem poniżej punktu zgniecenia

**Co widzisz:** `Failure(AggregateSquashedException)`: „Historia agregatu 1234 jest spłaszczona
do wersji 50; wersji 3 nie można odczytać” (lub „jej stanu w ... nie można odczytać”).

**Dlaczego:** to właśnie robi squash: wydarzenia poniżej punktu squasha znikają. Czytanie wersji
poniżej (`GetAggregateForVersion`, `GetEventsForAggregateForVersion`) lub chwilę przed
czas zdarzenia squash (`GetAggregateAtInstant`) kończy się niepowodzeniem zamiast zwrócić błędną odpowiedź.
`GetEventsForAggregate` nadal działa: lista zaczyna się od zdarzenia squash.

**Co zrobić:**

- Odczytaj w punkcie squasha lub później: wyjątek wskaże Ci, gdzie się znajduje (`squashedUpTo`).
- W celu przeprowadzenia audytu zdecyduj *przed* zgnieceniem, które klasy zdarzeń muszą pozostać czytelne, nazwij je w
  `SquashPolicy.keep` i odczytaj je za pomocą `GetArchivedEvents`. Zdarzenia zgniecione przed klasą
  dodane do `keep` znikają na dobre.
- Zachowaj czytelność ostatniej historii dzięki funkcji `AutomaticSquash(afterEvents, keepLast, keepNewerThan)`
  ([rozdział 14](14-squash.md)).

```scala
import scala.util.{Niepowodzenie, Sukces, Próba}

(commandBus ? GetAggregateForVersion(cartId, AggregateVersion(3))).mapTo[Try[Aggregate[ShoppingCart]]].map {
  przypadek Sukces(koszyk) => Jakiś(koszyk)
  case Failure(e: AggregateSquashedException) => None // istnieją tylko wersje z e.squashedUpTo
  przypadek Failure(e) => throw e
} // z niejawnym ExecutionContext w zakresie
```

### Trwałe usunięcie czeka

**Co widzisz:** zapisano zdarzenie `PermanentDeleteEvent`, ale wiersze agregatu nadal znajdują się w
Baza danych. W dzienniku znajduje się ostrzeżenie: „Agregat 1234 typu ShoppingCart nie został jeszcze usunięty: ...” lub
błąd, „... nie został usunięty: ...”.

**Jak to działa:** usuwanie jest uruchamiane, gdy każdy subskrybent potwierdzi zdarzenie. Gdy coś
stoi na przeszkodzie, wydarzenie pozostaje w skrzynce nadawczej, a próba usunięcia jest podejmowana za każdym razem
Ankieta skrzynki odbiorczej ponownie ją publikuje (po 3 i 6 minutach, a następnie co 30 minut). Subskrybent
który jest zatrzymany lub znacznie opóźniony, również opóźnia usuwanie.

| Powód mówi | Co robić |
|---|---|
| „subskrypcja tego elementu nie powiodła się” | [Napraw subskrypcję](#a-subscription-failed). Projekcja, którą usunąłeś z kodu, mogła pozostawić rekordy błędów: w przypadku projekcji zarządzanej wywołaj `projections.retire(name)`, a w przypadku projekcji niezarządzanej `subscriptionsState.clearSubscriptionsInfo(...)`, podając nazwę jej klasy. |
| "pierwsze zdarzenie duplikatu ..., które jest odłączane podczas usuwania, nadal czeka w skrzynce nadawczej" | Nic: zostanie wypróbowane ponownie po dostarczeniu zdarzenia. |
| "subskrypcja duplikatu ..., którą odłączono podczas usuwania, nie powiodła się" | Napraw tę subskrypcję. |
| "pierwsze zdarzenie duplikatu ..., które zostało odłączone podczas usuwania, zostało cofnięte" | Takiego duplikatu nie można odłączyć; należy się nim najpierw zająć, na przykład usuwając go trwale. |
| "w międzyczasie duplikaty agregatu uległy zmianie" | Nic: próba jest podejmowana ponownie. |
| „...zostanie napisane, gdy każdy węzeł będzie uruchamiał wersję ... lub nowszą” | Zakończ aktualizację ciągłą. |
| Błąd: „agregaty ... zostały z niego zduplikowane, a typ nie ma zdarzenia squash, aby je odłączyć” | Duplikaty odczytują historię tego agregatu i tylko zdarzenie squash może je odłączyć: nadaj kontekstowi `SquashPolicy`. Do tego czasu usunięcie nigdy się nie powiedzie. |
| Błąd: „duplikat ...: ” po którym następuje problem ze zdarzeniem squash | Zdarzenie squash nie przechodzi kontroli stanu duplikatu; napraw to w sposób opisany w [Odmowa squasha](#a-squash-is-refused). |

> **Pamiętaj:** trwałe usunięcie usuwa zdarzenia agregatu, archiwum, wiersze skrzynki nadawczej i migawkę
> i wiersze subskrypcji. Twoje modele odczytu należą do Ciebie: projekcje usuwają swoje dokumenty, gdy
> zobacz `PermanentDeleteEvent` ([rozdział 6](06-events.md)).

### Nowa wersja historii odpowiada na sukces, ale nic się nie zmieniło

**Dlaczego:** dzisiaj przepis, którego obsługa zwraca przepisane zdarzenia, ale nie otrzymuje odpowiedzi na żadne nowe zdarzenie
`SuccessResponse` i nic nie jest zapisywane. (Kompletne przepisanie historii nastąpi w wersji 1.0.)

**Co zrobić:** zawsze dodawaj zdarzenie kompensujące, tak jak robi to koszyk:
`RewriteCommandSuccess(rewritten, CartNameRewritten(name))`. Informuje również Twoje projekcje, że
coś się zmieniło ([rozdział 15](15-rewriting-history.md)).

---

## Uruchomienia, modernizacje i klastry

### Uruchomienie jest powolne

**Prawdopodobne przyczyny:**

- **Przywraca.** Właściciel odtwarza całą historię swojego agregatu w pierwszej wiadomości, chyba że
  Kontekst tworzy migawki. Po ponownym uruchomieniu każdy użyty agregat płaci tę kwotę jednorazowo. Szacunkowo
  w [docs/SCALING.md](../SCALING.md), sekcja 3: węzeł przywraca około 80 agregatów po 50 000
  zdarzeń na sekundę bez migawek i około 2 700 z nimi.
- **Skrzynka nadawcza po awarii.** Podczas pierwszego odpytywania, 10 sekund po uruchomieniu, zdarzenia zapisane przed
  starty są ponownie publikowane: do 1000 agregatów na ankietę, każde rozpoczynające i przywracające
  właściciel.
- **Projekcje ponawiające stare błędy,** 20 na sekundę na początku.
- **Magistrala zdarzeń czeka na swoich abonentów.** Węzeł nie jest gotowy, dopóki wszyscy nie odpowiedzą
  ("Magistrala zdarzeń oczekuje na subskrypcje: ...").
- **Inicjalizacja schematu w oczekiwaniu na inny węzeł.** Węzły na zmianę podlegają blokadzie schematu; jeden
  poddaje się po pięciu minutach („Blokada schematu nie była zwolniona przez 5 minut: inny proces
  inicjuje schemat").
- **Ciężkie zadania na ścieżce startowej.** `buildIndexesConcurrently()`, `backfillEventKeys()` i
  `backfillSpaces(...)` powinno znajdować się w tle, nigdy przed rozpoczęciem pracy aktorów.

**Co zrobić:** włącz migawki dla typów agregatów z długą historią i usuń starą historię
([rozdział 13](13-snapshots.md), [rozdział 14](14-squash.md)); uruchamiaj ciężkie zadania w tle; zapytaj
`Readiness(system).waitingFor` to, na co węzeł nadal czeka. Nowa kompilacja zarządzanej projekcji
nie spowalnia startu: w międzyczasie działa aktywna kompilacja.

### Zapisane zdarzenie nie jest już odczytywane po zmianie klasy

**Co widzisz:** po wdrożeniu polecenia dla niektórych agregatów odpowiadają `CommandHandlingError`,
i zapytania `Failure(AggregateRestoreFailedException)`; w dzienniku pojawia się komunikat „Przywracanie agregatu ... nie powiodło się”
z błędem JSON lub klasy. Kompiluje rekordy nieudanych subskrypcji, a `GetEventsForAggregate` kończy się niepowodzeniem.
Lub `UnsupportedEventVersionException`: „... jest przechowywany z event_type_version ..., ale zdarzenie
wersjonowanie zostało usunięte w wersji 1.0".

**Dlaczego:** zdarzenia są przechowywane jako JSON pod nazwą klasy i odczytywane z powrotem do tej klasy. Zmiana nazwy
lub przenoszenie klasy zdarzeń, zmiana typu pola lub usuwanie klasy, w której przechowywane zdarzenia nadal są używane
Przerywa czytanie. Wiersze zapisane wersjami zdarzeń wydań wcześniejszych niż 1.0 również nie są odczytywane.

**Co zrobić:**

1. Nigdy nie zmieniaj klasy zapisanego zdarzenia. Zachowaj starą klasę (oznacz ją `@deprecated`), dodaj nową
   (`ItemAddedV2`) i obsługiwać oba w obsłudze zdarzeń i projekcjach ([rozdział 6](06-events.md)).
2. Przed każdą aktualizacją uruchom `StoredDataCheck` na kopii produkcyjnej, używając nowego kodu:

   ```scala
   import io.reactivecqrs.core.tools.StoredDataCheck

   val report = StoredDataCheck.run(mpjsons, typesNames,
     Seq(StoredDataCheck.ProjectionTable[String]("storeA"))) // jeden na dokument, przechowuje wraz z jego klasą dokumentu
   println(tekst raportu)
   if (report.unreadable > 0) sys.error("Napraw nieczytelne wiersze przed aktualizacją")
   ```

   Odczytuje każde zapisane zdarzenie, zdarzenie zarchiwizowane, kolejność sagi, odpowiedź idempotentną i projekcję
   tabele, które wymieniasz, i raporty, dla każdej klasy, wiersze, których nie można odczytać.
3. Przed usunięciem starej klasy z kodu, zapytaj `StoredDataCheck.usersOf(className)`: usuń ją
   tylko gdy oba `wydarzenia` i `zarchiwizowane` mają wartość 0.
4. Gdy zmieni się kształt klasy głównej agregatu, wywołaj `SnapshotPolicy.schemaVersion`, aby stare
   migawki są odkładane na bok, zamiast być odczytywane w nowym kształcie.

### Ciągła aktualizacja z nową klasą zdarzeń

**Co widzisz:** podczas wdrażania polecenia odpowiadają „CommandRejected” z powodem takim jak
„ItemAddedV2 jest zapisywany, gdy każdy węzeł korzysta z wersji 2.0.0 lub nowszej”, a funkcje squash są odrzucane
z tego samego powodu. (`ItemAddedV2(nazwa, źródło)` to nowe zdarzenie koszyka w toczącym się zestawie testów
uaktualnij, `TestCluster.scala`.)

**Dlaczego:** kontekst wymienia klasę w `writtenFrom`. Węzeł starszej wersji nie mógł odczytać
wydarzenie, więc nic nie zapisuje go, dopóki każdy członek klastra nie ogłosi co najmniej tego
`pekko.cluster.app-version`. Węzeł, który nie zna członków klastra, ale nie zapisuje nowej klasy
w ogóle.

**Co zrobić:**

1. Pozwól, aby w międzyczasie procedury obsługi zdarzeń zapisały starsze zdarzenie, ponieważ wersja 2.0.0 zestawu testowego
   wózek robi:

   ```scala
   nadpisz val writtenFrom: Mapa[Klasa[_], String] = Mapa(klasa[DodanyElementV2] -> "2.0.0")

   nadpisz def commandHandlers = cart => {
     przypadek c: AddItemConcurrently jeśli everyNodeRuns("2.0.0") => CommandSuccess(ItemAddedV2(c.name, "wersja 2"))
     case other => super.commandHandlers(cart)(other) // programy obsługi wersji 1.0.0, które zapisują ItemAdded
   }
   ```

   Ta sama wersja obsługuje `ItemAddedV2` w swoich modułach obsługi zdarzeń i projekcjach, więc każdy węzeł
   ta wersja może to przeczytać.

2. Ustaw `pekko.cluster.app-version` na wersję wydania na każdym węźle. Jeśli żaden członek nie ustawi
   po pierwsze, przełącznik jest wyłączony i wszystkie klasy są zapisywane jednocześnie, czego stary węzeł może nie odczytać.
3. Nigdy nie wycofuj się do wersji niższej niż ta, dla której napisano nowe klasy.
4. Wyślij nową klasę poleceń tylko wtedy, gdy każdy węzeł uruchomi wersję, która ją zna.
5. Pole dodawane do klasy, które przemieszcza się między węzłami (polecenie, odpowiedź, odpowiedź na zapytanie)
   musi być `Opcją`: brakujące pole jest odczytywane jako `Brak`, zero lub `null`, nigdy jako wartość domyślna.
6. Zmiana położenia typu lub `numberOfShards` wymaga kropki, a nie ciągłej aktualizacji
   ([rozdział 17](17-cluster.md)).

### `MessageTooLargeException` w klastrze

**Co widzisz:** zapytanie kończy się niepowodzeniem od razu z wyjątkiem `MessageTooLargeException`: „... z ... bajtów jest zbyt
duży do wysłania między węzłami; limit wynosi ... bajtów”. Węzeł wysyłający rejestruje klasę i
rozmiar, maksymalnie raz na klasę co 10 sekund. Na polecenie typu „Sharded” odpowiada
Zamiast tego `FailureResponse`. Dostawa zbyt duża dla projekcji na innym węźle powoduje jej opóźnienie.
dla tej łącznej kwoty, aż przerwa stanie się nieudaną subskrypcją po 10 minutach.

**Dlaczego:** wiadomość między węzłami musi mieścić się w ramce Pekko, domyślnie 256 KiB; limit biblioteki wynosi
8 KiB mniej, 248 KiB. Na pojedynczym węźle nic nie jest serializowane, więc pojawia się to tylko w klastrze.

**Co zrobić:**

- Utrzymuj polecenia, odpowiedzi, stany i odpowiedzi na zapytania o rozmiarze znacznie poniżej 248 KiB; trzymaj dokumenty poza
  zdarzenia i polecenia (zobacz [Zdarzenie jest za duże](#an-event-is-too-large)).
- Odczytaj duży stan na swoim własnym węźle za pomocą `GetAggregateAtInstant(id, Instant.now())`: to jest
  odrestaurowane ze sklepu, a nie wysłane przez właściciela.
- Strona z odpowiedziami na duże zapytania.
- Jeżeli nie można uniknąć większych wiadomości, należy zgłosić `pekko.remote.artery.advanced.maximum-frame-size` lub
  Kieruj duże wiadomości przez `large-message-destinations`. Pamiętaj o pamięci: Pekko przechowuje do 128
  bufory rozmiaru ramki, więc ramka 1 MiB może zająć do 128 MiB pamięci bezpośredniej (szacowanej w
  [docs/SCALING.md](../SCALING.md), sekcja 6).

---

## Nawyki, które zapobiegają większości z tych rzeczy

- Nadaj każdemu poleceniu, które klient mógłby wysłać dwukrotnie, klucz idempotentności.
- Ustaw limit czasu żądania klienta na dłuższy niż limit czasu polecenia.
- Utrzymuj zdarzenia, polecenia i stany w małej skali; przechowuj dokumenty w innym miejscu i zawsze miej do nich dostęp.
- Włącz migawki dla agregatów o długim okresie istnienia; zaplanuj `SquashPolicy.keep` przed pierwszym snapowaniem.
- Nigdy nie zmieniaj nazwy ani kształtu zapisanej klasy zdarzeń: dodaj obok niej `V2`.
- Przed każdą aktualizacją uruchom `StoredDataCheck` na kopii produkcyjnej.
- Obserwuj `GetProjectionStatus`, `GetDeliveryStatus`, `GetOutboxStatus` i metryki oraz alerty
  W logu widnieją wpisy „Niedostarczone” i „Zrezygnowano”.

To koniec książki. Wróć do [spisu treści](README.md), kiedy tylko będziesz tego potrzebować.
# Dodatek C. Rozwiązywanie problemów

Coś odpowiedziało błędem, ekran pokazuje wczorajsze dane albo log krzyczy „Undelivered”. Bez paniki:
ReactiveCQRS jest zbudowany tak, żeby nic nie ginęło po cichu, a prawie każdy problem zostawia wyraźny
ślad. Ten dodatek prowadzi od objawu, przez prawdopodobne przyczyny, do tego, co zrobić. Znajdź swój
objaw wśród nagłówków, przeczytaj przyczyny po kolei (najczęstsze są na początku) i przejdź przez
kolejne kroki.

---

## Najpierw trzy miejsca, w które warto zajrzeć

1. **Log.** `CommandHandlingError` i `EventHandlingError` niosą `errorId`, losowy ciąg 16 liter
   i cyfr. Wyszukaj go w logu: linia, w której występuje, podaje przyczynę razem z wyjątkiem.
2. **Komunikaty o statusie.** Każda część biblioteki odpowiada na pytanie o swój status: szyna
   zdarzeń (`GetDeliveryStatus`), poller outboxu (`GetOutboxStatus`), każda projekcja
   (`GetProjectionStatus`), nadzorca każdej projekcji zarządzanej (`GetBuilds`) i scheduler squasha
   (`GetSquashStatus`). [Dodatek B](appendix-b-cheat-sheet.md) pokazuje, co odpowiada każde z nich.
3. **Metryki.** Gdy wywołasz `ReactiveMetrics(system).use(...)`, zobaczysz wyniki i czasy poleceń,
   poziom outboxu, czekające i wstrzymane dostarczenia oraz awarie projekcji
   ([rozdział 18](18-production.md)).

---

## Polecenia

### Polecenie odpowiada `CommandHandlingError`

**Co widzisz:** `CommandHandlingError(commandName, errorId, commandId)`. Nic nie zostało zapisane.

**Prawdopodobne przyczyny**, według tego, co log mówi obok `errorId`:

| Log mówi | Co się stało |
|---|---|
| `Command AddItem timed out after 60 seconds and is not applied, errorId: [...]` | Handler nie skończył w ramach `commandTimeout` kontekstu. W trybie szeregowym czas liczy się od nadejścia polecenia, więc wlicza się też czekanie w kolejce. |
| `Error handling command AddItem, errorId: [...]`, a potem wyjątek | Twój handler polecenia rzucił wyjątek albo nie ma przypadku dla tego polecenia (`MatchError`). Albo agregat nie istnieje („Aggregate 1234 does not exist”) lub został usunięty („is already deleted”). Albo nie dało się go odczytać z magazynu (`AggregateRestoreFailedException`). |
| `Restoring aggregate 1234 of type ShoppingCart failed, retry after 5 seconds` | Właściciel nie zdołał odczytać agregatu: baza danych nie działa albo zapisanego zdarzenia nie da się już odczytać (zob. [niżej](#zapisanego-zdarzenia-nie-da-się-już-odczytać-po-zmianie-klasy)). Przez 5 sekund każde polecenie do tego agregatu dostaje ten błąd; potem nowy właściciel próbuje ponownie. |
| `No pool of aggregate ids for ShoppingCart came` i `Cannot route command ... without ids` | Generator identyfikatorów nie wydał identyfikatorów; `commandId` to wtedy `CommandId(0)`. Następne polecenie poprosi o nie ponownie. |

Przekroczenia limitu czasu mają klasyczną przyczynę: handlery, które blokują swój wątek. Handlery
poleceń działają na `reactivecqrs.handler-dispatcher`, czyli na 32 wątkach. Pomiar na jednej maszynie
([docs/perf/heavy-M5.md](../perf/heavy-M5.md), 17/10/2026): 36 handlerów po 5 sekund każdy, wysłanych
naraz: wszystkie zakończyły się sukcesem, ostatni po 10,0 sekundy, bo jednocześnie działają 32; lekkie
polecenie do innego agregatu czekało w tym czasie 5,4 sekundy.

**Co zrobić:**

1. Znajdź `errorId` w logu i przeczytaj wyjątek.
2. Przy wolnym handlerze wynieś powolną pracę poza polecenie: niech zrobi ją saga albo osobny worker,
   a wynik zapisze drugie polecenie ([rozdział 12](12-sagas.md)). Jeśli polecenie musi ją wykonać
   samo, zwróć `AsyncCommandResult`, którego `Future` działa na twojej własnej puli wątków, i podnieś
   `commandTimeout` tego kontekstu.
3. Ponawiaj bezpiecznie: daj poleceniom klucz idempotencji ([rozdział 5](05-command-flavors.md)), żeby
   ponowne wysłanie nigdy nie zastosowało polecenia dwa razy.
4. Przy błędach odtwarzania sprawdź najpierw bazę danych, a potem sekcję o zapisanych zdarzeniach
   niżej.

> **Pamiętaj:** polecenie, które przekroczyło limit czasu, nigdy już nie zostanie zapisane, nawet jeśli
> jego handler skończy później. Bez względu na przyczynę `CommandHandlingError` oznacza, że to
> polecenie niczego nie zapisało.

### Polecenie odpowiada `AggregateConcurrentModificationError`

**Co widzisz:** `AggregateConcurrentModificationError(aggregateId, aggregateType, expected, was)`.
Nic nie zostało zapisane. `expected` to wersja podana w twoim poleceniu, `was` to wersja, którą ma
agregat.

**Prawdopodobne przyczyny:**

- **Ktoś był pierwszy.** Inne polecenie zmieniło agregat po twoim odczycie. To blokowanie
  optymistyczne spełnia swoje zadanie ([rozdział 5](05-command-flavors.md)).
- **`was` to wersja 0:** agregat nie istnieje. Sprawdź identyfikator i szynę poleceń, do której
  wysyłasz polecenie.
- **`ConcurrentCommand` zderzył się 10 razy z rzędu** z zapisami, których nie zrobił jego własny
  właściciel: z właścicielami tego samego agregatu na innych węzłach (typ `Local` w klastrze) albo
  z zapisami spoza biblioteki. Kolizje między poleceniami jednego właściciela nigdy nie zużywają tych
  prób.
- **`FirstCommand`, którego `chosenAggregateId` już istnieje:** polecenie tworzące wysłano dwa razy
  bez klucza idempotencji albo ten identyfikator był już wcześniej użyty.

**Co zrobić:**

1. Gdy agregat edytuje człowiek: odczytaj go ponownie (`GetAggregate`), pokaż nowy stan i wyślij
   polecenie jeszcze raz, z nową `expectedVersion`.
2. Gdy kolejność nie ma znaczenia (dodanie produktu do koszyka), użyj `ConcurrentCommand`: biblioteka
   wykona je ponownie na nowym stanie.
3. Gdy wielu piszących naraz uderza w jeden agregat, przełącz kontekst na tryb szeregowy
   (`override val commandProcessing: CommandProcessing = CommandProcessing.Serial()`), w którym kolizji
   nie ma. W klastrze typ `Sharded` przepuszcza wszystkie polecenia agregatu przez jednego właściciela.
   A jeszcze lepiej: podziel gorący agregat.
4. Przy tworzeniu, które może zostać wysłane dwa razy, połącz `chosenAggregateId` z kluczem
   idempotencji: druga kopia dostanie wtedy `SuccessResponse` pierwszej zamiast błędu.

> **Wskazówka:** polecenia współbieżne sporo wytrzymują. Pomiar na jednej maszynie
> ([docs/perf/load-M5.md](../perf/load-M5.md), 17/10/2026): 10 000 poleceń współbieżnych wysłanych
> naraz do jednego agregatu: wszystkie się powiodły, 2 621 na sekundę.

### Polecenie odpowiada `FailureResponse`, ale nie z twoim komunikatem

**Co widzisz:** `FailureResponse(exceptions)`. Zwykle komunikat jest twój, z `CommandFailure`. Jeśli
nie, to biblioteka odrzuciła polecenie, zanim je wykonała:

| Komunikat zaczyna się od | Dlaczego | Co zrobić |
|---|---|---|
| „A new aggregate needs a space” | Pierwsze zdarzenie nowego agregatu nie ma przestrzeni: jego `spaceId` jest ujemne, na przykład `SpaceId.unknown`. | Każde `FirstEvent` wskazuje przestrzeń: `SpaceId(0)` dla platformy albo przestrzeń organizacji ([rozdział 16](16-organizations.md)). |
| „aggregate id ... is not positive”, „... is not below 2^53” albo „... is in the range of installation ...” | `chosenAggregateId`, na który biblioteka nie pozwala. | Bierz wybrane identyfikatory z `ReserveAggregateId`, zamiast je wymyślać. Identyfikatory poniżej 10^12 (systemowe i dawne) też są dozwolone. |
| „Command ... is too large to be sent between nodes” | W klastrze polecenie typu `Sharded`, które nie mieści się w jednym komunikacie między węzłami (domyślnie 248 KiB). | Niech polecenia będą małe: duże dokumenty najpierw zapisz gdzie indziej i wyślij odnośnik. Zob. [MessageTooLargeException](#messagetoolargeexception-w-klastrze). |
| „Command ... cannot be sent between nodes” | Serializator klastra nie potrafi zapisać polecenia. | Zrób z niego zwykłą case class, którą mpjsons umie zapisać i odczytać ([rozdział 17](17-cluster.md)). |

### Polecenie odpowiada `EventHandlingError`

**Co widzisz:** `EventHandlingError(eventName, errorId, commandId)`. Nic nie zostało zapisane.

**Prawdopodobne przyczyny**, według linii logu z `errorId`:

| Log mówi | Co się stało |
|---|---|
| `Error handling event ItemAdded, errorId: [...]` | Twój handler zdarzenia rzucił wyjątek na nowym zdarzeniu albo nie ma dla niego przypadku. |
| `Error persisting events, errorId: [...]` z `TooManyEventsException` | Agregat doszedł do limitu zdarzeń; zob. [niżej](#agregat-przestaje-przyjmować-polecenia-po-10-000-zdarzeń). |
| ... z `EventTooLargeException` | JSON zdarzenia przekracza limit rozmiaru; zob. [niżej](#zdarzenie-jest-za-duże). |
| ... z błędem połączenia | Baza danych nie działa albo żadne połączenie nie zwolniło się na czas (5 sekund przy domyślnej puli ScalikeJDBC). |
| ... „Undo cannot reach the squashed history” | Cofnięcie anulowałoby zdarzenia poniżej punktu squasha ([rozdział 14](14-squash.md)). |
| ... „Duplication cannot read the squashed history” | Duplikowanie wersji, która leży poniżej punktu squasha bazy. |
| ... „Maybe FirstEvent was not used” | Pierwsze zdarzenie nowego agregatu to zwykłe `Event`, a nie `FirstEvent`. |
| ... `RewriteChangesClassException` | Przepisanie historii próbowało zmienić klasę zdarzenia ([rozdział 15](15-rewriting-history.md)). |
| ... błąd dotyczący tabeli `commands_responses` | Polecenie idempotentne, a na starcie nie wywołano `PostgresCommandResponseState.initSchema()`. |

**Co zrobić:** napraw to, co wskazuje log. Przy błędach połączenia daj puli JDBC co najmniej tyle
połączeń, ile wątków ma `reactivecqrs.blocking-dispatcher` (domyślnie 32), i ustaw `max_connections`
w PostgreSQL tak, żeby starczyło na pule wszystkich węzłów.

### Polecenie odpowiada `CommandRejected`

**Co widzisz:** `CommandRejected(aggregateId, reason)`. Polecenie w ogóle się nie wykonało, więc
ponowne wysłanie go później jest bezpieczne.

| Powód mówi | Dlaczego | Co zrobić |
|---|---|---|
| „1000 commands of the aggregate wait already” | Tryb szeregowy, a kolejka agregatu jest pełna. | Odczekaj i wyślij ponownie, z kluczem idempotencji. Jeśli zdarza się to często, agregat jest gorącym punktem: podziel go. |
| „... is written once every node runs version ... or later” | Trwa aktualizacja krocząca. | Zob. [niżej](#aktualizacja-krocząca-z-nową-klasą-zdarzenia). |

### Ask przekracza limit czasu, a polecenie wykonuje się później

**Co widzisz:** twój kod dostaje `AskTimeoutException`, a mimo to zmiana później się pojawia.

**Dlaczego:** limit czasu twojego wywołania ask jest krótszy niż czas wykonania polecenia. Polecenia
mogą czekać: w kolejce trybu szeregowego, za wolnymi handlerami, a w klastrze także wtedy, gdy ich
właściciel jest przekazywany na inny węzeł (do 90 sekund). Własny limit czasu biblioteki wynosi
domyślnie 60 sekund.

**Co zrobić:**

1. Daj wywołaniu ask limit czasu dłuższy niż `commandTimeout` kontekstu albo skróć `commandTimeout`,
   żeby odpowiedź biblioteki zawsze przychodziła pierwsza.
2. Daj poleceniom klucz idempotencji i ponawiaj z tym samym kluczem: polecenie, które przeszło
   z opóźnieniem, nigdy nie zostanie wtedy zastosowane dwa razy.
3. Ogranicz liczbę poleceń w toku na wejściu do aplikacji, a ponad ten limit odpowiadaj „zajęte” (HTTP
   429 albo 503). [docs/SCALING.md](../SCALING.md), sekcja 2, pokazuje, jak dobrać ten limit.

### `GetAggregateMinVersion` kończy się błędem `AggregateInIncorrectVersionException`

**Co widzisz:** `Failure(AggregateInIncorrectVersionException)`: agregat nie osiągnął wersji
w ciągu `maxMillis`.

**Prawdopodobne przyczyny:** polecenie się nie powiodło albo w ogóle nie zostało wysłane; pytasz
o wersję, której polecenie nie tworzy (polecenie z trzema zdarzeniami przesuwa wersję o trzy); albo
właściciel zatrzymał się w trakcie czekania.

**Co zrobić:** weź wersję z `SuccessResponse` polecenia i pytaj dokładnie o nią; sprawdź, co
odpowiedziało polecenie; daj więcej `maxMillis`, jeśli agregat jest zajęty.

### Agregat przestaje przyjmować polecenia po 10 000 zdarzeń

**Co widzisz:** każde polecenie do jednego agregatu odpowiada `EventHandlingError`, a log wymienia
`TooManyEventsException`: „Too many versions for aggregate 1234@..., max version is 10000”.

**Dlaczego:** `aggregateVersionLimit` magazynu zdarzeń, domyślnie 10 000, chroni przed agregatami,
które rosną bez końca. Wersje nigdy nie maleją: migawki i squash sprawiają, że długi agregat ładuje
się taniej, ale żadne z nich nie znosi tego limitu.

**Co zrobić:**

1. Już na etapie projektu: dziel długowieczne rzeczy wzdłuż naturalnej granicy (miesiąc, sezon,
   wersja dokumentu) na osobne agregaty.
2. Jeśli agregat naprawdę potrzebuje więcej, zbuduj magazyn z wyższym limitem, na przykład
   `new PostgresEventStoreState(mpjsons, typesNames, aggregateVersionLimit = 50000)`. Limit dotyczy
   wszystkich typów agregatów tego magazynu.
3. Włącz migawki dla takich typów ([rozdział 13](13-snapshots.md)). Pomiar na jednej maszynie
   ([docs/perf/load-M5.md](../perf/load-M5.md), 17/10/2026): odtworzenie agregatu z 50 000 zdarzeń
   zajęło 392 ms przy odtwarzaniu wszystkich zdarzeń i 12 ms z migawki.

### Zdarzenie jest za duże

**Co widzisz:** `EventHandlingError`, a log wymienia `EventTooLargeException`: „Event of type ... is
too large: ... characters of JSON, max size is 100000 characters”.

**Co zrobić:**

1. Trzymaj dokumenty poza zdarzeniami (wzorzec „claim check”, jak numerek z szatni): najpierw zapisz
   plik w magazynie obiektów albo we własnej tabeli, potem wyślij polecenie, które go wskazuje
   (identyfikator, skrót, rozmiar), a w zdarzeniu i stanie trzymaj tylko ten odnośnik.
2. Jeśli klasa zdarzenia naprawdę musi nieść więcej, podnieś limit tylko dla niej przez
   `eventSizeLimitPerClassName = Map(classOf[ReportAttached].getName -> 1000000)` przy budowaniu
   `PostgresEventStoreState` (`ReportAttached` oznacza tu twoją własną klasę).

> **Ostrzeżenie:** duże zdarzenia kosztują wszędzie: przy każdym odtwarzaniu, ponownej publikacji,
> przebudowie i komunikacie między węzłami. Pomiar na jednej maszynie
> ([docs/perf/large-M5.md](../perf/large-M5.md), 17/10/2026): odtworzenie agregatu z 20 zdarzeniami
> po 5 MB zajęło 517 ms, a sterta urosła ze 117 do 323 MB; jedno takie zdarzenie zserializowane dla
> innego węzła ma 4,8 MB, daleko ponad 248 KiB, które może unieść komunikat.
> [docs/SCALING.md](../SCALING.md), sekcja 7, ma pełną listę tego, co można dostroić.

---

## Dostarczanie i projekcje

### Szyna zdarzeń niczego nie publikuje

**Co widzisz:** polecenia się udają, ale żadna projekcja się nie zmienia. Co 5 sekund log mówi
„Event bus waits for RegisterSubscribers from the host; nothing is published until then” albo „Event
bus waits for the subscriptions of: ...” z listą nazw. Węzeł nie zgłasza gotowości.

**Prawdopodobne przyczyny:**

- Aplikacja nigdy nie wysłała do szyny `RegisterSubscribers` ([rozdział 4](04-first-command.md)).
- Subskrybent z listy nigdy nie odpowiada na pytanie szyny o swoje subskrypcje: to aktor, który nie
  jest projekcją, albo projekcja zarządzana, której nadzorca nie może odczytać rejestru (log mówi
  „Projection ... could not read its builds; trying again in 10 seconds”, na przykład dlatego, że nie
  wywołano `PostgresProjectionRegistry.initSchema()`).
- Subskrybent odpowiada błędem: szyna pisze „Event bus asked ... for its subscriptions and got
  a failure, asks again: ...” z przyczyną, przy każdym cyklu, dopóki subskrybent nie odpowie.

W tym czasie nic nie ginie: zdarzenia są zapisane i czekają w outboxie, a gdy szyna wystartuje,
właściciele publikują je ponownie. Po `registrationTimeout` (2 minuty) szyna startuje bez
subskrybentów, którzy nie odpowiedzieli („Event bus starts without the subscriptions of: ...”):
dostarcza do pozostałych, ale trzyma każde zdarzenie w outboxie, dopóki brakujący nie odpowiedzą.

**Co zrobić:** wyślij `RegisterSubscribers` raz na starcie, ze wszystkimi projekcjami,
aktorami-listenerami i nadzorcami (`RegisterSubscribers(Nil)`, gdy nie ma żadnych). Linia „waits for
the subscriptions of” wymienia tych, których wciąż brakuje.

> **Pamiętaj:** subskrybentów ustala się na starcie. Projekcja, której brakuje na liście, po prostu
> nic nie dostaje i nikt się nie skarży: zdarzenia typu, którego nikt nie subskrybuje, od razu
> opuszczają outbox.

### Projekcja przestaje się aktualizować

**Co widzisz:** model odczytu przestaje się zmieniać, dla wszystkich agregatów albo tylko dla
niektórych.

Wyślij projekcji `GetProjectionStatus`, szynie `GetDeliveryStatus`, a potem:

| Co znajdujesz | Prawdopodobna przyczyna | Co zrobić |
|---|---|---|
| Agregat jest w `ProjectionStatus.failures` | Listener zawiódł na nim i się poddał | [Subskrypcja nie powiodła się](#subskrypcja-nie-powiodła-się) |
| Agregat jest w `retrying` | Listener zawodzi i jest ponawiany, przez mniej więcej dwie minuty | Przeczytaj w logu ostrzeżenie „Projection ... failed on ...” |
| Agregat jest w `waitingForGap` | Projekcji brakuje wersji i poprosiła o nie właściciela | Zwykle rozwiązuje się samo; po 10 minutach staje się nieudaną subskrypcją. Sprawdź, czy właściciel potrafi odtworzyć agregat |
| `DeliveryStatus.stopped` wymienia projekcję | Aktor projekcji się zatrzymał; jego zdarzenia czekają w outboxie | Znajdź przyczynę w logu. Niezarządzana: zrestartuj węzeł. Zarządzana: nadzorca uruchomi ją ponownie po 10 sekundach |
| `DeliveryStatus.behind` ją wymienia | Jest wolniejsza niż zapisy; szyna wstrzymuje to, co nie mieści się w jej oknie | Pozwól jej nadrobić zaległości; jeśli jest daleko w tyle, przebuduj ją ([rozdział 10](10-rebuilding-projections.md)); dbaj, żeby listenery były tanie |
| Zupełnie nic | Nie jest zarejestrowana w szynie albo szyna nie wystartowała | [Szyna zdarzeń niczego nie publikuje](#szyna-zdarzeń-niczego-nie-publikuje) |
| Projekcja zarządzana podaje stare dane, a `GetBuilds` pokazuje budowę w toku | Uruchomiono ją z `ServeStale` | Tak ma być: przełączy się, gdy nowa budowa nadrobi zaległości |

> **Wskazówka:** projekcja utworzona bez `.withDispatcher(BlockingDispatcher.id)` mówi o tym na
> starcie: „Projection ... runs its listener transactions on the default dispatcher”. Napraw to
> najpierw: to spowalnia także wszystko inne na domyślnym dispatcherze.

### Subskrypcja nie powiodła się

**Co widzisz:** błąd w logu, „Projection ... gave up on ShoppingCart 1234 at version 7 (...);
deliveries of it fail until it is retried or skipped”, i agregat w `ProjectionStatus.failures`.
Pozostałe agregaty płyną dalej.

**Prawdopodobne przyczyny:**

- Listener rzucał wyjątek na tej wersji przez mniej więcej dwie minuty ponowień: błąd w kodzie,
  ograniczenie twojej własnej tabeli albo dokument, którego nie ma („Document for update does not
  exist”).
- O brakujące wersje proszono przez 10 minut bez odpowiedzi („versions from ... are missing”):
  właściciel nie potrafi odtworzyć agregatu albo, w klastrze, zdarzenie jest za duże, by dotrzeć do
  węzła projekcji.
- Podczas budowy: zdarzenie, którego nie da się odczytać z magazynu.

Dopóki subskrypcja agregatu jest nieudana, tego agregatu nie da się ani zwinąć squashem, ani trwale
usunąć.

**Co zrobić:**

1. Przeczytaj `error` w zapisie awarii i log.
2. Usuń przyczynę i wdróż poprawkę.
3. Wyślij `RetryFailedSubscription(aggregateId)` albo `RetryAllFailedSubscriptions`. Awarie
   zapisane przed startem i tak są ponawiane na starcie, 20 na sekundę.
4. Tylko jeśli tej wersji nie da się zastosować nigdy, wyślij `SkipFailedVersion(aggregateId)`.
5. Dla budowy w toku opakuj komunikat: `ToBuildInProgress(RetryAllFailedSubscriptions)`. Budowa
   z nieudanymi subskrypcjami nie zastąpi aktywnej.

Tak jak w [rozdziale 9](09-projection-failures.md), na przykładzie tamtejszej listy koszyków:

<!-- snippet: cart/FailuresSpec.scala#status -->
```scala
import io.reactivecqrs.core.projection.{GetProjectionStatus, ProjectionStatus}

val status = Await.result((cartsList ? GetProjectionStatus).mapTo[ProjectionStatus], 5.seconds)
status.failures.foreach { f =>
  println(f.aggregateType.simpleName + " " + f.aggregateId.asLong + " stuck at version " + f.version.asInt + ": " + f.error)
}
// ShoppingCart 43 stuck at version 1: PSQLException: ERROR: duplicate key value violates unique constraint ...
```

a po wdrożeniu poprawki:

<!-- snippet: cart/FailuresSpec.scala#retry -->
```scala
import io.reactivecqrs.core.projection.{FailedSubscriptionsHandled, RetryFailedSubscription}

val handled = Await.result((cartsList ? RetryFailedSubscription(cartId)).mapTo[FailedSubscriptionsHandled], 5.seconds)
// FailedSubscriptionsHandled(Id(43),1)
```

> **Ostrzeżenie:** `SkipFailedVersion` oznacza wersję jako zastosowaną, nie stosując jej, więc
> modelowi odczytu brakuje tej zmiany. Potem przebuduj to, co projekcja zbudowała z tego agregatu:
> `RebuildAggregates(RebuildTarget.OneAggregate(cartId))` dla projekcji zarządzanej, która implementuje
> `clearForRebuild` ([rozdział 10](10-rebuilding-projections.md)).

### Zdarzenia zostają w outboxie („Undelivered” w logu)

**Co widzisz:** błędy od pollera outboxu, „Undelivered: ShoppingCart 1234 from version 7 still waits
in the outbox after 10 minutes; it is re-published again every 30 minutes”, i od szyny, „Undelivered
after 10 minutes: ... it stays in the outbox until it is published again”. Wcześniej ostrzeżenia
w rodzaju „Delivery waits for 1 minute: ...”. `GetOutboxStatus` wymienia je w `gaveUp`.

**Prawdopodobne przyczyny:**

- **Subskrybent, który nigdy nie odpowiada:** listener utknął w długiej transakcji (czeka na blokadę
  w bazie danych, woła powolną usługę zewnętrzną) albo samodzielnie napisany subskrybent nie wysyła
  `MessageAck`.
- **Subskrybent, który się zatrzymał:** jego zdarzenia zostają w outboxie, dopóki węzeł nie
  wystartuje ponownie (`DeliveryStatus.stopped` go wymienia).
- **Szyna nie wystartowała:** zob. [wyżej](#szyna-zdarzeń-niczego-nie-publikuje).
- **Właściciel nie może opublikować ich ponownie:** agregatu nie da się odtworzyć; albo log mówi „No
  command bus registered for ..., cannot re-publish”, bo szynę poleceń tego typu zbudowano z inną
  szyną zdarzeń niż ta, której używa poller.
- **Subskrybent daleko w tyle:** to, czego jeszcze nie przyjął, czeka na swoją kolej w magazynie.
- **W klastrze:** zdarzenie albo stan za duży, by dotrzeć do węzła subskrybenta.

I jedna przyczyna bez żadnego śladu w logu: **nie utworzono `OutboxPollerActor`.** Wtedy zdarzenia,
których pierwsze dostarczenie się nie zakończyło (awaria, restart, subskrybent, który akurat nie
działał), nigdy nie zostaną opublikowane ponownie, a outbox tylko rośnie.

**Co zrobić:**

1. Wyślij `GetDeliveryStatus` (do szyny każdego węzła), `GetOutboxStatus` i `GetProjectionStatus`.
2. Napraw albo zrestartuj subskrybenta; czekające zdarzenia dotrą do niego same.
3. Sprawdź, czy w systemie działa dokładnie jeden poller outboxu (w klastrze uruchomiony przez
   `Singletons.start`).
4. Po długiej awarii pamiętaj, że poller publikuje ponownie najwyżej 1 000 agregatów na 30-sekundowy
   przegląd: milion czekających agregatów zajmie około 8,3 godziny (oszacowanie
   w [docs/SCALING.md](../SCALING.md), sekcja 4). Przebudowa projekcji może być szybsza; możesz też
   podkręcić tempo, dopóki zaległości nie znikną:

   <!-- snippet: cart/FailuresSpec.scala#pollerOptions -->
   ```scala
   import io.reactivecqrs.core.delivery.{OutboxPollerActor, OutboxPollerOptions, RedeliveryPolicy}

   val outboxPoller = system.actorOf(Props(new OutboxPollerActor(eventStore, eventBus,
     OutboxPollerOptions(redelivery = RedeliveryPolicy.Options(maxRepublishPerPoll = 5000)))), "outboxPoller")
   ```

> **Pod maską:** poller outboxu jest jedynym właścicielem ponownych dostarczeń (D8): co 30 sekund
> odczytuje, które agregaty czekają, a czysta `RedeliveryPolicy` decyduje, na kogo przyszła kolej.
> Cała historia w [ARCHITECTURE.md, sekcja 5.1](../ARCHITECTURE.md).

### Nowa projekcja nie widzi starych agregatów

**Co widzisz:** po dodaniu nowej projekcji widać w niej agregaty, które od tamtej pory się zmieniły,
ale nie pozostałe.

**Dlaczego:** projekcja niezarządzana startuje, nic nie wiedząc. Gdy agregat dostaje nowe zdarzenie,
projekcja zauważa lukę i prosi o wcześniejsze zdarzenia tego agregatu; agregat, który już nigdy się
nie zmieni, nigdy do niej nie trafi.

**Co zrobić:** uruchom projekcję jako zarządzaną, przez `Projections.start`: jej pierwsza budowa czyta
cały magazyn zdarzeń, zanim zacznie obsługiwać zapytania ([rozdział 10](10-rebuilding-projections.md)).
Jeśli coś poszło nie tak z projekcją zarządzaną, wyślij jej nadzorcy `RebuildProjection`.

### Zapytanie zwraca `None` (albo stare dane) tuż po poleceniu

**Dlaczego:** strona odczytu jest ostatecznie spójna. Polecenie się udało; projekcja stosuje jego
zdarzenia chwilę później.

**Co zrobić:** odpowiadaj na zapytanie przez `delayIfNotAvailable` z terminem, żeby czekało na dane
([rozdział 8](08-projections.md)); albo wyślij szynie poleceń `GetAggregateMinVersion` z wersją
z `SuccessResponse` ([rozdział 7](07-reading-the-past.md)). Jeśli odpowiedź wciąż brzmi `None`,
sprawdź projekcję według sekcji powyżej.

---

## Długie historie

### Squash zostaje odrzucony

**Co widzisz:** `SquashRefused(id, reason)` i nic się nie zmienia. Scheduler squasha liczy swoje
odmowy (`SquashStatus.refused`) i próbuje tych agregatów ponownie w późniejszym przebiegu.

| Powód mówi | Co to znaczy | Co zrobić |
|---|---|---|
| „squash is not enabled for ...” | Kontekst nie ma polityki `squash`. | Daj ją kontekstowi: `override val squash: Option[SquashPolicy[ShoppingCart]] = Some(SquashPolicy(...))` ([rozdział 14](14-squash.md)). |
| „version ... is not between 1 and ...” | `upTo` to 0, liczba ujemna albo więcej niż bieżąca wersja. | Wybierz wersję, którą agregat ma. |
| „already squashed up to version ...” | Agregat jest już zwinięty aż do tej wersji. | Wybierz późniejszą wersję albo zostaw go. |
| „events at or below version ... still wait in the outbox” | Któryś subskrybent nie potwierdził jeszcze tych zdarzeń. | Poczekaj albo zob. [Zdarzenia zostają w outboxie](#zdarzenia-zostają-w-outboxie-undelivered-w-logu). |
| „a subscription of the aggregate is failed” | Projekcja poddała się na tym agregacie. | Najpierw [napraw subskrypcję](#subskrypcja-nie-powiodła-się). |
| „an undo after version ... undid events at or below it” | Późniejsze cofnięcie anuluje zdarzenia, które squash by zwinął. | Zwiń historię do wersji cofnięcia lub dalej albo poniżej pierwszego zdarzenia, które ono cofnęło. |
| „the first event of duplicate ..., which the squash detaches, was undone” | Duplikatu skopiowanego poniżej punktu nie da się odłączyć. | Zwiń historię najwyżej do wersji, w której skopiowano ten duplikat. |
| „the first event of duplicate ..., which the squash detaches, still waits in the outbox” | Pierwsze zdarzenie duplikatu nie jest jeszcze dostarczone. | Poczekaj i spróbuj ponownie. |
| „a subscription of duplicate ..., which the squash detaches, is failed” | Projekcja poddała się na tym duplikacie. | Najpierw napraw tę subskrypcję. |
| „the duplicates of the aggregate changed meanwhile” | Ktoś zduplikował agregat w trakcie squasha. | Spróbuj ponownie. |
| „the aggregate is deleted at version ...” | W tej wersji jego stan jest usunięty. | Nie ma tam czego zwijać. |
| „making the squash event failed: ...” | Twoje `toEvent` rzuciło wyjątek. | Popraw `toEvent`. |
| „the event handlers fail on the squash event ...” | Twoje handlery zdarzeń nie mają przypadku dla zdarzenia squasha albo rzucają na nim wyjątek. | Obsłuż zdarzenie squasha w handlerach zdarzeń. |
| „the squash event ... does not give back the state it replaces” | Zastosowane do stanu początkowego zdarzenie squasha daje inny stan. | Popraw `toEvent` albo handler, żeby stan wracał taki sam. Jeśli pominięcie danych jest zamierzone, domieszaj `SquashMayChangeState` do klasy zdarzenia. |
| „duplicate ...: ”, a po nim jeden z trzech ostatnich | Ta sama kontrola nie przeszła dla duplikatu, który squash by odłączył. | Jak wyżej. |
| „... is written once every node runs version ... or later” | Trwa aktualizacja krocząca, a klasa zdarzenia squasha jest nowa. | Najpierw dokończ wdrażanie. |
| „the aggregate cannot be restored: ...” | Właściciel nie potrafi odczytać agregatu. | Zob. przyczyny błędów odtwarzania [wyżej](#polecenie-odpowiada-commandhandlingerror). |
| „the squash failed: ...” | Zawiodła baza danych. | Sprawdź bazę danych i spróbuj ponownie. |

> **Wskazówka:** kontrola, czy zdarzenie squasha oddaje ten sam stan, porównuje przez `==`. Case class
> trzymające `Vector`, `List`, `Map` albo `Set` porównują się po zawartości, tak jak można się
> spodziewać. `Array` porównuje się po referencji, więc stan, który ją zawiera, nigdy nie jest równy
> swojej kopii i każdy squash zostaje odrzucony. Używaj `Vector`.

### Odczyt historyczny poniżej punktu squasha kończy się błędem

**Co widzisz:** `Failure(AggregateSquashedException)`: „The history of aggregate 1234 is squashed up
to version 50; version 3 cannot be read” (albo „its state at ... cannot be read”).

**Dlaczego:** tak właśnie działa squash: zdarzeń poniżej punktu squasha już nie ma. Odczyt wersji
poniżej niego (`GetAggregateForVersion`, `GetEventsForAggregateForVersion`) albo chwili sprzed czasu
zdarzenia squasha (`GetAggregateAtInstant`) kończy się błędem, zamiast dać złą odpowiedź.
`GetEventsForAggregate` wciąż działa: lista zaczyna się od zdarzenia squasha.

**Co zrobić:**

- Czytaj od punktu squasha albo później: wyjątek mówi, gdzie ten punkt jest (`squashedUpTo`).
- Na potrzeby audytu zdecyduj *przed* squashem, które klasy zdarzeń mają pozostać czytelne, wymień je
  w `SquashPolicy.keep` i czytaj je przez `GetArchivedEvents`. Zdarzenia zwinięte, zanim klasa trafiła
  do `keep`, przepadły na dobre.
- Zachowaj czytelną najnowszą historię dzięki `AutomaticSquash(afterEvents, keepLast, keepNewerThan)`
  ([rozdział 14](14-squash.md)).

<!-- snippet: cart/SquashingSpec.scala#squashedRead -->
```scala
(commandBus ? GetAggregateForVersion(cartId, AggregateVersion(3))).mapTo[Try[Aggregate[ShoppingCart]]].map {
  case Success(cart) => Some(cart)
  case Failure(e: AggregateSquashedException) => None // only versions from e.squashedUpTo on exist
  case Failure(e) => throw e
} // with an implicit ExecutionContext in scope
```

### Trwałe usunięcie czeka

**Co widzisz:** `PermanentDeleteEvent` jest zapisane, a wiersze agregatu wciąż są w bazie danych.
W logu jest ostrzeżenie „Aggregate 1234 of type ShoppingCart is not deleted yet: ...” albo błąd „...
is not deleted: ...”.

**Jak to działa:** usunięcie rusza, gdy każdy subskrybent potwierdzi zdarzenie. Gdy coś stoi na
przeszkodzie, zdarzenie zostaje w outboxie, a usunięcie jest ponawiane za każdym razem, gdy poller
outboxu każe opublikować je ponownie (po 3 i 6 minutach, potem co 30 minut). Subskrybent zatrzymany
albo daleko w tyle też opóźnia usunięcie.

| Powód mówi | Co zrobić |
|---|---|
| „a subscription of it is failed” | [Napraw subskrypcję](#subskrypcja-nie-powiodła-się). Projekcja usunięta z kodu mogła zostawić po sobie zapisy niepowodzeń: dla zarządzanej wywołaj `projections.retire(name)`, dla niezarządzanej `subscriptionsState.clearSubscriptionsInfo(...)` z nazwą jej klasy. |
| „the first event of duplicate ..., which the delete detaches, still waits in the outbox” | Nic: usunięcie zostanie ponowione, gdy to zdarzenie zostanie dostarczone. |
| „a subscription of duplicate ..., which the delete detaches, is failed” | Napraw tę subskrypcję. |
| „the first event of duplicate ..., which the delete detaches, was undone” | Takiego duplikatu nie da się odłączyć; najpierw się nim zajmij, na przykład usuwając trwale również jego. |
| „the duplicates of the aggregate changed meanwhile” | Nic: usunięcie zostanie ponowione. |
| „... is written once every node runs version ... or later” | Dokończ aktualizację kroczącą. |
| Błąd: „aggregates ... were duplicated from it, and the type has no squash event to detach them” | Duplikaty czytają historię tego agregatu, a odłączyć je może tylko zdarzenie squasha: daj kontekstowi `SquashPolicy`. Do tego czasu usunięcie nigdy nie nastąpi. |
| Błąd: „duplicate ...: ”, a po nim problem ze zdarzeniem squasha | Zdarzenie squasha nie przechodzi kontroli dla stanu duplikatu; popraw je tak, jak opisuje sekcja [Squash zostaje odrzucony](#squash-zostaje-odrzucony). |

> **Pamiętaj:** trwałe usunięcie usuwa zdarzenia agregatu, jego archiwum, wiersze outboxu, migawkę
> i wiersze subskrypcji. Modele odczytu są twoje: projekcje usuwają swoje dokumenty, gdy zobaczą
> `PermanentDeleteEvent` ([rozdział 6](06-events.md)).

### Przepisanie historii się udaje, ale modele odczytu wciąż pokazują starą wartość

**Dlaczego:** przepisanie, którego handler zwraca przepisane zdarzenia, ale żadnego nowego zdarzenia,
zostaje zapisane w tej samej wersji i niczego nie publikuje: projekcje nigdy się o nim nie dowiadują.
(Przed 1.0 takie przepisanie dostawało odpowiedź `SuccessResponse` i nie zapisywało w ogóle niczego,
zgłoszenie F146.) Projekcja zachowuje też to, co skopiowała gdzie indziej, bez względu na to, co
zmieniło przepisanie.

**Co zrobić:** dodaj zdarzenie kompensujące, tak jak robi to koszyk zakupów:
`RewriteCommandSuccess(rewritten, CartNameRewritten(name))`, i obsłuż je w każdej projekcji, która
pokazuje tę wartość; albo potem przebuduj projekcje. Żeby wymazać wartość wszędzie, gdzie biblioteka
ją trzyma, użyj pełnego przepisania historii, `HistoryRewriter` ([rozdział 15](15-rewriting-history.md)).

---

## Start, aktualizacje i klastry

### Start trwa długo

**Prawdopodobne przyczyny:**

- **Odtwarzanie.** Właściciel przy pierwszym komunikacie odtwarza całą historię swojego agregatu,
  chyba że kontekst robi migawki. Po restarcie każdy używany agregat płaci tę cenę raz. Oszacowanie
  w [docs/SCALING.md](../SCALING.md), sekcja 3: węzeł odtwarza na sekundę około 80 agregatów po
  50 000 zdarzeń bez migawek i około 2 700 z migawkami.
- **Outbox po awarii.** Przy pierwszym przeglądzie, 10 sekund po starcie, zdarzenia zapisane przed
  startem są publikowane ponownie: do 1 000 agregatów na przegląd, a każdy z nich uruchamia
  i odtwarza właściciela.
- **Projekcje, które na starcie ponawiają stare awarie,** 20 na sekundę.
- **Szyna zdarzeń, która czeka na swoich subskrybentów.** Węzeł nie jest gotowy, dopóki każdy z nich
  nie odpowie („Event bus waits for the subscriptions of: ...”) albo nie minie `registrationTimeout`
  (2 minuty).
- **Inicjalizacja schematu, która czeka na inny węzeł.** Węzły po kolei biorą blokadę schematu;
  czekający węzeł rezygnuje po pięciu minutach („The schema lock was not free for 5 minutes: another process
  initializes the schema”).
- **Ciężkie zadania na ścieżce startu.** `buildIndexesConcurrently()`, `backfillEventKeys()`
  i `backfillSpaces(...)` mają działać w tle, nigdy przed startem aktorów.

**Co zrobić:** włącz migawki dla typów agregatów z długą historią i zwiń starą historię squashem
([rozdział 13](13-snapshots.md), [rozdział 14](14-squash.md)); uruchamiaj ciężkie zadania w tle;
sprawdź w `Readiness(system).waitingFor`, na co węzeł jeszcze czeka. Nowa budowa projekcji
zarządzanej nie spowalnia startu: w tym czasie zapytania obsługuje aktywna budowa.

### Zapisanego zdarzenia nie da się już odczytać po zmianie klasy

**Co widzisz:** po wdrożeniu polecenia do niektórych agregatów odpowiadają `CommandHandlingError`,
a zapytania `Failure(AggregateRestoreFailedException)`; log mówi „Restoring aggregate ... failed”
z błędem JSON-a albo klasy. Budowy zapisują nieudane subskrypcje, a `GetEventsForAggregate` kończy się
błędem. Albo `UnsupportedEventVersionException`: „... is stored with event_type_version ..., but event
versioning was removed in 1.0”.

**Dlaczego:** zdarzenia są zapisywane jako JSON pod nazwą swojej klasy i odczytywane z powrotem do tej
klasy. Zmiana nazwy albo przeniesienie klasy zdarzenia, zmiana typu pola albo usunięcie klasy, której
wciąż używają zapisane zdarzenia, psuje odczyt. Wierszy zapisanych z wersjami zdarzeń z wydań sprzed
1.0 też nie da się odczytać.

**Co zrobić:**

1. Nigdy nie zmieniaj klasy zapisanego zdarzenia. Zostaw starą klasę (oznacz ją `@deprecated`), dodaj
   nową (`ItemAddedV2`) i obsługuj obie w handlerach zdarzeń i projekcjach ([rozdział 6](06-events.md)).
2. Przed każdą aktualizacją uruchom `StoredDataCheck` na kopii produkcji, z nowym kodem:

   <!-- snippet: cart/ProductionSpec.scala#upgradeCheck -->
   ```scala
   import io.reactivecqrs.core.tools.StoredDataCheck

   val report = StoredDataCheck.run(mpjsons, typesNames,
     Seq(StoredDataCheck.ProjectionTable[String]("storeA"))) // one per document store, with its document class
   println(report.text)
   if (report.unreadable > 0) sys.error("Fix the unreadable rows before the upgrade")
   ```

   Czyta każde zapisane zdarzenie, zdarzenie z archiwum, zlecenie sagi, odpowiedź idempotentną
   i wskazane przez ciebie tabele projekcji, a potem zgłasza dla każdej klasy wiersze, których nie da
   się odczytać.
3. Zanim usuniesz starą klasę z kodu, zapytaj `StoredDataCheck.usersOf(className)`: usuń ją tylko
   wtedy, gdy zarówno `events`, jak i `archived` wynoszą 0.
4. Gdy klasa korzenia agregatu zmienia kształt, podnieś `SnapshotPolicy.schemaVersion`, żeby stare
   migawki zostały odłożone na bok, zamiast być wczytywane do nowego kształtu.

### Aktualizacja krocząca z nową klasą zdarzenia

**Co widzisz:** w trakcie wdrażania polecenia odpowiadają `CommandRejected` z powodem w rodzaju
„ItemAddedV2 is written once every node runs version 2.0.0 or later”, a próby squasha są odrzucane
z tym samym powodem. (`ItemAddedV2(name, source)` to nowe zdarzenie koszyka w wersji 2.0.0 koszyków
z [rozdziału 17](17-cluster.md).)

**Dlaczego:** kontekst wymienia tę klasę w `writtenFrom`. Węzeł starszej wersji nie umiałby odczytać
takiego zdarzenia, więc nikt go nie zapisuje, dopóki każdy członek klastra nie ogłosi co najmniej tej
`pekko.cluster.app-version`. Węzeł, który jeszcze nie zna członków klastra, nie zapisuje żadnej nowej
klasy.

**Co zrobić:**

1. Niech handlery w tym czasie zapisują starsze zdarzenie, tak jak robi to wersja 2.0.0 koszyków
   z rozdziału 17:

   <!-- snippet: cart/CartsV2.scala#writtenFrom -->
   ```scala
   override val writtenFrom: Map[Class[_], String] = Map(classOf[ItemAddedV2] -> "2.0.0")

   override def commandHandlers = cart => {
     case c: AddItemConcurrently if everyNodeRuns("2.0.0") => CommandSuccess(ItemAddedV2(c.name, "version 2"))
     case other => super.commandHandlers(cart)(other) // ItemAdded, as in 1.0.0
   }
   ```

   To samo wydanie obsługuje `ItemAddedV2` w handlerach zdarzeń i projekcjach, więc każdy węzeł tej
   wersji potrafi je odczytać.

2. Ustaw `pekko.cluster.app-version` na wersję wydania na każdym węźle. Gdy żaden członek klastra jej
   nie ustawia, przełącznik jest wyłączony i każda klasa jest zapisywana od razu, czego stary węzeł
   może nie odczytać.
3. Nigdy nie wycofuj wdrożenia poniżej wersji, której nowe klasy zostały już zapisane.
4. Wysyłaj nową klasę polecenia dopiero wtedy, gdy każdy węzeł działa w wersji, która ją zna.
5. Pole dodawane do klasy, która podróżuje między węzłami (polecenie, odpowiedź, odpowiedź na
   zapytanie), musi być `Option`: brakujące pole odczytuje się jako `None`, zero albo `null`, nigdy
   jako jego wartość domyślną.
6. Zmiana rozmieszczenia typu albo `numberOfShards` wymaga pełnego zatrzymania, a nie aktualizacji
   kroczącej ([rozdział 17](17-cluster.md)).

### Węzły nie mogą wysyłać sobie komunikatów

**Co widzisz:** polecenia do agregatów, których właściciele działają na innym węźle, dostają
`FailureResponse` z „cannot be sent between nodes: ... Java serialization ... was disabled”, log co
kilka sekund mówi „Failed to serialize message”, a węzeł nigdy nie staje się gotowy: na starcie
zapisał w logu „Messages between nodes would go through ..., not the library's serializer”.

**Dlaczego:** komunikaty między węzłami muszą przechodzić przez serializator biblioteki. Własny
`reference.conf` Pekko wiąże `java.io.Serializable` z serializacją Javy, która jest wyłączona; plik
biblioteki wiąże go z jej serializatorem; a z tych dwóch wygrywa ten, który jest wcześniej na
classpath. Pod sbt biblioteka jest pierwsza; w spakowanej aplikacji kolejność na classpath może być
dowolna.

**Co zrobić:** umieść to powiązanie w swojej konfiguracji, gdzie wygrywa z każdym `reference.conf`:

```hocon
pekko.actor.serialization-bindings { "java.io.Serializable" = reactivecqrs }
```

### `MessageTooLargeException` w klastrze

**Co widzisz:** ask od razu kończy się błędem `MessageTooLargeException`: „A ... of ... bytes is too
large to be sent between nodes; the limit is ... bytes”. Węzeł wysyłający zapisuje w logu klasę
i rozmiar, najwyżej raz na klasę co 10 sekund. Polecenie typu `Sharded` dostaje zamiast tego
`FailureResponse`. Dostarczenie za duże dla projekcji na innym węźle zostawia ją w tyle dla tego
agregatu, dopóki po 10 minutach luka nie stanie się nieudaną subskrypcją.

**Dlaczego:** komunikat między węzłami musi zmieścić się w ramce Pekko, domyślnie 256 KiB; limit
biblioteki jest o 8 KiB mniejszy i wynosi 248 KiB. Na jednym węźle nic nie jest serializowane, więc
problem pojawia się tylko w klastrze.

**Co zrobić:**

- Utrzymuj polecenia, odpowiedzi, stany i odpowiedzi na zapytania wyraźnie poniżej 248 KiB; trzymaj
  dokumenty poza zdarzeniami i poleceniami (zob. [Zdarzenie jest za duże](#zdarzenie-jest-za-duże)).
- Duży stan czytaj na własnym węźle przez `GetAggregateAtInstant(id, Instant.now())`: zostaje tam
  odtworzony z magazynu, zamiast przychodzić od właściciela.
- Dziel duże odpowiedzi na zapytania na strony.
- Jeśli większych komunikatów nie da się uniknąć, podnieś `pekko.remote.artery.advanced.maximum-frame-size`
  albo kieruj duże komunikaty przez `large-message-destinations`. Uważaj na pamięć: Pekko trzyma do 128
  buforów o rozmiarze ramki, więc ramka 1 MiB może zająć do 128 MiB pamięci bezpośredniej
  (oszacowanie w [docs/SCALING.md](../SCALING.md), sekcja 6).

---

## Nawyki, które zapobiegają większości tych kłopotów

- Daj klucz idempotencji każdemu poleceniu, które klient może wysłać dwa razy.
- Ustaw limit czasu wywołania ask po stronie klienta dłuższy niż limit czasu polecenia.
- Niech zdarzenia, polecenia i stany będą małe; dokumenty zapisuj gdzie indziej i trzymaj odnośnik.
- Włącz migawki dla długowiecznych agregatów; zaplanuj `SquashPolicy.keep` przed pierwszym squashem.
- Nigdy nie zmieniaj nazwy ani kształtu klasy zapisanego zdarzenia: dodaj obok niej `V2`.
- Przed każdą aktualizacją uruchom `StoredDataCheck` na kopii produkcji.
- Obserwuj `GetProjectionStatus`, `GetDeliveryStatus`, `GetOutboxStatus` i metryki, a na „Undelivered”
  i „gave up” w logu ustaw alerty.

To koniec książki. Do [spisu treści](README.md) wracaj, kiedy tylko go potrzebujesz.

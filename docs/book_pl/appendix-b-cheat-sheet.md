# Załącznik B. Ściągawka: Wiadomości i odpowiedzi

Ten dodatek to strona, którą trzymasz otwartą obok edytora. Zawiera listę wiadomości, które wysyłasz do
aktorzy biblioteki, pogrupowani według tego, kto ich otrzymuje i co dokładnie wraca: klasa i czy
Jest on dostarczany w pakiecie „Try”. Na końcu znajdziesz metody, które Twoja aplikacja wywołuje sama.
(konfiguracja schematu, uzupełnianie, sprawdzanie) oraz ustawienia domyślne, które warto znać na pamięć.

Każdy wiersz został sprawdzony pod kątem zgodności z kodem ReactiveCQRS 1.0 (`1.0.0-SNAPSHOT`). Biblioteka wysyła wiadomości
wysyłane tylko do siebie są pomijane.

---

## Jak prosić i dwa rodzaje „nie”

We wszystkich przykładach użyto funkcji Pekko `ask` z limitem czasu i `mapTo` do nazwania typu odpowiedzi:

```scala
zaimportuj org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
importuj io.reactivecqrs.api._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

niejawny limit czasu wartości: Limit czasu = Limit czasu (5 sekund)

// Polecenie: odpowiedź jest odpowiedzią na polecenie.
dodano wartość: Future[CustomCommandResponse[_]] =
  (commandBus ? AddItem(userId, cartId, AggregateVersion(1), "milk")).mapTo[CustomCommandResponse[_]]

// Zapytanie: odpowiedzią jest Try.
val cart: Future[Try[Aggregate[ShoppingCart]]] =
  (commandBus ? GetAggregate(cartId)).mapTo[Try[Aggregate[ShoppingCart]]]
```

> **Pamiętaj:** istnieją dwa sposoby, aby odpowiedzieć „nie”. `Failure(e)` wewnątrz `Try` to
> zwykła odpowiedź: Twój `Future` kończy się sukcesem i zawiera nieudany `Try`. `Status.Failure(e)` tworzy
> Samo `Future` kończy się niepowodzeniem z `e`. Poniższe tabele podają, z którego z nich korzysta każda wiadomość.

---

## Magistrala poleceń (`AggregateCommandBusActor`)

Jeden na typ agregatu (i na węzeł). ​​Wiadomości z `io.reactivecqrs.api._`, z wyjątkiem
`PassivateAggregate`, który znajduje się w `io.reactivecqrs.core.commandhandler.AggregateCommandBusActor`.

| Wysyłasz | Dostajesz z powrotem | Warto wiedzieć |
|---|---|---|
| Polecenie `FirstCommand`, takie jak `CreateShoppingCart(None, userId, "Groceries")` | Odpowiedź polecenia (następna sekcja), na przykład `SuccessResponse(newId, AggregateVersion(1))` | Magistrala nadaje nowemu agregatowi identyfikator, chyba że `chosenAggregateId` polecenia go określa. Wybrany identyfikator, który nie jest dodatni, nie jest mniejszy niż 2^53 ani nie mieści się w zakresie innej instalacji, otrzymuje odpowiedź `FailureResponse` przed uruchomieniem czegokolwiek. |
| Polecenie `Command`, takie jak `AddItem(userId, cartId, AggregateVersion(1), "milk")` | Odpowiedź na polecenie | Oczekiwana wersja musi być wersją bieżącą agregatu, w przeciwnym razie odpowiedzią jest `AggregateConcurrentModificationError`. |
| A `ConcurrentCommand` | Odpowiedź na polecenie | Działa na bieżącej wersji; po kolizji uruchamia się ponownie, maksymalnie 10 prób. |
| `RewriteHistoryCommand` lub `RewriteHistoryConcurrentCommand` | Odpowiedź na polecenie | Zobacz [rozdział 15](15-rewriting-history.md). |
| `ReserveAggregateId` | `AggregateId` (niezapakowane) | Dla `chosenAggregateId`. Jeśli pula identyfikatorów nie jest dostępna, magistrala odpowiada `Status.Failure`, więc zapytanie kończy się niepowodzeniem. |
| `GetAggregate(id)` | `Try[Aggregate[R]]`: `Success(Aggregate(id, version, Some(root)))` | `aggregateRoot` ma wartość `None` w wersji 0 (agregat nigdy nie istniał) i po zwróceniu przez obsługę zdarzeń wartości `null` (usunięty). `Failure(AggregateRestoreFailedException)`, gdy nie można odczytać agregatu z magazynu. |
| `GetAggregateMinVersion(id, version, maxMillis)` | `Try[Aggregate[R]]`, gdy agregat osiągnie `version` | `Failure(AggregateInIncorrectVersionException)`, gdy nie osiągnie `maxMillis` lub gdy jego właściciel zatrzyma się pierwszy; `Failure(AggregateRestoreFailedException)`, gdy nie można go odczytać z magazynu. |
| `GetAggregateForVersion(id, version)` | `Try[Aggregate[R]]`: stan w tej wersji | Odczyt ze sklepu, a nie od właściciela. `Failure(AggregateSquashedException)` poniżej punktu squasha. |
| `GetAggregateAtInstant(id, instant)` | `Try[Aggregate[R]]`: stan w tym momencie | `Failure(AggregateSquashedException)` na chwilę przed czasem zdarzenia squash. |
| `GetEventsForAggregate(id)` | `Try[List[EventInfo[R]]]`: wszystkie zdarzenia, najstarsze na początku | Dołączono zdarzenia niewykonane; lista duplikatów zaczyna się od zdarzeń z bazy. Cała historia jest zawarta w jednej wiadomości. |
| `GetEventsForAggregateForVersion(id, version)` | `Try[List[EventInfo[R]]]`: zdarzenia do `version` | `Failure(AggregateSquashedException)` poniżej punktu zgniecenia. |
| `GetArchivedEvents(id)` | `Try[ArchivedEvents[R]]`: `ArchivedEvents(id, events)` | Każde `ArchivedEvent(info, undoneFrom, archived)`: zdarzenie w takiej formie, w jakiej zostało zapisane, wersja operacji cofania, która je anulowała (jeśli taka istniała) oraz data archiwizacji przez squash. Najpierw najstarsze. |
| `SquashAggregate(id, upTo)`, opcjonalnie z `userId` i `metadata` | `AggregateSquashed(id, upTo)` lub `SquashRefused(id, reason)`, niezapakowane | Oba rozszerzają `SquashResponse`. Dodatek C wyjaśnia każdą odmowę. |
| `Materialize(id, userId)`, do efemerycznej magistrali | `Materialize(id, persistentId)` lub `MaterializeRefused(id, reason)`, niezapakowane | Oba rozszerzają `MaterializeResponse`; projekt jest usuwany po materializacji ([rozdział 20](20-ephemeral-aggregates.md)). Polecenia dla identyfikatora, którego magistrala nie obsługuje, otrzymują `EphemeralAggregateLost` lub `EphemeralAggregateUnknown`. |
| `RewriteEverywhere(id, change, userId)` | `HistoryRewritten(id, events, archived, fields)` lub `RewriteRefused(id, reason)`, niezapakowane | Całkowite przepisanie jednego agregatu w jego właścicielu; oba rozszerzają `RewriteResponse`. `HistoryRewriter` wysyła to również do każdej kopii ([rozdział 15](15-rewriting-history.md)). |
| `AggregateCommandBusActor.PassivateAggregate(id)` | Nic | Zatrzymuje właściciela agregatu po udzieleniu odpowiedzi na polecenia, które uruchomił; następna wiadomość uruchamia nowego właściciela. |
| Coś jeszcze | `Status.Failure(IllegalArgumentException)` | Zapytanie kończy się niepowodzeniem z błędem „Nie można obsłużyć tego rodzaju wiadomości”. |

Odczyty historyczne (`GetAggregateForVersion`, `GetAggregateAtInstant` i listy zdarzeń) są zawijane
jakiekolwiek inne problemy ze sklepem w `Failure(AggregateRestoreFailedException)`.

> **Ostrzeżenie:** w klastrze odpowiedź pochodząca od właściciela innego węzła musi mieścić się w jednym
> wiadomość między węzłami (około 248 KiB z domyślnym rozmiarem ramki Pekko). Większy rozmiar sprawia, że
> zapytanie kończy się niepowodzeniem z wyjątkiem `MessageTooLargeException`. `GetEventsForAggregate` i innymi odczytami historycznymi
> odpowiedzi zawsze są udzielane na Twoim własnym węźle, z magazynu. Więcej w [rozdziale 17](17-cluster.md).

---

## Odpowiedzi na polecenia (`CustomCommandResponse.scala`)

Każde polecenie otrzymuje dokładnie jedno z nich. Kopia idempotentnego polecenia, które zostało już zastosowane.
otrzymuje odpowiedź, jaką otrzymała pierwsza kopia.

| Odpowiedź | Co to oznacza | Typowe powody |
|---|---|---|
| `SuccessResponse(aggregateId, aggregateVersion)` | Zdarzenia są zapisywane; `aggregateVersion` to nowa wersja. | Twój moduł obsługi zwrócił `CommandSuccess(...)`. Bez zdarzeń nic nie jest zapisywane, a wersja pozostaje. |
| `CustomSuccessResponse(aggregateId, aggregateVersion, info)` | To samo, ale z Twoim `info`. | Twój moduł obsługi zwrócił `CommandSuccess(event, info)`, podobnie jak `AddItemWithReceipt` zwraca `ItemReceipt`. |
| `FailureResponse(exceptions)` | Odrzucono; nic nie zostało zapisane. `exceptions` to `List[String]` komunikatów. | Twój handler zwrócił `CommandFailure("...")`. Biblioteka również odrzuca: wybrany identyfikator, którego nie dopuszcza, nowy agregat, którego pierwsze zdarzenie nie zawiera spacji, polecenie typu sharded, zbyt duże, aby wysłać je między węzłami. |
| `AggregateConcurrentModificationError(aggregateId, aggregateType, expected, was)` | Konflikt wersji; nic nie zostało zapisane. `expected` to to, co powiedziałeś, `was` to, co ma agregat. | `Command` ze starą, oczekiwaną wersją; agregat, który nie istnieje (`was` to wersja 0); `ConcurrentCommand`, który kolidował 10 razy; `FirstCommand`, którego wybrany identyfikator jest już zajęty. |
| `CommandHandlingError(commandName, errorId, commandId)` | Nie można obsłużyć polecenia; nic nie zostało zapisane. W dzienniku przyczyna jest podana pod `errorId`. | Przekroczono limit czasu polecenia; program obsługi zgłosił lub nie obsłużył polecenia; nie można przywrócić agregatu; nie istnieje lub został usunięty; nie można uzyskać identyfikatorów (wówczas `commandId` to `CommandId(0)`). |
| `EventHandlingError(eventName, errorId, commandId)` | Zdarzenia nie mogły zostać zastosowane ani zapisane; nic nie zostało zapisane. W dzienniku przyczyna jest podana pod `errorId`. | Obsługa zdarzeń zgłosiła błąd; zdarzenie jest zbyt duże; agregat ma zbyt wiele zdarzeń; baza danych uległa awarii lub nie miała wolnego połączenia. |
| `CommandRejected(aggregateId, reason)` | Nie uruchamia się wcale; ponowne wysłanie może zadziałać. | W trybie szeregowym kolejka agregatu jest pełna; podczas aktualizacji stopniowej jest to klasa zdarzeń, której nie każdy węzeł może jeszcze odczytać. |

W załączniku C znajduje się sekcja poświęcona każdej odpowiedzi na błąd: co go powoduje i co należy zrobić.

---

## Szyna zdarzeń (`EventsBusActor`)

Po jednym na węzeł. Wiadomości od `io.reactivecqrs.core.eventbus.EventsBusActor._`.

| Wysyłasz | Dostajesz z powrotem | Warto wiedzieć |
|---|---|---|
| `RegisterSubscribers(subscribers)` | Nic | Wyślij raz na początku, z każdą projekcją, aktorem słuchaczem i nadzorcą zarządzanej projekcji. Szyna pyta każdego o subskrypcje i nic nie publikuje, dopóki wszyscy nie odpowiedzą; w międzyczasie loguje co 5 sekund, na kogo czeka. `RegisterSubscribers(Nil)` uruchamia szynę bez subskrybentów. |
| `GetDeliveryStatus` | `DeliveryStatus(oczekiwanie, niedoręczenie, zatrzymanie, wstrzymanie, opóźnienie)` | Odebrano, nawet gdy autobus czekał na abonentów. Zobacz poniżej. |

Pola „DeliveryStatus”:

- `waiting: Int`: przesyłki w trakcie realizacji (wysłane lub wstrzymane, jeszcze nieodebrane);
- `undelivered: Seq[Undelivered]`: ostatnie dostawy, na które nikt nie odpowiedział w ciągu 10 minut,
  każdy `Niedostarczony(aggregateType, aggregateId, wersja, subskrybent, rodzaj, w)`;
- `stopped: Seq[String]`: subskrybenci, którzy zatrzymali subskrypcję; ich zdarzenia pozostają w skrzynce nadawczej;
- `held: Int`: dostawy wstrzymane, ponieważ okno abonenta było pełne;
- `behind: Map[String, Int]`: dla każdego subskrybenta znajdującego się za, ile agregatów jest dla niego przechowywanych.

### Jeśli piszesz subskrybenta ręcznie

`ProjectionActor` robi to wszystko za Ciebie, w tym sprawdzanie wersji. Jeśli kiedykolwiek napiszesz
abonent od podstaw, oto rozmowa, jaką musi odbyć z autobusem:

| Autobus cię wysyła | Odpowiadasz |
|---|---|
| `GetSubscriptionRequests(registered)` | `SubscriptionRequests(requests, registered)`, z jednym `SubscribeForEvents(aggregateType, self)`, `SubscribeForAggregates(aggregateType, self)` lub `SubscribeForAggregatesWithEvents(aggregateType, self)` na subskrypcję |
| `IdentifiableEvents(aggregateType, aggregateId, events, startsAtSquash)` | `MessageAck(self, aggregateId, versions, EventsDelivery)` po zastosowaniu (lub zastosowaniu wcześniej); `MessageFailed(self, aggregateId, versions, error, EventsDelivery)` po rezygnacji |
| `AggregateWithType(aggregateType, id, version, events, aggregateRoot, startsAtSquash)` | To samo z `AggregatesDelivery` |
| `AggregateWithTypeAndEvents(aggregateType, id, aggregateRoot, events, startsAtSquash)` | To samo z `AggregatesWithEventsDelivery` |
| (nic: zauważyłeś brakujące wersje) | `RequestRepublish(self, aggregateType, aggregateId, from)` do magistrali, która dostarczyła |

Odpowiedz aktorowi, który wysłał Ci przesyłkę.

---

## Odpytywanie skrzynki nadawczej („OutboxPollerActor”)

Jeden na system. Wiadomości od `io.reactivecqrs.core.delivery.OutboxPollerActor._`.

| Wysyłasz | Dostajesz z powrotem | Warto wiedzieć |
|---|---|---|
| `GetOutboxStatus` | `OutboxStatus(waiting, gaveUp)` | `waiting`: ile agregatów ze zdarzeniami w skrzynce nadawczej śledzi moduł sondujący od czasu ostatniego sondowania (maksymalnie 100 000). `gaveUp`: zgłoszone jako niedostarczone, każde `WaitingInOutbox(aggregateType, aggregateId, lowestVersion, since)`. |
| `Ankieta` | Nic | Teraz odczytuje skrzynkę nadawczą. Własny timer wysyła ją co 30 sekund, pierwszy raz 10 sekund po uruchomieniu. |

---

## Projekcje (`ProjectionActor`)

Wiadomości z `io.reactivecqrs.core.projection._`. Wyślij je do projekcji lub do jej
przełożonego, jeżeli jest zarządzane: przełożony przekazuje je dalej.

| Wysyłasz | Dostajesz z powrotem | Warto wiedzieć |
|---|---|---|
| `GetProjectionStatus` | `ProjectionStatus(projectionName, błędy, ponawianie, waitingForGap)` | `błędy`: każde `SubscriptionFailure(subscriptionType, aggregateType, aggregateId, wersja, błąd, od)`; `ponowne wypróbowanie` i `waitingForGap`: zbiorcze identyfikatory. |
| `RetryFailedSubscription(aggregateId)` | `FailedSubscriptionsHandled(aggregateId, count)` | Ponownie pyta właściciela o nieudane wersje; każda nieudana subskrypcja jest czyszczona po zastosowaniu jej wersji. `count`: nieudane subskrypcje danego agregatu (jedna na każdy rodzaj nasłuchiwacza). |
| `RetryAllFailedSubscriptions` | `FailedSubscriptionsRetried(count)` | Ponawianie prób po każdym błędzie, 20 na sekundę. |
| `SkipFailedVersion(aggregateId)` | `FailedSubscriptionsHandled(aggregateId, count)` | Oznacza każdą nieudaną wersję jako zastosowaną bez jej stosowania, a następnie kontynuuje od następnej wersji. |
| Twoje własne zapytanie, np. `GetAllCartsNames()` | Cokolwiek wyśle ​​Twoje `receiveQuery` | Na zapytanie, które rzuca zapytanie, odpowiada `Status.Failure(e)`, a projekcja jest kontynuowana. |

### Opóźnione zapytania

W `receiveQuery` możesz odpowiedzieć zapytaniem, które czeka na swoje dane. Są to metody, które
wywołanie z `sender()` jako `respondTo`:

| Ty dzwonisz | Pytający otrzymuje |
|---|---|
| `delayIfNotAvailable(respondTo, search, forMaximumMillis)`, z `search: () => Option[T]` | `Some(result)`, gdy `search` go znajdzie (uruchamia się ponownie po każdej zastosowanej aktualizacji); `None`, gdy `forMaximumMillis` zakończono; `Status.Failure(e)`, jeśli `search` rzuci |
| `delayIfNotAvailableAsync(respondTo, search, forMaximumMillis)`, z `search: () => Future[Option[T]]` | To samo |
| `delayIfNotAvailableAsyncCustom(respondTo, search, found, forMaximumMillis)`, z `search: () => Future[T]` i `found: T => Boolean` | Sam wynik, `T` (niezawinięty), gdy `found(result)` jest prawdą; `None` w momencie ostatecznym; `Status.Failure(e)` w przypadku niepowodzenia |

```scala
nadpisz chronione def receiveQuery: Receive = {
  case GetCartName(cartId) => // własna wiadomość zapytania
    delayIfNotAvailable(sender(), () => documentStore.getDocument(cartId.asLong).map(_.document), 2000)
}
```

Wycofana kompilacja zarządzanej projekcji odpowiada na oczekujące zapytania wartością `None`.

### Projekcje subskrybowalne (`SubscribableProjectionActor`)

Wiadomości od `io.reactivecqrs.core.projection.SubscribableProjectionActor._`.

| Wysyłasz (lub dzwonisz) | Otrzymujesz z powrotem |
|---|---|
| Twoja własna wiadomość subskrypcji, którą projekcja obsługuje w `receiveSubscriptionRequest` poprzez wywołanie `handleSubscribe(subscriptionId, listener, filter)` | Słuchacz otrzymuje `SubscribedForProjectionUpdates(subscriptionId)`, a następnie `SubscriptionUpdated(subscriptionId, data)` dla każdej aktualizacji (`sendUpdate`), którą filtr zamienia na `Some(data)` |
| `RenewSubscription(subscriptionId)` | Nic, dopóki projekcja zna subskrypcję; `ProjectionSubscriptionsCancelled(List(subscriptionId))`, gdy jej nie zna (ponowna subskrypcja) |
| `Anuluj subskrypcje projekcji (subskrypcje)` | `Anulowano subskrypcje projekcji (subskrypcje)` |

Subskrypcja push, która nie zostanie odnowiona w ciągu 10 minut, wygasa.

---

## Zarządzani nadzorcy projekcji (`ProjectionSupervisor`)

`ActorRef` zwracany przez `Projections.start`. Wiadomości z
`io.reactivecqrs.core.rebuild.ProjectionSupervisor._`; `RebuildTarget` pochodzi z
`io.reactivecqrs.core.projection`.

| Wysyłasz | Dostajesz z powrotem | Warto wiedzieć |
|---|---|---|
| `RebuildProjection` | `RebuildStarted(build)` lub `RebuildRefused(reason)` | Rozpoczyna nową kompilację bieżącej wersji projekcji; zastępuje aktywną kompilację po jej dogonieniu. Odrzucona podczas uruchamiania innej kompilacji lub docelowej kompilacji. |
| `RebuildAggregates(target)`, z `RebuildTarget.OneAggregate(id)`, `RebuildTarget.OneType(aggregateType)` lub `RebuildTarget.OneSpace(spaceId)` | `AggregatesRebuilt(target, aggregates)` po zakończeniu lub `RebuildRefused(reason)` | Przebudowuje w miejscu, w aktywnej kompilacji. Odpowiedź pojawia się dopiero po zakończeniu przebudowy, więc daj pytanie o długi czas oczekiwania. Odrzucane, gdy projekcja nie implementuje `clearForRebuild`, podczas kompilacji w toku oraz dla organizacji, gdy niektóre z jej agregatów nie mają jeszcze wolnej przestrzeni. |
| `GetBuilds` | `Builds(name, active, building, caughtUp)` | `active` i `building` to `Option[BuildRecord]`, każdy `BuildRecord(name, number, version, suffix, subscriber, contexts, position)`. `caughtUp`: trwająca kompilacja zawiera wszystkie zdarzenia. |
| `ToBuildInProgress(message)` | Niezależnie od odpowiedzi na trwającą kompilację lub `RebuildRefused("brak uruchomionej kompilacji")` | Na przykład `ToBuildInProgress(RetryAllFailedSubscriptions)`: kompilacja z nieudanymi subskrypcjami nie zastępuje aktywnej. |
| Coś jeszcze (Twoje zapytania, `GetProjectionStatus`, komunikaty ponownych prób) | Odpowiedź instancji, która obsługuje | Nadzorca przekazuje ją dalej, zachowując Cię jako nadawcę. |

Wiadomość, której nie udało się rozwiązać samemu nadzorcy, ma odpowiedź `Status.Failure(e)`.

---

## Harmonogram squasha (`SquashScheduler`)

Po jednym na system. Wiadomości z `io.reactivecqrs.core.aggregate.SquashScheduler._`.

| Wysyłasz | Dostajesz z powrotem | Warto wiedzieć |
|---|---|---|
| `GetSquashStatus` | `SquashStatus(squashed, denied, waiting)` | Zlicza od momentu uruchomienia harmonogramu; `waiting` to czas, który bieżące uruchomienie jeszcze się nie zakończył. |
| `Uruchom` | Nic | Rozpoczyna teraz bieg, chyba że ktoś jeszcze nie wykonuje biegu. Jego timer wysyła go co godzinę, pierwszy raz minutę po rozpoczęciu. |

---

## Biegacze Sagi (`SagaActor`)

Wiadomości od `io.reactivecqrs.core.saga._`.

| Wysyłasz | Dostajesz z powrotem | Warto wiedzieć |
|---|---|---|
| Twój `SagaOrder`, na przykład `CreateMultipleCarts(userId, "Groceries", 3)` | `SagaResponse`, którą Twój krok zwraca w `SagaSucceded(response)` lub `SagaFailed(response)` | `SagaFailureResponse(exceptions)`, gdy krok kończy się niepowodzeniem z powodu wyjątku, gdy nie można uzyskać identyfikatora sagi lub gdy nie można jej zapisać. W klastrze odpowiedź może pojawić się dwukrotnie: potraktuj drugą odpowiedź dla tej samej sagi jako powtórzenie. |

---

## Operacje hosta (metody, które wywołujesz)

Nie wiadomości, lecz połączenia wykonywane przez Twoją aplikację, głównie na początku lub jako jednorazowe zadania.

| Ty dzwonisz | Ono wraca | Kiedy |
|---|---|---|
| `new PostgresTypesNamesState().initSchema()` | `PostgresTypesNamesState` | Przy każdym starcie, przed innymi. |
| `new PostgresEventStoreState(mpjsons, typesNames).initSchema()` | `PostgresEventStoreState` | Przy każdym uruchomieniu. |
| `new PostgresCommandResponseState(mpjsons, typesNames).initSchema()` | `PostgresCommandResponseState` | Przy każdym uruchomieniu. Bez niego polecenia idempotentne nie mogą przechowywać swoich odpowiedzi i kończą się niepowodzeniem. |
| `new PostgresSubscriptionsState(typesNames).initSchema()` | `PostgresSubscriptionsState` | Przy każdym uruchomieniu, gdy masz projekcje. |
| `new PostgresSagaState(mpjsons, typesNames).initSchema()` | `Unit` | Przy każdym starcie, gdy masz sagi. |
| `new PostgresProjectionRegistry().initSchema()` | `PostgresProjectionRegistry` | Przy każdym uruchomieniu, gdy zarządzasz projekcjami. |
| `eventStore.buildIndexesConcurrently()` | `Unit` | Raz po uaktualnieniu, w tle: buduje nowsze indeksy bez blokowania zapisu i nie robi nic, gdy istnieją. |
| `eventStore.backfillEventKeys()` | `EventKeysBackfill(written, missing)` | Raz, w tle, po uruchomieniu przez każdy węzeł wersji z kluczami zdarzeń. `missing` zlicza zdarzenia, których wiersz agregatu brakuje. |
| `eventStore.backfillSpaces(spaceOf)` | `SpaceBackfill(filled, left)` | Kiedyś dla agregatów przechowywanych zanim wymagane były spacje, przed odbudowaniem organizacji. `left` wyświetla te, których nie udało się umieścić. |
| `StoredDataCheck.run(mpjsons, typesNames, projections)` | `StoredDataCheck.Report` | Przed uaktualnieniem, na kopii produkcyjnej. `report.text` to czytelna tabela; `report.unreadable` zlicza wiersze, których nie da się odczytać. |
| `StoredDataCheck.usersOf(className)` | `StoredDataCheck.ClassUsers(className, events, archived, aggregates)` | Przed usunięciem klasy zdarzeń z kodu: gdy `events` i `archived` mają wartość 0, nic z niej nie korzysta. |
| `projections.retire(name)` | `Jednostka` | Na początku, po usunięciu każdej zarządzanej projekcji z kodu, usuwa jej tabele, wersje i awarie. |
| `subscriptionsState.clearSubscriptionsInfo(subscriberName)` | `Jednostka` | W przypadku niezarządzanej projekcji usuniętej z kodu: zapomina wersje i błędy przechowywane pod nazwą jej klasy. |
| `ReactiveMetrics(system).use(metrics)` | `Jednostka` | W dowolnym momencie; do tego czasu nic nie jest mierzone. |
| `UidGeneratorActor.postgres(system)` | `Props` | Na początku, aby utworzyć generator uid. |
| `Singletons.start(system, name, props)` | `ActorRef` serwera proxy | W klastrze, dla programu sondującego skrzynkę nadawczą, harmonogramu squasha i każdego programu uruchamiającego sagę. |
| `Readiness(system).ready`, `Readiness(system).waitingFor` | `Boolean`, `Set[String]` | Podczas kontroli stanu: czy węzeł jest gotowy i na co jeszcze czeka. |

`StoredDataCheck` znajduje się w `io.reactivecqrs.core.tools`; `Singletons` i `Readiness` w
`io.reactivecqrs.core.cluster`; `ReactiveMetrics` w `io.reactivecqrs.core.metrics`. Identyfikator użytkownika
Generator i magazyny dokumentów tworzą własne tabele i sekwencje.

---

## Domyślne ustawienia, które warto znać

Są to wartości w kodzie; ostatnia kolumna wskazuje, gdzie je zmienić.

| Co | Domyślne | Zmieniono za pomocą |
|---|---|---|
| Limit czasu polecenia | 60 s | `AggregateContext.commandTimeout` |
| Próby wykonania polecenia współbieżnego | 10, oczekiwanie 10 ms, podwojenie do 500 ms | (naprawiono) |
| Kolejka w trybie szeregowym | 1 000 oczekujących poleceń na agregację | `CommandProcessing.Serial(maxQueuePerAggregate)` |
| Rozmiar zdarzenia | 100 000 znaków JSON | `PostgresEventStoreState(eventSizeLimit, eventSizeLimitPerClassName)` |
| Liczba zdarzeń w agregacie | 10 000 | `PostgresEventStoreState(aggregateVersionLimit)` |
| Migawki, raz włączone | co 200 zdarzeń i gdy właściciel zatrzyma | `SnapshotPolicy(every, onPassivation)` |
| Bezczynni właściciele (typy `Lokalne`) | czyszczenie, maksymalnie co 5 minut, zatrzymuje te bezczynne 2 minuty po 200 ostatnio używanych i wszystkie bezczynne 12 godzin | `AggregateCommandBusActor(..., maxInactivityMillis, keepAliveLimit)` |
| Bezczynni właściciele (typy `Sharded`) | pasywowani przez Pekko po 120 s bezczynności | `pekko.cluster.sharding.passivation` |
| Właściciel, który nie mógł przywrócić | odpowiada błędem, a następnie zostaje zastąpiony po 5 s | `AggregateCommandBusActor(..., failedRestoreRetryAfter)` |
| Przechowywanie idempotentnych odpowiedzi | 30 dni | `PostgresCommandResponseState(mpjsons, typesNames, retention)` |
| Termin dostawy | 10 minut | `Opcje dostawy (termin)` |
| Okno na abonenta | 10 000 zdarzeń i 64 MB | `DeliveryOptions(window, windowBytes)` |
| Ankieta Outbox | ankieta co 30 sekund; publikuje ponownie po 3 i 6 minutach; „niedostarczona” po 10 minutach; następnie co 30 minut; maksymalnie 1000 agregatów na ankietę | `OutboxPollerOptions`, `RedeliveryPolicy.Options` |
| Ponowne próby projekcji nieudanego odbiornika | około 2 minuty (1 sek. podwaja się do 30 sek., 9 prób) | `ProjectionActorOptions(retries)` |
| Po 10 minutach następuje przerwa | `ProjectionActorOptions(gapGiveUpAfter)` |
| Agregaty na transakcję projekcji | do 100 | `ProjectionActorOptions(maxBatchesPerTransaction)` |
| Kompilacje w tym samym czasie | 2 | `ProjectionsSettings(buildsAtOnce)` |
| Harmonogram Squash | pierwsze uruchomienie po 1 minucie, następnie co godzinę, do 100 agregatów na typ na uruchomienie | `SquashSchedulerOptions` |
| Subskrypcje push | wygasają po 10 minutach bez odnowienia | `SubscribableProjectionActor(subscriptionTTL)` |
| Wątki dotyczące właścicieli i pracy z bazą danych | 32 | `reactivecqrs.blocking-dispatcher` |
| Wątki dla procedur obsługi poleceń | 32 | `reactivecqrs.handler-dispatcher` |
| Przekazanie właścicieli shardingu | 90 s | `pekko.cluster.sharding.handoff-timeout` |
| Wiadomość między węzłami | 248 KiB (ramka Pekko o rozmiarze 256 KiB minus 8 KiB) | `pekko.remote.artery.advanced.maximum-frame-size` |

---

Dalej: [Dodatek C](appendix-c-troubleshooting.md) na wypadek, gdyby odpowiedź nie była taka, na jaką liczyłeś.
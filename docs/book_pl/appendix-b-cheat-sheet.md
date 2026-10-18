# Dodatek B. Ściągawka: komunikaty i odpowiedzi

Ten dodatek to strona, którą trzymasz otwartą obok edytora. Zebrane są tu komunikaty, które wysyłasz
do aktorów biblioteki, pogrupowane według odbiorcy, i dokładnie to, co wraca: klasa oraz to, czy
przychodzi opakowana w `Try`. Na końcu znajdziesz metody, które twoja aplikacja wywołuje sama
(przygotowanie schematu, uzupełnienia wsteczne, kontrole), i wartości domyślne, które warto znać na
pamięć.

Każdy wiersz sprawdzono z kodem ReactiveCQRS 1.0 (`1.0.0`). Komunikatów, które biblioteka
wysyła tylko sama do siebie, tu nie ma.

---

## Jak pytać i dwa rodzaje „nie”

Wszystkie przykłady używają `ask` z Pekko (z limitem czasu) i `mapTo`, które podaje typ odpowiedzi:

<!-- snippet: cart/CommandKindsSpec.scala#basics -->
```scala
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import io.reactivecqrs.api._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

implicit val timeout: Timeout = Timeout(5.seconds)

// A command: the answer is a command response.
val added: Future[CustomCommandResponse[_]] =
  (commandBus ? AddItem(userId, cartId, AggregateVersion(1), "milk")).mapTo[CustomCommandResponse[_]]

// A query: the answer is a Try.
val cart: Future[Try[Aggregate[ShoppingCart]]] =
  (commandBus ? GetAggregate(cartId)).mapTo[Try[Aggregate[ShoppingCart]]]
```

> **Pamiętaj:** odpowiedź może powiedzieć „nie” na dwa sposoby. `Failure(e)` wewnątrz `Try` to zwykła
> odpowiedź: twój `Future` kończy się sukcesem i zawiera nieudany `Try`. `Status.Failure(e)` sprawia,
> że sam `Future` kończy się błędem `e`. Tabele poniżej mówią, którego sposobu używa każdy komunikat.

---

## Szyna poleceń (`AggregateCommandBusActor`)

Jedna na typ agregatu (i na węzeł). Komunikaty z `io.reactivecqrs.api._`, poza `PassivateAggregate`,
który mieszka w `io.reactivecqrs.core.commandhandler.AggregateCommandBusActor`.

| Wysyłasz | Dostajesz | Warto wiedzieć |
|---|---|---|
| `FirstCommand`, na przykład `CreateShoppingCart(None, userId, "Groceries")` | Odpowiedź na polecenie (następna sekcja), na przykład `SuccessResponse(newId, AggregateVersion(1))` | Szyna nadaje nowemu agregatowi identyfikator, chyba że wskazuje go `chosenAggregateId` polecenia. Na wybrany identyfikator, który nie jest dodatni, nie jest mniejszy niż 2^53 albo należy do zakresu innej instalacji, szyna odpowiada `FailureResponse`, zanim cokolwiek się wykona. |
| `Command`, na przykład `AddItem(userId, cartId, AggregateVersion(1), "milk")` | Odpowiedź na polecenie | Oczekiwana wersja musi być bieżącą wersją agregatu, inaczej odpowiedzią jest `AggregateConcurrentModificationError`. |
| `ConcurrentCommand` | Odpowiedź na polecenie | Wykonuje się na bieżącej wersji; po kolizji wykonuje się ponownie, łącznie najwyżej 10 prób. |
| `RewriteHistoryCommand` albo `RewriteHistoryConcurrentCommand` | Odpowiedź na polecenie | Zob. [rozdział 15](15-rewriting-history.md). |
| `ReserveAggregateId` | `AggregateId` (bez opakowania) | Do `chosenAggregateId`. Jeśli nie da się zdobyć puli identyfikatorów, szyna odpowiada `Status.Failure`, więc ask kończy się błędem. |
| `GetAggregate(id)` | `Try[Aggregate[R]]`: `Success(Aggregate(id, version, Some(root)))` | `aggregateRoot` to `None` w wersji 0 (agregat nigdy nie istniał) oraz wtedy, gdy handler zdarzenia zwrócił `null` (agregat usunięty). `Failure(AggregateRestoreFailedException)`, gdy agregatu nie da się odczytać z magazynu. |
| `GetAggregateMinVersion(id, version, maxMillis)` | `Try[Aggregate[R]]`, gdy tylko agregat osiągnie `version` | `Failure(AggregateInIncorrectVersionException)`, gdy nie dojdzie do niej w ciągu `maxMillis` albo gdy jego właściciel wcześniej się zatrzyma; `Failure(AggregateRestoreFailedException)`, gdy nie da się go odczytać z magazynu. |
| `GetAggregateForVersion(id, version)` | `Try[Aggregate[R]]`: stan w tej wersji | Czytany z magazynu, nie od właściciela. `Failure(AggregateSquashedException)` poniżej punktu squasha. |
| `GetAggregateAtInstant(id, instant)` | `Try[Aggregate[R]]`: stan w tej chwili | `Failure(AggregateSquashedException)` dla chwili sprzed czasu zdarzenia squasha. |
| `GetEventsForAggregate(id)` | `Try[List[EventInfo[R]]]`: wszystkie zdarzenia, od najstarszego | Razem z cofniętymi; lista duplikatu zaczyna się od zdarzeń jego bazy. Cała historia przychodzi w jednym komunikacie. |
| `GetEventsForAggregateForVersion(id, version)` | `Try[List[EventInfo[R]]]`: zdarzenia do `version` | `Failure(AggregateSquashedException)` poniżej punktu squasha. |
| `GetArchivedEvents(id)` | `Try[ArchivedEvents[R]]`: `ArchivedEvents(id, events)` | Każde zdarzenie to `ArchivedEvent(info, undoneFrom, archived)`: zdarzenie w postaci, w jakiej je zapisano, wersja cofnięcia, które je anulowało (jeśli było), i chwila, w której squash je zarchiwizował. Od najstarszego. |
| `SquashAggregate(id, upTo)`, opcjonalnie z `userId` i `metadata` | `AggregateSquashed(id, upTo)` albo `SquashRefused(id, reason)`, bez opakowania | Oba rozszerzają `SquashResponse`. Każdą odmowę wyjaśnia dodatek C. |
| `Materialize(id, userId)`, do szyny efemerycznej | `Materialized(id, persistentId)` albo `MaterializeRefused(id, reason)`, bez opakowania | Oba rozszerzają `MaterializeResponse`; po materializacji szkic znika ([rozdział 20](20-ephemeral-aggregates.md)). Polecenia dla identyfikatora, którego szyna nie trzyma, dostają `EphemeralAggregateLost` albo `EphemeralAggregateUnknown`. |
| `RewriteEverywhere(id, change, userId)` | `HistoryRewritten(id, events, archived, fields)` albo `RewriteRefused(id, reason)`, bez opakowania | Pełne przepisanie historii jednego agregatu, wykonywane w jego właścicielu; oba rozszerzają `RewriteResponse`. `HistoryRewriter` wysyła ten komunikat także do każdej kopii ([rozdział 15](15-rewriting-history.md)). |
| `AggregateCommandBusActor.PassivateAggregate(id)` | Nic | Zatrzymuje właściciela agregatu, gdy ten odpowie na rozpoczęte polecenia; następny komunikat uruchamia nowego właściciela. |
| Cokolwiek innego | `Status.Failure(IllegalArgumentException)` | Ask kończy się błędem „Cannot handle this kind of message”. |

Odczyty historyczne (`GetAggregateForVersion`, `GetAggregateAtInstant` i listy zdarzeń) każdy inny
kłopot z magazynem opakowują w `Failure(AggregateRestoreFailedException)`.

> **Ostrzeżenie:** w klastrze odpowiedź od właściciela z innego węzła musi zmieścić się w jednym
> komunikacie między węzłami (około 248 KiB przy domyślnym rozmiarze ramki Pekko). Przy większej twój
> ask kończy się błędem `MessageTooLargeException`. Na `GetEventsForAggregate` i pozostałe odczyty
> historyczne odpowiedź powstaje zawsze na twoim własnym węźle, z magazynu. Więcej
> w [rozdziale 17](17-cluster.md).

---

## Odpowiedzi na polecenia (`CustomCommandResponse.scala`)

Każde polecenie dostaje dokładnie jedną z nich. Kopia polecenia idempotentnego, które zostało już
zastosowane, dostaje tę samą odpowiedź co pierwsza kopia.

| Odpowiedź | Co oznacza | Typowe przyczyny |
|---|---|---|
| `SuccessResponse(aggregateId, aggregateVersion)` | Zdarzenia są zapisane; `aggregateVersion` to nowa wersja. | Twój handler zwrócił `CommandSuccess(...)`. Bez zdarzeń nic nie jest zapisywane, a wersja się nie zmienia. |
| `CustomSuccessResponse(aggregateId, aggregateVersion, info)` | To samo, z twoim `info`. | Twój handler zwrócił `CommandSuccess(event, info)`, tak jak `AddItemWithReceipt` z `ItemReceipt`. |
| `FailureResponse(exceptions)` | Odmowa; nic nie zapisano. `exceptions` to `List[String]` z komunikatami. | Twój handler zwrócił `CommandFailure("...")`. Odmawia też biblioteka: wybrany identyfikator, na który nie pozwala, nowy agregat, którego pierwsze zdarzenie nie ma przestrzeni, polecenie typu shardowanego zbyt duże, by przesłać je między węzłami. |
| `AggregateConcurrentModificationError(aggregateId, aggregateType, expected, was)` | Kolizja wersji; nic nie zapisano. `expected` to wersja podana przez ciebie, `was` to wersja agregatu. | `Command` ze starą oczekiwaną wersją; agregat, który nie istnieje (`was` to wersja 0); `ConcurrentCommand`, który zderzył się 10 razy; `FirstCommand`, którego wybrany identyfikator jest już zajęty. |
| `CommandHandlingError(commandName, errorId, commandId)` | Polecenia nie udało się obsłużyć; nic nie zapisano. Przyczyna jest w logu pod `errorId`. | Polecenie przekroczyło limit czasu; twój handler rzucił wyjątek albo nie obsłużył polecenia; agregatu nie da się odtworzyć; agregat nie istnieje albo jest usunięty; nie udało się zdobyć identyfikatorów (wtedy `commandId` to `CommandId(0)`). |
| `EventHandlingError(eventName, errorId, commandId)` | Zdarzeń nie udało się zastosować ani zapisać; nic nie zapisano. Przyczyna jest w logu pod `errorId`. | Twój handler zdarzenia rzucił wyjątek; zdarzenie jest za duże; agregat ma za dużo zdarzeń; baza danych zawiodła albo nie miała wolnego połączenia. |
| `CommandRejected(aggregateId, reason)` | Polecenie w ogóle się nie wykonało; ponowne wysłanie później może się udać. | W trybie szeregowym kolejka agregatu jest pełna; podczas aktualizacji kroczącej klasa zdarzenia, której jeszcze nie każdy węzeł potrafi odczytać. |

Dodatek C ma osobną sekcję dla każdej odpowiedzi z błędem: co ją powoduje i co z tym zrobić.

---

## Szyna zdarzeń (`EventsBusActor`)

Jedna na węzeł. Komunikaty z `io.reactivecqrs.core.eventbus.EventsBusActor._`.

| Wysyłasz | Dostajesz | Warto wiedzieć |
|---|---|---|
| `RegisterSubscribers(subscribers)` | Nic | Wyślij raz na starcie, ze wszystkimi projekcjami, aktorami-listenerami i nadzorcami projekcji zarządzanych. Szyna pyta każdego o subskrypcje i niczego nie publikuje, dopóki wszyscy nie odpowiedzą; w tym czasie co 5 sekund zapisuje w logu, na kogo czeka. `RegisterSubscribers(Nil)` uruchamia szynę bez subskrybentów. |
| `GetDeliveryStatus` | `DeliveryStatus(waiting, undelivered, stopped, held, behind)` | Szyna odpowiada nawet wtedy, gdy jeszcze czeka na subskrybentów. Szczegóły niżej. |

Pola `DeliveryStatus`:

- `waiting: Int`: dostarczenia w drodze (wysłane albo wstrzymane, jeszcze bez odpowiedzi);
- `undelivered: Seq[Undelivered]`: 100 ostatnich dostarczeń, na które nikt nie odpowiedział w ciągu
  10 minut, każde jako `Undelivered(aggregateType, aggregateId, version, subscriber, kind, at)`;
- `stopped: Seq[String]`: subskrybenci, którzy się zatrzymali; ich zdarzenia zostają w outboxie;
- `held: Int`: dostarczenia wstrzymane, bo okno subskrybenta było pełne;
- `behind: Map[String, Int]`: dla każdego subskrybenta, który zostaje w tyle, liczba agregatów
  wstrzymanych dla niego.

### Jeśli piszesz subskrybenta samodzielnie

`ProjectionActor` robi to wszystko za ciebie, łącznie ze sprawdzaniem wersji. Jeśli kiedyś napiszesz
subskrybenta od zera, oto rozmowa, którą musi on prowadzić z szyną:

| Szyna wysyła ci | Odpowiadasz |
|---|---|
| `GetSubscriptionRequests(registered)` | `SubscriptionRequests(requests, registered)`, z jednym `SubscribeForEvents(aggregateType, self)`, `SubscribeForAggregates(aggregateType, self)` albo `SubscribeForAggregatesWithEvents(aggregateType, self)` na każdą subskrypcję |
| `IdentifiableEvents(aggregateType, aggregateId, events, startsAtSquash)` | `MessageAck(self, aggregateId, versions, EventsDelivery)` po zastosowaniu (albo gdy zastosowano je już wcześniej); `MessageFailed(self, aggregateId, versions, error, EventsDelivery)`, gdy się poddajesz |
| `AggregateWithType(aggregateType, id, version, events, aggregateRoot, startsAtSquash)` | To samo, z `AggregatesDelivery` |
| `AggregateWithTypeAndEvents(aggregateType, id, aggregateRoot, events, startsAtSquash)` | To samo, z `AggregatesWithEventsDelivery` |
| (nic: sam zauważasz brakujące wersje) | `RequestRepublish(self, aggregateType, aggregateId, from)` do szyny, która dostarczyła zdarzenia |

Odpowiadaj temu aktorowi, który przysłał ci dostarczenie.

---

## Poller outboxu (`OutboxPollerActor`)

Jeden na system. Komunikaty z `io.reactivecqrs.core.delivery.OutboxPollerActor._`.

| Wysyłasz | Dostajesz | Warto wiedzieć |
|---|---|---|
| `GetOutboxStatus` | `OutboxStatus(waiting, gaveUp, gaveUpTotal)` | `waiting`: ile agregatów miało zdarzenia czekające w outboxie przy ostatnim przeglądzie pollera (czyta najwyżej 100 000, a ponad to 50 000 najnowszych). `gaveUp`: 100 zgłoszonych jako niedostarczone, które czekają najdłużej, każdy jako `WaitingInOutbox(aggregateType, aggregateId, oldestVersion, since)`; `gaveUpTotal`: wszystkie. |
| `Poll` | Nic | Czyta outbox od razu. Własny timer pollera wysyła ten komunikat co 30 sekund, pierwszy raz 10 sekund po starcie. |

---

## Projekcje (`ProjectionActor`)

Komunikaty z `io.reactivecqrs.core.projection._`. Wysyłaj je do projekcji albo, jeśli jest
zarządzana, do jej nadzorcy: nadzorca przekaże je dalej.

| Wysyłasz | Dostajesz | Warto wiedzieć |
|---|---|---|
| `GetProjectionStatus` | `ProjectionStatus(projectionName, failures, retrying, waitingForGap, failuresTotal)` | `failures`: 100 najstarszych awarii, każdą jako `SubscriptionFailure(subscriptionType, aggregateType, aggregateId, version, error, since)` z błędem przyciętym do 500 znaków; `failuresTotal`: wszystkie; `retrying` i `waitingForGap`: najwyżej po 1 000 identyfikatorów agregatów. |
| `RetryFailedSubscription(aggregateId)` | `FailedSubscriptionsHandled(aggregateId, count)` | Prosi właściciela ponownie o nieudane wersje; każda awaria znika, gdy jego wersja zostanie zastosowana. `count`: nieudane subskrypcje tego agregatu (po jednej na rodzaj listenera). |
| `RetryAllFailedSubscriptions` | `FailedSubscriptionsRetried(count)` | Ponawia wszystkie awarie, 20 na sekundę. |
| `SkipFailedVersion(aggregateId)` | `FailedSubscriptionsHandled(aggregateId, count)` | Oznacza każdą nieudaną wersję jako zastosowaną, nie stosując jej, i kontynuuje od następnej wersji; brakujące wersje tylko z `pastMissing = true`. |
| Twoje własne zapytanie, na przykład `GetAllCartsNames()` | To, co wyśle twoje `receiveQuery` | Na zapytanie, które rzuci wyjątek, odpowiedzią jest `Status.Failure(e)`, a projekcja działa dalej. |

### Opóźnione zapytania

W `receiveQuery` możesz odpowiedzieć zapytaniem, które czeka na swoje dane. To metody, które
wywołujesz, z `sender()` jako `respondTo`:

| Wywołujesz | Pytający dostaje |
|---|---|
| `delayIfNotAvailable(respondTo, search, forMaximumMillis)`, z `search: () => Option[T]` | `Some(result)`, gdy tylko `search` go znajdzie (uruchamia się ponownie po każdej zastosowanej aktualizacji); po upływie `forMaximumMillis` wynik jeszcze jednego, ostatniego wyszukiwania (`None`, jeśli nadal nic); `Status.Failure(e)`, jeśli `search` rzuci wyjątek |
| `delayIfNotAvailableAsync(respondTo, search, forMaximumMillis)`, z `search: () => Future[Option[T]]` | To samo |
| `delayIfNotAvailableAsyncCustom(respondTo, search, found, forMaximumMillis)`, z `search: () => Future[T]` i `found: T => Boolean` | Sam wynik, `T` (bez opakowania), gdy `found(result)` jest prawdą; po terminie `T` z jeszcze jednego, ostatniego wyszukiwania; `Status.Failure(e)` przy błędzie |

Tak na swoje zapytanie odpowiada projekcja nazw koszyków z przykładu E10:

<!-- snippet: e10/CartNames.scala#query -->
```scala
override protected def receiveQuery: Receive = {
  case GetCartName(id) => delayIfNotAvailable(sender(), () => store.getDocument(id.asLong).map(_.document), 2000)
}
```

Budowa projekcji zarządzanej, którą wycofano z użycia, odpowiada na czekające zapytania wartością
`None`.

### Projekcje subskrybowalne (`SubscribableProjectionActor`)

Komunikaty z `io.reactivecqrs.core.projection.SubscribableProjectionActor._`.

| Wysyłasz (albo wywołujesz) | Dostajesz |
|---|---|
| Twój własny komunikat subskrypcji, który projekcja obsługuje w `receiveSubscriptionRequest`, wywołując `handleSubscribe(subscriptionId, listener, filter)` | Listener dostaje `SubscribedForProjectionUpdates(subscriptionId)`, a potem `SubscriptionUpdated(subscriptionId, data)` przy każdej aktualizacji (`sendUpdate`), którą filtr zamieni w `Some(data)` |
| `RenewSubscription(subscriptionId)` | Nic, dopóki projekcja zna subskrypcję; `ProjectionSubscriptionsCancelled(List(subscriptionId))`, gdy jej nie zna (zasubskrybuj ponownie) |
| `CancelProjectionSubscriptions(subscriptions)` | `ProjectionSubscriptionsCancelled(subscriptions)`, bez subskrypcji innego subskrybenta, które zostają |

Subskrypcja push, której nikt nie odnowi przez 10 minut, wygasa.

---

## Nadzorcy projekcji zarządzanych (`ProjectionSupervisor`)

`ActorRef`, który zwraca `Projections.start`. Komunikaty
z `io.reactivecqrs.core.rebuild.ProjectionSupervisor._`; `RebuildTarget` pochodzi
z `io.reactivecqrs.core.projection`.

| Wysyłasz | Dostajesz | Warto wiedzieć |
|---|---|---|
| `RebuildProjection` | `RebuildStarted(build)` albo `RebuildRefused(reason)` | Rozpoczyna nową budowę bieżącej wersji projekcji; nowa budowa zastępuje aktywną, gdy nadrobi zaległości. Odmowa, gdy trwa inna budowa albo przebudowa celowana. |
| `RebuildAggregates(target)`, z `RebuildTarget.OneAggregate(id)`, `RebuildTarget.OneType(aggregateType)` albo `RebuildTarget.OneSpace(spaceId)` | `AggregatesRebuilt(target, aggregates)` po zakończeniu albo `RebuildRefused(reason)` | Przebudowuje w miejscu, na aktywnej budowie. Odpowiedź przychodzi dopiero po zakończeniu przebudowy, więc daj wywołaniu ask hojny limit czasu. Odmowa, gdy projekcja nie implementuje `clearForRebuild`, gdy trwa budowa, a dla organizacji także wtedy, gdy część jej agregatów nie ma jeszcze przestrzeni. |
| `GetBuilds` | `Builds(name, active, building, caughtUp)` | `active` i `building` to `Option[BuildRecord]`, każdy jako `BuildRecord(name, number, version, suffix, subscriber, contexts, position)`; `position` budowy w toku to pozycja, którą jej builder zapisał ostatnio. `caughtUp`: budowa w toku ma już wszystkie zdarzenia. |
| `GetBuildStatus` (z `ProjectionBuilder`) | `BuildStatus(phase, position, eventsSent, gapsFilled, handedOver, unreadable)` | Stan działającego buildera: `phase` to `Bulk`, `GoingLive`, `Reconciling` albo `CaughtUp`; gdy żaden builder nie pracuje, `Waiting` albo `NoBuild`. Działa też dla buildera przebudowy celowanej. |
| `ToBuildInProgress(message)` | To, co odpowie budowa w toku, albo `RebuildRefused("no build is running")` | Na przykład `ToBuildInProgress(RetryAllFailedSubscriptions)`: budowa z nieudanymi subskrypcjami nie zastąpi aktywnej. |
| Cokolwiek innego (twoje zapytania, `GetProjectionStatus`, komunikaty ponawiania) | Odpowiedź instancji, która obsługuje zapytania | Nadzorca przekazuje komunikat dalej, zostawiając ciebie jako nadawcę. |

Jeśli błąd wystąpi w samym nadzorcy, odpowiedzią jest `Status.Failure(e)`.

---

## Scheduler squasha (`SquashScheduler`)

Jeden na system. Komunikaty z `io.reactivecqrs.core.aggregate.SquashScheduler._`.

| Wysyłasz | Dostajesz | Warto wiedzieć |
|---|---|---|
| `GetSquashStatus` | `SquashStatus(squashed, refused, waiting)` | Liczniki od startu schedulera; `waiting` to to, czego bieżący przebieg jeszcze nie skończył. |
| `Run` | Nic | Uruchamia przebieg od razu, chyba że poprzedni jeszcze trwa. Timer schedulera wysyła ten komunikat co godzinę, pierwszy raz minutę po starcie. |

---

## Runnery sag (`SagaRunner`)

Komunikaty z `io.reactivecqrs.core.saga._`.

| Wysyłasz | Dostajesz | Warto wiedzieć |
|---|---|---|
| Twój `SagaOrder`, na przykład `CreateMultipleCarts(userId, "Groceries", 3)` | `SagaResponse`, które twój krok podaje w `SagaSucceded(response)` albo `SagaFailed(response)` | `SagaFailureResponse(exceptions)`, gdy krok kończy się wyjątkiem, gdy nie da się zdobyć identyfikatora sagi albo gdy sagi nie da się zapisać. W klastrze odpowiedź może przyjść dwa razy: drugą odpowiedź dla tej samej sagi traktuj jako powtórkę. |

---

## Operacje aplikacji (metody, które wywołujesz)

To nie komunikaty, tylko wywołania, które robi twoja aplikacja, zwykle na starcie albo jako
jednorazowe zadania.

| Wywołujesz | Zwraca | Kiedy |
|---|---|---|
| `new PostgresTypesNamesState().initSchema()` | `PostgresTypesNamesState` | Przy każdym starcie, przed pozostałymi. |
| `new PostgresEventStoreState(mpjsons, typesNames).initSchema()` | `PostgresEventStoreState` | Przy każdym starcie. |
| `new PostgresCommandResponseState(mpjsons, typesNames).initSchema()` | `PostgresCommandResponseState` | Przy każdym starcie. Gdy go zabraknie, polecenia idempotentne nie mogą zapisać swoich odpowiedzi i kończą się błędem. |
| `new PostgresSubscriptionsState(typesNames).initSchema()` | `PostgresSubscriptionsState` | Przy każdym starcie, jeśli masz projekcje. |
| `new PostgresSagaState(mpjsons, typesNames).initSchema()` | `Unit` | Przy każdym starcie, jeśli masz sagi. |
| `new PostgresProjectionRegistry().initSchema()` | `PostgresProjectionRegistry` | Przy każdym starcie, jeśli masz projekcje zarządzane. |
| `eventStore.buildIndexesConcurrently()` | `Unit` | Raz po aktualizacji, w tle: buduje nowsze indeksy bez blokowania zapisów, a gdy już istnieją, nic nie robi. |
| `eventStore.backfillEventKeys()` | `EventKeysBackfill(written, missing)` | Raz, w tle, gdy każdy węzeł działa już w wersji z kluczami zdarzeń. `missing` liczy zdarzenia, którym brakuje wiersza agregatu. |
| `eventStore.backfillSpaces(spaceOf)` | `SpaceBackfill(filled, left)` | Raz, dla agregatów zapisanych, zanim przestrzenie stały się obowiązkowe, przed przebudową organizacji. `left` wymienia te, którym nie udało się przypisać przestrzeni. |
| `StoredDataCheck.run(mpjsons, typesNames, projections)` | `StoredDataCheck.Report` | Przed aktualizacją, na kopii produkcji. `report.text` to czytelna tabela; `report.unreadable` liczy wiersze, których nie da się odczytać; `report.missingTables` wymienia tabele, których nie znaleziono. |
| `StoredDataCheck.usersOf(className)` | `StoredDataCheck.ClassUsers(className, events, archived, aggregates)` | Przed usunięciem klasy zdarzenia z kodu: gdy `events` i `archived` wynoszą 0, nic jej nie używa. |
| `projections.retire(name)` | `Unit` | Na starcie, dla każdej projekcji zarządzanej usuniętej z kodu; usuwa jej tabele, wersje i zapisane awarie. |
| `subscriptionsState.clearSubscriptionsInfo(subscriberName)` | `Unit` | Dla projekcji niezarządzanej usuniętej z kodu: zapomina wersje i awarie zapisane pod nazwą jej klasy. |
| `ReactiveMetrics(system).use(metrics)` | `Unit` | W dowolnej chwili; do tego czasu nic nie jest mierzone. |
| `UidGeneratorActor.postgres(system)` | `Props` | Na starcie, żeby utworzyć generator identyfikatorów. |
| `Singletons.start(system, name, props)` | `ActorRef` proxy | W klastrze: dla pollera outboxu, schedulera squasha i każdego runnera sagi. |
| `Readiness(system).ready`, `Readiness(system).waitingFor` | `Boolean`, `Set[String]` | W kontrolach stanu (health checks): czy węzeł jest gotowy i na co jeszcze czeka. |

`StoredDataCheck` mieszka w `io.reactivecqrs.core.tools`; `Singletons` i `Readiness`
w `io.reactivecqrs.core.cluster`; `ReactiveMetrics` w `io.reactivecqrs.core.metrics`. Generator
identyfikatorów i magazyny dokumentów same tworzą swoje tabele i sekwencje.

---

## Wartości domyślne, które warto znać

To wartości zapisane w kodzie; ostatnia kolumna mówi, gdzie je zmienić.

| Co | Domyślnie | Gdzie zmienić |
|---|---|---|
| Limit czasu polecenia | 60 s | `AggregateContext.commandTimeout` |
| Próby polecenia współbieżnego | 10, z przerwą 10 ms podwajaną do 500 ms | (na stałe) |
| Kolejka trybu szeregowego | 1 000 czekających poleceń na agregat | `CommandProcessing.Serial(maxQueuePerAggregate)` |
| Rozmiar zdarzenia | 100 000 znaków JSON-a | `PostgresEventStoreState(eventSizeLimit, eventSizeLimitPerClassName)` |
| Zdarzenia na agregat | 10 000 | `PostgresEventStoreState(aggregateVersionLimit)` |
| Migawki, gdy są włączone | co 200 zdarzeń i przy zatrzymaniu właściciela | `SnapshotPolicy(every, onPassivation)` |
| Bezczynni właściciele (typy `Local`) | sprzątanie, najwyżej co 5 minut, zatrzymuje bezczynnych od 2 minut spoza 200 ostatnio używanych oraz każdego bezczynnego od 12 godzin | `AggregateCommandBusActor(..., maxInactivityMillis, keepAliveLimit)` |
| Bezczynni właściciele (typy `Sharded`) | pasywowani przez Pekko po 120 s bezczynności | `pekko.cluster.sharding.passivation` |
| Właściciel, który nie zdołał odtworzyć stanu | odpowiada błędem, po 5 s zostaje zastąpiony | `AggregateCommandBusActor(..., failedRestoreRetryAfter)` |
| Przechowywanie odpowiedzi idempotentnych | 30 dni | `PostgresCommandResponseState(mpjsons, typesNames, retention)` |
| Termin dostarczenia | 10 minut | `DeliveryOptions(deadline)` |
| Okno na subskrybenta | 10 000 zdarzeń i 64 MB | `DeliveryOptions(window, windowBytes)` |
| Poller outboxu | przegląd co 30 s; ponowna publikacja po 3 i 6 minutach; „niedostarczone” po 10; potem co 30 minut; najwyżej 1 000 agregatów na przegląd | `OutboxPollerOptions`, `RedeliveryPolicy.Options` |
| Ponawianie nieudanego listenera w projekcji | około 2 minut (od 1 s, podwajane do 30 s, 9 prób) | `ProjectionActorOptions(retries)` |
| Rezygnacja z luki | po 10 minutach | `ProjectionActorOptions(gapGiveUpAfter)` |
| Agregaty na transakcję projekcji | do 100 | `ProjectionActorOptions(maxBatchesPerTransaction)` |
| Budowy jednocześnie | 2 | `ProjectionsSettings(buildsAtOnce)` |
| Scheduler squasha | pierwszy przebieg po 1 minucie, potem co godzinę, do 100 agregatów na typ w jednym przebiegu | `SquashSchedulerOptions` |
| Subskrypcje push | wygasają po 10 minutach bez odnowienia | `SubscribableProjectionActor(subscriptionTTL)` |
| Wątki dla właścicieli i pracy z bazą danych | 32 | `reactivecqrs.blocking-dispatcher` |
| Wątki dla handlerów poleceń | 32 | `reactivecqrs.handler-dispatcher` |
| Przekazanie właścicieli typów shardowanych | 90 s | `pekko.cluster.sharding.handoff-timeout` |
| Komunikat między węzłami | 248 KiB (ramka Pekko 256 KiB minus 8 KiB) | `pekko.remote.artery.advanced.maximum-frame-size` |

---

Dalej: [dodatek C](appendix-c-troubleshooting.md), na wypadek gdy odpowiedź nie jest tą, na którą
liczysz.

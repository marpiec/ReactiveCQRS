# Dodatek E. Konfiguracja

Wszystko, co możesz ustawić, w jednym miejscu. ReactiveCQRS ma dwa rodzaje ustawień:

- kilka kluczy w **`application.conf`**, pliku Typesafe Config, który Pekko czyta przy starcie;
- **parametry w kodzie**: parametry konstruktorów magazynów i aktorów, które tworzysz, klasy opcji,
  które im przekazujesz, i składowe twoich kontekstów agregatów.

Każda tabela podaje wartość domyślną, to, co robi ustawienie, i rozdział, który je objaśnia. Wartości
domyślne odczytano 18/10/2026 z kodu ReactiveCQRS 1.0 (`1.0.0`). Te, które najpewniej
zechcesz zmienić, są też w [dodatku B](appendix-b-cheat-sheet.md), w części „Wartości domyślne, które
warto znać”.

Rozmiary: 64 MB to 64 × 1 024 × 1 024 bajtów; czasy są zapisane jako `FiniteDuration` ze Scali.

---

## `application.conf`

| Klucz | Domyślnie | Co robi | Rozdział |
|---|---|---|---|
| `reactivecqrs.installation-number` | `0` | Zakres identyfikatorów agregatów danej instalacji: instalacja *n* tworzy je od *n* × 10^12 w górę. Każdy węzeł instalacji ustawia tę samą liczbę. | [5](05-command-flavors.md) |
| `reactivecqrs.blocking-dispatcher.thread-pool-executor.fixed-pool-size` | `32` | Wątki dla właścicieli agregatów i pracy biblioteki z bazą danych; tu działają też projekcje (`BlockingDispatcher.id`). Trzymaj tę liczbę blisko rozmiaru puli JDBC. | [2](02-setting-up.md), [8](08-projections.md) |
| `reactivecqrs.handler-dispatcher.thread-pool-executor.fixed-pool-size` | `32` | Wątki dla twoich handlerów poleceń. | [5](05-command-flavors.md) |
| `pekko.actor.serialization-bindings { "java.io.Serializable" = reactivecqrs }` | ustawiony w `reference.conf` biblioteki | Komunikaty między węzłami przechodzą przez serializator biblioteki. Ustaw to też we własnym `application.conf`: inaczej, zależnie od kolejności na classpath, może wygrać powiązanie Pekko, a węzeł pozostanie niegotowy (F178). | [17](17-cluster.md) |
| `pekko.cluster.app-version` | `0.0.0` (wartość Pekko) | Wydanie, na którym działa węzeł. Nadaj każdemu wydaniu własną wersję, na potrzeby aktualizacji kroczących i `writtenFrom`. | [17](17-cluster.md) |
| `pekko.cluster.sharding.handoff-timeout` | `90 s` (ustawia biblioteka) | Ile może trwać przekazanie właścicieli w shardach; dłużej niż domyślny limit czasu polecenia. | [17](17-cluster.md) |
| `pekko.coordinated-shutdown.phases.cluster-sharding-shutdown-region.timeout` | `90 s` (ustawia biblioteka) | Ile czasu ma odchodzący węzeł na przekazanie swoich shardów. | [17](17-cluster.md) |
| `pekko.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout` | `120 s` (wartość Pekko) | Po jakim czasie bezczynny właściciel w shardzie jest zatrzymywany. | [17](17-cluster.md) |
| `pekko.remote.artery.advanced.maximum-frame-size` | `256 KiB` (wartość Pekko) | Największy komunikat między węzłami; biblioteka odrzuca komunikat większy niż ta wartość pomniejszona o 8 KiB. | [17](17-cluster.md) |

Własne ustawienia klastra (provider, split brain resolver i jego lease, Cluster Bootstrap) należą do
Pekko, a [rozdział 17](17-cluster.md) pokazuje je na przykładzie E2.

## Magazyny

| Klasa i parametr | Domyślnie | Co robi | Rozdział |
|---|---|---|---|
| `PostgresEventStoreState(mpjsons, typesNames, ...)` `fetchSize` | `1000` | Ile wierszy zdarzeń czyta się naraz. Zmniejsz dla dużych zdarzeń. | [18](18-production.md) |
| `eventSizeLimit` | `100000` | Największe zdarzenie, liczone w znakach JSON; większe kończy swoje polecenie błędem `EventHandlingError`. | [6](06-events.md) |
| `eventSizeLimitPerClassName` | brak | Limit dla poszczególnych klas zdarzeń, zamiast `eventSizeLimit`. | [6](06-events.md) |
| `aggregateVersionLimit` | `10000` | Największa liczba zdarzeń w jednym agregacie; squash nie obniża wersji. | [6](06-events.md), [13](13-snapshots.md) |
| `outboxDeleteDelay` | `20 ms` | Najdłuższy czas, przez jaki dostarczone wiersze outboxu czekają, żeby zostać usunięte razem z innymi. | — |
| `PostgresCommandResponseState(mpjsons, typesNames, retention)` | `30 days` | Jak długo przechowuje się odpowiedzi poleceń idempotentnych. | [5](05-command-flavors.md) |
| `PostgresSubscriptionsState(typesNames, eventsLogger, maxCachedAggregates)` | bez loggera, `10000` | Notatnik projekcji: logger debugowania jego zapisów i to, dla ilu agregatów trzyma wersje w pamięci. | [8](08-projections.md) |
| `PostgresDocumentStore[T](name, mpjsons, cache, indexes)` | bez indeksów | Tabela modelu odczytu, `projection_<name>`, jej cache (`NoopDocumentStoreCache` niczego nie trzyma) i jej indeksy JSON. | [8](08-projections.md) |
| `PostgresSagaState(mpjsons, typesNames)` | — | Postęp sag. | [12](12-sagas.md) |
| `new PostgresProjectionRegistry()` | — | Budowy projekcji zarządzanych. | [10](10-rebuilding-projections.md) |

## Szyny poleceń

| Parametr | Domyślnie | Co robi | Rozdział |
|---|---|---|---|
| `AggregateCommandBusActor(context, uidGenerator, eventStore, responses, eventBus, ...)` `maxInactivityMillis` | `43200000` (12 godzin) | Właściciel bezczynny tak długo jest zatrzymywany. | [4](04-first-command.md) |
| `keepAliveLimit` | `200` | Powyżej tylu właścicieli w pamięci zatrzymywani są ci bezczynni od ponad 2 minut, zaczynając od najdawniej używanych. | [4](04-first-command.md) |
| `failedRestoreRetryAfter` | `5 s` | Jak długo agregat, którego nie udało się odczytać, odpowiada tym błędem, zanim kolejne żądanie spróbuje ponownie. | [4](04-first-command.md) |
| `AggregateCommandBusActor.ephemeral(context, persistentBus, uidGenerator, mpjsons, ...)` `maxAggregates` | `100000` | Największa liczba szkiców w pamięci; kolejny dostaje odpowiedź `CommandRejected`. | [20](20-ephemeral-aggregates.md) |
| `idleTime` | `12 hours` | Szkic nieużywany tak długo jest porzucany. | [20](20-ephemeral-aggregates.md) |
| `sweepEvery` | `5 minutes` | Jak często szuka się bezczynnych szkiców. | [20](20-ephemeral-aggregates.md) |
| `eventSizeLimit`, `eventSizeLimitPerClassName`, `aggregateVersionLimit` | jak w magazynie zdarzeń | Limity szkiców, takie same jak w magazynie (wyżej). | [20](20-ephemeral-aggregates.md) |

## Konteksty agregatów

Składowe twojego `AggregateContext`; nadpisz te, których potrzebujesz.

| Składowa | Domyślnie | Co robi | Rozdział |
|---|---|---|---|
| `version` | (podajesz ją ty) | Wersja zachowania kontekstu; podnieś ją, gdy handlery zdarzeń zmieniają to, co tworzą. | [3](03-first-aggregate.md) |
| `commandTimeout` | `60 s` | Ile może trwać polecenie, zanim wywołujący dostanie `CommandHandlingError`. | [5](05-command-flavors.md) |
| `commandProcessing` | `Concurrent` | `Serial(maxQueuePerAggregate = 1000, onOverflow = OverflowPolicy.Reject)` wykonuje w każdym agregacie jedno polecenie naraz. | [5](05-command-flavors.md) |
| `snapshots` | `None` | `SnapshotPolicy(every = 200, onPassivation = true, schemaVersion = 1)`. | [13](13-snapshots.md) |
| `squash` | `None` | `SquashPolicy(toEvent, automatic = None, keep = empty)`, z `AutomaticSquash(afterEvents, keepLast = 0, keepNewerThan = None)`. | [14](14-squash.md) |
| `placement` | `Placement.Local` | `Placement.Sharded(numberOfShards = 100)`: jeden właściciel na agregat w całym klastrze. | [17](17-cluster.md) |
| `writtenFrom` | pusta mapa | Nowe klasy zdarzeń, każda z app-version, od której czyta ją każdy węzeł. | [17](17-cluster.md) |
| `rewriteHistoryCommandHandlers` | odrzuca każde przepisanie | Handlery przepisań historii. | [15](15-rewriting-history.md) |
| `historyChanges`, `metadataChanges` | brak | Co zmiana pełnego przepisania robi ze zdarzeniami i ich metadanymi. | [15](15-rewriting-history.md) |
| `WithOutboxWriter` (domieszka) | brak | Twoje własne wiersze, zapisywane w transakcji zdarzeń. | [5](05-command-flavors.md) |

## Dostarczanie

| Klasa i parametr | Domyślnie | Co robi | Rozdział |
|---|---|---|---|
| `EventsBusActor(options, eventsLogger)` | `DeliveryOptions()`, bez loggera | Czasy i kontrola przepływu szyny; logger debugowania jej dostarczeń. | [9](09-projection-failures.md) |
| `DeliveryOptions` `deadline` | `10 minutes` | Dostarczenie, które do tego czasu nie dostało ani potwierdzenia, ani zgłoszenia błędu, kończy się jako niedostarczone; jego wiersz w outboxie zostaje. | [9](09-projection-failures.md) |
| `stallAfter`, `summaryInterval` | `1 minute`, `5 minutes` | Kiedy czekające dostarczenie trafia do logu jako utknięte i jak często utknięcia są podsumowywane. | [9](09-projection-failures.md) |
| `registrationTimeout` | `2 minutes` | Jak długo szyna czeka przy starcie na odpowiedź zarejestrowanego subskrybenta; potem startuje bez niego i trzyma każde zdarzenie w outboxie, dopóki nie odpowie. | [17](17-cluster.md) |
| `tickInterval`, `stallLinesPerTick` | `5 s`, `10` | Jak często szyna przegląda swoje dostarczenia i ile linii o utknięciach loguje przy każdym przeglądzie. | — |
| `window`, `windowBytes` | `10000` zdarzeń, `64 MB` | Najwięcej, ile może czekać wysłane do subskrybenta bez jego odpowiedzi; reszta jest wstrzymywana i później pobierana od właścicieli. | [SCALING.md](../SCALING.md), sekcja 5 |
| `maxPulls`, `pullTimeout` | `100`, `1 minute` | Ile wstrzymanych agregatów pobiera się naraz i kiedy pobranie bez odpowiedzi jest ponawiane. | — |
| `pullPartEvents`, `pullPartBytes` | `1000`, `16 MB` | Rozmiar części, w jakich właściciel odpowiada na pobranie. | — |
| `OutboxPollerActor(eventStore, eventBus, options, clock)` | `OutboxPollerOptions()`, zegar systemowy | Czasy pollera; w testach `TestClock`. | [9](09-projection-failures.md), [19](19-testing.md) |
| `OutboxPollerOptions` `firstPollAfter`, `pollInterval` | `10 s`, `30 s` | Pierwszy przegląd i kolejne po nim. | [4](04-first-command.md) |
| `OutboxPollerOptions` `readTimeout` | `5 minutes` | Odczyt outboxu, który trwa dłużej, jest porzucany, zapisywany w logu i powtarzany (połączenie, które przestało odpowiadać). | [18](18-production.md) |
| `maxTracked` | `100000` | Najwięcej czekających agregatów, ile czyta jeden przegląd. | — |
| `RedeliveryPolicy.Options` `attemptsAfter` | `3 minutes`, `6 minutes` | Kiedy czekające zdarzenie jest publikowane ponownie. | [9](09-projection-failures.md) |
| `giveUpAfter`, `retryGivenUpEvery` | `10 minutes`, `30 minutes` | Kiedy jest zgłaszane jako niedostarczone i jak często próbuje się potem znowu. | [9](09-projection-failures.md) |
| `maxRepublishPerPoll` | `1000` | Najwięcej agregatów publikowanych ponownie w jednym przeglądzie. | [9](09-projection-failures.md) |

## Projekcje

| Ustawienie | Domyślnie | Co robi | Rozdział |
|---|---|---|---|
| `ProjectionActorOptions.DEFAULT` `groupUpdatesDelayMillis` | `0` | Jak długo czekają dostarczenia koszyka, żeby kilka z nich zastosować razem. | [8](08-projections.md) |
| `minimumDelayVersion` | `1` | Dostarczenia, które zaczynają się poniżej tej wersji, nigdy nie czekają. | [8](08-projections.md) |
| `eventsToProcessImmediately` | brak | Klasy zdarzeń, które nigdy nie czekają. | [8](08-projections.md) |
| `parallelUpdateProcessing` | `0` | Powyżej 1: tyle wątków stosuje aktualizacje. | [8](08-projections.md) |
| `eventsLogger` | brak | Logger debugowania dostarczeń i paczek. | [8](08-projections.md) |
| `retries` | `Backoff(1 s, 30 s, maxAttempts = 9)`, około 2 minut | Jak ponawiane są próby listenera, który zawodzi, zanim jego błąd zostanie zapisany. | [9](09-projection-failures.md) |
| `retryFailuresOnStart`, `failureRetryInterval` | `true`, `50 ms` | Zapisane błędy są ponawiane przy starcie, jeden co 50 ms. | [9](09-projection-failures.md) |
| `gapRequestRepeat`, `gapGiveUpAfter` | `10 s`, `10 minutes` | Jak często prosi się o brakujące wersje i kiedy luka staje się zapisanym błędem. | [9](09-projection-failures.md) |
| `maxBatchesPerTransaction` | `100` | Najwięcej agregatów, których aktualizacje dzielą jedną transakcję. | [8](08-projections.md) |
| `purgedDropWindow` | `30 minutes` | Jak długo po usunięciu organizacji dostarczenia usuniętych agregatów są potwierdzane bez stosowania. | [16](16-organizations.md) |
| `SubscribableProjectionActor(updatesCacheTTL, subscriptionTTL, options)` | `Duration.ZERO`, `600000` ms, `DEFAULT` | Jak długo wysłane aktualizacje są trzymane dla nowych subskrybentów; jak długo subskrypcja żyje bez odnowienia. | [11](11-subscribable-projections.md) |
| `ProjectionsSettings(eventStore, subscriptionsState, registry, contexts, builder, buildsAtOnce, restartAfter)` | `BuilderSettings()`, `2`, `10 s` | Projekcje zarządzane: jak budowy czytają magazyn, ile z nich działa naraz i jak długo nadzorca czeka po awarii. | [10](10-rebuilding-projections.md) |
| `BuilderSettings` `pageSize`, `pageBytes` | `1000` zdarzeń, `64 MB` | Ile magazynu budowa czyta za jednym razem. | [10](10-rebuilding-projections.md), [18](18-production.md) |
| `chunkEvents`, `chunkBytes`, `chunksInFlight` | `1000`, `16 MB`, `2` | Części, które budowa przekazuje swojej projekcji, i ile z nich jest naraz w drodze. | [10](10-rebuilding-projections.md) |
| `maxReplayStates`, `retryAfter` | `10000`, `5 s` | Najwięcej stanów agregatów, które budowa trzyma otwarte, odtwarzając magazyn w kolejności zatwierdzeń; czas oczekiwania po awarii magazynu albo projekcji. | — |
| `Projections.start(name, version, actorName, whileBuilding)` | `ServeLive` | Czy aktywna budowa dalej przyjmuje dostarczenia, gdy powstaje nowa (`ServeStale`: nie przyjmuje). | [10](10-rebuilding-projections.md) |

## Sagi, squash, organizacje i przepisania

| Ustawienie | Domyślnie | Co robi | Rozdział |
|---|---|---|---|
| `SagaRunner.start(system, definition, uidGenerator, state, actorName)` | `name` definicji | Nazwa singletona runnera. | [12](12-sagas.md) |
| `SagaDefinition.persistRetries` | `Backoff(100 ms, 30 s, maxAttempts = 10)` | Jak ponawia się nieudany zapis postępu sagi. | [12](12-sagas.md) |
| `SagaDefinition.spaceOf(order)` | `None` | Organizacja, do której należy saga, na potrzeby usuwania organizacji. | [12](12-sagas.md) |
| `SquashSchedulerOptions` `firstRunAfter`, `interval` | `1 minute`, `1 hour` | Kiedy przypadają przebiegi schedulera. | [14](14-squash.md) |
| `SquashSchedulerOptions` `readTimeout` | `5 minutes` | Odczyt agregatów do squasha, który trwa dłużej, jest porzucany, zapisywany w logu i powtarzany. | [18](18-production.md) |
| `perRun`, `answerTimeout` | `100`, `1 minute` | Najwięcej agregatów jednego typu w jednym przebiegu i jak długo scheduler czeka na każdą odpowiedź. | [14](14-squash.md) |
| `SpacePurge(eventStore, commandBuses, projections, hooks, options)` | bez hooków, `SpacePurgeOptions()` | Usuwanie organizacji, z hookami twoich własnych tabel. | [16](16-organizations.md) |
| `SpacePurgeOptions` `aggregatesPerBatch`, `eventsPerBatch` | `1000`, `100000` | Kęsy, które bierze usuwanie, każdy w jednej transakcji. | [16](16-organizations.md) |
| `maxPasses`, `answerTimeout` | `3`, `2 minutes` | Ile okrążeń robi usuwanie i jak długo czeka na każdą szynę poleceń i projekcję. | [16](16-organizations.md) |
| `SpaceExport(eventStore, contexts, installation)` | `0` | Instalacja, z której pochodzi eksport. | [16](16-organizations.md) |
| `SpaceImport(eventStore, uidGenerator, rewriters, newMPJsons, eventsPerTransaction)` | brak, nowy `MPJsons`, `10000` | Dla każdego typu agregatu `ImportRewriter`, który mapuje identyfikatory trzymane jako zwykłe liczby albo tekst (typowane identyfikatory mapuje sam import); ile zdarzeń import zapisuje w jednej transakcji. | [16](16-organizations.md) |
| `HistoryRewriter(commandBus, eventStore, projections, answerTimeout)` | brak, `5 minutes` | Projekcje zarządzane do przebudowania po pełnym przepisaniu i jak długo czeka się na każdą odpowiedź. | [15](15-rewriting-history.md) |

## Wywołania serwisowe

| Wywołanie | Domyślnie | Co robi | Rozdział |
|---|---|---|---|
| `StoredDataCheck.run(mpjsons, typesNames, projections, examplesPerClass, fetchSize)` | brak, `5`, `1000` | Czyta każdą zapisaną wartość; wymienia do 5 nieczytelnych wierszy na klasę. | [6](06-events.md), [18](18-production.md) |
| `StoredDataCheck.usersOf(className, limit)` | `100` | Agregaty, które wciąż mają zdarzenia danej klasy. | [6](06-events.md), [14](14-squash.md) |
| `eventStore.backfillSpaces(spaceOf, batchSize)` | brak, `1000` | Nadaje przestrzeń agregatom zapisanym, zanim pojawiły się przestrzenie. | [16](16-organizations.md) |
| `eventStore.backfillEventKeys(batchSize)` | `10000` | Nadaje klucz zdarzeniom zapisanym, zanim pojawiły się klucze zdarzeń. | [16](16-organizations.md) |

## Właściwości systemowe

| Właściwość | Domyślnie | Co robi | Rozdział |
|---|---|---|---|
| `reactivecqrs.log-values` | `false` | Tylko do diagnozy: polecenia SQL biblioteki trafiają do logu z wartościami, a awarie w całości do `io.reactivecqrs.detail`. Te linie zawierają dane osobowe. | [18](18-production.md) |

## Test kit

Ustawiane jako właściwości systemowe, na przykład `sbt -Dreactivecqrs.test.clusterOfOne=true test`.

| Właściwość | Domyślnie | Co robi | Rozdział |
|---|---|---|---|
| `reactivecqrs.test.url`, `.user`, `.password` | `jdbc:postgresql://localhost:5432/reactivecqrs`, `reactivecqrs`, `reactivecqrs` | Baza danych, z którą łączy się `TestDatabase`. | [19](19-testing.md) |
| `reactivecqrs.test.schema` | nowy `test_run_<seconds>_<n>` przy każdym uruchomieniu testów | Schemat danego uruchomienia; schematy uruchomień starszych niż sześć godzin są usuwane. | [19](19-testing.md) |
| `reactivecqrs.test.clusterOfOne` | `false` | `TestSystems.newSystem()` tworzy klaster z jednego węzła. | [19](19-testing.md) |
| `reactivecqrs.schema.record` | `false` | `SchemaReference` nagrywa pliki wzorcowe zamiast je sprawdzać. | [19](19-testing.md) |

---

Powrót do [spisu treści](README.md).

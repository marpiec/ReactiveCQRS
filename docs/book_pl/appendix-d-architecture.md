# Dodatek D. Architektura w obrazkach

Rozdziały tłumaczą ReactiveCQRS kawałek po kawałku. Ten dodatek kładzie te kawałki obok siebie,
w postaci diagramów: co gdzie działa, co dzieje się z poleceniem, jak zdarzenie trafia do modelu
odczytu, co jest w bazie danych, jak model odczytu powstaje od nowa i jak węzły klastra dzielą się pracą.
GitHub rysuje je z tekstu; gdzie indziej wklej blok do dowolnej przeglądarki diagramów Mermaid.

Przy każdym diagramie podany jest rozdział, który go objaśnia. Pełną historię, dla osób rozwijających
bibliotekę, opisuje [ARCHITECTURE.md](../ARCHITECTURE.md).

---

## Części składowe

Twoja aplikacja daje bibliotece swoje konteksty agregatów i modele odczytu, a przy starcie tworzy garść
aktorów ([rozdział 2](02-setting-up.md)). Wszystko, co trwałe, mieszka w jednej bazie PostgreSQL.

```mermaid
flowchart LR
  subgraph app["Twoja aplikacja"]
    caller["Wywołujący: REST, UI, zadania"]
    contexts["Konteksty agregatów<br/>(reguły i zdarzenia)"]
    readmodels["Projekcje<br/>(modele odczytu)"]
    sagas["Definicje sag"]
  end
  subgraph node["Biblioteka, na każdym węźle"]
    uid["UidGeneratorActor"]
    bus["EventsBusActor"]
    cmdbus["AggregateCommandBusActor<br/>jeden na typ agregatu"]
    owner["AggregateOwner<br/>jeden na agregat"]
    poller["OutboxPollerActor<br/>raz w klastrze"]
    supervisor["Nadzorcy projekcji<br/>(Projections.start)"]
    runner["SagaRunner"]
  end
  db[("PostgreSQL<br/>zdarzenia, outbox,<br/>subskrypcje, dokumenty")]

  caller -- polecenia, zapytania --> cmdbus
  caller -- zapytania --> supervisor
  cmdbus --> owner
  contexts -.->|używane przez| owner
  owner -- zatwierdzenie --> db
  owner -- PublishEvents --> bus
  bus -- dostarczenia --> supervisor
  readmodels -.->|działają w| supervisor
  supervisor -- dokumenty --> db
  poller -- ponowna publikacja --> bus
  poller -- czyta outbox --> db
  sagas -.->|uruchamiane przez| runner
  runner -- polecenia --> cmdbus
  uid -- pule identyfikatorów --> cmdbus
```

## Polecenie, od wywołującego do modelu odczytu

To, co rozdział 4 opowiada słowami: jedna transakcja zapisuje zdarzenia i ich wiersze w outboxie,
wywołujący dostaje nową wersję, a model odczytu dostaje zdarzenia po zatwierdzeniu transakcji.

```mermaid
sequenceDiagram
  autonumber
  participant C as Wywołujący
  participant B as Szyna poleceń
  participant O as Właściciel agregatu
  participant DB as PostgreSQL
  participant E as Szyna zdarzeń
  participant P as Projekcja

  C->>B: AddItem(cartId, expectedVersion 1)
  B->>O: CommandEnvelope (tworzy właściciela, gdy trzeba)
  O->>O: sprawdzenie wersji, potem handler polecenia
  O->>DB: jedna transakcja: zdarzenia + wiersze outboxu (+ odpowiedź idempotentna)
  DB-->>O: zatwierdzone, wersja 2
  O-->>C: SuccessResponse(cartId, 2)
  O->>E: PublishEvents(events, state)
  E->>P: zdarzenia, stan albo jedno i drugie
  P->>DB: jedna transakcja: dokument + wersja subskrypcji
  P-->>E: MessageAck
  E-->>B: PublishEventsAck
  B->>DB: usuwa wiersze outboxu (partiami)
```

## Dostarczenie: potwierdzone, nieudane, brakujące albo spóźnione

Projekcja odpowiada na każde dostarczenie ([rozdział 9](09-projection-failures.md)). To, czego nie
potwierdzi, zostaje w outboxie, a poller outboxu zleca ponowną publikację.

```mermaid
sequenceDiagram
  participant E as Szyna zdarzeń
  participant P as Projekcja
  participant O as Właściciel
  participant Q as Poller outboxu

  E->>P: wersje 5–6 koszyka 42
  alt zastosowane
    P-->>E: MessageAck
  else listener wciąż zawodzi (około 2 minut ponowień)
    P-->>E: MessageFailed (zapisane w subscription_failures)
  else wersja 4 nigdy nie dotarła (luka)
    P->>E: RequestRepublish(cart 42, from 4)
    E->>O: RepublishRange
    O->>E: znowu wersje 4–6
    E->>P: wersje 4–6
    P-->>E: MessageAck
  end
  Note over E: brak odpowiedzi przed terminem (10 minut): „undelivered”, wiersz outboxu zostaje
  Q->>E: Republish po 3 i 6 minutach, potem co 30 minut
```

## Bramka kolejności

Każda projekcja pamięta ostatnią zastosowaną wersję dla każdego agregatu (i rodzaju listenera). Każde
dostarczenie przechodzi przez jedną decyzję ([rozdział 8](08-projections.md)):

```mermaid
flowchart TD
  d["Dostarczenie: wersje od–do"] --> q1{"do ≤ zastosowana?"}
  q1 -- tak --> ack["Potwierdź: już zastosowane"]
  q1 -- nie --> q2{"od ≤ zastosowana + 1,<br/>albo zaczyna się od zdarzenia squasha?"}
  q2 -- tak --> apply["Zastosuj nowe wersje w jednej transakcji<br/>z nową wersją subskrypcji"]
  q2 -- nie --> gap["Czekaj: luka. Poproś właściciela o brakujące wersje"]
  apply --> ok{"Listener zadziałał?"}
  ok -- tak --> acked["MessageAck"]
  ok -- "nie, po ponowieniach" --> failed["Zapisz błąd, MessageFailed;<br/>późniejsze wersje agregatu czekają"]
```

## Co jest w bazie danych

Tabele, które zapisuje biblioteka ([rozdziały 2](02-setting-up.md), [10](10-rebuilding-projections.md)
i [18](18-production.md)). Twoje modele odczytu dodają po jednej tabeli `projection_<name>` na każdy
magazyn dokumentów.

```mermaid
erDiagram
  aggregates ||--o{ events : "ma"
  events ||--o| noop_events : "cofnięte przez"
  events ||--o| events_to_publish : "czeka w outboxie"
  aggregates ||--o| snapshots : "ostatni stan"
  aggregates ||--o{ events_archive : "zachowane przez squash"
  aggregates ||--o{ subscriptions : "zastosowane wersje"
  subscriptions ||--o| subscription_failures : "zatrzymana na"
  aggregates ||--o{ commands_responses : "odpowiedzi idempotentne"
  projections ||--|{ projection_builds : "aktywna i w budowie"
  projection_builds ||--o{ projection_build_objects : "tabele budowy"

  aggregates {
    bigint id
    bigint base_id "łańcuch duplikatów"
    bigint space_id "organizacja"
    int base_version "bieżąca wersja"
    int squashed_version
  }
  events {
    bigint id
    bigint aggregate_id
    int version
    text event "JSON"
    jsonb metadata
  }
  events_to_publish {
    bigint event_id
    bigint aggregate_id
    int version
  }
  subscriptions {
    smallint subscriber_type_id
    smallint subscription_type
    bigint aggregate_id
    int aggregate_version
  }
  sagas {
    varchar name
    bigint saga_id
    varchar phase
    int step
    text saga_order
  }
```

## Kopie, odłączanie i squash

Kopia (`DuplicationEvent`, [rozdział 6](06-events.md)) czyta historię swojego oryginału do wersji, przy
której powstała. Squash ([rozdział 14](14-squash.md)) zastępuje najstarsze zdarzenia jednym zdarzeniem
squasha; kopia, która wciąż je czyta, zostaje najpierw odłączona: dostaje własne pierwsze zdarzenie.

```mermaid
flowchart LR
  subgraph before["Przed squashem"]
    b1["Lista A: v1 … v6"]
    c1["Kopia B: czyta v1–v3 z A,<br/>potem własne v4, v5"]
    b1 -.->|historia do v3| c1
  end
  subgraph after["Po squashu A do v5"]
    b2["Lista A: zdarzenie squasha w v5, potem v6"]
    c2["Kopia B: odłączona, jej własne v1 zawiera jej stan z v3;<br/>potem v4, v5 jak wcześniej"]
  end
  before --> after
```

## Ponowna budowa modelu odczytu

Projekcja zarządzana buduje nową wersję we własnych tabelach, podczas gdy aktywna budowa obsługuje
zapytania, a potem przełącza się jedną aktualizacją ([rozdział 10](10-rebuilding-projections.md)):

```mermaid
stateDiagram-v2
  [*] --> Waiting: nowa wersja, zmieniony kontekst albo RebuildProjection
  Waiting --> Bulk: wolne miejsce na budowę
  Bulk --> GoingLive: magazyn zdarzeń przeczytany
  GoingLive --> Reconciling: bieżące dostarczenia zastosowane
  Reconciling --> CaughtUp: wersja każdego agregatu się zgadza
  CaughtUp --> Switched: brak nieudanej subskrypcji
  CaughtUp --> CaughtUp: nieudane subskrypcje, ponowione albo pominięte przez ToBuildInProgress
  Switched --> [*]: stara budowa schodzi z szyn, jej tabele są usuwane
  Bulk --> Waiting: zatrzymana (wznawia od zapisanej pozycji)
```

## Klaster trzech węzłów

Każdy węzeł uruchamia generator identyfikatorów, szynę zdarzeń i szynę poleceń dla każdego typu. To, co
musi działać raz, działa na najstarszym węźle, a każdy węzeł dociera do tego przez proxy
([rozdział 17](17-cluster.md)).

```mermaid
flowchart TB
  subgraph A["Węzeł A (najstarszy)"]
    busA["Szyna zdarzeń"]
    cartsA["Szyna poleceń koszyków<br/>+ region shardów"]
    ownersA["Właściciele części shardów"]
    single["Singletony: nadzorcy projekcji,<br/>poller outboxu, scheduler squasha, runnery sag"]
  end
  subgraph B["Węzeł B"]
    busB["Szyna zdarzeń"]
    cartsB["Szyna poleceń koszyków<br/>+ region shardów"]
    ownersB["Właściciele innych shardów"]
    proxyB["Proxy do singletonów"]
  end
  subgraph C["Węzeł C"]
    busC["Szyna zdarzeń"]
    cartsC["Szyna poleceń koszyków<br/>+ region shardów"]
    ownersC["Właściciele pozostałych"]
    proxyC["Proxy do singletonów"]
  end
  db[("PostgreSQL")]

  cartsA --- ownersA
  cartsB --- ownersB
  cartsC --- ownersC
  cartsB -.->|polecenie dla sharda na A| ownersA
  ownersA --> busA
  ownersB --> busB
  ownersC --> busC
  busA --> single
  busB --> proxyB --> single
  busC --> proxyC --> single
  ownersA & ownersB & ownersC --> db
  single --> db
```

## Aktualizacja krocząca

Nowe węzły dołączają, stare odchodzą po jednym, najstarszy na końcu, więc to, co działa raz, przenosi
się raz. Nowa klasa zdarzenia jest zapisywana dopiero wtedy, gdy każdy węzeł umie ją przeczytać
([rozdział 17](17-cluster.md)).

```mermaid
sequenceDiagram
  participant Old1 as Węzeł 1 (1.0, najstarszy)
  participant Old2 as Węzeł 2 (1.0)
  participant New3 as Węzeł 3 (2.0)
  participant New4 as Węzeł 4 (2.0)

  New3->>Old1: dołącza (app-version 2.0)
  New4->>Old1: dołącza
  Note over New3,New4: nowe shardy trafiają tylko do węzłów 2.0,<br/>a handlery nadal zapisują klasy zdarzeń z 1.0
  Old2->>Old2: odchodzi: przekazuje swoje shardy
  Old1->>Old1: odchodzi ostatni: shardy i singletony przenoszą się na Węzeł 3
  Note over New3,New4: każdy członek działa w 2.0:<br/>everyNodeRuns("2.0") zwraca true, nowe klasy zdarzeń są zapisywane
```

## Przenoszenie organizacji

Zdarzenia organizacji opuszczają jedną instalację jako plik i trafiają do pustej przestrzeni w innej,
pod jej własnymi identyfikatorami ([rozdział 16](16-organizations.md)):

```mermaid
flowchart LR
  subgraph source["Instalacja A"]
    spaceA["Przestrzeń 7: agregaty, zdarzenia,<br/>zarchiwizowane zdarzenia"]
  end
  file[/"Plik JSON lines:<br/>nagłówek, agregaty, zdarzenia<br/>(klasy po nazwach)"/]
  subgraph target["Instalacja B"]
    check["1. Sprawdzenie: format, pusta przestrzeń,<br/>każda klasa, każdy rozmiar; nowe identyfikatory dla zajętych"]
    write["2. Zapis: agregaty, potem zdarzenia w kolejności zatwierdzeń,<br/>z przemapowanymi identyfikatorami i przestrzenią"]
    rebuild["3. Przebudowa modeli odczytu przestrzeni:<br/>RebuildAggregates(OneSpace(target))"]
  end
  spaceA -- "SpaceExport.write" --> file
  file -- "SpaceImport.read" --> check --> write --> rebuild
  spaceA -.->|"potem, jeśli przeniosła się na stałe: SpacePurge"| purge(["Przestrzeń 7 usunięta"])
```

---

Dalej: [dodatek E](appendix-e-configuration.md), czyli wszystkie ustawienia w jednym miejscu.

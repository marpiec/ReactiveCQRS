# Dodatek A. Słownik pojęć

Każde szczególne słowo, którego używa ta książka, w porządku alfabetycznym: z angielskim
odpowiednikiem, znaczeniem w jednym lub dwóch zdaniach i rozdziałem, który omawia je porządnie. Nazwy
zapisane `taką czcionką` spotkasz w kodzie: to klasy, komunikaty, ustawienia i tabele. Gdy hasło
odsyła cię do innego („zob. *Przestrzeń*”), właśnie tam znajdziesz pełne wyjaśnienie.

---

## A

- **Agregat** (*aggregate*): jedna rzecz z własnym pamiętnikiem zdarzeń, na przykład jeden koszyk
  zakupów. Ma identyfikator, wersję i stan, a reguły sprawdza się zawsze w obrębie jednego agregatu.
  ([rozdział 1](01-cqrs-without-the-headache.md), [rozdział 3](03-first-aggregate.md))
- **Agregat bazowy** (*base aggregate*): agregat, z którego skopiowano duplikat. Zob. *Duplikowanie*.
  ([rozdział 6](06-events.md))
- **Agregat efemeryczny** (*ephemeral aggregate*): agregat efemerycznej szyny poleceń
  (`AggregateCommandBusActor.ephemeral`), który żyje w pamięci jednego węzła z ujemnym identyfikatorem
  i nie jest nigdzie zapisywany ani publikowany, dopóki się go nie zmaterializuje.
  ([rozdział 20](20-ephemeral-aggregates.md))
- **Aktor** (*actor*): mały pracownik ze skrzynką odbiorczą, który obsługuje jeden komunikat naraz.
  Wszystko, co w ReactiveCQRS działa, jest aktorem Apache Pekko.
  ([rozdział 1](01-cqrs-without-the-headache.md))
- **Aktor-listener** (*listener actor*, `AggregateListenerActor`): projekcja bez zapytań, do efektów
  ubocznych, takich jak powiadomienia. Dostaje te same dostarczenia i te same gwarancje.
  ([rozdział 8](08-projections.md))
- **Aktualizacja krocząca** (*rolling upgrade*): aktualizowanie klastra po jednym węźle, tak że przez
  jakiś czas stara i nowa wersja działają obok siebie, bez przestoju. ([rozdział 17](17-cluster.md))
- **Aplikacja** (*host*): twój program, który tworzy magazyny i aktorów biblioteki, a potem łączy je
  ze sobą. ([rozdział 4](04-first-command.md))
- **Archiwum** (*archive*, `events_archive`): miejsce, w którym squash zachowuje zdarzenia klas
  audytowych wymienionych w `SquashPolicy.keep`, zamiast je usuwać. Czyta je tylko `GetArchivedEvents`.
  ([rozdział 14](14-squash.md))
- **Ask**: wysłanie komunikatu przez `?` i odebranie odpowiedzi jako `Future`; wymaga niejawnego
  (implicit) `Timeout`. Zob. też *Tell*. ([rozdział 1](01-cqrs-without-the-headache.md))
- **Asynchroniczny wynik polecenia** (*async command result*, `AsyncCommandResult`): wynik handlera
  polecenia, który przychodzi później, opakowany w `Future`. W kontekście agregatu `Future` z wynikiem
  zamienia się w `AsyncCommandResult` bez twojego udziału. ([rozdział 3](03-first-aggregate.md),
  [rozdział 5](05-command-flavors.md))
- **Automatyczny squash** (*automatic squash*, `AutomaticSquash`): reguła w `SquashPolicy.automatic`,
  według której scheduler squasha sam wybiera agregaty do zwinięcia: po `afterEvents` zdarzeniach,
  z zachowaniem `keepLast` najnowszych i tych młodszych niż `keepNewerThan`.
  ([rozdział 14](14-squash.md))

## B

- **Blokada schematu** (*schema lock*, `SchemaLock`): blokada doradcza (advisory lock) PostgreSQL,
  dzięki której procesy inicjalizują schemat po kolei. Proces czeka na nią do pięciu minut.
  ([rozdział 2](02-setting-up.md))
- **Blokowanie optymistyczne** (*optimistic locking*): nikt niczego nie blokuje z góry; zapis, który
  zakładał starszą wersję, zostaje odrzucony w chwili zapisywania.
  ([rozdział 1](01-cqrs-without-the-headache.md), [rozdział 5](05-command-flavors.md))
- **Budowa** (*build*): jedna wersja projekcji zarządzanej, z własnymi tabelami i własnym postępem.
  Nową budowę wypełnia się z magazynu zdarzeń, podczas gdy aktywna budowa nadal obsługuje zapytania,
  a potem nowa zastępuje ją jednym przełączeniem. ([rozdział 10](10-rebuilding-projections.md))

## C

- **Co najmniej raz** (*at least once*): obietnica dostarczania. Każde zdarzenie dociera do każdego
  subskrybenta co najmniej raz, czasem częściej; projekcje odrzucają powtórki po wersji, więc każde
  zdarzenie zostaje zastosowane raz. ([rozdział 1](01-cqrs-without-the-headache.md),
  [rozdział 9](09-projection-failures.md))
- **Cofnięcie** (*undo*, `UndoEvent`): zdarzenie, które znosi skutki ostatnich N zdarzeń agregatu.
  Te zostają w historii, oznaczone jako cofnięte. ([rozdział 6](06-events.md))
- **CQRS** (*Command Query Responsibility Segregation*): jeden model do podejmowania decyzji (strona
  zapisu) i inne modele, dopasowane do pytań, do odczytu (strona odczytu).
  ([rozdział 1](01-cqrs-without-the-headache.md))

## D

- **Dispatcher blokujący** (*blocking dispatcher*, `reactivecqrs.blocking-dispatcher`): pula 32
  wątków, na której działają właściciele agregatów i praca biblioteki z bazą danych. Swoje projekcje
  twórz na nim przez `Props(...).withDispatcher(BlockingDispatcher.id)`.
  ([rozdział 8](08-projections.md), [rozdział 18](18-production.md))
- **Dispatcher handlerów** (*handler dispatcher*, `reactivecqrs.handler-dispatcher`): pula 32 wątków,
  na której działają handlery poleceń, więc wolne handlery spowalniają tylko polecenia.
  ([rozdział 5](05-command-flavors.md), [rozdział 18](18-production.md))
- **Dociąganie** (*pull*): przy kontroli przepływu szyna zdarzeń prosi właścicieli, żeby ponownie
  opublikowali wstrzymane zdarzenia dla subskrybenta, gdy połowa jego okna jest już wolna.
  ([rozdział 9](09-projection-failures.md))
- **Dostarczenie** (*delivery*): przekazanie zdarzeń jednego agregatu jednemu subskrybentowi przez
  szynę zdarzeń. Szyna śledzi je, dopóki subskrybent nie odpowie, nie zatrzyma się albo nie minie
  10 minut. ([rozdział 8](08-projections.md), [rozdział 9](09-projection-failures.md))
- **Dublery w pamięci** (*in-memory doubles*, `MemoryEventStoreState`, `MemorySubscriptionsState`,
  `MemoryDocumentStore` i inne): zamienniki magazynów PostgreSQL do szybkich testów. Trzymają obiekty
  zamiast JSON-a i niczego nie wycofują, więc dowodzą mniej niż test na PostgreSQL.
  ([rozdział 19](19-testing.md))
- **Duplikowanie** (*duplication*, `DuplicationEvent`): tworzenie nowego agregatu jako kopii innego,
  w danej wersji. Kopia dzieli historię swojej bazy do tej wersji, a potem idzie własną drogą.
  ([rozdział 6](06-events.md))

## E

- **Event sourcing**: trzymanie historii zdarzeń jako źródła prawdy i uzyskiwanie bieżącego stanu
  przez ich ponowne odtworzenie. ([rozdział 1](01-cqrs-without-the-headache.md))

## G

- **Generator identyfikatorów** (*uid generator*, `UidGeneratorActor`): aktor, który rozdaje pule
  identyfikatorów dla agregatów, poleceń i sag, z sekwencji PostgreSQL. Tworzysz go przez
  `UidGeneratorActor.postgres(system)`. ([rozdział 4](04-first-command.md),
  [rozdział 18](18-production.md))
- **Gotowość** (*readiness*, `Readiness`, `ReadinessCheck`): węzeł jest gotowy na ruch, gdy każda
  działająca na nim szyna zdarzeń dostała odpowiedź od wszystkich swoich subskrybentów.
  `ReadinessCheck` wpina to w Pekko Management. ([rozdział 17](17-cluster.md))

## H

- **Handler polecenia** (*command handler*): twoja funkcja, która dostaje bieżący stan i polecenie,
  a zwraca `CommandSuccess` (ze zdarzeniami), `CommandFailure` (odmowę) albo `AsyncCommandResult`.
  ([rozdział 3](03-first-aggregate.md))
- **Handler zdarzenia** (*event handler*): twoja czysta funkcja, która stosuje jedno zdarzenie do
  stanu i zwraca nowy stan. Działa na żywo, przy odtwarzaniu i podczas przebudów, więc za każdym
  razem musi dawać ten sam wynik. ([rozdział 3](03-first-aggregate.md), [rozdział 6](06-events.md))

## I

- **Identyfikator agregatu** (*aggregate id*, `AggregateId`): liczba (`Long`), która identyfikuje
  jeden agregat. Szyna poleceń nadaje nowemu agregatowi identyfikator, chyba że polecenie wybrało go samo.
  ([rozdział 4](04-first-command.md))
- **Informacja o zdarzeniu** (*event info*, `EventInfo`): zdarzenie w postaci, w jakiej widzą je
  listenery i zapytania: jego wersja, samo zdarzenie, użytkownik, czas i koperta metadanych.
  ([rozdział 8](08-projections.md))
- **`initSchema()`**: metoda, która tworzy tabele biblioteki albo dodaje to, czego potrzebuje nowa
  wersja. Można ją bezpiecznie wywoływać przy każdym starcie. ([rozdział 2](02-setting-up.md))

## K

- **Klaster** (*cluster*): kilka węzłów (JVM), które razem prowadzą jedną instalację na jednej bazie
  danych, dzięki Pekko Cluster. ([rozdział 17](17-cluster.md))
- **Klucz idempotencji** (*idempotency key*, `CommandIdempotencyId`): klucz, który niesie polecenie
  idempotentne; jego `asDbKey` musi być unikalny we wszystkich typach agregatów. Jednym z takich
  kluczy jest `SagaStep`, możesz też napisać własny. Zapisane odpowiedzi są domyślnie przechowywane
  przez 30 dni. ([rozdział 5](05-command-flavors.md))
- **Kontekst agregatu** (*aggregate context*, `AggregateContext`): klasa, którą piszesz dla jednego
  typu agregatu: jego stan początkowy (`initialAggregateRoot`), handlery poleceń, handlery zdarzeń,
  jego `version` i funkcje opcjonalne (migawki, squash, tryb szeregowy, rozmieszczenie).
  ([rozdział 3](03-first-aggregate.md))
- **Kontrola przepływu** (*flow control*): szyna zdarzeń nigdy nie wysyła subskrybentowi więcej niż
  okno zdarzeń bez odpowiedzi (domyślnie 10 000 zdarzeń i 64 MB). Reszta czeka w magazynie i jest
  dociągana dla subskrybenta, w miarę jak ten nadrabia zaległości.
  ([rozdział 9](09-projection-failures.md))
- **Koperta metadanych** (*metadata envelope*, `EventMetadata`): kto działał i dlaczego: aktor,
  identyfikatory korelacji i przyczyny, kontekst śledzenia, przestrzeń i twoje własne wpisy. Zapisuje
  się ją z każdym zdarzeniem polecenia, które domieszkuje `WithMetadata`; listenery ją widzą, handlery
  zdarzeń nie. ([rozdział 6](06-events.md))
- **Korzeń agregatu** (*aggregate root*): niezmienna case class, która trzyma stan agregatu, na
  przykład `ShoppingCart(name, items)`. Handler zdarzenia, który zwraca `null`, oznacza agregat jako
  usunięty. ([rozdział 3](03-first-aggregate.md))
- **Krok sagi** (*saga step*, `SagaStep`): identyfikator sagi i numer kroku. To klucz idempotencji
  poleceń wysyłanych przez krok, więc saga wznowiona po awarii nigdy nie zastosuje polecenia dwa razy.
  ([rozdział 12](12-sagas.md))

## L

- **Limit czasu polecenia** (*command timeout*, `commandTimeout`): ile może trwać polecenie, zanim
  wywołujący dostanie `CommandHandlingError`; domyślnie 60 sekund. Wynik, który przyjdzie później,
  nigdy nie zostanie zapisany. ([rozdział 5](05-command-flavors.md))
- **Listener** (`EventsListener`, `AggregateListener`, `AggregateWithEventsListener`): funkcja zwrotna
  projekcji dla jednego typu agregatu. Trzy rodzaje dostają nowe zdarzenia, najnowszy stan albo jedno
  i drugie; każdy ma wariant `noDBSession` do pracy poza bazą danych. ([rozdział 8](08-projections.md))
- **Luka** (*gap*): wersje, których projekcji brakuje dla agregatu (ma wersję 5, a dostaje wersję 7).
  Projekcja prosi właściciela o ponowne ich opublikowanie i zapisuje awarię, jeśli nie nadejdą
  w ciągu 10 minut. ([rozdział 9](09-projection-failures.md))

## M

- **Magazyn dokumentów** (*document store*, `DocumentStore`, `PostgresDocumentStore`): prosty magazyn
  dokumentów JSON dostępnych po identyfikatorze, w tabeli `projection_<name>`, w którym projekcje trzymają
  swoje modele odczytu. ([rozdział 8](08-projections.md))
- **Magazyn zdarzeń** (*event store*, `PostgresEventStoreState`): część biblioteki, która trzyma
  zdarzenia i agregaty w PostgreSQL, razem z outboxem. ([rozdział 2](02-setting-up.md))
- **Materializacja** (*materialize*): zapisanie agregatu efemerycznego jako nowego, trwałego agregatu,
  z jednego zdarzenia squasha jego stanu. ([rozdział 20](20-ephemeral-aggregates.md))
- **`MessageTooLargeException`**: w klastrze to, co dostaje pytający zamiast odpowiedzi zbyt dużej, by
  przejść między węzłami (około 248 KiB przy domyślnym rozmiarze ramki Pekko).
  ([rozdział 17](17-cluster.md))
- **Metryki** (*metrics*, `Metrics`, `ReactiveMetrics`): pomiary biblioteki (czasy poleceń, zapisane
  zdarzenia, czekające dostarczenia i więcej), wysyłane do twojego systemu metryk, gdy tylko wywołasz
  `ReactiveMetrics(system).use(...)`. ([rozdział 18](18-production.md))
- **Migawka** (*snapshot*, `SnapshotPolicy`): zapisana kopia stanu agregatu, domyślnie zapisywana co
  200 zdarzeń i przy zatrzymaniu właściciela, dzięki czemu odtworzenie czyta migawkę i tylko
  zdarzenia po niej. Trzeba ją włączyć; można ją bezpiecznie wyrzucić. ([rozdział 13](13-snapshots.md))
- **Model odczytu** (*read model*): dane ułożone pod jeden rodzaj pytania, utrzymywane przez
  projekcję: lista koszyków każdego klienta, liczba sztuk każdego produktu.
  ([rozdział 1](01-cqrs-without-the-headache.md), [rozdział 8](08-projections.md))
- **mpjsons**: biblioteka JSON, która zapisuje zdarzenia, stany i dokumenty oraz przenosi komunikaty
  między węzłami. ([rozdział 2](02-setting-up.md))

## N

- **Nadzorca** (*supervisor*, `ProjectionSupervisor`): aktor, którego `Projections.start` zwraca dla
  projekcji zarządzanej. Używaj go wszędzie tam, gdzie normalnie użyłoby się projekcji: do zapytań
  i w `RegisterSubscribers`. ([rozdział 10](10-rebuilding-projections.md))
- **Niedostarczone zdarzenie** (*undelivered*): zdarzenie, na które subskrybent nie odpowiedział od
  10 minut. Trafia do logu, zostaje w outboxie i później jest publikowane ponownie.
  ([rozdział 9](09-projection-failures.md))
- **Nieudana subskrypcja** (*failed subscription*, `SubscriptionFailure`): zapis projekcji, że poddała
  się na jednym agregacie po mniej więcej dwóch minutach ponawiania. Na dostarczenia tego agregatu
  projekcja odpowiada niepowodzeniem, dopóki tej subskrypcji nie ponowisz albo nie pominiesz.
  ([rozdział 9](09-projection-failures.md))
- **Nieudane odtworzenie** (*restore failure*, `RestoreFailure`, `restoreFailures()`): zapis agregatu, którego
  nie dało się odtworzyć, zrobiony przez jego właściciela, budowę albo `RestoreCheck`, ze zdarzeniem, na którym
  się nie udało, i przyczyną. ([rozdział 18](18-production.md))
- **Numer instalacji** (*installation number*, `reactivecqrs.installation-number`): daje instalacji
  własny zakres identyfikatorów agregatów (od numeru × 10^12 wzwyż), żeby identyfikatory pozostały
  unikalne, gdy dane różnych instalacji się spotkają. 0, wartość domyślna, to zakres wszystkich
  identyfikatorów nadanych przed 1.0. ([rozdział 2](02-setting-up.md), [rozdział 18](18-production.md))

## O

- **Oczekiwana wersja** (*expected version*, `expectedVersion`): wersja, o której `Command` mówi, że
  właśnie ją zmienia. Jeśli agregat jest już dalej, polecenie dostaje odpowiedź
  `AggregateConcurrentModificationError`. ([rozdział 5](05-command-flavors.md))
- **Odczyt historyczny** (*historical read*): pytanie szyny poleceń o agregat w dawnej wersji lub
  chwili albo o jego zdarzenia (`GetAggregateForVersion`, `GetAggregateAtInstant`,
  `GetEventsForAggregate`, `GetEventsForAggregateForVersion`). Odpowiedź pochodzi z magazynu.
  ([rozdział 7](07-reading-the-past.md))
- **Odłączenie** (*detach*): to, co squash albo trwałe usunięcie robi z duplikatem, który wciąż czyta
  usuwaną historię. Pierwsze zdarzenie duplikatu staje się zdarzeniem squasha jego własnego stanu,
  więc duplikat nie potrzebuje już swojej bazy. ([rozdział 14](14-squash.md))
- **Odpowiedź na polecenie** (*command response*): to, co wywołujący dostaje za polecenie:
  `SuccessResponse`, `CustomSuccessResponse`, `FailureResponse`, `AggregateConcurrentModificationError`,
  `CommandHandlingError`, `EventHandlingError` albo `CommandRejected`. Wszystkie wymienia dodatek B.
  ([rozdział 4](04-first-command.md), [rozdział 5](05-command-flavors.md))
- **Odtworzenie** (*restore*): sposób, w jaki właściciel uzyskuje stan swojego agregatu przy starcie:
  przez odtworzenie zdarzeń albo z migawki i zdarzeń po niej. ([rozdział 6](06-events.md),
  [rozdział 13](13-snapshots.md))
- **Okno** (*window*): zob. *Kontrola przepływu*. ([rozdział 9](09-projection-failures.md))
- **Opóźnione zapytanie** (*delayed query*): zapytanie do projekcji, które czeka na szukane dane, ale
  najdłużej do wyznaczonego terminu; piszesz je przez `delayIfNotAvailable`. Gdy czas minie,
  odpowiada `None`. ([rozdział 8](08-projections.md))
- **Organizacja** (*organization*): zob. *Przestrzeń*. ([rozdział 16](16-organizations.md))
- **Ostatecznie spójny** (*eventually consistent*): strona odczytu dogania zapis chwilę po udanym
  poleceniu, zwykle w ciągu milisekund. ([rozdział 1](01-cqrs-without-the-headache.md))
- **Outbox** (`events_to_publish`): tabela zdarzeń, których jakiś subskrybent jeszcze nie potwierdził.
  Zapisuje się ją w tej samej transakcji co zdarzenia, więc nic nie ginie, gdy maszyna stanie
  w połowie. ([rozdział 1](01-cqrs-without-the-headache.md), [rozdział 9](09-projection-failures.md))

## P

- **Pasywacja** (*passivation*): zatrzymanie bezczynnego właściciela agregatu, żeby zwolnić pamięć;
  następny komunikat uruchamia nowego właściciela, który odtwarza stan. Możesz o nią poprosić przez
  `PassivateAggregate`. ([rozdział 13](13-snapshots.md), [rozdział 18](18-production.md))
- **Pełne przepisanie historii** (*complete rewrite*, `HistoryRewriter`, `RewriteEverywhere`):
  wymazanie danych. `HistoryChange` zastosowana do każdego zapisanego i zarchiwizowanego zdarzenia
  agregatu i wszystkich jego kopii, a po niej przebudowa wskazanych przez ciebie projekcji, z wierszem
  audytu, który nigdy nie przechowuje samych wartości. ([rozdział 15](15-rewriting-history.md))
- **Pierwsze polecenie** (*first command*, `FirstCommand`): polecenie, które tworzy nowy agregat.
  ([rozdział 3](03-first-aggregate.md), [rozdział 5](05-command-flavors.md))
- **Pierwsze zdarzenie** (*first event*, `FirstEvent`): zdarzenie, od którego zaczyna się historia
  agregatu. Wskazuje przestrzeń agregatu (`SpaceId(0)` dla platformy).
  ([rozdział 3](03-first-aggregate.md), [rozdział 6](06-events.md))
- **Polecenie** (*command*): prośba o zmianę jednego agregatu, zapisana jako case class.
  W odróżnieniu od zdarzenia polecenie można odrzucić. `Command` to też nazwa rodzaju polecenia, który
  niesie oczekiwaną wersję. ([rozdział 1](01-cqrs-without-the-headache.md),
  [rozdział 5](05-command-flavors.md))
- **Polecenie idempotentne** (*idempotent command*, `IdempotentCommand`): polecenie, które działa raz
  na klucz idempotencji, bez względu na to, ile razy zostanie wysłane. Każda kopia dostaje odpowiedź
  tej kopii, która wygrała. ([rozdział 5](05-command-flavors.md))
- **Polecenie współbieżne** (*concurrent command*, `ConcurrentCommand`): polecenie bez oczekiwanej
  wersji. Działa na tej wersji, którą agregat akurat ma, a po kolizji biblioteka wykonuje je ponownie,
  łącznie najwyżej 10 prób. ([rozdział 5](05-command-flavors.md))
- **Poller outboxu** (*outbox poller*, `OutboxPollerActor`): jeden na system. Co 30 sekund przegląda
  outbox i każe właścicielom ponownie opublikować czekające zdarzenia: 3 i 6 minut po pierwszej
  próbie; po 10 minutach zgłasza je jako niedostarczone, a potem próbuje co 30 minut.
  ([rozdział 4](04-first-command.md), [rozdział 9](09-projection-failures.md))
- **Ponowna publikacja** (*re-publication*): ponowne wysłanie zdarzeń przez właściciela: tych jego
  zdarzeń, które wciąż czekają w outboxie, albo zakresu, o który projekcja poprosiła, żeby wypełnić
  lukę. ([rozdział 9](09-projection-failures.md))
- **Potwierdzenie** (*ack*, `MessageAck`): „mam” od subskrybenta w odpowiedzi na dostarczenie zdarzeń.
  Gdy każda subskrypcja potwierdzi zdarzenie albo zgłosi dla niego `MessageFailed`, zdarzenie opuszcza
  outbox. ([rozdział 9](09-projection-failures.md))
- **Projekcja** (*projection*, `ProjectionActor`): aktor, który słucha zdarzeń, utrzymuje aktualny
  model odczytu i odpowiada na zapytania o niego. ([rozdział 1](01-cqrs-without-the-headache.md),
  [rozdział 8](08-projections.md))
- **Projekcja subskrybowalna** (*subscribable projection*, `SubscribableProjectionActor`): projekcja,
  która dodatkowo wypycha zmiany do klientów, którzy ją zasubskrybowali, jako komunikaty
  `SubscriptionUpdated`. ([rozdział 11](11-subscribable-projections.md))
- **Projekcja zarządzana** (*managed projection*): projekcja uruchomiona przez `Projections.start`,
  dzięki czemu prowadzi ją nadzorca, który potrafi przebudować ją z magazynu zdarzeń bez przestoju.
  Projekcje z efektami ubocznymi pozostają niezarządzane. ([rozdział 10](10-rebuilding-projections.md))
- **Przebudowa** (*rebuild*): ponowne wypełnienie projekcji z magazynu zdarzeń, w całości (nowa
  budowa) albo w miejscu, dla wybranych agregatów (przebudowa celowana).
  ([rozdział 10](10-rebuilding-projections.md))
- **Przebudowa celowana** (*targeted rebuild*, `RebuildAggregates`): przebudowa, w miejscu, tego, co
  projekcja zbudowała z jednego agregatu, jednego typu agregatu albo jednej organizacji
  (`RebuildTarget.OneAggregate`, `OneType`, `OneSpace`). ([rozdział 10](10-rebuilding-projections.md))
- **Przekazanie** (*hand-over*): w klastrze przeniesienie właściciela typu shardowanego z jednego węzła
  na inny (bo węzeł odchodzi albo shardy są równoważone na nowo). Zanim właściciel się przeniesie,
  kończy rozpoczęte polecenia. ([rozdział 17](17-cluster.md))
- **Przepisanie historii** (*history rewrite*, `RewriteHistoryCommand`,
  `RewriteHistoryConcurrentCommand`): polecenie, które zmienia wartości w dawnych zdarzeniach, a każde
  z nich zachowuje swoją klasę, na przykład żeby usunąć dane osobowe. Jego handler zwraca
  `RewriteCommandSuccess`. ([rozdział 15](15-rewriting-history.md))
- **Przestrzeń** (*space*, `SpaceId`): organizacja (tenant), do której należy agregat, wskazana przez
  jego pierwsze zdarzenie. Przestrzeń 0 to sama platforma. ([rozdział 16](16-organizations.md))
- **Pula połączeń** (*connection pool*): pula połączeń z bazą danych ScalikeJDBC. Twoja aplikacja
  przygotowuje ją, zanim ruszy jakikolwiek kod biblioteki. ([rozdział 2](02-setting-up.md))
- **Punkt squasha** (*squash point*): wersja, do której agregat jest zwinięty. Niczego poniżej niej nie
  da się już odczytać, poza zdarzeniami audytowymi zachowanymi w archiwum.
  ([rozdział 14](14-squash.md))

## R

- **`RecordingMetrics`**: implementacja `Metrics`, która trzyma każdy pomiar w pamięci, żeby test mógł
  sprawdzić, co zmierzyła biblioteka. ([rozdział 19](19-testing.md))
- **Rejestr** (*registry*, `ProjectionRegistry`, `PostgresProjectionRegistry`): tabele, w których
  projekcje zarządzane zapisują swoją aktywną budowę i budowę w toku.
  ([rozdział 10](10-rebuilding-projections.md))
- **Rodowód** (*lineage*): zapis biblioteki o agregatach, z których skopiowano odłączoną kopię
  (`aggregate_lineage`), żeby pełne przepisanie historii oryginału wciąż tę kopię odnalazło.
  ([rozdział 15](15-rewriting-history.md))
- **Rozmieszczenie** (*placement*, `Placement`): gdzie działają właściciele danego typu agregatu:
  `Placement.Local` (domyślnie) albo `Placement.Sharded`. ([rozdział 17](17-cluster.md))

## S

- **Saga** (`SagaDefinition`, uruchamiana przez `SagaRunner`): menedżer procesu dla pracy, która
  obejmuje kilka agregatów. Wykonuje kroki, po każdym zapisuje postęp, a gdy krok się nie uda,
  wykonuje kroki wycofania. `SagaActor`, sposób pisania sag sprzed 1.0, jest przestarzały.
  ([rozdział 12](12-sagas.md))
- **Scheduler squasha** (*squash scheduler*, `SquashScheduler`): jeden na system. Raz na godzinę zwija,
  po jednym naraz, agregaty wybrane przez regułę `AutomaticSquash` ich kontekstów.
  ([rozdział 14](14-squash.md))
- **Serializator klastra** (*cluster serializer*, `ClusterSerializer`): jedyny serializator wszystkich
  komunikatów między węzłami, zbudowany na mpjsons; serializacja Javy pozostaje wyłączona.
  ([rozdział 17](17-cluster.md))
- **ServeLive / ServeStale**: co robi aktywna budowa projekcji zarządzanej, gdy powstaje nowa: dalej
  stosuje bieżące zdarzenia (`ServeLive`, domyślnie) albo udostępnia swoje tabele bez zmian
  (`ServeStale`). ([rozdział 10](10-rebuilding-projections.md))
- **Singleton** (`Singletons.start`): aktor, który działa w jednym egzemplarzu w całym klastrze, na
  przykład nadzorca projekcji, poller outboxu, scheduler squasha albo runner sagi. Każdy węzeł
  rozmawia z nim przez proxy. ([rozdział 17](17-cluster.md))
- **Squash** (`SquashAggregate`): zwinięcie historii, czyli zastąpienie zdarzeń agregatu od 1 do N
  jednym zdarzeniem squasha w wersji N; wersja i stan pozostają takie, jakie były. Włącza się go
  osobno w każdym kontekście agregatu przez `SquashPolicy`. ([rozdział 14](14-squash.md))
- **Stan** (*state*): to, co dostajesz, odtwarzając zdarzenia agregatu; przechowuje go korzeń
  agregatu. ([rozdział 1](01-cqrs-without-the-headache.md))
- **`StoredDataCheck`**: narzędzie tylko do odczytu, które czyta każdą zapisaną wartość JSON klasami,
  jakie twoja aplikacja ma teraz, i zgłasza, czego nie da się już odczytać. Uruchamiaj je na kopii
  produkcji przed aktualizacją. ([rozdział 6](06-events.md), [rozdział 18](18-production.md))
- **Strona odczytu** (*read side*): projekcje i ich modele odczytu. Zob. *CQRS*.
  ([rozdział 1](01-cqrs-without-the-headache.md))
- **Strona zapisu** (*write side*): polecenia, agregaty i zdarzenia, czyli miejsce, w którym zapadają
  decyzje. Zob. *CQRS*. ([rozdział 1](01-cqrs-without-the-headache.md))
- **Subskrybent** (*subscriber*): aktor zarejestrowany w szynie zdarzeń przez `RegisterSubscribers`,
  zwykle projekcja albo aktor-listener. ([rozdział 8](08-projections.md))
- **Subskrypcja** (*subscription*): zainteresowanie jednego subskrybenta jednym typem agregatu, dla
  jednego rodzaju listenera. Jej postęp, czyli ostatnia zastosowana wersja dla każdego agregatu, jest
  zapisywany w tabeli `subscriptions`. ([rozdział 8](08-projections.md))
- **Subskrypcja push** (*push subscription*): subskrypcja klienta na aktualizacje projekcji
  subskrybowalnej; wygasa po 10 minutach bez `RenewSubscription`.
  ([rozdział 11](11-subscribable-projections.md))
- **Szyna poleceń** (*command bus*, `AggregateCommandBusActor`): drzwi wejściowe jednego typu
  agregatu. Przyjmuje polecenia i zapytania, rozdaje identyfikatory, kieruje każdy komunikat do
  właściciela agregatu, a na odczyty historyczne odpowiada sama.
  ([rozdział 1](01-cqrs-without-the-headache.md), [rozdział 4](04-first-command.md))
- **Szyna zdarzeń** (*event bus*, `EventsBusActor`): aktor, jeden na węzeł, który przekazuje
  opublikowane zdarzenia każdemu subskrybentowi ich typu agregatu i czeka, aż każdy odpowie.
  ([rozdział 1](01-cqrs-without-the-headache.md), [rozdział 4](04-first-command.md))

## T

- **Tell**: wysłanie komunikatu przez `!` i pójście dalej, bez czekania na odpowiedź. Zob. też *Ask*.
  ([rozdział 1](01-cqrs-without-the-headache.md))
- **Trwałe usunięcie** (*permanent delete*, `PermanentDeleteEvent`): zdarzenie, które, gdy dostanie je
  już każdy subskrybent, usuwa zdarzenia i wiersze agregatu z bazy danych na dobre.
  ([rozdział 6](06-events.md))
- **Tryb szeregowy** (*serial mode*, `CommandProcessing.Serial`): wykonuje polecenia jednego agregatu
  po kolei, w kolejności nadejścia, więc każdy handler widzi najnowszy stan. Gdy czeka już 1 000
  poleceń, następne dostaje odpowiedź `CommandRejected`. ([rozdział 5](05-command-flavors.md))
- **Typ agregatu** (*aggregate type*, `AggregateType`): pełna nazwa klasy korzenia agregatu. Służy za
  klucz routingu, subskrypcji i zapisanych wierszy, więc zmiana nazwy albo przeniesienie klasy korzenia
  tworzy nowy typ. ([rozdział 3](03-first-aggregate.md))
- **Typ lokalny** (*local type*, `Placement.Local`): rozmieszczenie domyślne. Każdy węzeł ma własnych
  właścicieli, a gdy dwa węzły zapisują ten sam agregat, rozstrzyga kontrola wersji w bazie danych.
  ([rozdział 17](17-cluster.md))
- **Typ shardowany** (*sharded type*, `Placement.Sharded`): jeden właściciel na agregat w całym
  klastrze, rozmieszczany przez Pekko Cluster Sharding; trafiają do niego polecenia z każdego węzła.
  ([rozdział 17](17-cluster.md))

## U

- **Uzupełnianie wsteczne** (*backfill*): zadanie wykonywane na działającym systemie, które uzupełnia
  dane w wierszach zapisanych, zanim pojawiła się dana funkcja. `backfillSpaces` daje starym agregatom
  ich przestrzeń.
  ([rozdział 16](16-organizations.md), [rozdział 18](18-production.md))

## W

- **Wersja** (*version*, `AggregateVersion`): liczba zdarzeń w historii agregatu: 0 przed pierwszym
  zdarzeniem, o jeden więcej z każdym kolejnym. Nigdy nie maleje, nawet po squashu.
  ([rozdział 1](01-cqrs-without-the-headache.md))
- **Wersja aplikacji** (*app-version*): numer wydania, który każdy węzeł klastra ogłasza
  w `pekko.cluster.app-version`. Podczas aktualizacji kroczącej decyduje, gdzie trafiają nowe shardy
  i kiedy wolno zapisywać nowe klasy zdarzeń. ([rozdział 17](17-cluster.md))
- **Właściciel agregatu** (*aggregate owner*, `AggregateOwner`): aktor, który opiekuje się jednym
  agregatem. Odtwarza stan, przepuszcza polecenia przez twoje handlery, zapisuje zdarzenia i je
  publikuje; startuje wraz z pierwszym komunikatem i jest zatrzymywany, gdy nic nie robi.
  ([rozdział 1](01-cqrs-without-the-headache.md), [rozdział 4](04-first-command.md))
- **Wstrzymanie budów** (*build hold*, `BuildHold`, `ProjectionsSettings.holdBuilds`): to, na co pełne budowy
  projekcji zarządzanych czekają, zanim ruszą, na przykład magazyn gotowy na budowy
  (`BuildHold.untilStoreReady`). ([rozdział 10](10-rebuilding-projections.md))
- **Writer outboxu** (*outbox writer*, `WithOutboxWriter`, `OutboxWriter`): pozwala kontekstowi
  agregatu zapisywać własne wiersze (zadania, wiadomości do wysłania) w tej samej transakcji co jego
  zdarzenia. ([rozdział 18](18-production.md))
- **`writtenFrom`**: mapa w kontekście agregatu: od nowej klasy zdarzenia do wersji aplikacji, od
  której każdy węzeł potrafi ją odczytać. Dopóki nie każdy węzeł działa w tej wersji, polecenie, które
  zapisałoby tę klasę, dostaje odpowiedź `CommandRejected`. ([rozdział 17](17-cluster.md))
- **Wybrany identyfikator agregatu** (*chosen aggregate id*, `chosenAggregateId`): identyfikator,
  który najpierw rezerwujesz przez `ReserveAggregateId`, a potem wkładasz do `FirstCommand`, dzięki
  czemu dwukrotne wysłanie polecenia tworzącego nigdy nie stworzy dwóch agregatów.
  ([rozdział 5](05-command-flavors.md))

## Z

- **Zapytanie o minimalną wersję** (*min-version query*, `GetAggregateMinVersion`): prosi o agregat,
  gdy tylko osiągnie on daną wersję, żeby ekran mógł pokazać zmianę, którą przed chwilą wprowadził.
  ([rozdział 7](07-reading-the-past.md))
- **Zdarzenie** (*event*, `Event`): fakt o jednym agregacie, w czasie przeszłym, na przykład
  `ItemAdded("milk")`. Zdarzenia przechowuje się na zawsze, jako JSON zapisany pod nazwą ich klasy.
  ([rozdział 1](01-cqrs-without-the-headache.md), [rozdział 6](06-events.md))
- **Zdarzenie-sierota** (*orphan event*): zdarzenie, którego agregat nie ma własnego wiersza w `aggregates`,
  zwykle pozostałość po dawnym usunięciu. Nic go nie czyta; wymieniają je uzupełnienia i `StoredDataCheck`.
  ([rozdział 21](21-rebuilds-and-repairs.md))
- **Zdarzenie kompensujące** (*compensating event*): nowe zdarzenie, które zapisujesz obok przepisania
  historii, na przykład `CartNameRewritten`, żeby projekcje dowiedziały się o zmianie.
  ([rozdział 15](15-rewriting-history.md))
- **Zdarzenie squasha** (*squash event*): `FirstEvent`, które `SquashPolicy.toEvent` tworzy ze stanu.
  Twoje handlery zdarzeń muszą zamienić je z powrotem dokładnie w ten sam stan, inaczej squash
  zostanie odrzucony, chyba że klasa tego zdarzenia domieszkuje `SquashMayChangeState`.
  ([rozdział 14](14-squash.md))
- **Zmiana historii** (*history change*, `HistoryChange`): to, co ma zrobić pełne przepisanie
  historii, zapisane jako case class (`EraseCustomer(id)`); każdy kontekst mówi w `historyChanges`,
  co robi ze swoimi zdarzeniami. ([rozdział 15](15-rewriting-history.md))

---

Dalej: [dodatek B](appendix-b-cheat-sheet.md) wymienia każdy komunikat, który możesz wysłać, i to, co
dostaniesz w odpowiedzi.

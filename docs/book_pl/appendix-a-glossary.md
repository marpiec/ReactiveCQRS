# Załącznik A. Słownik

Każde specjalne słowo użyte w tej książce, ułożone w kolejności alfabetycznej, z jednym lub dwoma zdaniami i
Rozdział, który to właściwie wyjaśnia. Nazwy w czcionce „code font” to te, które spotkasz w kodzie:
Klasy, wiadomości, ustawienia i tabele. Gdy słowo odsyła do innego wpisu („patrz *Spacja*”),
w tym wpisie jest opisana cała historia.

---

## A

- **Ack** (`MessageAck`): subskrybent „otrzymał” potwierdzenie dostarczenia zdarzeń. Raz na
  subskrypcja potwierdziła zdarzenie lub zgłosiła dla niego komunikat „MessageFailed”, zdarzenie opuszcza
  skrzynka nadawcza. ([rozdział 9](09-projection-failures.md))
- **Aktor**: mały pracownik ze skrzynką pocztową, która obsługuje jedną wiadomość na raz. Wszystko w
  ReactiveCQRS to aktor Apache Pekko. ([rozdział 1](01-cqrs-without-the-headache.md))
- **Agregat**: jedna rzecz z własnym dziennikiem zdarzeń, np. jeden koszyk zakupowy. Ma identyfikator,
  wersja i stan, a ich reguły są sprawdzane zbiorczo w danym momencie.
  ([rozdział 1](01-cqrs-without-the-headache.md), [rozdział 3](03-first-aggregate.md))
- **Kontekst agregatu** (`AggregateContext`): klasa, którą piszesz dla jednego typu agregatu: jego
  stan początkowy (`initialAggregateRoot`), procedury obsługi poleceń, procedury obsługi zdarzeń, jego `wersja` i
  funkcje opcjonalne (migawki, squash, tryb szeregowy, rozmieszczanie). ([rozdział 3](03-first-aggregate.md))
- **Identyfikator agregatu** (`AggregateId`): liczba (długa) oznaczająca jeden agregat. Magistrala poleceń
  nadaje nowemu agregatowi swój identyfikator, chyba że polecenie wybrało jeden. ([rozdział 4](04-first-command.md))
- **Właściciel agregatu** (`AggregateOwner`): aktor, który opiekuje się jednym agregatem. Przywraca
  stan, uruchamia polecenia za pośrednictwem Twoich modułów obsługi, przechowuje zdarzenia i publikuje je; zaczyna się od
  pierwsza wiadomość i zostaje zatrzymana, gdy jest bezczynna. ([rozdział 1](01-cqrs-without-the-headache.md),
  [rozdział 4](04-pierwsze-polecenie.md))
- **Korzeń agregatu**: niezmienna klasa przypadków przechowująca stan agregatu, np.
  `ShoppingCart(nazwa, pozycje)`. Obsługujący zdarzenie moduł, który zwraca `null`, oznacza, że ​​agregat został usunięty.
  ([rozdział 3](03-first-aggregate.md))
- **Typ agregatu** (`AggregateType`): pełna nazwa klasy głównego agregatu. Klucze do routingu,
  subskrypcje i przechowywane wiersze, więc zmiana nazwy lub przeniesienie klasy głównej powoduje utworzenie nowego typu.
  ([rozdział 3](03-first-aggregate.md))
- **Wersja aplikacji**: numer wydania ogłaszany przez każdy węzeł klastra
  `pekko.cluster.app-version`. Podczas aktualizacji ciągłej decyduje, gdzie trafią nowe shardy i kiedy nowe
  można zapisać klasy zdarzeń. ([rozdział 17](17-cluster.md))
- **Archiwum** (`events_archive`): gdzie squash przechowuje zdarzenia klas audytu nazwanych w
  `SquashPolicy.keep`, zamiast je usuwać. Tylko `GetArchivedEvents` je odczytuje.
  ([rozdział 14](14-squash.md))
- **Zapytaj**: wysyłanie wiadomości z `?` i otrzymywanie odpowiedzi w postaci `Przyszłości`; wymagane jest
  dorozumiane „Timeout”. Zobacz także *Tell*. ([rozdział 1](01-cqrs-without-the-headache.md))
- **Wynik polecenia asynchronicznego** (`AsyncCommandResult`): wynik obsługi polecenia, który dociera później,
  owinięte wokół „Przyszłości”. W kontekście agregatu „Przyszłość” wyniku jest przekształcana w jeden
  dla ciebie. ([rozdział 3](03-first-aggregate.md), [rozdział 5](05-command-flavors.md))
- **Przynajmniej raz**: obietnica dostarczenia. Każde wydarzenie dociera do każdego subskrybenta przynajmniej raz,
  czasami więcej; projekcje pomijają powtórzenia według wersji, więc każde zdarzenie jest stosowane raz.
  ([rozdział 1](01-cqrs-without-the-headache.md), [rozdział 9](09-projection-failures.md))
- **Automatyczne squash** (`AutomaticSquash`): reguła w `SquashPolicy.automatic`, która pozwala
  harmonogram squash wybiera agregaty do squasha: po zdarzeniach `afterEvents`, zachowując najnowsze
  `keepLast` i te młodsze niż `keepNewerThan`. ([rozdział 14](14-squash.md))

## B

- **Uzupełnianie**: zadanie online, które uzupełnia dane w wierszach zapisanych przed utworzeniem funkcji.
  `backfillSpaces` przypisuje starym agregatom ich przestrzeń; `backfillEventKeys` przypisuje starym zdarzeniom ich przestrzeń
  klucze. ([rozdział 16](16-organizations.md), [rozdział 18](18-production.md))
- **Agregat bazowy**: agregat, z którego skopiowano duplikat. Zobacz *Duplikacja*.
  ([rozdział 6](06-events.md))
- **Blokujący dyspozytor** (`reactivecqrs.blocking-dispatcher`): pula 32 wątków, w których
  Właściciele agregatów i baza danych biblioteki działają. Twórz na ich podstawie projekcje za pomocą
  `Props(...).withDispatcher(BlockingDispatcher.id)`. ([rozdział 8](08-projections.md),
  [rozdział 18](18-production.md))
- **Kompilacja**: jedna wersja zarządzanej projekcji, z własnymi tabelami i własnym postępem. Nowa
  kompilacja jest wypełniana ze sklepu zdarzeń, podczas gdy aktywna kompilacja nadal działa, a następnie zastępuje ją
  jeden przełącznik. ([rozdział 10](10-rebuilding-projections.md))

## C

- **Wybrany identyfikator agregatu** (`chosenAggregateId`): identyfikator, który rezerwujesz jako pierwszy za pomocą `ReserveAggregateId`
  i umieść w `FirstCommand`, więc dwukrotne wysłanie utworzenia nigdy nie utworzy dwóch agregatów.
  ([rozdział 5](05-command-flavors.md))
- **Klaster**: kilka węzłów (maszyn wirtualnych Java) uruchamiających jedną instalację razem, na jednej bazie danych, przez
  Gromada Pekko. ([rozdział 17](17-cluster.md))
- **Serializator klastra** (`ClusterSerializer`): jeden serializator każdej wiadomości pomiędzy węzłami,
  zbudowano na mpjsons; serializacja Javy pozostaje wyłączona. ([rozdział 17](17-cluster.md))
- **Polecenie**: żądanie zmiany jednego agregatu, zapisane jako klasa przypadku. W przeciwieństwie do zdarzenia, może
  zostać odrzuconym. `Command` to także nazwa rodzaju, który niesie oczekiwaną wersję.
  ([rozdział 1](01-cqrs-without-the-headache.md), [rozdział 5](05-command-flavors.md))
- **Magistrala poleceń** (`AggregateCommandBusActor`): drzwi wejściowe jednego typu agregatu. Przyjmuje
  polecenia i zapytania, rozdaje identyfikatory, kieruje każdą wiadomość do właściciela agregatu i odpowiada
  historyczne czyta się samo. ([rozdział 1](01-cqrs-without-the-headache.md),
  [rozdział 4](04-pierwsze-polecenie.md))
- **Obsługa poleceń**: funkcja, która pobiera bieżący stan i polecenie, a następnie zwraca
  `CommandSuccess` (ze zdarzeniami), `CommandFailure` (odmowa) lub `AsyncCommandResult`.
  ([rozdział 3](03-first-aggregate.md))
- **Odpowiedź na polecenie**: to, co osoba wywołująca otrzymuje w odpowiedzi na polecenie: `SuccessResponse`,
  `CustomSuccessResponse`, `FailureResponse`, `AggregateConcurrentModificationError`,
  `CommandHandlingError`, `EventHandlingError` lub `CommandRejected`. W załączniku B wymieniono je wszystkie.
  ([rozdział 4](04-pierwsze-polecenie.md), [rozdział 5](05-polecenie-smaki.md))
- **Limit czasu polecenia** (`commandTimeout`): ile czasu może upłynąć zanim polecenie zostanie wykonane przez osobę je wywołującą
  `CommandHandlingError`, domyślnie 60 sekund. Wynik, który pojawia się później, nigdy nie jest zapisywany.
  ([rozdział 5](05-command-flavors.md))
- **Wydarzenie kompensujące**: nowe zdarzenie rejestrowane obok ponownego zapisu historii, np.
  `CartNameRewritten`, aby projekcje dowiedziały się o zmianie. ([rozdział 15](15-rewriting-history.md))
- **Całkowite przepisanie** (`HistoryRewriter`, `RewriteEverywhere`): usunięcie. Zastosowano `HistoryChange` do
  każde zapisane i zarchiwizowane zdarzenie agregatu i wszystkich jego kopii, a następnie odbudowa
  projekcje, które nazwiesz, z wierszem audytu, który nigdy nie przechowuje wartości. ([rozdział 15](15-rewriting-history.md))
- **Polecenie współbieżne** (`ConcurrentCommand`): polecenie bez oczekiwanej wersji. Działa na
  niezależnie od wersji agregatu, a po kolizji biblioteka uruchamia go ponownie, maksymalnie 10
  prób w sumie. ([rozdział 5](05-command-flavors.md))
- **Pula połączeń**: pula połączeń z bazą danych ScalikeJDBC. Twoja aplikacja ją konfiguruje.
  przed uruchomieniem jakiegokolwiek kodu bibliotecznego. ([rozdział 2](02-setting-up.md))
- **CQRS** (Command Query Responsibility Segregation): jeden z modeli podejmowania decyzji (zapis
  (strona do czytania) i inne modele, ukształtowane do zadawania pytań, do czytania (strona do czytania).
  ([rozdział 1](01-cqrs-without-the-headache.md))

## D

- **Opóźnione zapytanie**: zapytanie projekcyjne, które czeka, aż upłynie określony termin, na poszukiwane dane;
  Piszesz go za pomocą `delayIfNotAvailable`. Odpowiada `None`, gdy czas się skończy.
  ([rozdział 8](08-projections.md))
- **Dostarczanie**: magistrala zdarzeń przekazująca zdarzenia jednego agregatu jednemu subskrybentowi. Jest śledzona do momentu
  abonent odbiera, abonent przestaje lub mija 10 minut. ([rozdział 8](08-projections.md),
  [rozdział 9](09-projection-failures.md))
- **Odłącz**: co robi zduplikowanie lub trwałe usunięcie duplikatu, który nadal odczytuje historię
  zostanie usunięte. Pierwsze zdarzenie duplikatu staje się zdarzeniem squasha w swoim własnym stanie, więc nie
  nie potrzebuje już swojej podstawy. ([rozdział 14](14-squash.md))
- **Magazyn dokumentów** (`DocumentStore`, `PostgresDocumentStore`): prosty magazyn dokumentów JSON
  id w tabeli `projection_<name>`, w której projekcje przechowują swoje odczytane modele.
  ([rozdział 8](08-projections.md))
- **Duplikacja** (`DuplicationEvent`): tworzenie nowego agregatu jako kopii innego agregatu w danym momencie
  wersja. Kopia dzieli historię swojej bazy aż do tej wersji i podąża własną drogą po niej.
  ([rozdział 6](06-events.md))

I

- **Wydarzenie** (`Event`): fakt dotyczący jednego agregatu, w czasie przeszłym, np. `ItemAdded("milk")`.
  Wydarzenia są przechowywane na zawsze w formacie JSON zapisanym pod nazwą ich klasy.
  ([rozdział 1](01-cqrs-without-the-headache.md), [rozdział 6](06-events.md))
- **Szyna zdarzeń** (`EventsBusActor`): aktor, jeden na węzeł, który przekazuje opublikowane zdarzenia do każdego węzła
  abonenta o danym typie zbiorczym i czeka na odpowiedź każdego z nich.
  ([rozdział 1](01-cqrs-without-the-headache.md), [rozdział 4](04-first-command.md))
- **Obsługa zdarzeń**: czysta funkcja, która stosuje jedno zdarzenie do stanu i zwraca nowe
  stan. Działa na żywo, podczas przywracania i przebudowy, więc musi dawać ten sam wynik za każdym razem.
  ([rozdział 3](03-first-aggregate.md), [rozdział 6](06-events.md))
- **Informacje o zdarzeniu** (`EventInfo`): zdarzenie, jakie widzą słuchacze i zapytania: jego wersja, zdarzenie,
  użytkownik, czas i otoczka metadanych. ([rozdział 8](08-projections.md))
- **Klucze zdarzeń** (`event_keys`): tabela rejestrująca zbiorczy typ i przestrzeń każdego zdarzenia, więc
  odbudowa kilku typów lub jednej organizacji, odczytuje tylko własne wydarzenia.
  ([rozdział 16](16-organizacje.md), [rozdział 18](18-produkcja.md))
- **Źródło zdarzeń**: przechowywanie historii zdarzeń jako źródła prawdy i uzyskiwanie aktualnych informacji
  stan poprzez ich ponowne odtworzenie. ([rozdział 1](01-cqrs-without-the-headache.md))
- **Magazyn zdarzeń** (`PostgresEventStoreState`): część biblioteki przechowująca zdarzenia i
  agregaty w PostgreSQL, wraz ze skrzynką nadawczą. ([rozdział 2](02-setting-up.md))
- **Ostatecznie spójne**: strona odczytująca nadrabia zaległości chwilę po pomyślnym wykonaniu polecenia, zwykle
  w ciągu milisekund. ([rozdział 1](01-cqrs-without-the-headache.md))
- **Agregat efemeryczny**: agregat efemerycznej magistrali poleceń (`AggregateCommandBusActor.ephemeral`),
  żyjący w pamięci jednego węzła z negatywnym identyfikatorem, nigdzie nie zapisywany i nigdzie niepublikowany, dopóki nie zostanie
  zmaterializował się. ([rozdział 20](20-efemeryczne-agregaty.md))
- **Wersja oczekiwana** (`expectedVersion`): wersja, którą `Command` wskazuje na zmianę. Kiedy
  agregat został przeniesiony, na komendę odpowiedziano `AggregateConcurrentModificationError`.
  ([rozdział 5](05-command-flavors.md))

## F

- **Nieudana subskrypcja** (`SubscriptionFailure`): rekord projekcji informujący o jej rezygnacji z jednego
  Łącznie, po około dwóch minutach prób. Dostawy tego łącznego zestawu są odbierane jako nieudane.
  dopóki nie spróbujesz ponownie lub nie pominiesz. ([rozdział 9](09-projection-failures.md))
- **Pierwsze polecenie** (`FirstCommand`): polecenie tworzące nowy agregat.
  ([rozdział 3](03-first-aggregate.md), [rozdział 5](05-command-flavors.md))
- **Pierwsze zdarzenie** (`FirstEvent`): zdarzenie rozpoczynające historię agregatu. Nazywa ono
  przestrzeń agregatu (SpaceId(0) dla platformy). ([rozdział 3](03-first-aggregate.md),
  [rozdział 6](06-events.md))
- **Kontrola przepływu**: magistrala zdarzeń nigdy nie wysyła abonentowi więcej niż okno nieodpowiedzianych zdarzeń
  (10 000 zdarzeń i 64 MB domyślnie). Reszta jest przechowywana w magazynie i pobierana dla subskrybenta.
  w miarę jak nadrabia zaległości. ([rozdział 9](09-projection-failures.md))

## G

- **Luka**: brakuje wersji projekcji dla agregatu (ma wersję 5 i otrzymuje
  wersja 7). Prosi właściciela o ponowne opublikowanie ich i rejestruje błąd, jeśli nie zostaną opublikowane.
  w ciągu 10 minut. ([rozdział 9](09-projection-failures.md))

## H

- **Dyspozytor obsługi** (`reactivecqrs.handler-dispatcher`): pula 32 wątków, w których wykonywane są polecenia
  handlery są uruchamiane, więc powolne handlery spowalniają tylko polecenia. ([rozdział 5](05-command-flavors.md),
  [rozdział 18](18-production.md))
- **Przekazanie**: w klastrze właściciel podzielony przechodzi z jednego węzła do drugiego (węzeł opuszcza lub
  (odłamki są ponownie równoważone). Właściciel kończy polecenia, które rozpoczął, zanim się ruszy.
  ([rozdział 17](17-cluster.md))
- **Odczyt historyczny**: żądanie od magistrali poleceń podania agregatu w poprzedniej wersji lub chwili albo
  jego zdarzenia (`GetAggregateForVersion`, `GetAggregateAtInstant`, `GetEventsForAggregate`,
  `GetEventsForAggregateForVersion`). Jest odczytywany z magazynu. ([rozdział 7](07-reading-the-past.md))
- **Przepisywanie historii** (`RewriteHistoryCommand`, `RewriteHistoryConcurrentCommand`): polecenie, które
  zmienia wartości wewnątrz przeszłych zdarzeń, przy czym każde z nich zachowuje swoją klasę, na przykład w celu usunięcia danych osobowych.
  Jego program obsługi zwraca `RewriteCommandSuccess`. ([rozdział 15](15-rewriting-history.md))
- **Zmiana historii** (`HistoryChange`): co powinno zostać zrobione po całkowitym przepisaniu, jako klasa przypadku
  (`EraseCustomer(id)`); każdy kontekst określa, co robi ze zdarzeniami w `historyChanges`.
  ([rozdział 15](15-przepisywanie-historii.md))
- **Host**: Twoja aplikacja, która tworzy magazyny i aktorów biblioteki oraz je łączy
  razem. ([rozdział 4](04-pierwsze-polecenie.md))

## I

- **Klucz idempotencji** (`CommandIdempotencyId`): klucz, który niesie polecenie idempotentne; jego
  `asDbKey` musi być unikalny we wszystkich typach agregatów. `SagaStep` jest jednym z nich i możesz napisać swój
  własne. Zapisane odpowiedzi są domyślnie przechowywane przez 30 dni. ([rozdział 5](05-command-flavors.md))
- **Polecenie idempotentne** (`IdempotentCommand`): polecenie, które działa raz na idempotentność
  Klucz, niezależnie od częstotliwości jego wysyłania. Każda kopia otrzymuje odpowiedź kopii, która wygrała.
  ([rozdział 5](05-command-flavors.md))
- **`initSchema()`**: metoda tworząca tabele biblioteki lub dodająca nową wersję
  potrzeby. Można bezpiecznie dzwonić przy każdym starcie. ([rozdział 2](02-setting-up.md))
- **Podwójne wartości w pamięci** (`MemoryEventStoreState`, `MemorySubscriptionsState`, `MemoryDocumentStore`
  i inne): zamienniki dla magazynów PostgreSQL, do szybkich testów. Przechowują obiekty zamiast
  JSON i nie przywracają niczego, więc nie dowodzą niczego poza testem w PostgreSQL. ([rozdział 19](19-testing.md))
- **Numer instalacji** (`reactivecqrs.installation-number`): przypisuje jednej instalacji własny numer
  zakres zbiorczych identyfikatorów (liczba × 10^12 w górę), dzięki czemu identyfikatory pozostają unikalne, gdy dane
  instalacje się spotykają. 0, wartość domyślna, to zakres każdego identyfikatora utworzonego przed wersją 1.0.
  ([rozdział 2](02-setting-up.md), [rozdział 18](18-production.md))

## L

- **Nasłuchiwacz** (`Nasłuchiwacz zdarzeń`, `Nasłuchiwacz zagregowany`, `Nasłuchiwacz zagregowany z wydarzeniami`): a
  wywołanie zwrotne projekcji dla jednego typu agregatu. Trzy typy otrzymują nowe zdarzenia, najnowsze
  stan lub oba; każdy z nich ma wariant `noDBSession` przeznaczony do pracy poza bazą danych.
  ([rozdział 8](08-projections.md))
- **Aktor słuchacza** (`AggregateListenerActor`): projekcja bez zapytań, w przypadku efektów ubocznych
  takie jak powiadomienia. Otrzymuje te same dostawy i gwarancje. ([rozdział 8](08-projections.md))
- **Linia**: zapis biblioteczny agregatów, z których skopiowano oddzieloną kopię
  (`aggregate_lineage`), dzięki czemu całkowite przepisanie oryginału nadal znajdzie kopię.
  ([rozdział 15](15-przepisywanie-historii.md))
- **Typ lokalny** (`Placement.Local`): domyślne umiejscowienie. Każdy węzeł ma swoich właścicieli,
  a kontrola wersji bazy danych decyduje, czy dwa węzły zapisują ten sam agregat.
  ([rozdział 17](17-cluster.md))

## M

- **Projekcja zarządzana**: projekcja uruchomiona za pomocą `Projections.start`, którą uruchamia nadzorca
  i można go odbudować z magazynu zdarzeń bez przestoju. Projekcje z efektami ubocznymi pozostają
  niezarządzane. ([rozdział 10](10-rebuilding-projections.md))
- **`MessageTooLargeException`**: w klastrze zamiast zbyt długiej odpowiedzi otrzymuje to, co pytający
  duży, aby przemieszczać się między węzłami (około 248 KiB przy domyślnym rozmiarze ramki Pekko).
  ([rozdział 17](17-cluster.md))
- **Materializowanie**: przechowywanie efemerycznego agregatu jako nowego, trwałego, z jednego zdarzenia squasha
  jego stan. ([rozdział 20](20-ephemeral-aggregates.md))
- **Koperta metadanych** (`EventMetadata`): kto działał i dlaczego: aktor, korelacja i związek przyczynowo-skutkowy
  identyfikatory, kontekst śledzenia, przestrzeń i własne wpisy. Są one przechowywane z każdym zdarzeniem
  polecenie, które łączy się z `WithMetadata`; słuchacze je widzą, ale procedury obsługi zdarzeń nie.
  ([rozdział 6](06-events.md))
- **Metryki** (`Metryki`, `Metryki Reaktywne`): miary biblioteki (czasy poleceń, przechowywane zdarzenia,
  oczekujące dostawy i inne), wysyłane do Twojego systemu metrycznego po wykonaniu połączenia
  `ReactiveMetrics(system).use(...)`. ([rozdział 18](18-production.md))
- **Zapytanie o wersję minimalną** (`GetAggregateMinVersion`): żąda agregatu zaraz po osiągnięciu
  wersja, aby ekran mógł pokazać wprowadzoną zmianę. ([rozdział 7](07-reading-the-past.md))
- **mpjsons**: biblioteka JSON przechowująca zdarzenia, stany i dokumenty oraz przesyłająca wiadomości
  między węzłami. ([rozdział 2](02-setting-up.md))

Ten

- **Optymistyczne blokowanie**: nikt niczego nie blokuje z góry; zapis, który oczekiwał starszej wersji, jest
  odrzucono podczas przechowywania. ([rozdział 1](01-cqrs-without-the-headache.md),
  [rozdział 5](05-command-flavors.md))
- **Organizacja**: patrz *Kosmos*. ([rozdział 16](16-organizations.md))
- **Skrzynka nadawcza** (`events_to_publish`): tabela zdarzeń, których jakiś subskrybent jeszcze nie potwierdził.
  Jest on zapisywany w tej samej transakcji co zdarzenia, więc nic nie zostaje utracone, gdy maszyna się zatrzyma
  w połowie drogi. ([rozdział 1](01-cqrs-without-the-headache.md), [rozdział 9](09-projection-failures.md))
- **Poller skrzynki nadawczej** (`OutboxPollerActor`): jeden na system. Co 30 sekund sprawdza skrzynkę nadawczą.
  i nakazuje właścicielom ponowne opublikowanie wydarzeń oczekujących: 3 i 6 minut po pierwszej próbie; o 10
  minut zgłasza, że ​​nie dostarczono ich, a następnie próbuje co 30 minut. ([rozdział 4](04-first-command.md),
  [rozdział 9](09-projection-failures.md))
- **Autor skrzynki nadawczej** (`WithOutboxWriter`, `OutboxWriter`): pozwala kontekstowi agregatu zapisywać wiersze
  własne (zadania, wiadomości do wysłania) w tej samej transakcji co zdarzenia.
  ([rozdział 18](18-produkcja.md))

## P

- **Pasywacja**: zatrzymanie bezczynnego właściciela agregatu w celu zwolnienia pamięci; następna wiadomość rozpoczyna nową
  właściciela, który przywraca stan. Możesz o to poprosić za pomocą `PassivateAggregate`.
  ([rozdział 13](13-snapshots.md), [rozdział 18](18-production.md))
- **Trwałe usunięcie** (`PermanentDeleteEvent`): zdarzenie, które po jego wystąpieniu u każdego subskrybenta,
  usuwa zdarzenia i wiersze agregatu z bazy danych na stałe. ([rozdział 6](06-events.md))
- **Placement** (`Placement`): gdzie właściciele typu agregatu uruchamiają: `Placement.Local` (
  domyślne) lub `Placement.Sharded`. ([rozdział 17](17-cluster.md))
- **Projekcja** (`ProjectionActor`): aktor nasłuchujący zdarzeń, przechowujący model odczytu do
  datę i odpowiada na pytania na jej temat. ([rozdział 1](01-cqrs-without-the-headache.md),
  [rozdział 8](08-projections.md))
- **Pull**: w przypadku kontroli przepływu magistrala zdarzeń prosi właścicieli o ponowne opublikowanie wstrzymanych zdarzeń
  abonent, gdy połowa jego okna będzie wolna. ([rozdział 9](09-projection-failures.md))
- **Subskrypcja push**: subskrypcja klienta dotycząca aktualizacji projekcji podlegającej subskrypcji;
  wygasa po 10 minutach bez opcji `RenewSubscription`. ([rozdział 11](11-subscribable-projections.md))

## R

- **Model odczytu**: dane ukształtowane dla jednego rodzaju pytania, przechowywane przez projekcję: lista koszyków na
  klient, liczba pozycji na produkt. ([rozdział 1](01-cqrs-without-the-headache.md),
  [rozdział 8](08-projections.md))
- **Strona odczytu**: projekcje i ich modele odczytu. Zobacz *CQRS*.
  ([rozdział 1](01-cqrs-without-the-headache.md))
- **Gotowość** (`Gotowość`, `Sprawdzanie gotowości`): węzeł jest gotowy na ruch po każdym zdarzeniu na magistrali
  Otrzymaliśmy odpowiedź od wszystkich subskrybentów. `ReadinessCheck` integruje to z Pekko Management.
  ([rozdział 17](17-cluster.md))
- **Odbudowa**: ponowne wypełnienie projekcji ze sklepu zdarzeń, w całości (nowa budowa) lub w częściach
  miejsce na pewne agregaty (celowa przebudowa). ([rozdział 10](10-rebuilding-projections.md))
- **`RecordingMetrics`**: implementacja `Metrics`, która przechowuje wszystkie pomiary w pamięci, dzięki czemu test może
  sprawdź, co zmierzyła biblioteka. ([rozdział 19](19-testing.md))
- **Rejestr** (`ProjectionRegistry`, `PostgresProjectionRegistry`): tabele, w których zarządzano
  Projekcje rejestrują aktywną i trwającą kompilację.
  ([rozdział 10](10-rebuilding-projections.md))
- **Ponowna publikacja**: właściciel ponownie wysyła zdarzenia: te, które wciąż czekają w jego skrzynce nadawczej lub zakres
  projekcja, o którą poproszono w celu wypełnienia luki. ([rozdział 9](09-projection-failures.md))
- **Przywróć**: w jaki sposób właściciel uzyskuje stan swojego agregatu podczas uruchamiania: poprzez odtworzenie zdarzeń lub
  ze migawki i zdarzeń następujących po niej. ([rozdział 6](06-events.md), [rozdział 13](13-snapshots.md))
- **Aktualizacja ciągła**: aktualizacja klastra po jednym węźle na raz, dzięki czemu stare i nowe wersje działają równolegle
  stronie przez jakiś czas, bez przestoju. ([rozdział 17](17-cluster.md))

## S

- **Saga** (`SagaActor`): menedżer procesów obejmujący wiele agregatów. Uruchamia kroki,
  zapisuje postęp każdego kroku i uruchamia funkcję cofania kroków, gdy krok się nie powiedzie.
  ([rozdział 12](12-sagas.md))
- **Krok sagi** (`SagaStep`): identyfikator i numer kroku sagi. Jest to klucz idempotentności
  polecenia wysyłane przez krok, więc saga wznowiona po awarii nigdy nie stosuje polecenia dwa razy.
  ([rozdział 12](12-sagas.md))
- **Blokada schematu** (`SchemaLock`): doradcza blokada PostgreSQL, która sprawia, że ​​procesy inicjują
  schemat po jednym. Proces czeka na niego do pięciu minut. ([rozdział 2](02-setting-up.md))
- **Tryb szeregowy** (`CommandProcessing.Serial`): uruchamia polecenia jednego agregatu po drugim, w
  kolejność ich przybycia, aby każdy obsługujący widział najnowszy stan. Gdy 1000 poleceń już czeka,
  następną odpowiedzią jest `CommandRejected`. ([rozdział 5](05-command-flavors.md))
- **ServeLive / ServeStale**: co robi aktywna kompilacja zarządzanej projekcji podczas nowej kompilacji
  jest zrobione: kontynuuj stosowanie wydarzeń na żywo (`ServeLive`, domyślnie) lub serwuj jego tabele bez zmian
  (`ServeStale`). ([rozdział 10](10-rebuilding-projections.md))
- **Typ sharded** (`Placement.Sharded`): jeden właściciel na agregat w całym klastrze, umieszczony przez
  Pekko Cluster Sharding; polecenia z dowolnego węzła docierają do niego. ([rozdział 17](17-cluster.md))
- **Singleton** (`Singletons.start`): aktor, który uruchamia się raz w całym klastrze, np.
  nadzorca projekcji, sonda skrzynki nadawczej, harmonogram Squash lub runner Saga. Każdy węzeł
  komunikuje się z nim za pośrednictwem serwera proxy. ([rozdział 17](17-cluster.md))
- **Migawka** (`SnapshotPolicy`): zapisana kopia stanu agregatu, domyślnie zapisywana co
  200 zdarzeń i gdy właściciel zatrzyma się, więc przywrócenie odczyta migawkę i tylko zdarzenia po niej
  To. Opt-in i można bezpiecznie wyrzucić. ([rozdział 13](13-snapshots.md))
- **Space** (`SpaceId`): organizacja (najemca), do której należy agregat, nazwana według jego pierwszej nazwy
  wydarzenie. Przestrzeń 0 jest samą platformą. ([rozdział 16](16-organizations.md))
- **Squash** (`SquashAggregate`): zastąpienie zdarzeń agregatu od 1 do N jednym zdarzeniem squasha w
  wersja N; wersja i stan pozostają takie same. Opt-in dla kontekstu zbiorczego z
  `SquashPolicy`. ([rozdział 14](14-squash.md))
- **Zdarzenie squasha**: `Pierwsze zdarzenie`, które `SquashPolicy.toEvent` tworzy ze stanu. Twoje zdarzenie
  osoby obsługujące muszą przywrócić go do dokładnie tego samego stanu, w przeciwnym razie dynia zostanie odrzucona, chyba że jej klasa
  miesza w `SquashMayChangeState`. ([rozdział 14](14-squash.md))
- **Punkt spłaszczenia**: wersja, do której agregat jest spłaszczony. Niczego poniżej nie można odczytać.
  więcej, z wyjątkiem zdarzeń audytu przechowywanych w archiwum. ([rozdział 14](14-squash.md))
- **Harmonogram Squash** (`SquashScheduler`): jeden na system. Raz na godzinę zgniata agregaty
  że reguła „AutomaticSquash” ich kontekstów wybiera je po kolei. ([rozdział 14](14-squash.md))
- **Stan**: to, co uzyskujesz, odtwarzając zdarzenia agregatu, przechowywane w jego korzeniu agregatu.
  ([rozdział 1](01-cqrs-without-the-headache.md))
- **`StoredDataCheck`**: narzędzie tylko do odczytu, które odczytuje każdą zapisaną wartość JSON z klasami, które posiadasz
  Aplikacja ma teraz i raportuje, czego już nie odczytuje. Uruchom ją na kopii produkcyjnej przed
  aktualizacja. ([rozdział 6](06-events.md), [rozdział 18](18-production.md))
- **Projekcja subskrybowalna** (`SubscribableProjectionActor`): projekcja, która również wypycha
  zmiany dla klientów, którzy zasubskrybowali, w postaci wiadomości `SubscriptionUpdated`.
  ([rozdział 11](11-subskrybowalne-projekcje.md))
- **Subskrybent**: aktor zarejestrowany w magistrali zdarzeń za pośrednictwem `RegisterSubscribers`, zwykle
  projekcja lub aktor-słuchacz. ([rozdział 8](08-projections.md))
- **Subskrypcja**: zainteresowanie jednego subskrybenta jednym typem zbiorczym, dla jednego typu słuchacza.
  postęp, czyli ostatnia wersja zastosowana dla każdego agregatu, jest przechowywany w tabeli `subskrypcje`.
  ([rozdział 8](08-projections.md))
- **Supervisor** (`ProjectionSupervisor`): aktor `Projections.start` zwraca dla zarządzanego
  projekcja. Używaj jej wszędzie tam, gdzie używasz projekcji: w zapytaniach i w
  `RejestracjaSubskrybentów`. ([rozdział 10](10-rebuilding-projections.md))

## T

- **Przebudowa ukierunkowana** (`RebuildAggregates`): odbudowa na miejscu tego, co zostało wytworzone na podstawie projekcji
  agregat, jeden typ agregatu lub jedna organizacja (`RebuildTarget.OneAggregate`, `OneType`,
  `OneSpace`). ([rozdział 10](10-rebuilding-projections.md))
- **Tell**: wysłanie wiadomości za pomocą `!` i odejście bez czekania na odpowiedź. Zobacz także
  *Zapytaj*. ([rozdział 1](01-cqrs-without-the-headache.md))

## W

- **Generator Uid** (`UidGeneratorActor`): aktor, który rozdaje pule identyfikatorów dla agregatów,
  Polecenia i sagi z sekwencji PostgreSQL. Utwórz je za pomocą `UidGeneratorActor.postgres(system)`.
  ([rozdział 4](04-pierwsze-polecenie.md), [rozdział 18](18-produkcja.md))
- **Niedostarczone**: zdarzenie, w którym abonent nie odpowiedział przez 10 minut. Jest ono rejestrowane i pozostaje
  w skrzynce nadawczej i zostanie ponownie opublikowana później. ([rozdział 9](09-projection-failures.md))
- **Cofnij** (`UndoEvent`): zdarzenie, które anuluje efekt ostatnich N zdarzeń agregatu.
  Zostają zapisane w historii i oznaczone jako niewykonane. ([rozdział 6](06-events.md))

## W

- **Wersja** (`AggregateVersion`): ile zdarzeń zawiera historia agregatu: 0 przed pierwszym
  wydarzenie, po jednym więcej z każdym. Nigdy nie pada, nawet po rozbiciu.
  ([rozdział 1](01-cqrs-without-the-headache.md))

## W

- **Okno**: zobacz *Kontrola przepływu*. ([rozdział 9](09-projection-failures.md))
- **Strona zapisu**: polecenia, agregaty i zdarzenia, w których podejmowane są decyzje. Zobacz *CQRS*.
  ([rozdział 1](01-cqrs-without-the-headache.md))
- **`writtenFrom`**: mapa w kontekście agregatu z nowej klasy zdarzeń do wersji aplikacji z
  który każdy węzeł może odczytać. Dopóki każdy węzeł nie uruchomi tej wersji, polecenie, które zapisze
  klasa ma odpowiedź `CommandRejected`. ([rozdział 17](17-cluster.md))

---

Dalej: [Dodatek B](appendix-b-cheat-sheet.md) zawiera listę wszystkich wiadomości, które możesz wysłać, i informacje zwrotne.
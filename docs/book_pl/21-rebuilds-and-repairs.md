# Rozdział 21. Przebudowy i naprawy od podszewki

[Rozdział 10](10-rebuilding-projections.md) pokazał, jak wypuścić nową wersję modelu odczytu, gdy stary
dalej obsługuje zapytania. To codzienny przypadek i zwykle w zupełności wystarcza. Ten rozdział jest na
pozostałe sytuacje: duży magazyn, aktualizację, budowę, która nie chce się przełączyć, dane, których
twój kod już nie rozumie. Po jego lekturze będziesz wiedzieć, przez co budowa przechodzi krok po kroku,
co może ją zatrzymać na każdym kroku i jak to zobaczyć, jak zaplanować przebudowę na dużej instalacji i
co zrobić z każdym rodzajem zapisanych danych, które nie pasują.

To tyleż rozdział, co ściąga. Przeczytaj go raz od początku, a do tabel wracaj, gdy coś
się wydarzy.

---

## Część 1: życie budowy

### Rejestr pamięta, nadzorca decyduje

Wyobraź sobie plac budowy z biurem budowy. Biuro przechowuje plany: który budynek jest w użyciu, który
rośnie po sąsiedzku i jak daleko posunęły się prace. Kierownik budowy co rano czyta plany i decyduje,
co robić tego dnia.

**Rejestr** to biuro budowy: dwie małe tabele, dla każdej projekcji jej aktywna budowa i budowa w
toku, a dla każdej budowy jej numer, wersja, przyrostek tabel, nazwa subskrybenta, wersje kontekstów,
z którymi ją zrobiono, i zapisana pozycja. **Nadzorca** to kierownik budowy. Za każdym razem, gdy
startuje (startuje aplikacja albo nadzorca przenosi się na inny węzeł klastra), czyta rejestr i
decyduje:

| Co mówi rejestr | Co robi nadzorca |
|---|---|
| nigdy nie słyszał tej nazwy | buduje ją z magazynu: budowa 1, która w trakcie budowania też obsługuje zapytania |
| aktywna budowa jest w wersji z kodu, nic nie jest w toku | obsługuje zapytania aktywną budową; nie ma czego budować |
| aktywna budowa jest w innej wersji | zaczyna budowę wersji z kodu |
| budowa w toku jest w wersji z kodu | kontynuuje ją od zapisanej pozycji |
| budowa w toku jest w innej wersji | porzuca ją, usuwa jej tabele i zaczyna budowę wersji z kodu |
| kontekst, którego stany dostaje projekcja, ma inną `version` | zaczyna budowę (tylko dla projekcji z listenerami stanów) |
| budowy, które nie są ani aktywne, ani w toku | usuwa je (zatrzymanie wypadło między przełączeniem a usunięciem) |

Projekcja zbudowana przez ReactiveCQRS 0.12 zostaje **przejęta** jako budowa 0, z tabelami i wierszami
subskrypcji takimi, jakie są, w wersji zapisanej przez 0.12: przy aktualizacji nic nie jest
przebudowywane, chyba że wersja się różni ([rozdział 18](18-production.md), „Przejście na 1.0”).

> **Pamiętaj:** wersja, którą podajesz w `start`, to obietnica dotycząca tabel. Podnoś ją zawsze, gdy
> dokumenty zapisywane przez twój kod zmieniają znaczenie albo kształt. Zostaw ją, gdy się nie
> zmieniają, nawet jeśli zmienił się kod: podniesienie wersji kosztuje pełną budowę.

### Czekanie na miejsce

Budowa, na którą przyszła pora, nie zawsze rusza od razu. Prosi o swoją kolej **miejsca na budowy**
(*build slots*): naraz działa najwyżej `buildsAtOnce` budów (dwie), pozostałe czekają w kolejności, w
jakiej prosiły. Przy wstrzymaniu (`holdBuilds`, [rozdział 10](10-rebuilding-projections.md)) żadna
budowa nie dostaje miejsca, dopóki wstrzymanie się nie skończy. W czasie czekania `GetBuildStatus`
odpowiada fazą `Waiting`, a gdy trzyma ją wstrzymanie, `heldBy` mówi dlaczego.

Czekająca budowa nie próżnuje: jest już zapisana do szyny zdarzeń i potwierdza każde dostarczenie, nie
nanosząc go. To bezpieczne, bo główny przebieg i tak przeczyta te zdarzenia z magazynu. Długie czekanie
kosztuje więc tylko czas.

### Trzy fazy

Gdy budowa ma już swoje miejsce, wypełnia ją **budowniczy**. Wyobraź sobie przeprowadzkę biblioteki do
nowego budynku, gdy cały czas jest otwarta: najpierw po kolei przenosisz regały, potem otwierasz nowe
drzwi, a na koniec przechodzisz między regałami z katalogiem i dokładasz tych kilka książek, które ktoś
wypożyczył i oddał w trakcie przeprowadzki.

1. **Główny przebieg** (`Bulk`). Budowniczy dostarcza typy agregatów projekcji w kolejności jej
   listenerów, każdy typ w całości przed następnym. Dla danego typu czyta stronę jego agregatów (1 000,
   i najwyżej 64 MB ich zdarzeń), czyta zdarzenia każdego agregatu na kilku wątkach (domyślnie
   czterech), tnie je na porcje (1 000 zdarzeń, najwyżej 16 MB) w kolejności agregatów i wysyła każdą
   porcję do budowy, która nanosi ją w jednej transakcji razem z wersjami subskrypcji, do których
   dochodzi. Listener stanów dostaje zamiast zdarzeń końcowy stan każdego agregatu, jedną pozycję na
   agregat. Agregat mieszczący się w porcji jest czytany raz, zdarzenia razem ze stanem; dłuższy dostaje
   najpierw stan, a potem zdarzenia porcja po porcji, więc budowa nigdy nie trzyma całej jego historii. W locie są dwie porcje; następna strona jest czytana, gdy one są nanoszone. Po każdej
   naniesionej porcji pozycja (typ i agregat, do którego doszła) trafia do rejestru.
2. **Przejście na żywo** (`GoingLive`). Budowa sama zaczyna nanosić dostarczenia z szyny i ponawia
   awarie, które zapisała w trakcie przebiegu.
3. **Uzgadnianie** (`Reconciling`). Budowniczy przechodzi przez każdy agregat tych typów i porównuje
   wersję w magazynie z wersją w budowie. To, czego brakuje (zdarzenia zatwierdzone po tym, jak
   przebieg przeczytał ich agregat), czyta jeszcze raz według wersji i dostarcza.

Wtedy budowa **nadrobiła zaległości** (`CaughtUp`). Zanim zacznie obsługiwać zapytania, potrzebne są
jeszcze dwie rzeczy: przebudowy celowane, o które poproszono w międzyczasie, wykonują się także na niej
(jej główny przebieg mógł odczytać te agregaty, zanim się zmieniły), i nie może mieć żadnej nieudanej
subskrypcji. Wtedy nadzorca **przełącza**, jedną aktualizacją rejestru.

### Przełączenie i to, co po nim

Przełączenie to aktualizacja jednego wiersza: od tej chwili zapytania trafiają do nowej budowy. Potem:

- wykonuje się `onActivated()` nowej budowy, a `rebuilding` zmienia się dla niej na fałsz;
- stara budowa odłącza się od szyny zdarzeń każdego węzła, przekazuje swoje subskrypcje push nowej
  budowie ([rozdział 11](11-subscribable-projections.md)), odpowiada na swoje odłożone zapytania `None`
  i się zatrzymuje;
- jej tabele i sekwencje zostają usunięte, jej wersje subskrypcji skasowane (partiami po 10 000), a jej
  zapisy o awariach i zapisy o agregatach, których nie umiała odtworzyć, usunięte.

> **Pod maską:** główny przebieg czyta każdy agregat według wersji, od wersji 1 (albo od jego
> zdarzenia zgniecenia) w górę. Część jednego agregatu, która pomija wersję, kończy się przed luką, a
> następny odczyt zaczyna się od niej; wersje, których brakuje w magazynie, kończą tam subskrypcję
> agregatu porażką. Polecenia, które zmieniają agregat po tym, jak przebieg go przeczytał, wyłapuje
> uzgadnianie. Kolejność agregatów nie gra żadnej roli: dostarczanie na żywo też je miesza (D96). Dlatego budowa kończy się taka sama jak projekcja
> utrzymywana na żywo, co `RebuildEquivalenceSpec` sprawdza na losowych historiach
> ([ARCHITECTURE.md, sekcja 8](../ARCHITECTURE.md)).

### Gdy coś stanie w połowie

Budowa jest zrobiona tak, żeby dało się ją przerwać. Oto, co robi każde przerwanie:

| Co się dzieje | Co robi budowa |
|---|---|
| aplikacja się zatrzymuje albo węzeł odchodzi i nadzorca się przenosi | następny nadzorca czyta zapisaną pozycję i kontynuuje główny przebieg od niej; porcje, które były w locie, są nanoszone jeszcze raz, co wersje subskrypcji czynią nieszkodliwym |
| budowniczy zawodzi (magazynu przez chwilę nie da się czytać) | czyta stronę jeszcze raz po `retryAfter` (5 s); budowniczy, który całkiem się zatrzyma, zostaje uruchomiony ponownie po `restartAfter` (10 s), od zapisanej pozycji |
| zatrzymuje się instancja budowy (nie da się utworzyć jej magazynów) | aktywna budowa dalej obsługuje zapytania; nowa instancja budowy rusza po `restartAfter` |
| baza danych nie działa | partie są ponawiane bez liczenia ich jako awarii; nic nie zostaje zapisane na konto twoich listenerów |
| wdrażasz nowszą wersję w trakcie budowy | budowa zrobiona w połowie zostaje porzucona i usunięta; buduje się nowsza wersja |
| wracasz do starszego kodu w trakcie budowy | nowa budowa zostaje porzucona; aktywna budowa nadrabia to, co ją ominęło, uzgadniając każdy agregat swoich typów |

> **Ostrzeżenie:** stan, który twój listener trzyma w pamięci między wywołaniami, poza swoimi tabelami,
> nie przetrwa tych przerwań. Budowa zatwierdza porcja po porcji i może wznowić pracę od każdej z nich.

### Wiele węzłów

W klastrze ([rozdział 17](17-cluster.md)) każda projekcja zarządzana działa raz, jako singleton:
nadzorca, jego budowy i budowniczowie żyją na jednym węźle, a szyna zdarzeń każdego węzła dostarcza do
niego przez proxy. Budowa czyta magazyn bezpośrednio, więc nigdy nie wędruje między węzłami. Gdy ten
węzeł odchodzi, singleton rusza na innym węźle, czyta rejestr i kontynuuje jak po zatrzymaniu.
Wstrzymanie czyta bazę danych, więc każdy węzeł się z nim zgadza.

## Część 2: co zatrzymuje budowę i jak to zobaczyć

### Listener, który zawodzi

Listener, który rzuca wyjątek na jakimś agregacie, jest ponawiany przez mniej więcej dwie minuty, tak
jak przy dostarczeniach na żywo ([rozdział 9](09-projection-failures.md)); potem awaria zostaje
zapisana pod subskrybentem budowy, a budowa idzie dalej z pozostałymi agregatami. Gdy nadrobi
zaległości z awariami, nie przełącza się: nadzorca zapisuje w logu „caught up with N failed
subscriptions” i sprawdza ponownie co 30 sekund.

Co robisz: patrzysz (`ToBuildInProgress(GetProjectionStatus)`), poprawiasz listener, wdrażasz,
ponawiasz (`ToBuildInProgress(RetryAllFailedSubscriptions)`); albo, dla agregatu, którego dokumenty
mogą poczekać, pomijasz go (`SkipFailedVersion` dla jednej wersji, `SkipAggregate` dla całego).

### Dane, których budowa nie umie użyć

Cztery rzeczy z daleka wyglądają podobnie, a awaria pozwala je odróżnić:

| Awaria mówi | Co to znaczy | Gdzie jeszcze to widać |
|---|---|---|
| `a stored event cannot be read: ...` | JSON zdarzenia nie daje się już odczytać do jego klasy (pole zmieniło typ, klasa zmieniła nazwę) | `unreadable` w `StoredDataCheck` |
| `the aggregate cannot be restored: ...` | zdarzenia dają się odczytać, ale twój handler zdarzeń rzuca wyjątek na jednym z nich, gdy stan jest odtwarzany | `eventStore.restoreFailures()`, `RestoreCheck` |
| `versions from N to M are missing from the store` | zapisane wersje agregatu mają dziurę: tych wersji w ogóle nie ma w magazynie | `eventStore.restoreFailures()` (`missing versions`), `RestoreCheck` (`report.gaps`) |
| `versions from N are missing: ...` | o wersje poproszono, a nigdy nie przyszły (rzadkie w trakcie budowy: budowniczy sam czyta magazyn) | `ProjectionStatus.waitingForGap`, zanim awaria zostanie zapisana |

Trzy pierwsze są zapisywane od razu, z agregatem, zdarzeniem, na którym się nie udało, i przyczyną, a
budowa idzie dalej bez tego agregatu. Część 3 mówi, co zrobić z każdą z nich.

> **Wskazówka:** projekcja, która słucha tylko zdarzeń (`EventsListener`), nie potrzebuje stanów, więc
> handler rzucający wyjątek przy odtwarzaniu nie dotyka jej budów. Taki agregat zatrzymuje tylko
> budowę projekcji z listenerami stanów jego typu.

### Magazyn, który nie jest gotowy

Na instalacji zaktualizowanej ze starszej wersji budowa może działać, ale powoli:

- dopóki nie istnieje indeks własnych wierszy agregatów według typu (`buildIndexesConcurrently`), każda
  strona agregatów zajmuje około 100 ms zamiast 1 ms;
- dopóki nie istnieje indeks zdarzeń według agregatu i wersji (to samo zadanie), zdarzenia każdego
  agregatu są czytane przez wolniejszy indeks.

`eventStore.buildReadiness()` mówi, czego brakuje. Lekarstwem jest uruchomienie zadań online
([rozdział 18](18-production.md), krok 7) i wstrzymanie budów, dopóki się nie skończą.

## Część 3: zapisane dane, które nie pasują

### Zasada: biblioteka znajduje, ty decydujesz

Stare dane mają swoje zakamarki. Pole usunięte trzy wydania temu, skrypt, który w 2020 roku usunął
połowę czegoś, zdarzenie, którego twój handler przestał przyjmować. ReactiveCQRS ma wobec nich
wszystkich jedną postawę: nigdy sama nie naprawia, nie usuwa ani nie pomija zapisanych danych. Je
**znajduje** (sprawdzeniem, które uruchamiasz, albo gdy na nie trafi), **zgłasza** (z identyfikatorami,
klasami, wersjami i przyczynami, nigdy z wartościami), utrzymuje **wszystko inne w ruchu** i pozwala
**tobie** wybrać naprawę. Tabela jest mapą; każdy jej wiersz jest wyjaśniony niżej.

| Co jest nie tak | Jak to znaleźć | Twoje możliwości |
|---|---|---|
| JSON zdarzenia nie daje się już odczytać | `StoredDataCheck` (`unreadable`); budowy: „a stored event cannot be read” | popraw klasę albo dodaj konwerter; wymaż wartość ([rozdział 15](15-rewriting-history.md)) |
| zdarzenie o niezerowej wersji typu zdarzenia | `StoredDataCheck` (`eventsByTypeVersion`) | zmigruj je ręcznie, zanim przeczyta je 1.0 (D29) |
| handler rzuca wyjątek, gdy stan jest odtwarzany | `restoreFailures()`, `RestoreCheck`, `AggregateRestoreFailedException` polecenia | popraw handler; pomiń agregat w budowie; usuń agregat |
| zdarzenia, których agregat nie ma własnego wiersza (sieroty) | `StoredDataCheck` (`orphans`) | zostaw je (nic ich nie czyta); usuń je |
| wersja, której brakuje w historii agregatu | `RestoreCheck` (`gaps`), zapytanie z [rozdziału 18](18-production.md); budowy: „versions from N to M are missing from the store” | pomiń dziurę w każdej budowie (`SkipFailedVersion(id, pastMissing = true)`); pomiń agregat; usuń go |
| dwa zdarzenia jednej wersji | `StoredDataCheck` (`repeatedVersions`) | zdecyduj, które jest prawdziwe, i usuń drugie ręcznie |
| agregaty bez organizacji | `backfillSpaces` (`left`), `StoredDataCheck` (`aggregatesWithoutSpace`) | funkcja `spaceOf`, która je przydziela ([rozdział 16](16-organizations.md)) |
| agregaty typu, którego nie ma żaden kontekst | `StoredDataCheck` (`typesWithoutContext`, z `contexts`) | oddaj typowi jego kontekst albo zostaw je nieczytane |
| dokument, który jest błędny, choć zdarzenia są dobre | twoi użytkownicy albo porównanie ze świeżą budową | `RebuildAggregates` dla niego ([rozdział 10](10-rebuilding-projections.md)) |
| listener zawodzący na jakimś agregacie | `GetProjectionStatus` | popraw i ponów; pomiń ([rozdział 9](09-projection-failures.md)) |

### Zdarzenia, których nie da się już odczytać

Zwykła przyczyna to zmiana klasy: pole zmieniło typ, klasa przeniosła się do innego pakietu. Takiego
zdarzenia nie odczyta żaden czytelnik, więc polecenie na jego agregacie się nie udaje, a budowa zapisuje
awarię subskrypcji tego agregatu. Znajdź je wszystkie przed aktualizacją przez `StoredDataCheck`
([rozdział 18](18-production.md), krok 8), na kopii produkcji, z nowym kodem. Lekarstwo jest w kodzie:
zachowaj starą klasę obok `V2` ([rozdział 6](06-events.md)) albo zarejestruj konwerter dla starej
postaci. Gdy zdarzenie zawiera dane osobowe, które i tak muszą zniknąć, przepisanie historii potrafi
zmienić nieczytelny JSON jako tekst (`HistoryChange.inUnreadableEvent`,
[rozdział 15](15-rewriting-history.md)).

### Agregaty, których twój kod nie umie odtworzyć

Zdarzenia dają się odczytać, ale stosowanie ich po kolei rzuca wyjątek: `MatchError` dla zdarzenia, o
którym twoje handlery zapomniały, sprawdzenie, którego stare dane nie przechodzą. Taki agregat nie
przyjmuje poleceń (jego właściciel odpowiada `AggregateRestoreFailedException`) i blokuje budowy
projekcji z listenerami stanów jego typu. Każde miejsce, które na niego trafi, go zapisuje, raz na
agregat i miejsce:

```scala
import io.reactivecqrs.core.eventstore.RestoreFailureFilter

eventStore.restoreFailures(RestoreFailureFilter(projection = Some("carts")))
// RestoreFailure(aggregateId, aggregateType, spaceId, version, eventId, errorClass, error,
//                source, firstSeen, lastSeen, times, skipped)
```

`source` mówi gdzie: `Owner`, `Check` (`RestoreCheck`) albo `Build(projection, build, subscriber)`.
Przed aktualizacją `RestoreCheck.run(eventStore, contexts)` odtwarza każdy agregat nowym kodem i
wypełnia te same zapisy, więc znasz listę, zanim trafi na nią jakakolwiek budowa.

Potem, od najlepszego wyjścia:

1. **Popraw handler.** Jeśli zdarzenie to prawdziwa historia, błąd tkwi w handlerze: niech przyjmuje
   to zdarzenie (często wystarczy jeden `case` więcej), wdróż i ponów awarie. Zapisy znikają same, gdy
   agregat znów się odtwarza.
2. **Pomiń go w jednej budowie.** Jeśli model odczytu może na razie obyć się bez tego agregatu, wyślij
   projekcji `SkipAggregate(id)` (przez `ToBuildInProgress` do budowy w toku). Budowa się przełącza;
   zapis mówi „skipped”; zdarzenia zostają.
3. **Usuń go.** Jeśli agregat w ogóle nie powinien istnieć (dane testowe, pozostałości błędu), usuń go
   w jednej transakcji poleceniami z [rozdziału 18](18-production.md) („Usuwanie agregatu ręcznie”),
   a potem ponów awarie. Dokumenty projekcji, które z niego powstały, zostają, dopóki ich nie
   wyczyścisz.

### Zdarzenia-sieroty

Sierota to zdarzenie, którego agregat nie ma własnego wiersza w `aggregates`, zwykle pozostałość po
dawnym usunięciu, które skasowało wiersz, a zdarzeń nie. Sieroty nic nie czyta: odtworzenie, budowa i
uzgadnianie zaczynają od wiersza agregatu. Sieroty są więc tam, gdzie są, nieszkodliwe, poza miejscem
na dysku, które zajmują. `StoredDataCheck` je wymienia (ile ich jest, z których agregatów, ich klasy,
pierwsze i ostatnie zdarzenie). Zatrzymaj je albo usuń tymi samymi poleceniami co agregat (linie `aggregates` niczego wtedy
nie usuwają).

### Wersje zapisane dwa razy

Jedno zdarzenie na wersję to pierwsza zasada magazynu, a 1.0.1 pilnuje jej unikalnym indeksem. Magazyn
zapisany przez starsze wersje albo nieudany import mogą jednak mieć dla jakiegoś agregatu dwa zdarzenia
jednej wersji. Odtworzenie czyta wtedy oba. `StoredDataCheck` je wymienia (`repeatedVersions`), a
dopóki nie znikną, `buildIndexesConcurrently` zostawia stary, nieunikalny indeks i zapisuje w logu
błąd. Obejrzyj oba zdarzenia, zdecyduj, które agregat naprawdę miał, i usuń drugie po identyfikatorze
zdarzenia: najpierw jego wiersz `noop_events`, jeśli go ma, tak jak robią to polecenia usuwania.

### Modele odczytu, które się rozjechały

Zdarzenia są dobre, a dokument zły: ręczna poprawka w SQL-u, poprawiony już błąd listenera, dokument
usunięty przez pomyłkę. Do tego właśnie służą przebudowy celowane:
`RebuildAggregates(RebuildTarget.OneAggregate(id))` dla jednego agregatu, `OneType` albo `OneSpace` dla
większej liczby, w miejscu, gdy projekcja obsługuje zapytania. Przy błędzie, który dotknął wszystkiego,
`RebuildProjection` buduje całą projekcję jeszcze raz obok i przełącza.

> **Ostrzeżenie:** nigdy nie naprawiaj modelu odczytu, poprawiając tylko jego tabele. Następna
> przebudowa celowana albo pełna budowa zapisze dokumenty od nowa ze zdarzeń i twoja poprawka zniknie.
> Popraw zdarzenia (albo kod), potem przebuduj.

## Część 4: planowanie przebudowy na dużej instalacji

Mała projekcja przebudowuje się w kilka sekund i nie potrzebuje planu. Przy magazynie z dziesiątkami
milionów zdarzeń warto najpierw wszystko przemyśleć.

1. **Oszacuj czas.** Na mierzonej maszynie budowa nanosiła około 5 800 zdarzeń na sekundę (projekcja
   oparta na zdarzeniach, dokument zapisywany przy każdym zdarzeniu; `docs/perf/d96-after.md`,
   27/10/2026); `docs/SCALING.md` szacuje około 4,8 godziny na 100 milionów zdarzeń. Projekcja tylko na stanach
   czyta stany końcowe, po jednym na agregat, i ma o wiele mniej pracy. Zmierz na kopii produkcji:
   decydują twoje zdarzenia, twoje listenery i twoja baza danych.
2. **Najpierw przygotuj magazyn.** Na zaktualizowanej instalacji uruchom zadania online i wstrzymaj
   budowy, dopóki się nie skończą (`OnlineJobs.start` i `BuildHold.untilStoreReady`, rozdział 18). Bez
   ich indeksów każda strona, którą czyta budowa, jest wolna.
3. **Poszukaj danych, które nie będą pasować**, na kopii, z nowym kodem: `StoredDataCheck` i
   `RestoreCheck`. Każdy agregat z ich list zatrzyma budowę, dopóki o nim nie zdecydujesz.
4. **Wybierz, co robi stara budowa**: `ServeLive`, jeśli nowy kod umie pracować na starych dokumentach,
   `ServeStale`, jeśli nie (rozdział 10). Przy `ServeStale` stara budowa podaje zamrożone dane przez
   całą budowę, łącznie ze wstrzymaniem.
5. **Uważaj na obciążenie.** Każda trwająca budowa czyta magazyn i zapisuje własne tabele; naraz
   działają dwie. Podnoś wersje kilku dużych projekcji w jednym wydaniu tylko wtedy, gdy baza danych to
   udźwignie, albo obniż `buildsAtOnce` do 1.
6. **Obserwuj, jak idzie.** `GetBuilds` (`building`, `caughtUp`, `heldBy`), `GetBuildStatus` (faza,
   pozycja, wysłane zdarzenia, nieczytelne), `ToBuildInProgress(GetProjectionStatus)` (awarie),
   wskaźnik `reactivecqrs.builds.held` i `eventStore.restoreFailures()`.
7. **Wiedz, co zrobisz z awarią**, zanim się zdarzy: kto decyduje między poprawką kodu, pominięciem a
   usunięciem, i jak szybko.

> **Wskazówka:** gdy budowa trwa, aktywna budowa jak zwykle obsługuje zapytania. Długa budowa kosztuje
> czas i obciążenie bazy danych, nie przestój. To, co może ją wykoleić, prawie zawsze tkwi w danych,
> dlatego kroki 3 i 7 przychodzą przed wdrożeniem.

## Co już umiesz

- Rejestr pamięta budowy; nadzorca czyta go przy każdym starcie i decyduje, co zbudować, kontynuować,
  porzucić albo usunąć.
- Budowa czeka na miejsce (i na koniec wstrzymania), potem przechodzi przez główny przebieg, przejście
  na żywo i uzgadnianie, a przełącza się dopiero wtedy, gdy nadrobi zaległości bez awarii; każde
  przerwanie da się wznowić.
- Awaria mówi, jakiego rodzaju dane ją zatrzymały: zdarzenie, którego nie da się odczytać, agregat,
  którego nie da się odtworzyć, wersje, które nigdy nie przyszły.
- Biblioteka nigdy sama nie naprawia zapisanych danych: `StoredDataCheck`, `RestoreCheck`,
  `restoreFailures()` i uzupełnienia je znajdują; ty poprawiasz kod, pomijasz agregat w budowie albo
  usuwasz.
- Przebudowę na dużym magazynie się planuje: magazyn przygotowany, złe dane znalezione na kopii,
  obciążenie i decyzje ustalone przed wdrożeniem.

To już koniec książki. W dodatkach znajdziesz słownik pojęć, ściągawkę z komunikatami, przewodnik po
rozwiązywaniu problemów, architekturę w obrazkach i wszystkie ustawienia.

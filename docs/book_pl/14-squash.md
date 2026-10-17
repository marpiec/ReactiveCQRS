# Rozdział 14. Squash: Odrzucanie starej historii

Niektóre agregaty żyją latami. Każde polecenie dodaje wiersz do ich dziennika i za każdym razem, gdy takie
Agregat się budzi, jego właściciel czyta wszystkie te linijki jeszcze raz. W tym rozdziale nauczysz się
*zgniatanie* agregat: złożenie starej części jej historii w jedno wydarzenie podsumowujące, na żądanie lub na żądanie
harmonogram, a wydarzenia, które interesują Twoich audytorów, są przechowywane w archiwum. Dowiesz się również
kiedy biblioteka mówi „nie teraz” i dlaczego jest to dobra rzecz.

---

## Strona podsumowania

Wyobraź sobie dziennik Alicji z koszyka na zakupy po pracowitym roku: trzy tysiące wierszy. Nikt nie ma
spojrzał na pierwsze dwa tysiące dziewięćset miesięcy, ale za każdym razem, gdy właściciel wózka zaczyna, to
odtwarza je wszystkie po kolei, aby odbudować wózek.

Teraz wyobraź sobie, że wyrywasz strony od 1 do 2900 i wklejasz na stronie 2900 jedną stronę podsumowania:

> *W tym momencie wózek nazywał się „Sklep spożywczy” i przewoził gruszki i śliwki.*

Kolejne strony pozostają dokładnie takie same. Numeracja się nie zmienia: podsumowanie to strona
2 900, a następny wiersz to nadal strona 2 901. Każdy, kto czyta dziennik od góry, otrzymuje to samo
wózek jak poprzednio, po przeczytaniu stu linijek zamiast trzech tysięcy.

To jest **squash**. Słowami ReactiveCQRS:

- zdarzenia w wersjach od 1 do N są zastępowane jednym **zdarzeniem squash** zapisanym w wersji N,
  **punkt zgniatania**;
- agregat zachowuje swoją wersję, swój stan i każde zdarzenie po N;
- przywracanie odczytuje zdarzenie squash i zdarzenia następujące po nim, więc jest znów szybkie;
- wiersze starych zdarzeń są usuwane, dzięki czemu baza danych może ponownie wykorzystać ich przestrzeń.

Jak powolny jest długi dziennik? Na maszynie, na której mierzona jest wydajność biblioteki, nowy właściciel
przywrócono łącznie 50 000 zdarzeń w ciągu 392 ms (zmierzone 17.10.2026 r., pojedyncze uruchomienie)
test `load-M5` z PostgreSQL na tej samej maszynie; patrz [docs/SCALING.md, sekcja 3](../SCALING.md)).
Dobrze na raz, ale nie dobrze dla każdego wózka, który się obudzi.

> **Pamiętaj:** dynia zmienia historię na zawsze. Gdy stare wydarzenia przeminą, nikt nie będzie mógł ich odczytać.
> wózek taki, jaki był w wersji 3, cofnij operację w punkcie zgniecenia lub skopiuj wózek z poprzedniego.
> To jest cena strony podsumowującej, więc ten rozdział poświęca trochę czasu na to, co biblioteka
> sprawdza zanim pozwoli Ci zapłacić.

### Squash czy snapshot?

[Rozdział 13](13-snapshots.md) dał ci migawki, kolejny sposób na długie historie. Rozwiązują
różne problemy:

| | Migawka | Squash |
|---|---|---|
| Czym jest | kopia stanu, przechowywana obok historii | wydarzenie podsumowujące, które do pewnego stopnia zastępuje historię |
| Historia | niezmieniona | wydarzenia przed punktem zniknęły |
| Czytanie starej wersji | działa jak poprzednio | odrzucono poniżej punktu |
| Jeśli zniknie, biblioteka odczyta wszystkie wydarzenia, a nic nie może zniknąć: teraz to *jest* historia |
| Dysk | jeden wiersz więcej na agregat | mniej wierszy, gdy PostgreSQL odkurzy |
| Włączono za pomocą | `snapshots = Some(SnapshotPolicy(...))` | `squash = Some(SquashPolicy(...))` |

Jeśli potrzebujesz tylko szybkości, migawki to delikatny wybór. Sięgnij po squasha, gdy stara historia jest już nieaktualna.
obciążenie samo w sobie: miejsce na dysku, stare klasy zdarzeń, które chcesz usunąć, dane, których nie masz powodu
zachować. Jeden typ agregatu może używać obu.

> **Pamiętaj:** squash zachowuje wersję. Limit wersji w magazynie zdarzeń na jednostkę
> (`aggregateVersionLimit`, domyślnie 10 000) zlicza wersje, a nie zapisane zdarzenia, więc squash to robi
> nie robić pod nim miejsca.

## Krok 1: zapewnij swojemu zespołowi wydarzenie w squasha

Strona podsumowania to wydarzenie jak każde inne, a Ty ją piszesz. Musi być w stanie odbudować
cały stan samodzielnie. W domenie testowej:

```scala
/** Zdarzenie squasha wózka: jego stan w punkcie squasha. */
klasa przypadku CartSquashed(spaceId: SpaceId, nazwa: String, elementy: Vector[element]) rozszerza FirstEvent[koszyk]
```

Dlaczego „Pierwsze zdarzenie”? Po zgnieceniu jest to pierwsze zdarzenie w historii agregatu i pierwsze
zdarzenia określają organizację, do której należy agregat, jej przestrzeń ([rozdział 16](16-organizations.md)).
Każdy koszyk domeny testowej znajduje się w przestrzeni 0.

Następnie opisz kontekst:

```scala
importuj io.reactivecqrs.api._
importuj io.reactivecqrs.api.id.SpaceId

klasa ShoppingCartAggregateContext rozszerza AggregateContext[ShoppingCart] {
  // ... wersja, initialAggregateRoot i programy obsługi poleceń, jak poprzednio

  nadpisz def eventHandlers = (userId, timestamp, shoppingCart) => {
    // ... inne zdarzenia wózka, jak poprzednio
    przypadek e: CartSquashed => ShoppingCart(e.name, e.items)
  }

  nadpisz val squash: Option[SquashPolicy[ShoppingCart]] =
    Some(SquashPolicy(cart => CartSquashed(SpaceId(0), cart.name, cart.items)))
}
```

`SquashPolicy` wymaga trzech rzeczy:

- **`toEvent`**: funkcja ze stanu do zdarzenia squash. Biblioteka wywołuje ją za pomocą
  stan w punkcie squasha.
- **`automatyczna`**: opcjonalna reguła do spychania bez pytania; domyślnie `Brak`. Jest dostępna
  później w tym rozdziale.
- **`keep`**: klasy zdarzeń audytu, które mają być archiwizowane zamiast usuwane; domyślnie puste. Również
  później w tym rozdziale.

> **Pamiętaj:** zdarzenie squasha jest przechowywane tak długo, jak długo istnieje agregat. Traktuj jego klasę jak
> jakakolwiek inna klasa zdarzeń: gdy jej kształt musi się zmienić, dodaj nową klasę (`CartSquashedV2`) i zachowaj
> stary ([rozdział 6](06-events.md)). Dlatego też biblioteka nigdy nie przechowuje surowego zrzutu
> stan jako historia: klasa zdarzeń to coś, co można rozwijać; zrzut stanu z zeszłego roku
> klasa nie jest.

### Projekcje również muszą to zrozumieć

Kiedy squashujesz, nikt nie jest informowany: squash niczego nie publikuje. Projekcje, które podążają za wózkiem
już masz każde wydarzenie aż do punktu squasha (poniżej zobaczysz, że biblioteka dba o to,
to), a podsumowanie daje ten sam stan, więc ich modele odczytu pozostają poprawne.

Istnieją dwa rodzaje prognoz dotyczące wydarzenia squash: jedna to przebudowa
([rozdział 10](10-rebuilding-projections.md)) i jeden dodany po zgnieceniu. Dla nich to jest
*pierwsze* wydarzenie, jakie kiedykolwiek zobaczą w związku z tym wózkiem, w wersji N. Więc traktuj to jak dzieło, które przynosi
Cały stan. Lista koszyków domeny testowej wygląda tak:

```scala
case CartSquashed(_, name, _) => // zdarzenie squash przechowuje cały stan: dokument jest z niego tworzony
  documentStore.updateDocument(0, aggregateId.asLong, _ => Document(name))
```

`updateDocument` wstawia dokument, gdy go brakuje i zastępuje go, gdy się tam znajduje, więc to
Działa niezależnie od tego, czy projekcja znała wcześniej wózek, czy nie. Projekcja, która nasłuchuje stanów
(`AggregateListener`, [rozdział 8](08-projections.md)) otrzymuje po prostu informację, że koszyk został utworzony
(`created = true`) gdy jego subskrypcja rozpoczyna się na imprezie squash.

> **Ostrzeżenie:** projekcja bez żadnego uzasadnienia dla wydarzenia squasha działa dzisiaj i nie działa prawidłowo
> następna przebudowa: zawodzi przy każdym zgniecionym agregacie lub go pomija. Naucz każdą projekcję
> typ o zdarzeniu squash *zanim* cokolwiek zmiażdżysz.

## Krok 2: zgniatanie na życzenie

Zmiażdżenie jednego agregatu oznacza wysłanie jednej wiadomości do jego magistrali poleceń:

```scala
importuj io.reactivecqrs.api._
importuj io.reactivecqrs.api.id.UserId
zaimportuj org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._

niejawny limit czasu wartości: Limit czasu = Limit czasu (10 sekund)

val answer: Future[SquashResponse] =
  (wózki ? SquashAggregate(cartId, AggregateVersion(5), UserId(7))).mapTo[SquashResponse]
```

Magistrala poleceń przekazuje żądanie do właściciela wózka, który odpowiada jedną z dwóch wiadomości
(nie zapakowane w `Try`):

- `AggregateSquashed(cartId, AggregateVersion(5))`: wykonano.
- `SquashRefused(cartId, reason)`: nic nie zostało zmienione, a `reason` wyjaśnia dlaczego wprost.

`SquashAggregate` przyjmuje cztery wartości:

- **`id`**: agregat.
- **`upTo`**: punkt squasha, od 1 do bieżącej wersji agregatu.
- **`userId`**: kto pytał. Jest przechowywane ze zdarzeniem squash; `UserId(0)` jeśli je pominiesz.
- **`metadane`**: opcjonalna koperta metadanych dla zdarzenia squasha ([rozdział 6](06-events.md)),
  przydatne do zapisywania przyczyny: `Some(EventMetadata("operator:retention", causationId = Some("ticket 4711")))`.

Właściciel wykonuje pracę pomiędzy dwoma poleceniami wózka. Wersja i stan wózka pozostają takie same.
są, więc polecenia nadal działają, dopóki to się dzieje.

### Co się stało z pamiętnikiem

Pójdźmy za przykładem zestawu testowego. Wózek o nazwie „Spożywcze” otrzymuje jabłko, gruszkę i
śliwka, traci jabłko (przedmiot o identyfikatorze 1) i otrzymuje figę. Następnie zostaje spłaszczona do wersji 5:

```
wersja przed po SquashAggregate(cartId, AggregateVersion(5), ...)
1 ShoppingCartCreated("Artykuły spożywcze")
2 Dodano element („jabłko”)
3 Dodano element („para”)
4 Dodano element („śliwka”)
5 ItemRemoved(1) CartSquashed(SpaceId(0), "Artykuły spożywcze",
                                                Wektor(Element(2, "gruszka"), Element(3, "śliwka")))
6 Dodano element("fig") Dodano element("fig")
```

Po squashu:

- Koszyk nadal jest w wersji 6, z tą samą nazwą i tymi samymi przedmiotami. Jego właścicielem jest zupełnie nowy
  właściciel i świeżo odrestaurowany klient zgodnie twierdzą.
- Następne polecenie nadal mówi `AggregateVersion(6)` i jest odpowiedzią
  `SuccessResponse(cartId, AggregateVersion(7))`.
- `GetEventsForAggregate(cartId)` zwraca dwa zdarzenia: zdarzenie squash w wersji 5 z użytkownikiem
  i metadane żądania squasha i czas zdarzenia, które zastąpiło, oraz rys.
  wersja 6.

> **Pod maską:** właściciel sprawdza warunki wstępne i najpierw tworzy zdarzenie squasha, a następnie
> Sklep wykonuje resztę w jednej transakcji. Blokuje łańcuch duplikacji agregatu, sprawdza
> warunki wstępne ponownie, odłącza kopie, przenosi zachowane zdarzenia do archiwum, usuwa stare zdarzenia
> i ich znaczniki cofania zastępuje zdarzenie w wersji N zdarzeniem squash (zachowując jego identyfikator wiersza)
> i czas, więc kolejność zatwierdzania wszystkich zdarzeń jest zachowana) i zapisuje punkt w
> `aggregates.squashed_version`. Decyzje D47 do D52; patrz
> [ARCHITECTURE.md, sekcja 4.6](../ARCHITECTURE.md).

## Sprawdzenie uczciwości

Strona podsumowania, która kłamie, jest gorsza niż żadna. Dlatego zanim cokolwiek zostanie zapisane, biblioteka sprawdza Twoje
zdarzenie squash: stosuje je do `initialAggregateRoot` za pomocą swoich procedur obsługi zdarzeń, dokładnie tak samo jak przywrócenie
woli i porównuje wynik z rzeczywistym stanem w punkcie zgniatania, używając `==`. Jeśli się różnią,
squash jest odrzucany, a powód mówi, że impreza squashowa „nie oddaje stanu, w którym jest
zastępuje".

Funkcja squash jest również odrzucana, gdy funkcja `toEvent` zgłasza wyjątek lub gdy obsługa zdarzeń kończy się niepowodzeniem
wydarzenie squasha.

> **Ostrzeżenie:** sprawdzenie stosuje zdarzenie squash w taki sam sposób, jak przywracanie: w imieniu użytkownika, który
> poprosił o squash (`SquashAggregate.userId`) w momencie zdarzenia, które zastępuje. Jeśli twoje
> obsługa zdarzeń kopiuje użytkownika lub czas do stanu („ostatnio zmieniony przez Boba”), zdarzenie squash
> musi sam przenosić te wartości, a jego program obsługi musi je stamtąd pobrać. W przeciwnym razie sprawdzenie
> odmawia squasha (zawsze w przypadku automatycznych squashów, które pytają jako użytkownik 0) lub pozwala mu przejść tylko wtedy, gdy
> właściwa osoba akurat pyta. Odmowa przypomina ci o tym (problem F155).

### Celowe pomijanie danych

Czasami podsumowanie *powinno* o czymś zapomnieć: o starym, zamkniętym koszyku, którego przedmiotów nikt nie potrzebuje
więcej. Połącz `SquashMayChangeState` z taką klasą zdarzenia squasha, a sprawdzenie upewni się tylko, że
Obsługujący mogą go zastosować bez porównywania. Zestaw testowy zawiera taki, który przechowuje tylko nazwę koszyka:

```scala
klasa przypadku CartArchived(spaceId: SpaceId, nazwa: String) rozszerza FirstEvent[ShoppingCart] o SquashMayChangeState
```

z `case e: CartArchived => ShoppingCart(e.name, Vector.empty)` wśród swoich procedur obsługi zdarzeń. Koszyk
z jabłkiem, gruszką i śliwką (wersje 2 do 4), zgniecione do 3, zawiera tylko śliwkę
później: elementy do momentu zmiażdżenia zostają zapomniane.

Ponieważ historia przedstawia teraz inny stan niż wcześniej, biblioteka wyrzuca również wszystkie
migawkę agregatu i kopii, które odczytują jego historię, a ich właściciele odczytują ich
ponownie stwierdza.

> **Ostrzeżenie:** projekcje *nie* są podawane. Model odczytu zbudowany na podstawie zdarzeń ciągle pokazuje
> zapomniane dane, dopóki ich nie odbudujesz ([rozdział 10](10-rebuilding-projections.md)). Zaplanuj odbudowę
> razem z dynią.

## Kiedy odpowiedź brzmi „nie teraz”

Odmowa niczego nie zmienia, a większość odmów znika sama. Powodem jest zdanie, które oznacza
dla ludzi; oto znaczenie najczęściej używanych:

| Powód wspomina | Dlaczego | Co zrobić |
|---|---|---|
| "squash nie jest włączony" | kontekst nie ma polityki `squash` | wprowadź ją (krok 1) |
| "nie mieści się pomiędzy 1 a" | `upTo` wynosi 0 lub jest poza bieżącą wersją | wybierz istniejącą wersję |
| "już upchnięte do wersji" | jest upchnięte do tego punktu lub dalej | nic; lub wybierz późniejszy moment |
| „nadal czekam w skrzynce nadawczej” | niektórzy subskrybenci nie potwierdzili jeszcze zdarzeń na poziomie lub poniżej tego punktu | spróbuj ponownie wkrótce; problem zniknie, gdy prognozy się sprawdzą |
| „subskrypcja agregatu nie powiodła się” | projekcja tego agregatu została przerwana ([rozdział 9](09-projection-failures.md)) | najpierw napraw awarię |
| "cofnij po wersji" | późniejsze cofnięcie anulowało zdarzenia w punkcie lub poniżej | zgnieć w wersji cofnięcia lub nowszej |
| „nie zwraca stanu, który zastępuje” | kontrola uczciwości nie powiodła się | napraw „toEvent” lub procedury obsługi, albo użyj „SquashMayChangeState” |
| "duplikat" | kopia, którą należy odłączyć, nie jest gotowa (poniżej) | poczekaj lub napraw błąd projekcji tej kopii |
| "w międzyczasie się zmieniło" | ktoś skopiował agregat podczas squasha | spróbuj ponownie |
| „każdy węzeł uruchamia nową wersję” | trwa uaktualnianie ciągłe, a klasa zdarzenia squasha jest wymieniona w „writtenFrom” | poczekaj, aż każdy węzeł uruchomi nową wersję ([rozdział 17](17-cluster.md)) |

Reguły dotyczące skrzynek nadawczych i błędów chronią Twoje projekcje. Projekcja musi zawierać każde zdarzenie do
punkt squasha zanim te zdarzenia znikną; w przeciwnym razie nie będzie mógł ich już nigdy odzyskać.

> **Wskazówka:** harmonogram squasha, z którym zaraz się spotkasz, po prostu próbuje ponownie odrzuconego agregatu w
> później uruchom. Kiedy ściskasz ręką, zrób to samo: zapisz przyczynę i spróbuj później.

## Życie po dyni

### Czytanie przeszłości

Historyczne odczyty [rozdziału 7] (07-reading-the-past.md) nadal działają w punkcie zgniatania i
po nim. Poniżej odmawiają zamiast zgadywać:

```scala
import scala.util.Try

val old: Future[Try[Aggregate[ShoppingCart]]] =
  (koszyki ? GetAggregateForVersion(cartId, AggregateVersion(3))).mapTo[Try[Aggregate[ShoppingCart]]]
// Failure(AggregateSquashedException): "Historia agregatu ... jest spłaszczona do wersji 5;
// wersji 3 nie można odczytać"
```

`AggregateSquashedException` informuje o punkcie squasha (`squashedUpTo`) i o co poprosiłeś
(`requested` i `at` w celu odczytania natychmiast). Taka sama odpowiedź pojawia się dla
`GetEventsForAggregateForVersion` poniżej punktu i dla `GetAggregateAtInstant` w chwili obecnej
przed czasem trwania wydarzenia squash. `GetEventsForAggregate` po prostu rozpoczyna się w czasie trwania wydarzenia squash.

### Cofanie i duplikowanie

Cofnięcie, które doprowadziłoby do punktu zgniecenia, jest odrzucane, podobnie jak duplikacja poniżej. Oba są
odpowiedział „EventHandlingError”, a log wyjaśnia przyczynę. Późniejsze zdarzenia można cofnąć jak poprzednio, a
Wózek można skopiować w punkcie zgniatania lub później. Z wózkiem zgniecionym do 4 i teraz w
wersja 5:

```scala
UndoShoppingCartChange(userId, cartId, AggregateVersion(5), 2) // osiągnęłoby wersję 4: EventHandlingError
Cofnij zmianę koszyka zakupowego (identyfikator użytkownika, identyfikator koszyka, łączna wersja (5), 1) // Odpowiedź sukcesu (identyfikator koszyka, łączna wersja (6))
DuplicateShoppingCart(userId, cartId, AggregateVersion(3)) // poniżej punktu: EventHandlingError
DuplicateShoppingCart(userId, cartId, AggregateVersion(4)) // dobrze: kopiowanie rozpoczyna się od podsumowania
```

### Migawki

Migawka znajdująca się w punkcie zgniatania lub poniżej zostaje usunięta (zdarzenie zgniatania wykonuje teraz swoje zadanie); migawka
powyżej punktu pozostaje.

### Kopie: odłączanie duplikatów

Duplikat ([rozdział 6](06-events.md)) jest jak nowy dziennik, który zaczyna się od słów „na pierwszych stronach,
przeczytaj oryginał do strony 3". Wyrwij te strony z oryginału, a kopia straci swoją
początek. Zatem przed zgnieceniem biblioteka *odłącza* każdą kopię, która odczytuje oryginał poniżej
punkt squasha: pierwsze zdarzenie kopii, zdarzenie jej duplikacji, zostaje zastąpione przez utworzone przez Ciebie zdarzenie squasha
z własnego stanu kopii w wersji 1. Od tego momentu kopia stoi sama, dokładnie z
stwierdzić, że tak było.

- Kopie wykonane w punkcie zgniatania lub później pozostawia się bez zmian: czyta się stronę podsumowującą, która podaje
  ten sam stan.
- Kopię kopii odłącza się w ten sam sposób.
- Samo zgniecenie kopii powoduje również oddzielenie jej od oryginału.

Odłączenie czeka, a wraz z nim zgniatanie, podczas gdy pierwsze zdarzenie kopii zostało cofnięte, nadal czeka w
Skrzynka nadawcza lub subskrypcja kopii nie powiodła się. Odłączenie przywraca również każdą kopię w wersji 1.
po pierwsze, zmiażdżenie agregatu, który został skopiowany wiele razy, zajmuje więcej czasu.

### Miejsce na dysku

Usunięte wiersze nie zmniejszają rozmiaru plików bazy danych od razu. Odkurzanie PostgreSQL, zazwyczaj automatyczne,
oznacza przestrzeń wolną dla nowych wierszy, ale nie oddaje jej systemowi operacyjnemu (z wyjątkiem
(dla wolnych stron na samym końcu tabeli). Aby zmniejszyć pliki, potrzebujesz polecenia `VACUUM FULL`, które
przepisuje tabelę i blokuje ją podczas działania lub rozszerzenie `pg_repack`, które robi to samo
online. Źródła: [ARCHITECTURE.md, sekcja 4.6](../ARCHITECTURE.md), podręcznik PostgreSQL na temat
[rutynowe odkurzanie](https://www.postgresql.org/docs/current/routine-vacuuming.html) i
[dokumentacja pg_repack](https://reorg.github.io/pg_repack/).

> **Wskazówka:** zgniatanie kilku agregatów co godzinę dobrze sprawdza się w przypadku odkurzacza automatycznego: zwolniona przestrzeń jest ponownie wykorzystywana przez
> nowe wydarzenia. Jednorazowe zmiażdżenie całego sklepu uwalnia wiele naraz; to znaczy, gdy zmniejsza się
> pliki z `pg_repack` mogą być tego warte.

## Automatyczne zgniatanie

Zgniatanie ręczne jest w porządku dla jednego wózka. Dla tysięcy, podaj regułę kontekstową i pozwól bibliotece…
Zastosuj to.

### Zasada

```scala
import scala.concurrent.duration._

nadpisz val squash: Option[SquashPolicy[ShoppingCart]] = Some(SquashPolicy(
  toEvent = koszyk => CartSquashed(SpaceId(0), nazwa.koszyka, elementy koszyka),
  automatyczne = Some(AutomaticSquash(afterEvents = 1000, keepLast = 100))))
```

`AutomaticSquash(afterEvents, keepLast = 0, keepNewerThan = None)` oznacza: „przynajmniej raz
Wydarzenia `afterEvents`, nie licząc najnowszych `keepLast`, nie są jeszcze kompresowane, są kompresowane do zaledwie
przed `keepLast`. Oto powyższa reguła działająca na jednym koszyku:

| Koszyk jest w wersji | Zgnieciony do | Wydarzenia, które mogłyby się odbyć | Zgnieciony? |
|---|---|---|---|
| 1 250 | nigdy | 1 250 − 100 = 1 150 | tak, do 1 150 |
| 1 900 | 1 150 | 1 900 − 100 − 1 150 = 650 | jeszcze nie |
| 2 300 | 1 150 | 2 300 − 100 − 1 150 = 1 050 | tak, do 2 200 |

Dzięki `keepNewerThan` ostatnie zdarzenia również zostają zachowane, bez względu na to, ile ich jest:

```scala
AutomaticSquash(afterEvents = 1000, keepLast = 100, keepNewerThan = Some(30.days))
```

Teraz punkt squasha nie jest późniejszy niż najnowsze zdarzenie sumaryczne starsze niż 30 dni. Jeśli to
pozostawia mniej zdarzeń niż `afterEvents` do skompresowania, agregat czeka na późniejsze uruchomienie.
`afterEvents` musi być dodatnie, a `keepLast` nie może być ujemne, w przeciwnym razie reguła nie zostanie utworzona.

### Uruchamianie harmonogramu

Reguła to tylko reguła; coś musi na nią oddziaływać. To jest „SquashScheduler”: jeden na system,
uruchamiane przez hosta obok magistrali poleceń.

```scala
import io.reactivecqrs.core.aggregate.SquashScheduler
import io.reactivecqrs.core.aggregate.SquashScheduler.SquashTarget
import io.reactivecqrs.core.cluster.Singletons
import org.apache.pekko.actor.{ActorRef, Rekwizyty}

val squashScheduler: ActorRef = Singletons.start(system, "squashScheduler",
  Rekwizyty (nowy SquashScheduler (eventStore, Seq (SquashTarget.of (nowy ShoppingCartAggregateContext, koszyki)))))
```

- **`SquashTarget.of(context, commandBus)`** nazywa jeden typ agregatu: regułę swojego kontekstu i jego
  Bus poleceń. Wypisz jeden cel dla każdego typu, który chcesz zmiażdżyć. Kontekst bez reguły „automatycznej”
  powoduje wyjątek `IllegalArgumentException`.
- **`Singletons.start`** jest tym samym co `system.actorOf(props, name)` na pojedynczym węźle; w
  klaster utrzymuje dokładnie jeden harmonogram działania ([rozdział 17](17-cluster.md)).

Co robi harmonogram przy domyślnych opcjach `SquashSchedulerOptions`:

1. Minutę po rozpoczęciu (`firstRunAfter = 1.minute`), a następnie co godzinę
   (`interval = 1.hour`), rozpoczyna *uruchomienie*.
2. Dla każdego typu wyszukuje do 100 agregatów wybieranych przez regułę (`perRun = 100`), w kolejności identyfikatorów.
   Gdy poprzedni przebieg osiągnie pełne 100, kontynuuje po ostatnim z nich; w przeciwnym razie rozpoczyna się
   od początku jeszcze raz.
3. Prosi magistralę poleceń typu o ich ściśnięcie, po jednym na raz, czekając do minuty na każdy
   odpowiedź (`answerTimeout = 1.minute`) przed przejściem do następnej.
4. Odrzucony agregat jest po prostu ponownie próbowany w kolejnym przebiegu. Przebieg, który następuje, gdy poprzedni
   jeden, który nadal trwa, jest pomijany.

Wszystkie cztery można zmienić:

```scala
importuj io.reactivecqrs.core.aggregate.SquashSchedulerOptions

nowy SquashScheduler(eventStore, Seq(SquashTarget.of(nowy ShoppingCartAggregateContext, koszyki)),
  SquashSchedulerOptions(firstRunAfter = 5.minutes, interval = 30.minutes, perRun = 500, answerTimeout = 2.minutes))
```

Zdarzenia squasha są przechowywane z `UserId(0)` i kopertą metadanych, której aktorem jest
`"system:squash-scheduler"` (`SquashScheduler.metadata`), dzięki czemu można je odróżnić od squashów wykonanych przez
ręka.

> **Wskazówka:** funkcja „squashuj pojedynczo” oznacza, że ​​automatyczne squashowanie obejmuje maksymalnie jedną bazę danych
> połączenie, niezależnie od liczby kwalifikujących się agregatów. To jest konserwacja i zachowuje się jak konserwacja.

> **Pod maską:** w przypadku `keepNewerThan` każde uruchomienie najpierw wyszukuje najstarsze zdarzenie, które nie jest starsze
> niż wiek, poprzez binarne przeszukanie klucza podstawowego tabeli zdarzeń (około 40 odczytów, według
> własna ocena biblioteki w [docs/SCALING.md, sekcja 3](../SCALING.md)), a następnie bada każdego kandydata
> agreguj raz. Wiek jest dokładny do najdłuższego czasu trwania polecenia i odchylenia zegarów między
> węzły. Decyzja D52 i wymaganie R11; patrz [ARCHITECTURE.md, sekcja 4.6](../ARCHITECTURE.md).

### Oglądam to

```scala
status wartości: Future[SquashScheduler.SquashStatus] =
  (squashScheduler ? SquashScheduler.GetSquashStatus).mapTo[SquashScheduler.SquashStatus]
// SquashStatus(squashed = 1, denied = 0, waiting = 0)
```

„squashed” i „refused” liczą się od momentu uruchomienia harmonogramu; „waiting” to czas, w którym bieżące uruchomienie jest kontynuowane
musi zrobić. „Odrzucony” licznik, który stale rośnie, zwykle oznacza, że ​​prognoza nie spełnia oczekiwań
agregaty ([rozdział 9](09-projection-failures.md)); każda odmowa jest rejestrowana wraz z jej przyczyną podczas debugowania
Poziom. Chcesz uruchomić teraz, na przykład w teście? Wyślij harmonogram `SquashScheduler.Run`.

## Przechowywanie zdarzeń, których potrzebują Twoi audytorzy

Wracając do pamiętnika. Zanim wyrwiesz stare strony, skseruj te, o które prosi audytor (np.
każdy „dodany element”) i włóż kopie do segregatora oznaczonego numerem koszyka. Dziennik zostaje
strona podsumowująca; oryginały przechowywane są w segregatorze.

Ten segregator to **archiwum**. Nazwij klasy zdarzeń audytu w `keep`:

```scala
nadpisz val squash: Option[SquashPolicy[ShoppingCart]] =
  Some(SquashPolicy(cart => CartSquashed(SpaceId(0), cart.name, cart.items), keep = Set(classOf[ItemAdded])))
```

Przy każdym squashu wydarzenia klas znajdujących się w punkcie squasha lub poniżej niego są przenoszone do archiwum (
Tabela `events_archive`) w transakcji własnej squasha. Każda z nich zachowuje wszystko, co miała: wersję,
użytkownika, czas, metadane i, jeśli cofnięcie anulowało operację, wersję tego cofnięcia. Obejmuje to
wydarzenie w samym punkcie squasha, zarchiwizowane przed rozpoczęciem imprezy squasha i pierwsze
w przypadku kopii, od której odłącza się squash, pod warunkiem, że są one utrzymane w klasie.

Przykład zestawu testowego: wózek otrzymuje jabłko (wersja 2) i gruszkę (3), traci jabłko (4) i
Dostaje śliwkę (5). Jest zgnieciona do 4. Teraz poproś o jej archiwum:

```scala
zarchiwizowane wartości: Przyszłość[Wypróbuj[Zarchiwizowane wydarzenia[Koszyk]]] =
  (koszyki ? GetArchivedEvents(cartId)).mapTo[Try[ArchivedEvents[ShoppingCart]]]
```

Odpowiedź brzmi `Success(ArchivedEvents(cartId, events))`, najstarsze na początku; tutaj dwa `ItemAdded`
wydarzenia w wersjach 2 i 3. Każde z nich jest wydarzeniem typu `ArchivedEvent(info, undoneFrom, archived)`:

- **`info`**: informacje o zdarzeniu `EventInfo` w takiej formie, w jakiej zostały zapisane: wersja, zdarzenie, użytkownik, czas i metadane;
- **`undoneFrom`**: wersja operacji cofania, która ją anulowała, jeśli takowa została wykonana (w tym przypadku `Brak`);
- **`zarchiwizowano`**: kiedy dynia ją przesunęła.

Usunięcie w wersji 4 nie znajduje się w archiwum: `ItemRemoved` nie jest klasą zachowaną, więc została przeniesiona
Reszta starej historii. Sam dziennik ma teraz „CartSquashed” na 4, a śliwkę na 5.

Archiwum dostępne jest wyłącznie na życzenie. Rekonstrukcje, wznowienia, odczyty historyczne i projekcje
Odbudowy nigdy na to nie patrzą: to, co prognozy wiedzą, pochodzi z wydarzenia squasha. Stały
delete ([rozdział 6](06-events.md)) usuwa archiwum wraz z agregatem.

Połącz elementy, a otrzymasz politykę przechowywania: „zachowaj ostatnie 90 dni bez zmian, zachowaj
„zrób audyt zdarzeń na dobre, a resztę zostaw”:

```scala
nadpisz val squash: Option[SquashPolicy[ShoppingCart]] = Some(SquashPolicy(
  toEvent = koszyk => CartSquashed(SpaceId(0), nazwa.koszyka, elementy koszyka),
  automatyczne = Some(AutomaticSquash(afterEvents = 500, keepNewerThan = Some(90.days))),
  zachowaj = Ustaw(klasa[Dodano element])))
```

> **Ostrzeżenie:** nazwij klasy audytu w `keep` *przed* włączeniem funkcji squash. Zdarzenia zostaną zgniecione
> bez zachowania znikają na dobre; dodanie klasy do `keep` później zapisuje tylko to, co nie zostało zachowane
> jeszcze zgniecione.

### Kto jeszcze korzysta z tej klasy zdarzeń?

Klasy zdarzeń kumulują się na przestrzeni lat: zmieniony kształt zdarzenia staje się nową klasą
([rozdział 6](06-events.md)), a stara klasa musi pozostać w kodzie tak długo, jak wszystkie zapisane
Zdarzenie z niego korzysta. Squash to sposób, w jaki stare zdarzenia ostatecznie opuszczają historię. Aby dowiedzieć się, czy klasa jest
nadal potrzebne, zapytaj `StoredDataCheck`:

```scala
import io.reactivecqrs.core.tools.StoredDataCheck

val users = StoredDataCheck.usersOf(classOf[ItemAdded].getName)
```

Odpowiada na pytanie `ClassUsers(className, events, archived, aggregates)`: ile zdarzeń jest przechowywanych i jak
wiele zarchiwizowanych zdarzeń ma tę klasę, a pierwsze 100 agregatów, które jej używają (limit = 100)
(domyślnie), każdy jako `ClassUser(aggregateId, events, archived)`. W przypadku powyższego koszyka, jego wpis to
`ClassUser(cartId.asLong, 1, 2)`: jeden `ItemAdded` nadal znajduje się w historii (the plum) i dwa w historii
archiwum. Gdy `events` i `archived` mają wartość 0, żadne zapisane zdarzenie nie korzysta już z tej klasy i
możesz usunąć go ze swojego kodu.

> **Ostrzeżenie:** `usersOf` odczytuje całą tabelę `events` i całe archiwum, ponieważ nie ma indeksu
> przechowywane przez klasę zdarzeń. Uruchom je na kopii produkcyjnej lub gdy baza danych jest cicha.

## Czego się nauczyłeś

- Squash zastępuje zdarzenia od 1 do N jednym zdarzeniem squash w wersji N; wersja, stan i
  późniejsze wydarzenia pozostają, a biblioteka sprawdza, czy podsumowanie zwraca dokładnie taki stan, jaki został podany
  zastępuje (chyba że jego klasa miesza się z `SquashMayChangeState`).
- Możesz wybrać typ agregatu za pomocą `SquashPolicy(toEvent, automatic, keep)`; Twoje procedury obsługi zdarzeń
  a Twoje projekcje traktują imprezę squasha jako wydarzenie, które łączy cały stan.
- `SquashAggregate(id, upTo, userId, metadata)` odpowiada `AggregateSquashed` lub
  `SquashRefused(reason)`; odmowa niczego nie zmienia i zwykle mija z czasem.
- Po zgnieceniu, odczyty, cofnięcia i duplikaty poniżej punktu są odrzucane, a kopie, które odczytują
  te znajdujące się poniżej są odłączane jako pierwsze.
- `AutomaticSquash(afterEvents, keepLast, keepNewerThan)` i `SquashScheduler` uruchomione przez
  host squash bez pytania; `GetSquashStatus` pokazuje jak to działa.
- `keep` przenosi zdarzenia audytu do archiwum, które `GetArchivedEvents` odczytuje i
  `StoredDataCheck.usersOf` informuje, kiedy można usunąć starą klasę zdarzeń.
- Miejsce na dysku jest zwracane poprzez (automatyczne) odkurzanie; pliki zmniejszają się tylko za pomocą poleceń `VACUUM FULL` lub `pg_repack`.

Dalej: [rozdział 15](15-rewriting-history.md) usuwa korektę: zmieniające się wydarzenia, które są
już przechowywane, ostrożnie.
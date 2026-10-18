# Rozdział 14. Squash: zwijanie starej historii

Niektóre agregaty żyją latami. Każde polecenie dopisuje wiersz do ich pamiętnika, a za każdym razem,
gdy taki agregat się budzi, jego właściciel czyta wszystkie te wiersze od nowa. W tym rozdziale
nauczysz się robić *squash* agregatu: zwijać starą część jego historii w jedno zdarzenie
podsumowujące, na żądanie albo według harmonogramu, a zdarzenia, na których zależy twoim audytorom,
zachowywać w archiwum. Dowiesz się też, kiedy biblioteka mówi „nie teraz” i dlaczego to dobrze.

---

## Strona z podsumowaniem

Wyobraź sobie pamiętnik koszyka Alicji po pracowitym roku: trzy tysiące wierszy. Od miesięcy nikt
nie zaglądał do pierwszych dwóch tysięcy dziewięciuset, a mimo to za każdym razem, gdy właściciel
koszyka startuje, odtwarza je wszystkie, jeden po drugim, żeby odbudować koszyk.

Teraz wyobraź sobie, że wyrywasz strony od 1 do 2 900, a w miejscu strony 2 900 wklejasz jedną
stronę z podsumowaniem:

> *W tym miejscu koszyk nazywał się „Groceries” i były w nim gruszki i śliwki.*

Strony po niej zostają dokładnie takie, jakie były. Numeracja się nie zmienia: podsumowanie to
strona 2 900, a następny wiersz to nadal strona 2 901. Kto przeczyta pamiętnik od początku, dostanie
ten sam koszyk co wcześniej, tylko po przeczytaniu stu wierszy zamiast trzech tysięcy.

To właśnie jest **squash** (zwinięcie historii). W języku ReactiveCQRS:

- zdarzenia w wersjach od 1 do N zastępuje jedno **zdarzenie squasha**, zapisane w wersji N, czyli
  w **punkcie squasha**;
- agregat zachowuje swoją wersję, swój stan i każde zdarzenie po N;
- odtworzenie czyta zdarzenie squasha i zdarzenia po nim, więc znów jest szybkie;
- wiersze starych zdarzeń zostają usunięte, więc baza może ponownie wykorzystać ich miejsce.

Jak wolny jest długi pamiętnik? Na maszynie, na której mierzy się wydajność biblioteki, nowy
właściciel odtworzył agregat z 50 000 zdarzeń w 392 ms (pomiar z 17/10/2026, jedno uruchomienie
benchmarku `load-M5`, PostgreSQL na tej samej maszynie; zobacz
[docs/SCALING.md, sekcja 3](../SCALING.md)). Raz – w porządku; przy każdym budzącym się koszyku – już
nie.

> **Pamiętaj:** squash zmienia historię na dobre. Gdy stare zdarzenia znikną, nikt już nie odczyta
> koszyka takiego, jaki był w wersji 3, nie wykona cofnięcia, które przekracza punkt squasha, ani
> nie skopiuje koszyka sprzed tego punktu. Taka jest cena strony z podsumowaniem, dlatego ten
> rozdział poświęca trochę miejsca temu, co biblioteka sprawdza, zanim pozwoli ci ją zapłacić.

### Squash czy migawka?

[Rozdział 13](13-snapshots.md) dał ci migawki, inne lekarstwo na długą historię. Te dwa sposoby
rozwiązują różne problemy:

| | Migawka | Squash |
|---|---|---|
| Czym jest | kopia stanu trzymana obok historii | zdarzenie podsumowujące, które zastępuje historię do pewnego punktu |
| Historia | bez zmian | zdarzeń sprzed punktu już nie ma |
| Odczyt starej wersji | działa jak dawniej | odmowa poniżej punktu |
| Gdy zaginie | biblioteka czyta zamiast niej wszystkie zdarzenia | nic nie może zaginąć: to teraz *jest* historia |
| Dysk | jeden wiersz więcej na agregat | mniej wierszy, gdy PostgreSQL zrobi vacuum |
| Jak włączyć | `snapshots = Some(SnapshotPolicy(...))` | `squash = Some(SquashPolicy(...))` |

Jeśli potrzebujesz tylko szybkości, migawki są łagodniejszym wyborem. Po squash sięgnij wtedy, gdy
stara historia sama w sobie jest ciężarem: zajmuje miejsce na dysku, trzyma przy życiu stare klasy
zdarzeń, których chcesz się pozbyć, albo dane, których nie masz powodu przechowywać. Jeden typ
agregatu może używać obu.

> **Pamiętaj:** squash zachowuje wersję. Limit wersji na agregat w magazynie zdarzeń
> (`aggregateVersionLimit`, domyślnie 10 000) liczy wersje, a nie zapisane zdarzenia, więc squash nie
> robi miejsca pod tym limitem.

## Krok 1: daj agregatowi zdarzenie squasha

Strona z podsumowaniem to zdarzenie jak każde inne, a napisać je musisz ty. Samo musi wystarczyć,
żeby odbudować cały stan. W domenie testowej:

<!-- snippet: shoppingcart/Events.scala#cartSquashed -->
```scala
/** The squash event of a cart: its state at the squash point. */
case class CartSquashed(spaceId: SpaceId, name: String, items: Vector[Item]) extends FirstEvent[ShoppingCart]
```

Dlaczego `FirstEvent`? Po squashu to ono *jest* pierwszym zdarzeniem w historii agregatu, a pierwsze
zdarzenia wskazują organizację, do której należy agregat, czyli jego przestrzeń
([rozdział 16](16-organizations.md)). Każdy koszyk domeny testowej żyje w przestrzeni 0.

Potem powiedz o nim kontekstowi. Jego handlery zdarzeń stosują zdarzenie squasha jak każde inne,

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#cartSquashedHandler -->
```scala
case e: CartSquashed => ShoppingCart(e.name, e.items)
```

a jego polityka squasha mówi, jak zrobić takie zdarzenie ze stanu:

<!-- snippet: shoppingcart/ShoppingCartAggregateContext.scala#squashPolicy -->
```scala
override val squash: Option[SquashPolicy[ShoppingCart]] =
  Some(SquashPolicy(cart => CartSquashed(SpaceId(0), cart.name, cart.items)))
```

`SquashPolicy` przyjmuje trzy rzeczy:

- **`toEvent`**: funkcję, która zamienia stan w twoje zdarzenie squasha. Biblioteka wywołuje ją ze
  stanem w punkcie squasha.
- **`automatic`**: opcjonalną regułę robienia squasha bez proszenia; domyślnie `None`. Przyjdzie na
  nią pora w dalszej części rozdziału.
- **`keep`**: klasy zdarzeń audytowych, które trafiają do archiwum, zamiast zostać usunięte;
  domyślnie nie ma żadnych. O tym też w dalszej części rozdziału.

> **Pamiętaj:** zdarzenie squasha jest przechowywane tak długo, jak żyje agregat. Traktuj jego klasę
> jak każdą inną klasę zdarzenia: gdy jej kształt musi się zmienić, dodaj nową klasę
> (`CartSquashedV2`) i zachowaj starą ([rozdział 6](06-events.md)). Właśnie dlatego biblioteka nigdy
> nie zapisuje jako historii surowego zrzutu stanu: klasę zdarzenia można rozwijać, a zrzutu
> zeszłorocznej klasy stanu już nie.

### Projekcje też muszą je rozumieć

Gdy robisz squash, nikt nie dostaje o tym wiadomości: squash niczego nie publikuje. Projekcje, które
śledzą koszyk, mają już każde zdarzenie aż do punktu squasha (niżej zobaczysz, że biblioteka tego
pilnuje), a podsumowanie daje ten sam stan, więc ich modele odczytu pozostają poprawne.

Zdarzenie squasha spotykają za to dwa rodzaje projekcji: projekcja w trakcie przebudowy
([rozdział 10](10-rebuilding-projections.md)) i projekcja dodana po squashu. Dla nich jest to
*pierwsze* zdarzenie tego koszyka, jakie w ogóle widzą, w wersji N. Obsłuż je więc jak utworzenie,
które przynosi cały stan. Lista koszyków w domenie testowej robi to tak:

<!-- snippet: shoppingcart/ShoppingCartsListProjection.scala#squashedDocument -->
```scala
case CartSquashed(_, name, _) => // a squash event holds the whole state: the document is made from it
  documentStore.updateDocument(0, aggregateId.asLong, _ => Document(name))
```

`updateDocument` wstawia dokument, gdy go brakuje, i zastępuje go, gdy już jest, więc ten kod działa
bez względu na to, czy projekcja znała wcześniej koszyk. Projekcja, która słucha stanów
(`AggregateListener`, [rozdział 8](08-projections.md)), dostaje po prostu informację, że koszyk
został utworzony (`created = true`), gdy jej subskrypcja zaczyna się od zdarzenia squasha.

> **Ostrzeżenie:** projekcja, która nie ma przypadku dla zdarzenia squasha, dziś działa, a psuje się
> przy najbliższej przebudowie: wykłada się na każdym agregacie po squashu albo go pomija. Naucz
> każdą projekcję tego typu obsługi zdarzenia squasha, *zanim* zrobisz jakikolwiek squash.

## Krok 2: squash na żądanie

Squash jednego agregatu to jeden komunikat do jego szyny poleceń. Bloki kodu z tego rozdziału
wykonuje `SquashingSpec` (`examples/src/test/scala/io/reactivecqrs/examples/cart/`), który
sprawdza, co robią:

<!-- snippet: cart/SquashingSpec.scala#request -->
```scala
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.UserId
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._

implicit val timeout: Timeout = Timeout(10.seconds)

val answer: Future[SquashResponse] =
  (carts ? SquashAggregate(cartId, AggregateVersion(5), UserId(7))).mapTo[SquashResponse]
```

Szyna poleceń przekazuje prośbę właścicielowi koszyka, a ten odpowiada jednym z dwóch komunikatów
(nieopakowanym w `Try`):

- `AggregateSquashed(cartId, AggregateVersion(5))`: gotowe.
- `SquashRefused(cartId, reason)`: nic się nie zmieniło, a `reason` mówi zwykłymi słowami dlaczego.

`SquashAggregate` przyjmuje cztery wartości:

- **`id`**: agregat.
- **`upTo`**: punkt squasha, od 1 do bieżącej wersji agregatu.
- **`userId`**: kto prosi. Zapisuje się razem ze zdarzeniem squasha; gdy go pominiesz, będzie to
  `UserId(0)`.
- **`metadata`**: opcjonalna koperta metadanych dla zdarzenia squasha ([rozdział 6](06-events.md)),
  przydatna do zapisania powodu: `Some(EventMetadata("operator:retention", causationId = Some("ticket 4711")))`.

Właściciel wykonuje tę pracę między dwoma poleceniami koszyka. Wersja i stan koszyka się nie
zmieniają, więc polecenia działają dalej, gdy to się dzieje.

### Co się stało z pamiętnikiem

Prześledźmy przykład z samego zestawu testów. Koszyk o nazwie „Groceries” dostaje jabłko, gruszkę i
śliwkę, traci jabłko (produkt o identyfikatorze 1) i dostaje figę. Potem robimy squash do wersji 5:

```
version  before                               after SquashAggregate(cartId, AggregateVersion(5), ...)
1        ShoppingCartCreated("Groceries")
2        ItemAdded("apple")
3        ItemAdded("pear")
4        ItemAdded("plum")
5        ItemRemoved(1)                       CartSquashed(SpaceId(0), "Groceries",
                                                Vector(Item(2, "pear"), Item(3, "plum")))
6        ItemAdded("fig")                     ItemAdded("fig")
```

Po squashu:

- Koszyk nadal jest w wersji 6, z tą samą nazwą i tymi samymi produktami. Jego działający
  właściciel, zupełnie nowy właściciel i świeże odtworzenie są co do tego zgodne.
- Następne polecenie nadal podaje `AggregateVersion(6)` i dostaje odpowiedź
  `SuccessResponse(cartId, AggregateVersion(7))`.
- `GetEventsForAggregate(cartId)` zwraca dwa zdarzenia: zdarzenie squasha w wersji 5, z
  użytkownikiem i metadanymi z prośby o squash oraz z czasem zdarzenia, które zastąpiło, a także
  figę w wersji 6.

> **Pod maską:** najpierw właściciel sprawdza warunki wstępne i buduje zdarzenie squasha, a resztę
> robi magazyn zdarzeń, w jednej transakcji. Blokuje łańcuch duplikatów agregatu, jeszcze raz
> sprawdza warunki wstępne, odłącza kopie, przenosi zachowywane zdarzenia do archiwum, usuwa stare
> zdarzenia i ich znaczniki cofnięcia, zastępuje zdarzenie w wersji N zdarzeniem squasha (zachowując
> identyfikator jego wiersza i czas, więc kolejność zatwierdzenia wszystkich zdarzeń się utrzymuje) i
> zapisuje punkt w `aggregates.squashed_version`. Decyzje od D47 do D52; zobacz
> [ARCHITECTURE.md, sekcja 4.6](../ARCHITECTURE.md).

## Test uczciwości

Strona z podsumowaniem, która kłamie, jest gorsza niż żadna. Dlatego zanim cokolwiek zostanie
zapisane, biblioteka testuje twoje zdarzenie squasha: stosuje je do `initialAggregateRoot` twoimi
handlerami zdarzeń, dokładnie tak, jak zrobi to odtworzenie, i porównuje wynik z prawdziwym stanem w
punkcie squasha, używając `==`. Jeśli się różnią, squash zostaje odrzucony, a powód mówi, że
zdarzenie squasha nie oddaje stanu, który zastępuje (`does not give back the state it replaces`).

Squash zostaje odrzucony także wtedy, gdy twoja funkcja `toEvent` rzuci wyjątek albo gdy twoje
handlery zdarzeń wyłożą się na zdarzeniu squasha.

> **Ostrzeżenie:** test stosuje zdarzenie squasha tak, jak zrobi to odtworzenie: w imieniu
> użytkownika, który poprosił o squash (`SquashAggregate.userId`), z czasem zdarzenia, które
> zastępuje. Jeśli twoje handlery zdarzeń kopiują do stanu użytkownika albo czas („ostatnio zmienione
> przez Boba”), zdarzenie squasha musi samo nieść te wartości, a jego handler musi brać je właśnie
> stamtąd. W przeciwnym razie test odrzuci squash (zawsze przy squashach automatycznych, które
> proszą jako użytkownik 0) albo przepuści go tylko wtedy, gdy akurat poprosi właściwa osoba. Odmowa
> ci o tym przypomina (problem F155).

### Celowe pomijanie danych

Czasem podsumowanie *powinno* coś zapomnieć: na przykład stary, zamknięty koszyk, którego produkty
nikomu już nie są potrzebne. Domieszaj `SquashMayChangeState` do klasy takiego zdarzenia squasha, a
test sprawdzi tylko, czy twoje handlery potrafią je zastosować, bez porównywania. W specu tego
rozdziału jest takie zdarzenie, które zachowuje tylko nazwę koszyka:

<!-- snippet: cart/SquashingSpec.scala#cartArchived -->
```scala
case class CartArchived(spaceId: SpaceId, name: String) extends FirstEvent[ShoppingCart] with SquashMayChangeState
```

a wśród handlerów zdarzeń spec ma `case e: CartArchived => ShoppingCart(e.name, Vector.empty)`.
Koszyk z jabłkiem, gruszką i śliwką (wersje od 2 do 4) po squashu tym zdarzeniem do wersji 3 ma w
sobie już tylko śliwkę: produkty aż do punktu squasha zostały zapomniane.

Ponieważ historia daje teraz inny stan niż wcześniej, biblioteka wyrzuca też każdą migawkę tego
agregatu i kopii, które czytają jego historię, i każe ich właścicielom wczytać stany od nowa.

> **Ostrzeżenie:** projekcje *nie* dostają o tym wiadomości. Model odczytu zbudowany ze zdarzeń
> pokazuje zapomniane dane, dopóki go nie przebudujesz ([rozdział 10](10-rebuilding-projections.md)).
> Zaplanuj przebudowę razem ze squashem.

## Gdy odpowiedź brzmi „nie teraz”

Odmowa niczego nie zmienia, a większość odmów mija sama. Powód to zdanie napisane dla ludzi; oto, co
znaczą najczęstsze:

| Powód zawiera | Dlaczego | Co zrobić |
|---|---|---|
| `squash is not enabled` | kontekst nie ma polityki `squash` | daj mu ją (krok 1) |
| `is not between 1 and` | `upTo` to 0 albo wykracza poza bieżącą wersję | wybierz wersję, która istnieje |
| `already squashed up to version` | squash sięga już tak daleko albo dalej | nic; ewentualnie wybierz późniejszy punkt |
| `still wait in the outbox` | któryś subskrybent nie potwierdził jeszcze zdarzeń w punkcie lub poniżej | spróbuj znowu za chwilę; przejdzie, gdy projekcje nadrobią zaległości |
| `a subscription of the aggregate is failed` | projekcja poddała się na tym agregacie ([rozdział 9](09-projection-failures.md)) | najpierw usuń awarię |
| `an undo after version` | późniejsze cofnięcie anulowało zdarzenia w punkcie lub poniżej | zrób squash w wersji tego cofnięcia albo później |
| `does not give back the state it replaces` | test uczciwości nie przeszedł | popraw `toEvent` albo handlery lub użyj `SquashMayChangeState` |
| `duplicate` | kopia, którą trzeba odłączyć, nie jest gotowa (niżej) | poczekaj albo usuń awarię projekcji tej kopii |
| `changed meanwhile` | ktoś skopiował agregat w trakcie squasha | spróbuj ponownie |
| `every node runs version` | trwa aktualizacja krocząca, a klasa twojego zdarzenia squasha jest wymieniona w `writtenFrom` | poczekaj, aż każdy węzeł będzie działał na nowej wersji ([rozdział 17](17-cluster.md)) |

Reguły dotyczące outboxu i awarii chronią twoje projekcje. Projekcja musi mieć każde zdarzenie aż do
punktu squasha, zanim te zdarzenia znikną; inaczej nigdy już by ich nie dostała.

> **Wskazówka:** scheduler squasha, który zaraz poznasz, po prostu wraca do odrzuconego agregatu w
> późniejszym przebiegu. Gdy robisz squash ręcznie, postępuj tak samo: zapisz powód w logu i spróbuj
> później.

## Życie po squashu

### Czytanie przeszłości

Odczyty historyczne z [rozdziału 7](07-reading-the-past.md) nadal działają w punkcie squasha i za
nim. Poniżej tego punktu odmawiają, zamiast zgadywać:

<!-- snippet: cart/SquashingSpec.scala#old -->
```scala
import scala.util.Try

val old: Future[Try[Aggregate[ShoppingCart]]] =
  (carts ? GetAggregateForVersion(cartId, AggregateVersion(3))).mapTo[Try[Aggregate[ShoppingCart]]]
// Failure(AggregateSquashedException): "The history of aggregate ... is squashed up to version 5;
// version 3 cannot be read"
```

`AggregateSquashedException` podaje punkt squasha (`squashedUpTo`) i to, o co pytasz (`requested`,
a przy odczycie w danej chwili `at`). Tę samą odpowiedź dostaniesz od
`GetEventsForAggregateForVersion` poniżej punktu i od `GetAggregateAtInstant` dla chwili
wcześniejszej niż czas zdarzenia squasha. `GetEventsForAggregate` po prostu zaczyna od zdarzenia
squasha.

### Cofnięcie i duplikowanie

Cofnięcie, które sięgnęłoby punktu squasha, zostaje odrzucone; tak samo duplikowanie poniżej tego
punktu. Oba dostają odpowiedź `EventHandlingError`, a log mówi dlaczego. Późniejsze zdarzenia można
cofać jak dawniej, a koszyk można skopiować w punkcie squasha albo później. Gdy koszyk ma squash do
wersji 4 i jest teraz w wersji 5:

<!-- snippet: cart/SquashingSpec.scala#belowThePoint -->
```scala
val commands = Seq(
  UndoShoppingCartChange(userId, cartId, AggregateVersion(5), 2), // would reach version 4: EventHandlingError
  UndoShoppingCartChange(userId, cartId, AggregateVersion(5), 1), // SuccessResponse(cartId, AggregateVersion(6))
  DuplicateShoppingCart(userId, cartId, AggregateVersion(3)),     // below the point: EventHandlingError
  DuplicateShoppingCart(userId, cartId, AggregateVersion(4)))     // fine: the copy starts from the summary
```

### Migawki

Migawka w punkcie squasha albo poniżej zostaje usunięta (jej zadanie wykonuje teraz zdarzenie
squasha); migawka powyżej punktu zostaje.

### Kopie: odłączanie duplikatów

Duplikat ([rozdział 6](06-events.md)) jest jak nowy pamiętnik, który zaczyna się od słów „pierwsze
strony przeczytaj z oryginału, aż do strony 3”. Wyrwij te strony z oryginału, a kopia straci swój
początek. Dlatego przed squashem biblioteka *odłącza* każdą kopię, która czyta oryginał poniżej
punktu squasha: pierwsze zdarzenie kopii, czyli jej zdarzenie duplikacji, zostaje zastąpione twoim
zdarzeniem squasha, utworzonym z własnego stanu kopii w wersji 1. Od tej chwili kopia stoi na
własnych nogach, z dokładnie takim stanem, jaki miała.

- Kopie zrobione w punkcie squasha albo później zostają w spokoju: czytają stronę z podsumowaniem,
  która daje ten sam stan.
- Kopia kopii zostaje odłączona w ten sam sposób.
- Squash samej kopii również odcina ją od oryginału.

Odłączenie, a razem z nim squash, czeka, dopóki pierwsze zdarzenie kopii jest cofnięte albo nadal
czeka w outboxie, albo dopóki któraś subskrypcja kopii jest w stanie awarii. Odłączanie najpierw
odtwarza też każdą kopię w wersji 1, więc squash agregatu skopiowanego wiele razy trwa dłużej.

### Miejsce na dysku

Usunięte wiersze nie zmniejszają od razu plików bazy. Vacuum w PostgreSQL, zwykle w postaci
autovacuum, oznacza ich miejsce jako wolne dla nowych wierszy, ale nie oddaje go systemowi
operacyjnemu (poza wolnymi stronami na samym końcu tabeli). Żeby zmniejszyć pliki, potrzebujesz
`VACUUM FULL`, które przepisuje tabelę i blokuje ją na czas działania, albo rozszerzenia
`pg_repack`, które robi to samo online. Źródła: [ARCHITECTURE.md, sekcja 4.6](../ARCHITECTURE.md),
rozdział podręcznika PostgreSQL o [regularnym vacuum](https://www.postgresql.org/docs/current/routine-vacuuming.html)
i [dokumentacja pg_repack](https://reorg.github.io/pg_repack/).

> **Wskazówka:** squash kilku agregatów co godzinę dobrze współgra z autovacuum: zwolnione miejsce
> zajmują nowe zdarzenia. Jednorazowy squash całego magazynu zwalnia dużo naraz; wtedy zmniejszenie
> plików przez `pg_repack` może być warte zachodu.

## Squash automatyczny

Ręczny squash wystarczy dla jednego koszyka. Przy tysiącach daj kontekstowi regułę i pozwól
bibliotece ją stosować.

### Reguła

Oto ona w `AutomaticallySquashedCarts`, kontekście koszyków, który rozszerza kontekst z domeny
testowej:

<!-- snippet: cart/SquashingSpec.scala#automatic -->
```scala
override val squash: Option[SquashPolicy[ShoppingCart]] = Some(SquashPolicy(
  toEvent = cart => CartSquashed(SpaceId(0), cart.name, cart.items),
  automatic = Some(AutomaticSquash(afterEvents = 1000, keepLast = 100))))
```

`AutomaticSquash(afterEvents, keepLast = 0, keepNewerThan = None)` czyta się tak: „gdy co najmniej
`afterEvents` zdarzeń, nie licząc najnowszych `keepLast`, nie przeszło jeszcze squasha, zrób squash
do miejsca tuż przed tymi `keepLast`”. Oto powyższa reguła w działaniu na jednym koszyku:

| Koszyk jest w wersji | Dotychczasowy squash | Zdarzenia, które mogą odejść | Squash? |
|---|---|---|---|
| 1 250 | brak | 1 250 − 100 = 1 150 | tak, do 1 150 |
| 1 900 | do 1 150 | 1 900 − 100 − 1 150 = 650 | jeszcze nie |
| 2 300 | do 1 150 | 2 300 − 100 − 1 150 = 1 050 | tak, do 2 200 |

Z `keepNewerThan` zostają też świeże zdarzenia, bez względu na to, ile ich jest:

<!-- snippet: cart/SquashingSpec.scala#keepNewerThan -->
```scala
AutomaticSquash(afterEvents = 1000, keepLast = 100, keepNewerThan = Some(30.days))
```

Teraz punkt squasha nie jest też późniejszy niż najnowsze zdarzenie agregatu starsze niż 30 dni.
Jeśli przez to do squasha zostaje mniej niż `afterEvents` zdarzeń, agregat czeka na późniejszy
przebieg. `afterEvents` musi być dodatnie, a `keepLast` nieujemne, inaczej reguły nie da się
utworzyć.

### Uruchomienie schedulera

Reguła to tylko reguła; ktoś musi według niej działać. Robi to `SquashScheduler`: jeden na system,
uruchamiany przez aplikację obok szyn poleceń.

<!-- snippet: cart/SquashingSpec.scala#scheduler -->
```scala
import io.reactivecqrs.core.aggregate.SquashScheduler
import io.reactivecqrs.core.aggregate.SquashScheduler.SquashTarget
import io.reactivecqrs.core.cluster.Singletons
import org.apache.pekko.actor.{ActorRef, Props}

val squashScheduler: ActorRef = Singletons.start(system, "squashScheduler",
  Props(new SquashScheduler(eventStore, Seq(SquashTarget.of(new AutomaticallySquashedCarts, carts)))))
```

- **`SquashTarget.of(context, commandBus)`** wskazuje jeden typ agregatu: regułę jego kontekstu i
  jego szynę poleceń. Podaj po jednym celu na każdy typ, który ma przechodzić squash. Dla kontekstu
  bez reguły `automatic` rzuca `IllegalArgumentException`.
- **`Singletons.start`** na pojedynczym węźle działa tak samo jak `system.actorOf(props, name)`; w
  klastrze pilnuje, żeby działał dokładnie jeden scheduler ([rozdział 17](17-cluster.md)).

Co robi scheduler przy domyślnych `SquashSchedulerOptions`:

1. Minutę po starcie (`firstRunAfter = 1.minute`), a potem co godzinę (`interval = 1.hour`) zaczyna
   *przebieg*.
2. Dla każdego typu znajduje do 100 agregatów, które wybiera reguła (`perRun = 100`), w kolejności
   identyfikatorów. Gdy poprzedni przebieg znalazł pełną setkę, idzie dalej od ostatniego z nich; w
   przeciwnym razie zaczyna znowu od początku.
3. Prosi szynę poleceń danego typu o ich squash, po jednym naraz, i na każdą odpowiedź czeka do
   minuty (`answerTimeout = 1.minute`), zanim przejdzie do następnego.
4. Do odrzuconego agregatu po prostu wraca w późniejszym przebiegu. Przebieg, który wypada, gdy
   poprzedni jeszcze trwa, zostaje pominięty.

Wszystkie cztery ustawienia można zmienić w `Props` schedulera:

<!-- snippet: cart/SquashingSpec.scala#options -->
```scala
import io.reactivecqrs.core.aggregate.SquashSchedulerOptions

val withOptions = Props(new SquashScheduler(eventStore, Seq(SquashTarget.of(new AutomaticallySquashedCarts, carts)),
  SquashSchedulerOptions(firstRunAfter = 5.minutes, interval = 30.minutes, perRun = 500, answerTimeout = 2.minutes)))
```

Jego zdarzenia squasha zapisują się z `UserId(0)` i kopertą metadanych, w której aktorem jest
`"system:squash-scheduler"` (`SquashScheduler.metadata`), więc odróżnisz je od squashy zrobionych
ręcznie.

> **Wskazówka:** jeden squash naraz oznacza, że automatyczny squash zajmuje najwyżej jedno połączenie
> z bazą, bez względu na to, ile agregatów się kwalifikuje. To prace konserwacyjne i tak się
> zachowują.

> **Pod maską:** z `keepNewerThan` każdy przebieg najpierw znajduje najstarsze zdarzenie, które nie
> jest starsze niż zadany wiek, wyszukiwaniem binarnym po kluczu głównym tabeli zdarzeń (około 40
> odczytów według szacunku samej biblioteki w [docs/SCALING.md, sekcja 3](../SCALING.md)), a potem
> raz sprawdza każdy agregat-kandydat. Wiek liczy się z dokładnością do czasu trwania najdłuższego
> polecenia i rozbieżności zegarów między węzłami. Decyzja D52 i wymaganie R11; zobacz
> [ARCHITECTURE.md, sekcja 4.6](../ARCHITECTURE.md).

### Jak go obserwować

<!-- snippet: cart/SquashingSpec.scala#status -->
```scala
val status: Future[SquashScheduler.SquashStatus] =
  (squashScheduler ? SquashScheduler.GetSquashStatus).mapTo[SquashScheduler.SquashStatus]
// SquashStatus(squashed = 1, refused = 0, waiting = 0)
```

`squashed` i `refused` liczą od startu schedulera; `waiting` to to, co bieżący przebieg ma jeszcze do
zrobienia. Licznik `refused`, który ciągle rośnie, zwykle oznacza, że któraś projekcja wykłada się
na tych agregatach ([rozdział 9](09-projection-failures.md)); każda odmowa trafia do logu z
powodem, na poziomie debug. Chcesz przebiegu od razu, na przykład w teście? Wyślij schedulerowi
`SquashScheduler.Run`.

## Jak zachować zdarzenia potrzebne audytorom

Wróćmy do pamiętnika. Zanim wyrwiesz stare strony, kserujesz te, których chce audytor (powiedzmy,
każde „dodano produkt”), i wpinasz kopie do segregatora podpisanego numerem koszyka. Pamiętnik
dostaje stronę z podsumowaniem, a segregator przechowuje oryginały.

Ten segregator to **archiwum**. Wymień klasy zdarzeń audytowych w `keep`:

<!-- snippet: cart/SquashingSpec.scala#keep -->
```scala
override val squash: Option[SquashPolicy[ShoppingCart]] =
  Some(SquashPolicy(cart => CartSquashed(SpaceId(0), cart.name, cart.items), keep = Set(classOf[ItemAdded])))
```

Przy każdym squashu zdarzenia tych klas w punkcie squasha i poniżej przechodzą do archiwum (tabela
`events_archive`), w tej samej transakcji co squash. Każde zachowuje wszystko, co miało: wersję,
użytkownika, czas, metadane, a jeśli anulowało je cofnięcie, także wersję tego cofnięcia. Dotyczy to
również zdarzenia w samym punkcie squasha, archiwizowanego, zanim jego miejsce zajmie zdarzenie
squasha, oraz pierwszego zdarzenia kopii odłączanej przez squash, o ile należą do zachowywanej klasy.

Przykład z zestawu testów: koszyk dostaje jabłko (wersja 2) i gruszkę (3), traci jabłko (4) i
dostaje śliwkę (5). Robimy squash do wersji 4. Teraz poproś o jego archiwum:

<!-- snippet: cart/SquashingSpec.scala#archived -->
```scala
val archived: Future[Try[ArchivedEvents[ShoppingCart]]] =
  (carts ? GetArchivedEvents(cartId)).mapTo[Try[ArchivedEvents[ShoppingCart]]]
```

Odpowiedź to `Success(ArchivedEvents(cartId, events))`, od najstarszego; tutaj dwa zdarzenia
`ItemAdded` w wersjach 2 i 3. Każde z nich to `ArchivedEvent(info, undoneFrom, archived)`:

- **`info`**: `EventInfo` w takiej postaci, w jakiej je zapisano: wersja, zdarzenie, użytkownik, czas
  i metadane;
- **`undoneFrom`**: wersja cofnięcia, które je anulowało, jeśli takie było (tutaj `None`);
- **`archived`**: kiedy squash je przeniósł.

Usunięcia z wersji 4 nie ma w archiwum: `ItemRemoved` nie jest zachowywaną klasą, więc odeszło razem
z resztą starej historii. Sam pamiętnik zawiera teraz `CartSquashed` w wersji 4 i śliwkę w wersji 5.

Archiwum czyta się tylko na żądanie. Odtworzenia, ponowne publikacje, odczyty historyczne i
przebudowy projekcji nigdy do niego nie zaglądają: wiedza projekcji zaczyna się od zdarzenia
squasha. Trwałe usunięcie ([rozdział 6](06-events.md)) kasuje archiwum razem z agregatem.

Złóż te elementy razem, a dostaniesz politykę retencji: „ostatnie 90 dni zostaw bez zmian, zdarzenia
audytowe zachowaj na zawsze, resztę wypuść”:

<!-- snippet: cart/SquashingSpec.scala#retention -->
```scala
override val squash: Option[SquashPolicy[ShoppingCart]] = Some(SquashPolicy(
  toEvent = cart => CartSquashed(SpaceId(0), cart.name, cart.items),
  automatic = Some(AutomaticSquash(afterEvents = 500, keepNewerThan = Some(90.days))),
  keep = Set(classOf[ItemAdded])))
```

> **Ostrzeżenie:** wymień klasy audytowe w `keep`, *zanim* włączysz squash. Zdarzenia zwinięte bez
> zachowania przepadają na dobre; dodanie klasy do `keep` później ocali tylko to, co jeszcze nie
> przeszło squasha.

### Kto jeszcze używa tej klasy zdarzenia?

Z latami klas zdarzeń przybywa: zmieniony kształt zdarzenia staje się nową klasą
([rozdział 6](06-events.md)), a stara klasa musi zostać w twoim kodzie tak długo, jak używa jej choć
jedno zapisane zdarzenie. Squash to sposób, w jaki stare zdarzenia w końcu opuszczają historię. Żeby
sprawdzić, czy klasa jest jeszcze potrzebna, zapytaj `StoredDataCheck`:

<!-- snippet: cart/SquashingSpec.scala#usersOf -->
```scala
import io.reactivecqrs.core.tools.StoredDataCheck

val users = StoredDataCheck.usersOf(classOf[ItemAdded].getName)
```

Odpowiada `ClassUsers(className, events, archived, aggregates)`: ile zapisanych i ile
zarchiwizowanych zdarzeń ma tę klasę oraz pierwsze 100 agregatów, które jej używają (domyślnie
`limit = 100`), każdy jako `ClassUser(aggregateId, events, archived)`. Dla koszyka z przykładu wyżej
wpis to `ClassUser(cartId.asLong, 1, 2)`: jedno `ItemAdded` wciąż w historii (śliwka) i dwa w
archiwum. Gdy zarówno `events`, jak i `archived` wynoszą 0, żadne zapisane zdarzenie nie używa już
tej klasy i możesz usunąć ją z kodu.

> **Ostrzeżenie:** `usersOf` czyta całą tabelę `events` i całe archiwum, bo nie ma indeksu po klasie
> zdarzenia. Uruchamiaj to sprawdzenie na kopii produkcji albo wtedy, gdy baza ma niewiele pracy.

## Co już umiesz

- Squash zastępuje zdarzenia od 1 do N jednym zdarzeniem squasha w wersji N; wersja, stan i
  późniejsze zdarzenia zostają, a biblioteka sprawdza, czy podsumowanie daje dokładnie ten stan,
  który zastępuje (chyba że jego klasa domiesza `SquashMayChangeState`).
- Squash włączasz osobno dla każdego typu agregatu przez `SquashPolicy(toEvent, automatic, keep)`;
  twoje handlery zdarzeń i projekcje obsługują zdarzenie squasha jak utworzenie, które przynosi cały
  stan.
- Na `SquashAggregate(id, upTo, userId, metadata)` przychodzi odpowiedź `AggregateSquashed` albo
  `SquashRefused(reason)`; odmowa niczego nie zmienia i zwykle później mija.
- Po squashu odczyty, cofnięcia i duplikowanie poniżej punktu są odrzucane, a kopie, które czytają
  poniżej niego, zostają najpierw odłączone.
- `AutomaticSquash(afterEvents, keepLast, keepNewerThan)` i `SquashScheduler` uruchomiony przez
  aplikację robią squash bez proszenia; `GetSquashStatus` pokazuje, jak im idzie.
- `keep` przenosi zdarzenia audytowe do archiwum, które czyta `GetArchivedEvents`, a
  `StoredDataCheck.usersOf` mówi, kiedy można pozbyć się starej klasy zdarzenia.
- Miejsce na dysku wraca dzięki (auto)vacuum; pliki kurczą się tylko po `VACUUM FULL` albo
  `pg_repack`.

Dalej: [rozdział 15](15-rewriting-history.md) wyciąga korektor: ostrożnie zmienimy zdarzenia, które
są już zapisane.
